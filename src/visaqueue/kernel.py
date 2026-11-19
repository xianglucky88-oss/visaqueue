"""Domain kernel: category interpretation, revision progress, conflict checks.

Responsibilities kept here (unchanged division of labour):

* **category interpretation** -- mapping raw status/cut-off text to a
  canonical lifecycle state with a human-readable explanation;
* **cross-month repeat judgement** -- deciding whether the same cut-off
  reappears for a category in consecutive visa months;
* **bulletin progress** -- ordering observations of the same bulletin into a
  revision chain and flagging same-instant contradictions.

The kernel is pure: it never touches the network or the database, which keeps
every rule unit-testable and replayable against historical input.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, Optional

from .framework import content_hash
from .models import (
    CATEGORIES,
    STATE_AVAILABLE,
    STATE_CURRENT,
    STATE_PENDING,
    STATE_REVOKED,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)

# Raw status text -> canonical state.  Real bulletins use short codes as well
# as full words; matching is case-insensitive (framework normalises case).
_STATUS_TEXT = {
    "current": STATE_CURRENT,
    "c": STATE_CURRENT,
    "unavailable": STATE_UNAVAILABLE,
    "u": STATE_UNAVAILABLE,
    "revoked": STATE_REVOKED,
    "r": STATE_REVOKED,
    "pending": STATE_PENDING,
    "p": STATE_PENDING,
}

_STATE_EXPLANATION = {
    STATE_AVAILABLE: "具体截止日期已公布，可按该排期申请",
    STATE_CURRENT: "Current：无排期限制，类别当前可用",
    STATE_UNAVAILABLE: "U：本月该类别不开放名额",
    STATE_REVOKED: "该类别在后续修订中被撤回",
    STATE_PENDING: "公告已列出但排期尚未生效",
    STATE_UNKNOWN: "状态文本无法识别，保留原始值待人工核对",
}

_MONTHS = {m: i for i, m in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
     "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), start=1)}


# --------------------------------------------------------------------------- #
# Category / status interpretation.
# --------------------------------------------------------------------------- #
def interpret_state(status: Any, cutoff: Any = None) -> str:
    """Map a raw status (and cut-off) to a canonical lifecycle state."""
    text = str(status or "").strip().lower()
    if text in _STATUS_TEXT:
        return _STATUS_TEXT[text]
    if parse_cutoff(cutoff) is not None:
        return STATE_AVAILABLE
    if cutoff and str(cutoff).strip().lower() in _STATUS_TEXT:
        return _STATUS_TEXT[str(cutoff).strip().lower()]
    return STATE_UNKNOWN


def explain_state(state: str) -> str:
    return _STATE_EXPLANATION.get(state, _STATE_EXPLANATION[STATE_UNKNOWN])


def parse_cutoff(value: Any) -> Optional[datetime]:
    """Parse the cut-off formats used by the two table kinds.

    Month tables write ``01JAN26`` / ``01JAN2026``; the dates table writes an
    ISO date ``2026-01-15``.  Sentinels such as ``Current``/``U`` return
    ``None`` because they carry no calendar date.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in _STATUS_TEXT:
        return None
    # ISO calendar date (dates table).
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d")
    except ValueError:
        pass
    # DOS-style 01JAN26 / 01JAN2026 (monthly table).
    if len(text) >= 7 and text[:2].isdigit() and text[2:5].upper() in _MONTHS:
        year_part = text[5:]
        if year_part.isdigit():
            year = int(year_part)
            if year < 100:
                year += 2000
            try:
                return datetime(year, _MONTHS[text[2:5].upper()], int(text[:2]))
            except ValueError:
                return None
    return None


def interpret_category(category: Any) -> dict[str, str]:
    """Explain a category code; unknown codes are kept verbatim, not dropped."""
    code = str(category or "").strip()
    labels = {
        "EB1": "杰出人才类（Employment-Based 1st）",
        "EB2": "高学位/特殊能力类（Employment-Based 2nd）",
        "EB3": "专业/技术工人类（Employment-Based 3rd）",
        "OtherWorkers": "其他工人类（EB-3 Other Workers）",
        "EW": "其他工人工类（EW 缩写）",
        "CertainReligious": "特定宗教工作者类",
    }
    return {
        "category": code,
        "label": labels.get(code, "未登记类别编码，保留原值"),
        "known": code in labels or code in CATEGORIES,
    }


# --------------------------------------------------------------------------- #
# Record identity and processing.
# --------------------------------------------------------------------------- #
def logical_id(record: dict[str, Any]) -> str:
    """Stable identity of a bulletin row, independent of its revision."""
    bd = str(record.get("bulletin_date", "")).strip()
    kind = str(record.get("table_kind", "")).strip().lower()
    cat = str(record.get("category", record.get("name", ""))).strip().lower()
    return "%s|%s|%s" % (bd, kind, cat)


def process(record: dict[str, Any]) -> dict[str, Any]:
    """Enrich one raw fetched row with interpreted domain fields."""
    out = dict(record)
    out["logical_id"] = logical_id(record)
    out["state"] = interpret_state(record.get("status"), record.get("cutoff"))
    out["state_explanation"] = explain_state(out["state"])
    cat_info = interpret_category(record.get("category", record.get("name")))
    out["category_label"] = cat_info["label"]
    out["category_known"] = int(cat_info["known"])
    parsed = parse_cutoff(record.get("cutoff"))
    out["cutoff_iso"] = parsed.date().isoformat() if parsed else None
    out["table_kind"] = str(record.get("table_kind", "")).strip().lower()
    return out


# --------------------------------------------------------------------------- #
# Cross-month repeat judgement.
# --------------------------------------------------------------------------- #
def is_repeated_across_months(history: Iterable[dict[str, Any]],
                              entry: dict[str, Any]) -> dict[str, Any]:
    """Judge whether ``entry`` repeats the previous visa month's cut-off.

    A repeat requires the same category and table kind in the immediately
    preceding target month carrying an identical, date-bearing cut-off.
    Non-date states (Current/U/...) are never treated as repeated dates.
    """
    target = str(entry.get("target_month", ""))
    kind = str(entry.get("table_kind", "")).strip().lower()
    category = str(entry.get("category", entry.get("name", ""))).strip().lower()
    cutoff = entry.get("cutoff")
    if parse_cutoff(cutoff) is None or not target:
        return {"repeated": False, "reason": "无日期型截止日，不参与跨月重复判断"}
    prior_month = _previous_month(target)
    if prior_month is None:
        return {"repeated": False, "reason": "目标月份无法解析"}
    for row in history:
        if (str(row.get("table_kind", "")).strip().lower() != kind
                or str(row.get("category", row.get("name", ""))).strip().lower() != category):
            continue
        if str(row.get("target_month", "")) != prior_month:
            continue
        if row.get("cutoff") == cutoff and parse_cutoff(row.get("cutoff")) is not None:
            return {
                "repeated": True,
                "previous_target_month": prior_month,
                "repeated_cutoff": str(cutoff),
                "reason": "与上一签证月 %s 的截止日 %s 相同" % (prior_month, cutoff),
            }
    return {"repeated": False, "previous_target_month": prior_month,
            "reason": "上一签证月 %s 未见相同截止日" % prior_month}


def _previous_month(month_text: str) -> Optional[str]:
    text = str(month_text).strip()
    try:
        dt = datetime.strptime(text[:7], "%Y-%m")
    except ValueError:
        return None
    year, month = dt.year, dt.month
    if month == 1:
        year, month = year - 1, 12
    else:
        month -= 1
    return "%04d-%02d" % (year, month)


# --------------------------------------------------------------------------- #
# Revision progress and same-instant contradiction handling.
# --------------------------------------------------------------------------- #
def reduce(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order observations of each logical row into a revision chain.

    Rows are sorted by observation timestamp.  Two rows observed at the exact
    same instant with different content are a *same-instant contradiction*:
    both are retained (visibility never shrinks), the deterministic
    content-hash tie-break picks a latest, and the loser is marked
    ``conflict_of`` the winner with an explanatory note.
    """
    by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        key = r.get("logical_id") or logical_id(r)
        r.setdefault("logical_id", key)
        by[key].append(r)
    for rows in by.values():
        rows.sort(key=lambda x: (str(x.get("ts", "")), content_hash(x)))
        prev = None
        chain_len = len(rows)
        for i, r in enumerate(rows):
            r["version_no"] = i + 1
            r["prev_version_id"] = prev
            r["is_latest"] = int(i == chain_len - 1)
            r["conflict_of"] = None
            if i > 0 and str(rows[i - 1].get("ts", "")) == str(r.get("ts", "")) \
                    and content_hash(rows[i - 1]) != content_hash(r):
                loser, winner = rows[i - 1], r
                loser["is_latest"] = 0
                winner["is_latest"] = 1
                loser["conflict_of"] = winner.get("id")
                loser.setdefault("note", "")
                loser["note"] = (loser.get("note", "") +
                                 " 同刻矛盾：与记录 %s 抓取时间相同但内容不同，"
                                 "按内容哈希确定性保留双方。" % winner.get("id")).strip()
                winner.setdefault("note", "")
                winner["note"] = (winner.get("note", "") +
                                  " 同刻矛盾：记录 %s 与本记录同刻且内容冲突。"
                                  % loser.get("id")).strip()
            prev = r.get("id")
    return records


def build_bulletins(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group processed rows into bulletin documents with progress metadata.

    Each (bulletin_date, table_kind) pair is one document; revision numbers
    come from the same snapshot-based assignment used by the storage layer,
    so in-memory summaries and persisted history never disagree.
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        groups[(str(r.get("bulletin_date", "")),
                str(r.get("table_kind", "")).strip().lower())].append(r)
    revision_by_id = assign_document_revisions(records)
    bulletins = []
    for (bulletin_date, table_kind), rows in sorted(groups.items()):
        per_revision: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            per_revision[revision_by_id.get(str(r.get("id", "")), 1)].append(r)
        bulletins.append({
            "bulletin_date": bulletin_date,
            "table_kind": table_kind,
            "revision_count": len(per_revision),
            "latest_revision": max(per_revision, default=0),
            "row_count": len(rows),
            "categories": sorted({str(x.get("category", x.get("name", "")))
                                   for x in rows}),
            "revisions": [
                {"revision": rev,
                 "observed_at": min(str(x.get("ts", "")) for x in chunk),
                 "row_ids": [x.get("id") for x in chunk]}
                for rev, chunk in sorted(per_revision.items())
            ],
        })
    return bulletins


def assign_document_revisions(records: list[dict[str, Any]]) -> dict[str, int]:
    """Assign a document-level revision number to every record.

    A document is a (bulletin_date, table_kind) pair.  Rows are replayed in
    source observation order; at each distinct observation instant the full
    per-category content snapshot is compared with the previous one.  A new
    revision opens only when the snapshot changes, so rows published together
    share one revision even though their individual timestamps differ by
    minutes.  Out-of-order arrivals that add previously unknown content open
    a new revision without renumbering existing ones -- history never moves
    backwards.
    """
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        groups[(str(r.get("bulletin_date", "")),
                str(r.get("table_kind", "")).strip().lower())].append(r)
    mapping: dict[str, int] = {}
    for rows in groups.values():
        snapshots: dict[str, dict[str, str]] = defaultdict(dict)
        for r in sorted(rows, key=lambda x: str(x.get("ts", ""))):
            instant = str(r.get("ts", ""))
            category = str(r.get("category", r.get("name", ""))).strip().lower()
            snapshots[instant][category] = content_hash({
                "target_month": r.get("target_month"),
                "cutoff": r.get("cutoff"),
                "status": r.get("status"),
                "state": r.get("state"),
            })
        revision = 0
        previous: dict[str, str] = {}
        instant_revision: dict[str, int] = {}
        for instant in sorted(snapshots):
            current = snapshots[instant]
            if revision == 0 or current != previous:
                revision += 1
            instant_revision[instant] = revision
            previous = current
        for r in rows:
            mapping[str(r.get("id", ""))] = instant_revision[str(r.get("ts", ""))]
    return mapping


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Traceable summary used by run reports and historical answers."""
    by_kind: dict[str, int] = defaultdict(int)
    by_state: dict[str, int] = defaultdict(int)
    conflicts = 0
    for r in records:
        by_kind[str(r.get("table_kind", "unknown"))] += 1
        by_state[str(r.get("state", STATE_UNKNOWN))] += 1
        if r.get("conflict_of"):
            conflicts += 1
    return {
        "total": len(records),
        "by_table_kind": dict(by_kind),
        "by_state": dict(by_state),
        "same_instant_conflicts": conflicts,
        "bulletins": build_bulletins(records),
    }
