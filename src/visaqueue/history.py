"""Historical read API: reconstruct what was visible at any past moment.

All functions are read-only and take an ``as_of`` system timestamp.  The
store guarantees that rows with a later ``first_seen_at`` are excluded, so a
historical answer can never contain information that had not been observed by
that time -- including bulletin revisions that were published later.
"""
from __future__ import annotations

from typing import Any, Optional

from .store import Store


def visible_snapshot(store: Store, as_of: Optional[str] = None) -> dict[str, Any]:
    """Latest-visible entries at ``as_of`` grouped for reporting."""
    entries = store.snapshot(as_of=as_of, only_latest=True)
    by_table: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        by_table.setdefault(e.get("table_kind", "unknown"), []).append(e)
    for rows in by_table.values():
        rows.sort(key=lambda r: (r.get("bulletin_date", ""),
                                 r.get("category", "")))
    return {
        "as_of": as_of,
        "total": len(entries),
        "table_kinds": sorted(by_table),
        "tables": by_table,
    }


def document_history(store: Store, bulletin_date: str, table_kind: str,
                     as_of: Optional[str] = None) -> dict[str, Any]:
    """Explain every revision of one bulletin document."""
    view = store.bulletin_view(bulletin_date, table_kind, as_of=as_of)
    view["explanation"] = _explain(view)
    return view


def _explain(view: dict[str, Any]) -> list[str]:
    notes = []
    revs = view["revisions"]
    if not revs:
        return ["该时刻尚无此公告的任何可见记录"]
    notes.append("公告 %s 的 %s 表在此时刻可见 %d 个修订版本"
                 % (view["bulletin_date"], view["table_kind"], len(revs)))
    for rev in revs:
        notes.append("修订 %d：首次见于 %s，覆盖 %d 个类别"
                     % (rev["revision"], rev["first_seen_at"],
                        rev["category_count"]))
    conflicts = [e for e in view["entries"] if e.get("conflict_of")]
    if conflicts:
        notes.append("存在 %d 条同刻矛盾记录，均已保留并标注，未覆盖任何一方"
                     % len(conflicts))
    superseded = [e for e in view["entries"] if e.get("superseded_by")]
    if superseded:
        notes.append("%d 条旧修订记录已被取代但仍可读，可追溯修订前后的排期差异"
                     % len(superseded))
    return notes


def progress_report(store: Store) -> dict[str, Any]:
    """Crawl/batch provenance plus per-document progress in one report."""
    batches = store.batches()
    docs = store.documents()
    return {
        "documents": docs,
        "document_count": len(docs),
        "batches": batches,
        "batch_count": len(batches),
        "last_status": batches[-1]["status"] if batches else None,
        "watermark": store.watermark(),
    }


def diff_revisions(store: Store, bulletin_date: str, table_kind: str,
                   left_revision: int, right_revision: int) -> dict[str, Any]:
    """Compare two revisions of the same document, category by category."""
    view = store.bulletin_view(bulletin_date, table_kind)
    by_key: dict[tuple[int, str], dict[str, Any]] = {
        (e["revision"], e.get("category", "")): e for e in view["entries"]
    }
    categories = sorted({e.get("category", "") for e in view["entries"]})
    changes = []
    for cat in categories:
        a = by_key.get((left_revision, cat))
        b = by_key.get((right_revision, cat))
        if a is None or b is None:
            changes.append({"category": cat, "change": "missing",
                            "left": a and a.get("cutoff"),
                            "right": b and b.get("cutoff")})
        elif a.get("cutoff") != b.get("cutoff") or a.get("state") != b.get("state"):
            changes.append({"category": cat, "change": "updated",
                            "left": a.get("cutoff"), "left_state": a.get("state"),
                            "right": b.get("cutoff"), "right_state": b.get("state")})
        else:
            changes.append({"category": cat, "change": "unchanged",
                            "left": a.get("cutoff"), "right": b.get("cutoff")})
    return {"bulletin_date": bulletin_date, "table_kind": table_kind,
            "left_revision": left_revision, "right_revision": right_revision,
            "changes": changes}
