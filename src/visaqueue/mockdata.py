"""Deterministic visa-bulletin source rows for the local fixture.

The fixture models a bulletin archive where:

* two table kinds (``monthly`` and ``dates``) are published for every
  bulletin date -- the crawler must keep them distinct;
* bulletin ``2026-02-15`` is re-published as a revision with changed cut-offs
  (the original rows remain available in later pages, modelling an archive
  that keeps superseded HTML);
* one row carries an observation timestamp later than its neighbours,
  modelling out-of-order / late-arriving content;
* one exact duplicate row is appended so the dedup path is exercised;
* the fixture module exposes scenario knobs used by the failure tests
  (partial page failure, repeated crawl of the same page, missing trailing
  page).  Scenario state lives in-process and resets through ``reset_scene``.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .config import SPEC, TABLE_KINDS

CATEGORY_CODES = ["EB1", "EB2", "EB3", "OtherWorkers"]

# (bulletin_date, target_month, monthly cutoff per category, dates cutoff)
_BULLETIN_SCENARIOS = [
    ("2026-01-15", "2026-02",
     ["01JAN26", "01NOV25", "15OCT25", "01AUG25"],
     ["2026-01-01", "2025-11-01", "2025-10-15", "2025-08-01"]),
    ("2026-02-15", "2026-03",
     ["15JAN26", "01DEC25", "01NOV25", "15SEP25"],
     ["2026-01-15", "2025-12-01", "2025-11-01", "2025-09-15"]),
]

# Revision of the February bulletin: the EB2 cut-off moves and EB3 becomes
# Current.  The original rows stay in the archive too.
_REVISION = ("2026-02-15", "2026-03",
             ["15JAN26", "15DEC25", "Current", "15SEP25"],
             ["2026-01-15", "2025-12-15", "Current", "2025-09-15"])

_OBS_BASE = datetime(2026, 1, 15, 9, 0, tzinfo=timezone.utc)


def _row(seq: int, bulletin_date: str, table_kind: str, target_month: str,
         category: str, cutoff: str, observed: datetime,
         status: str = "published", revision: int = 1) -> dict:
    d = {
        "id": "VQ-%05d" % seq,
        "name": category,
        "value": seq,
        "status": status,
        "ts": observed.isoformat().replace("+00:00", "Z"),
        "category": category,
        "bulletin_date": bulletin_date,
        "table_kind": table_kind,
        "target_month": target_month,
        "cutoff": cutoff,
    }
    # Only declared columns cross the transport boundary; revision numbering is
    # reconstructed by the kernel from observation order and content changes.
    return {c: d.get(c, "") for c in SPEC.columns}


def build_rows() -> list[dict]:
    """Fixed archive: 2 bulletins x 2 table kinds x 4 categories + revision.

    Observation timestamps model real bulletin publication: all rows of one
    bulletin release share one timestamp; the re-publication (revision 2) and
    the late correction each carry their own later timestamp.
    """
    rows: list[dict] = []
    seq = 0
    release_times = {
        ("2026-01-15", "monthly"): _OBS_BASE,
        ("2026-01-15", "dates"): _OBS_BASE + timedelta(minutes=5),
        ("2026-02-15", "monthly"): _OBS_BASE + timedelta(days=31),
        ("2026-02-15", "dates"): _OBS_BASE + timedelta(days=31, minutes=5),
    }
    for scenario in _BULLETIN_SCENARIOS:
        bulletin_date, target_month, monthly, dates = scenario
        for kind in TABLE_KINDS:
            cutoffs = monthly if kind == "monthly" else dates
            observed = release_times[(bulletin_date, kind)]
            for category, cutoff in zip(CATEGORY_CODES, cutoffs):
                seq += 1
                rows.append(_row(seq, bulletin_date, kind, target_month,
                                 category, cutoff, observed))
    # Re-publication of the February bulletin (revision 2): the EB2 cut-off
    # moves and EB3 becomes Current.  The original rows stay in the archive.
    bulletin_date, target_month, monthly, dates = _REVISION
    revised_at = _OBS_BASE + timedelta(days=62)
    for kind in TABLE_KINDS:
        cutoffs = monthly if kind == "monthly" else dates
        for category, cutoff in zip(CATEGORY_CODES, cutoffs):
            seq += 1
            rows.append(_row(seq, bulletin_date, kind, target_month,
                             category, cutoff, revised_at))
    # Late-arriving row: a correction for the January monthly EB1 row whose
    # observation timestamp is much later than its neighbours (out-of-order
    # arrival).  Its content differs from the original, so content-hash dedup
    # keeps both and the revision chain records the correction.
    late = dict(rows[0])
    late["id"] = "VQ-%05d" % (seq + 1)
    late["ts"] = (_OBS_BASE + timedelta(days=45)).isoformat().replace("+00:00", "Z")
    late["cutoff"] = "02JAN26"
    late["status"] = "published"
    rows.append(late)
    # Exact duplicate of an existing row (same content, same id) -> dedup.
    rows.append(dict(rows[2]))
    return rows


def extra_row(k: int) -> dict:
    """New rows appended after a /refresh call (incremental continuation)."""
    bulletin_date = "2026-03-15"
    observed = _OBS_BASE + timedelta(days=60 + k)
    kind = TABLE_KINDS[k % 2]
    category = CATEGORY_CODES[k % len(CATEGORY_CODES)]
    cutoff = "01FEB26" if kind == "monthly" else "2026-02-01"
    return _row(9000 + k, bulletin_date, kind, "2026-04", category, cutoff,
                observed)


# --------------------------------------------------------------------------- #
# Scenario knobs for failure / disorder tests.  In-process only.
# --------------------------------------------------------------------------- #
_SCENE = {
    "fail_pages": set(),     # page numbers that answer 500 every time
    "fail_once": set(),      # page numbers that fail on first hit only
    "drop_pages": set(),     # pages silently omitted (treated as end early)
    "repeat_page": None,     # page number whose first hit serves the prior page
    "reset_counter": 0,
}
_page_hits: dict[int, int] = {}


def set_scene(**kw) -> dict:
    for key, value in kw.items():
        if key in ("fail_pages", "fail_once", "drop_pages"):
            _SCENE[key] = set(value)
        else:
            _SCENE[key] = value
    return dict(_SCENE)


def reset_scene() -> dict:
    _SCENE["fail_pages"].clear()
    _SCENE["fail_once"].clear()
    _SCENE["drop_pages"].clear()
    _SCENE["repeat_page"] = None
    _SCENE["reset_counter"] += 1
    _page_hits.clear()
    return dict(_SCENE)


def scene() -> dict:
    return {k: (set(v) if isinstance(v, set) else v) for k, v in _SCENE.items()}


def page_policy(page: int) -> str:
    """Return what the fixture should do for ``page`` on this hit."""
    if page in _SCENE["fail_pages"]:
        return "fail"
    if page in _SCENE["fail_once"]:
        _SCENE["fail_once"].discard(page)
        return "fail"
    if page in _SCENE["drop_pages"]:
        return "drop"
    if _SCENE["repeat_page"] == page:
        # Only the first hit re-serves the previous page; retries then get
        # the genuine page, modelling a one-off server-side replay.
        hits = _page_hits.get(page, 0)
        _page_hits[page] = hits + 1
        if hits == 0:
            return "repeat"
    return "ok"
