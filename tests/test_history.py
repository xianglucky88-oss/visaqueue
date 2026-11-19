"""History read-model tests and contradiction persistence."""
import sqlite3

from visaqueue import kernel
from visaqueue.framework import _align_revisions, normalize
from visaqueue.history import (diff_revisions, document_history,
                               visible_snapshot)
from visaqueue.store import Store, stable_content_hash


def _store():
    con = sqlite3.connect(":memory:")
    return con, Store(con)


def _make(rid, ts, bulletin, kind, category, cutoff, status="published"):
    r = {"id": rid, "name": category, "ts": ts, "status": status,
         "bulletin_date": bulletin, "table_kind": kind,
         "target_month": "2026-03", "cutoff": cutoff, "category": category}
    r = normalize(r)
    r.update(kernel.process(r))
    return r


def test_same_instant_contradiction_persists_both_records():
    con, st = _store()
    seen = "2026-02-15T10:00:00Z"
    a = _make("A", "2026-02-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
              "01DEC25")
    b = _make("B", "2026-02-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
              "15DEC25")
    rows = kernel.reduce([a, b])
    rev = _align_revisions(st, rows, None)
    for r in rows:
        st.store_entry(r, seen_at=seen, batch_id="b",
                       revision_override=rev[r["id"]])
    # Neither side is dropped.
    assert st.entry_count() == 2
    view = document_history(st, "2026-02-15", "monthly")
    eb2 = [e for e in view["entries"] if e["category"] == "EB2"]
    assert len(eb2) == 2
    assert sum(e["is_latest"] for e in eb2) == 1
    assert any(e["conflict_of"] for e in eb2)
    assert any("同刻矛盾" in e["note"] for e in eb2)


def test_stable_hash_ignores_bookkeeping_fields():
    base = {"id": "A", "bulletin_date": "2026-02-15", "table_kind": "monthly",
            "category": "EB2", "cutoff": "01DEC25", "status": "published",
            "state": "available"}
    reprocessed = dict(base)
    reprocessed.update({"revision": 2, "is_latest": 0, "version_no": 7,
                        "first_seen_at": "later", "document_revision": 9})
    assert stable_content_hash(base) == stable_content_hash(reprocessed)
    changed = dict(base, cutoff="15DEC25")
    assert stable_content_hash(base) != stable_content_hash(changed)


def test_old_data_remains_readable_after_rule_change():
    """Simulate a rule change: an 'unknown' row ingested first, then a new
    run that interprets it.  The original payload must stay retrievable."""
    con, st = _store()
    r = _make("X", "2026-01-15T09:00:00Z", "2026-01-15", "monthly", "ZZ9",
              "weird-cutoff", status="mystery")
    assert r["state"] == "unknown"
    rev = _align_revisions(st, [r], None)
    st.store_entry(r, "2026-01-15T10:00:00Z", batch_id="old-rule",
                   revision_override=rev["X"])
    # Even after later runs, the original payload is intact and explainable.
    loaded = st.all_entries()
    assert len(loaded) == 1
    assert loaded[0]["cutoff"] == "weird-cutoff"
    assert kernel.explain_state(loaded[0]["state"])
    view = document_history(st, "2026-01-15", "monthly")
    assert view["explanation"]  # traceable narrative present


def test_visible_snapshot_table_grouping_and_diff():
    con, st = _store()
    rows = [
        _make("m1", "2026-02-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
              "01DEC25"),
        _make("d1", "2026-02-14T09:05:00Z", "2026-02-15", "dates", "EB2",
              "2025-12-01"),
        _make("m2", "2026-03-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
              "15DEC25"),
    ]
    rev = _align_revisions(st, rows, None)
    for r in rows:
        st.store_entry(r, "2026-03-15T10:00:00Z", batch_id="b",
                       revision_override=rev[r["id"]])
    snap = visible_snapshot(st)
    assert set(snap["tables"]) == {"monthly", "dates"}
    diff = diff_revisions(st, "2026-02-15", "monthly", 1, 2)
    assert diff["changes"][0]["change"] == "updated"


def test_as_of_snapshot_shows_then_current_revision():
    con, st = _store()
    old = _make("o", "2026-02-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
                "01DEC25")
    new = _make("n", "2026-03-14T09:00:00Z", "2026-02-15", "monthly", "EB2",
                "15DEC25")
    st.store_entry(old, "2026-02-15T10:00:00Z", batch_id="b1",
                   revision_override=1)
    st.store_entry(new, "2026-03-15T10:00:00Z", batch_id="b2",
                   revision_override=2)
    past = visible_snapshot(st, as_of="2026-03-01T00:00:00Z")
    assert past["tables"]["monthly"][0]["cutoff"] == "01DEC25"
    current = visible_snapshot(st)
    assert current["tables"]["monthly"][0]["cutoff"] == "15DEC25"
