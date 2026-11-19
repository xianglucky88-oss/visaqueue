"""End-to-end collection scenarios against the in-process fixture.

Covers the five operating modes demanded by the domain: out-of-order input,
cross-page interruption, partial page failure, full re-run and incremental
continuation -- asserting in each that the visible range never regresses and
history remains explainable.
"""
import importlib

import pytest

from visaqueue import mockdata
from visaqueue.framework import run
from visaqueue.history import (diff_revisions, document_history,
                               progress_report, visible_snapshot)
from visaqueue.pipeline import run as pipeline_run
from visaqueue.store import Store


@pytest.fixture()
def store():
    import sqlite3
    con = sqlite3.connect(":memory:")
    st = Store(con)
    yield st
    con.close()


@pytest.fixture(autouse=True)
def _clean_scene():
    mockdata.reset_scene()
    yield
    mockdata.reset_scene()


def test_pipeline_returns_a_status(store):
    result = run(con=store.con)
    assert isinstance(result, dict)
    from visaqueue.config import SPEC
    assert result.get("engine") == SPEC.engine
    assert result["status"] == "complete"
    assert result["failed_pages"] == []


def test_full_run_captures_both_table_kinds_and_revisions(store):
    result = run(con=store.con)
    assert result["duplicates"] == 1           # exact duplicate row collapsed
    assert result["ingested"] == result["entry_count"]

    docs = {(d["bulletin_date"], d["table_kind"]): d for d in store.documents()}
    assert ("2026-01-15", "monthly") in docs
    assert ("2026-01-15", "dates") in docs
    assert ("2026-02-15", "monthly") in docs
    assert ("2026-02-15", "dates") in docs
    # February bulletin was published then revised.
    feb = docs[("2026-02-15", "monthly")]
    assert feb["revision_count"] == 2
    assert feb["latest_rows"] == 4
    # January monthly received a late one-category correction.
    jan = docs[("2026-01-15", "monthly")]
    assert jan["revision_count"] == 2


def test_full_rerun_is_idempotent_and_keeps_history(store):
    first = run(con=store.con)
    before = store.snapshot(only_latest=False)
    batches_before = len(store.batches())

    second = run(mode="full", con=store.con)
    after = store.snapshot(only_latest=False)

    assert second["ingested"] == 0
    assert len(after) == len(before)
    assert len(store.batches()) == batches_before + 1
    # Revisions from the first run remain readable.
    view = document_history(store, "2026-02-15", "monthly")
    assert {r["revision"] for r in view["revisions"]} == {1, 2}
    assert first["watermark"] == second["watermark"]


def test_out_of_order_late_row_does_not_regress_visibility(store):
    run(con=store.con)
    # The late January correction must coexist with the original row.
    view = document_history(store, "2026-01-15", "monthly")
    eb1 = [e for e in view["entries"] if e["category"] == "EB1"]
    assert {e["cutoff"] for e in eb1} == {"01JAN26", "02JAN26"}
    latest = [e for e in eb1 if e["is_latest"]]
    assert len(latest) == 1 and latest[0]["cutoff"] == "02JAN26"
    # The original is still there, superseded but not deleted.
    assert any(not e["is_latest"] and e["cutoff"] == "01JAN26" for e in eb1)


def test_historical_snapshot_excludes_later_information(store):
    run(con=store.con)
    # Immediately after the first February publication there would be only
    # revision 1.  first_seen_at is the ingest instant, so pick an as_of
    # between the two batches by replaying into a second store manually.
    import sqlite3
    con2 = sqlite3.connect(":memory:")
    partial = Store(con2)
    from visaqueue.framework import normalize, dedup, _apply_kernel
    from visaqueue.mockdata import build_rows
    rows = [normalize(dict(r)) for r in build_rows()]
    rows, _ = dedup(rows)
    first_release = [r for r in rows if r["ts"] <= "2026-02-01T00:00:00Z"]
    first_release = _apply_kernel(first_release)
    kernel = importlib.import_module("visaqueue.kernel")
    rev = kernel.assign_document_revisions(first_release)
    for r in first_release:
        partial.store_entry(r, seen_at="2026-02-01T10:00:00Z",
                            batch_id="seed",
                            revision_override=rev.get(r["id"]))
    past = partial.snapshot(as_of="2026-02-02T00:00:00Z")
    feb = [e for e in past if e["bulletin_date"] == "2026-02-15"]
    # February bulletin had not been observed at that as_of moment.
    assert feb == []
    con2.close()


def test_partial_page_failure_keeps_visible_range_and_marks_batch(store):
    mockdata.set_scene(fail_pages={3})
    result = run(con=store.con)

    assert result["status"] == "partial"
    assert 3 in result["failed_pages"]
    stored_after_failure = store.entry_count()
    assert stored_after_failure > 0
    batches = store.batches()
    assert batches[-1]["status"] == "partial"
    assert "3" in batches[-1]["failed_pages"]
    # Watermark must not advance past the interruption.
    assert store.watermark() == batches[-1]["watermark_before"]

    # Clearing the fault and re-running fills the gap without losing rows.
    mockdata.reset_scene()
    resumed = run(mode="incremental", con=store.con)
    assert resumed["status"] == "complete"
    assert store.entry_count() >= stored_after_failure
    full_docs = { (d["bulletin_date"], d["table_kind"]) for d in store.documents()}
    assert ("2026-02-15", "monthly") in full_docs


def test_dropped_trailing_page_does_not_destroy_existing_data(store):
    # First a healthy run establishes the baseline.
    run(con=store.con)
    baseline = store.entry_count()

    mockdata.set_scene(drop_pages={2})
    result = run(mode="incremental", con=store.con)
    # The crawl ends early (empty page); prior rows remain untouched.
    assert store.entry_count() == baseline
    assert result["status"] in ("complete", "partial")


def test_duplicate_page_crawl_does_not_inflate_storage(store):
    mockdata.set_scene(repeat_page=2)
    result = run(con=store.con)
    assert result["duplicate_pages"]  # the repeated page was noticed
    # Same content set as a clean run.
    mockdata.reset_scene()
    import sqlite3
    con2 = sqlite3.connect(":memory:")
    clean = Store(con2)
    run(con=clean.con)
    assert store.entry_count() == clean.entry_count()
    con2.close()


def test_incremental_continuation_after_new_bulletin(store):
    run(con=store.con)
    initial = store.entry_count()

    # Simulate a newly published bulletin arriving at the source, then an
    # incremental run picking it up.
    from visaqueue.framework import normalize, _apply_kernel
    new_row = mockdata.extra_row(1)
    rows = _apply_kernel([normalize(dict(new_row))])
    kernel = importlib.import_module("visaqueue.kernel")
    rev = kernel.assign_document_revisions(rows)
    ingested = 0
    for r in rows:
        ingested += int(store.store_entry(
            r, seen_at="2026-04-01T10:00:00Z", batch_id="inc",
            revision_override=rev.get(r["id"])))
    assert ingested == 1
    assert store.entry_count() == initial + 1
    docs = {(d["bulletin_date"], d["table_kind"]) for d in store.documents()}
    assert ("2026-03-15", "monthly") in docs or ("2026-03-15", "dates") in docs


def test_revision_diff_explains_what_changed(store):
    run(con=store.con)
    diff = diff_revisions(store, "2026-02-15", "monthly", 1, 2)
    changes = {c["category"]: c["change"] for c in diff["changes"]}
    assert changes["EB2"] == "updated"
    assert changes["EB3"] == "updated"      # became Current
    assert changes["EB1"] == "unchanged"


def test_progress_report_is_traceable(store):
    run(con=store.con)
    run(mode="full", con=store.con)
    report = progress_report(store)
    assert report["batch_count"] == 2
    assert report["last_status"] == "complete"
    assert report["document_count"] >= 4
    for doc in report["documents"]:
        assert doc["latest_revision"] >= 1


def test_visible_snapshot_groups_by_table_kind(store):
    run(con=store.con)
    snap = visible_snapshot(store)
    assert set(snap["table_kinds"]) == {"monthly", "dates"}
    for kind, rows in snap["tables"].items():
        assert all(r["table_kind"] == kind for r in rows)


def test_pipeline_entrypoint_matches_framework(store):
    result = pipeline_run(con=store.con)
    assert result["engine"] == "crawl"
    assert result["entry_count"] == store.entry_count()
