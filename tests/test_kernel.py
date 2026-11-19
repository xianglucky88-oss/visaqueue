"""Kernel rules: category interpretation, repeats, revisions, conflicts."""
import importlib

from visaqueue.framework import content_hash


def _row(ts, bulletin="2026-02-15", kind="monthly", category="EB2",
         cutoff="01DEC25", status="published", rid=None,
         target_month="2026-03"):
    r = {"id": rid or ("%s|%s|%s|%s" % (bulletin, kind, category, ts)),
         "name": category, "ts": ts, "status": status,
         "bulletin_date": bulletin, "table_kind": kind,
         "target_month": target_month, "cutoff": cutoff, "category": category}
    return r


def test_transform_module_has_a_public_entrypoint():
    module = importlib.import_module("visaqueue.kernel")
    assert callable(getattr(module, "process", None))
    result = module.process({"id": "sample", "name": "item", "value": 7,
                             "status": "active", "ts": "t1"})
    assert isinstance(result, dict)


def test_status_interpretation_covers_all_lifecycle_states():
    k = importlib.import_module("visaqueue.kernel")
    assert k.interpret_state("Current") == "current"
    assert k.interpret_state("u") == "unavailable"
    assert k.interpret_state("Revoked") == "revoked"
    assert k.interpret_state("PENDING") == "pending"
    assert k.interpret_state("published", cutoff="01JAN26") == "available"
    assert k.interpret_state("mystery", cutoff="") == "unknown"
    # Every state has a traceable explanation.
    for state in ("available", "current", "unavailable", "revoked",
                  "pending", "unknown"):
        assert k.explain_state(state)


def test_cutoff_parsing_for_both_table_kinds():
    k = importlib.import_module("visaqueue.kernel")
    assert k.parse_cutoff("01JAN26").year == 2026
    assert k.parse_cutoff("01JAN26").month == 1
    assert k.parse_cutoff("2026-01-15").day == 15
    assert k.parse_cutoff("Current") is None
    assert k.parse_cutoff("U") is None
    assert k.parse_cutoff("") is None


def test_category_interpretation_keeps_unknown_codes():
    k = importlib.import_module("visaqueue.kernel")
    known = k.interpret_category("EB1")
    assert known["known"] is True and known["label"]
    unknown = k.interpret_category("ZZ9")
    assert unknown["known"] is False
    # The raw code is preserved rather than dropped.
    assert unknown["category"] == "ZZ9"


def test_cross_month_repeat_detection():
    k = importlib.import_module("visaqueue.kernel")
    history = [
        _row("2026-01-15T09:00:00Z", target_month="2026-02",
             cutoff="01NOV25", rid="h1"),
        _row("2026-01-15T09:00:00Z", kind="dates", target_month="2026-02",
             cutoff="2025-11-01", rid="h2"),
    ]
    repeated = _row("2026-02-15T09:00:00Z", target_month="2026-03",
                    cutoff="01NOV25", rid="r1")
    verdict = k.is_repeated_across_months(history, repeated)
    assert verdict["repeated"] is True
    assert verdict["previous_target_month"] == "2026-02"

    moved = _row("2026-02-15T09:00:00Z", target_month="2026-03",
                 cutoff="15DEC25", rid="r2")
    assert k.is_repeated_across_months(history, moved)["repeated"] is False

    # Table kinds never cross: monthly history cannot confirm a dates repeat.
    dates_row = _row("2026-02-15T09:00:00Z", kind="dates",
                     target_month="2026-03", cutoff="2025-11-01", rid="r3")
    assert k.is_repeated_across_months(history, dates_row)["repeated"] is True

    # Non-date states are never repeats.
    current_row = _row("2026-02-15T09:00:00Z", target_month="2026-03",
                       cutoff="Current", rid="r4")
    assert k.is_repeated_across_months(history, current_row)["repeated"] is False


def test_document_revisions_follow_snapshot_changes_not_each_row():
    k = importlib.import_module("visaqueue.kernel")
    t1, t2, t3 = "2026-01-15T09:00:00Z", "2026-02-14T09:00:00Z", "2026-03-01T09:00:00Z"
    rows = [
        _row(t1, category="EB1", cutoff="01JAN26", rid="a1"),
        _row(t1, category="EB2", cutoff="01NOV25", rid="a2"),
        # Re-publication: EB2 moves, EB1 unchanged -> revision 2 for both.
        _row(t2, category="EB1", cutoff="01JAN26", rid="b1"),
        _row(t2, category="EB2", cutoff="15DEC25", rid="b2"),
    ]
    rev = k.assign_document_revisions(rows)
    assert rev["a1"] == 1 and rev["a2"] == 1
    assert rev["b1"] == 2 and rev["b2"] == 2

    # Out-of-order late correction opens a new revision without renumbering.
    rows.append(_row(t3, category="EB1", cutoff="02JAN26", rid="late1"))
    rev = k.assign_document_revisions(rows)
    assert rev["a1"] == 1 and rev["b1"] == 2 and rev["late1"] == 3


def test_monthly_and_dates_tables_get_independent_revisions():
    k = importlib.import_module("visaqueue.kernel")
    rows = [
        _row("2026-01-15T09:00:00Z", kind="monthly", rid="m1"),
        _row("2026-01-15T09:05:00Z", kind="dates", rid="d1"),
    ]
    rev = k.assign_document_revisions(rows)
    assert rev["m1"] == 1 and rev["d1"] == 1


def test_same_instant_contradiction_keeps_both_sides():
    k = importlib.import_module("visaqueue.kernel")
    ts = "2026-02-14T09:00:00Z"
    a = _row(ts, cutoff="01DEC25", rid="X-1")
    b = _row(ts, cutoff="15DEC25", rid="X-2")
    out = k.reduce([a, b])
    by_id = {r["id"]: r for r in out}
    # Exactly one latest, but neither row is dropped.
    assert len(out) == 2
    assert sum(r["is_latest"] for r in out) == 1
    loser = min((r for r in out), key=lambda r: content_hash(r))
    assert by_id[loser["id"]]["conflict_of"] is not None
    assert "同刻矛盾" in loser["note"]


def test_reduce_builds_ordered_version_chain():
    k = importlib.import_module("visaqueue.kernel")
    rows = [_row("t2", rid="z"), _row("t1", rid="a")]
    out = k.reduce(rows)
    first = next(r for r in out if r["id"] == "a")
    second = next(r for r in out if r["id"] == "z")
    assert first["version_no"] == 1 and first["is_latest"] == 0
    assert second["version_no"] == 2 and second["prev_version_id"] == "a"
    assert second["is_latest"] == 1


def test_bulletin_summary_separates_table_kinds():
    k = importlib.import_module("visaqueue.kernel")
    rows = [
        _row("t1", kind="monthly", category="EB1", rid="m1"),
        _row("t1", kind="dates", category="EB1", rid="d1"),
        _row("t2", kind="monthly", category="EB1", cutoff="02JAN26", rid="m2"),
    ]
    for r in rows:
        r.update(k.process(r))
    summary = k.summarize(rows)
    assert summary["by_table_kind"]["monthly"] == 2
    assert summary["by_table_kind"]["dates"] == 1
    bulletin_dates = {(b["bulletin_date"], b["table_kind"])
                      for b in summary["bulletins"]}
    assert ("2026-02-15", "monthly") in bulletin_dates
    assert ("2026-02-15", "dates") in bulletin_dates
