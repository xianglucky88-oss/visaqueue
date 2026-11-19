"""Local transport, parsing, storage, and session helpers.

The collection pipeline is built around one invariant: **a crawl can only add
knowledge, never remove it**.  Page failures stop the crawl early but stored
rows, watermarks and revision history are untouched; the batch ledger records
exactly which pages failed so the gap is visible and an incremental re-run can
fill it later.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from lxml import html as _lh

from .config import SPEC
from .store import Store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


class FetchOutcome:
    """Result of a paginated fetch, including any interruption."""

    def __init__(self):
        self.records: list[dict] = []
        self.attempts = 0
        self.pages_fetched = 0
        self.failed_pages: list[int] = []
        self.interrupted = False
        self.duplicate_pages: list[int] = []

    @property
    def status(self) -> str:
        if self.interrupted:
            return "partial" if self.records else "failed"
        return "complete"


def fetch_all(http: Http, max_pages: int = 200):
    """Collect records, tolerating mid-pagination failures.

    On a page that keeps failing after retries the crawl stops and returns
    what was collected so far instead of raising: previously stored data is
    unaffected and the caller marks the batch ``partial``.  A page whose
    parsed records are identical to the previous one (a one-off server
    replay) is counted as a duplicate page and re-requested at the same page
    number rather than skipping content or inflating the batch.
    """
    out = FetchOutcome()
    page = 1
    previous_records: list[dict] | None = None
    duplicate_streak = 0
    while page <= max_pages:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        out.attempts += att + 1
        if status != 200:
            out.failed_pages.append(page)
            out.interrupted = True
            break
        page_recs = parse(body, SPEC.fmt)
        # Compare parsed records rather than the raw body: the envelope's
        # page number changes even when the server re-serves identical rows.
        if previous_records is not None and page_recs == previous_records:
            out.duplicate_pages.append(page)
            duplicate_streak += 1
            if duplicate_streak > 2:
                # A genuinely repeated empty tail marks the end of the archive.
                break
            # Re-request the same page number; the fixture's one-off replay
            # then returns the real content, so no page is skipped.
            continue
        duplicate_streak = 0
        previous_records = page_recs
        out.pages_fetched += 1
        out.records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
    return out


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False, default=str)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    if isinstance(d.get("table_kind"), str):
        d["table_kind"] = d["table_kind"].strip().lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


def _annotate_repeats(records):
    """Kernel-owned cross-month repeat judgement, attached per row."""
    k = importlib.import_module(".kernel", __package__)
    if not hasattr(k, "is_repeated_across_months"):
        return records
    history = [r for r in records]
    for r in records:
        verdict = k.is_repeated_across_months(history, r)
        r["repeated_across_months"] = int(verdict["repeated"])
        r["repeat_reason"] = verdict.get("reason", "")
    return records


def _document_revisions(records):
    """Kernel-owned, document-level revision numbers keyed by source id."""
    k = importlib.import_module(".kernel", __package__)
    if hasattr(k, "assign_document_revisions"):
        return k.assign_document_revisions(records)
    return {}


def _align_revisions(store, records, batch_map):
    """Align batch-local revision numbers with previously stored revisions.

    A continuation run re-fetches rows that an earlier (possibly interrupted)
    run already stored.  Their batch-local numbering restarts at 1, so map
    each source observation instant to the revision already known for that
    document; genuinely new instants take the next free number.  This keeps
    revision identity stable across partial/full/incremental runs.
    """
    docs: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in records:
        docs.setdefault((str(r.get("bulletin_date", "")),
                         str(r.get("table_kind", "")).strip().lower()), []).append(r)
    aligned: dict[str, int] = {}
    for (bd, kind), rows in docs.items():
        known = store.observed_revision_map(bd, kind)
        max_known = max(known.values(), default=0)
        # Batch-local ordering of distinct observation instants.
        instants = sorted({str(r.get("ts", "")) for r in rows})
        instant_to_rev: dict[str, int] = {}
        next_rev = max_known
        for instant in instants:
            if instant in known:
                instant_to_rev[instant] = known[instant]
            else:
                next_rev += 1
                instant_to_rev[instant] = next_rev
        for r in rows:
            aligned[str(r.get("id", ""))] = instant_to_rev[str(r.get("ts", ""))]
    return aligned


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def run(mode="full", con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    batch_id = uuid.uuid4().hex[:16]
    started_at = _now()
    wm_before = store.watermark()
    store.open_batch(batch_id, started_at, mode, wm_before)
    try:
        outcome = fetch_all(http)
        norm = [normalize(dict(r)) for r in outcome.records]
        norm, dup_in_batch = dedup(norm)
        norm = _apply_kernel(norm)
        norm = _annotate_repeats(norm)
        _document_revisions(norm)  # validates kernel assignment on the batch
        revision_map = _align_revisions(store, norm, None)

        # Incremental mode only filters by the watermark; a full re-run keeps
        # every row and relies on content-hash identity to avoid duplicates.
        skipped_by_watermark = 0
        if mode == "incremental" and wm_before:
            kept = []
            for r in norm:
                if str(r.get("ts", "")) > wm_before:
                    kept.append(r)
                else:
                    skipped_by_watermark += 1
            norm = kept

        ingested = 0
        for r in norm:
            rev = revision_map.get(str(r.get("id", "")))
            r["document_revision"] = rev
            if store.store_entry(r, seen_at=started_at, batch_id=batch_id,
                                 revision_override=rev):
                ingested += 1
            # Legacy table kept in sync for the original API.
            store.upsert(r)

        # The watermark advances only over observed timestamps and never
        # moves backwards; on an interrupted crawl it stays where it was so a
        # later continuation re-attempts the missing pages.
        candidate_ts = [str(r.get("ts", "")) for r in norm]
        new_wm = max(candidate_ts + [wm_before or ""])
        if new_wm and not outcome.interrupted:
            store.set_watermark(new_wm)
        wm_after = store.watermark()
        status = outcome.status
        store.update_batch(
            batch_id, fetched_pages=outcome.pages_fetched,
            fetched_rows=len(outcome.records), stored_rows=ingested,
            duplicate_rows=dup_in_batch,
            failed_pages=",".join(map(str, outcome.failed_pages)))
        store.close_batch(
            batch_id, _now(), status, wm_after,
            note=("duplicate_pages=%s;skipped_by_watermark=%d"
                  % (",".join(map(str, outcome.duplicate_pages)),
                     skipped_by_watermark)))
        return {
            "engine": "crawl", "mode": mode, "batch_id": batch_id,
            "fetched": len(outcome.records),
            "fetched_pages": outcome.pages_fetched,
            "duplicates": dup_in_batch,
            "duplicate_pages": outcome.duplicate_pages,
            "failed_pages": outcome.failed_pages,
            "ingested": ingested, "count": store.count(),
            "entry_count": store.entry_count(),
            "http_attempts": outcome.attempts,
            "slept": round(sleeper.slept, 4),
            "status": status,
            "watermark": wm_after,
            "watermark_before": wm_before,
            "skipped_by_watermark": skipped_by_watermark,
        }
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    batch_id = uuid.uuid4().hex[:16]
    started_at = _now()
    wm_before = store.watermark()
    store.open_batch(batch_id, started_at, "rpa", wm_before)
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        norm = _annotate_repeats(norm)
        revision_map = _align_revisions(store, norm, None)
        ingested = 0
        for x in norm:
            rev = revision_map.get(str(x.get("id", "")))
            x["document_revision"] = rev
            if store.store_entry(x, seen_at=started_at, batch_id=batch_id,
                                 revision_override=rev):
                ingested += 1
            store.upsert(x)
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        wm = max([str(x.get("ts", "")) for x in norm] + [wm_before or ""])
        if wm:
            store.set_watermark(wm)
        store.update_batch(batch_id, fetched_rows=len(items), stored_rows=ingested,
                           duplicate_rows=dup, status="complete")
        store.close_batch(batch_id, _now(), "complete", store.watermark())
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total, "entry_count": store.entry_count(),
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log,
                "batch_id": batch_id, "status": "complete",
                "watermark": store.watermark()}
    finally:
        if own:
            con.close()
