"""SQLite store with append-only, bi-temporal historical storage.

Design rules backing the historical guarantees:

* **append-only** -- crawls only insert rows and update bookkeeping flags;
  no crawl ever deletes or truncates previously stored content, so a partial
  page failure, a re-run or a late row can never shrink the visible range.
* **content identity** -- every observed row has a content hash; identical
  re-crawls collapse to one stored row while ``last_seen_at`` advances.
* **document revisions** -- rows of the same bulletin document
  (bulletin_date + table_kind) form a revision chain; a newer revision marks
  older ones ``superseded_by`` but keeps them readable.
* **batches** -- each run writes a ``crawl_batches`` ledger row, including
  failed pages and final status, so the provenance of every observation is
  traceable.
* **history queries** -- ``snapshot`` / ``bulletin_view`` take an ``as_of``
  system timestamp and return exactly what was visible then; rows first seen
  later cannot leak into an earlier answer.

The legacy ``records``/``meta`` tables and the original method names
(``upsert``, ``count``, ``all_records``, ``get``, ``watermark``,
``set_watermark``, ``reset``) are preserved for backward compatibility.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Optional

from .config import DB_PATH


class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    def connect(self):
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _init(self):
        c = self.con
        # Legacy table (kept for the original record-level API).
        c.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)")
        c.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        # One row per distinct observed content of a bulletin entry.
        c.execute(
            "CREATE TABLE IF NOT EXISTS bulletin_entries("
            "content_hash TEXT PRIMARY KEY,"
            "logical_id TEXT NOT NULL,"
            "source_id TEXT,"
            "bulletin_date TEXT NOT NULL,"
            "table_kind TEXT NOT NULL,"
            "category TEXT NOT NULL,"
            "target_month TEXT,"
            "cutoff TEXT,"
            "raw_status TEXT,"
            "state TEXT,"
            "state_explanation TEXT,"
            "revision INTEGER NOT NULL DEFAULT 1,"
            "observed_at TEXT,"
            "first_seen_at TEXT NOT NULL,"
            "last_seen_at TEXT NOT NULL,"
            "is_latest INTEGER NOT NULL DEFAULT 1,"
            "superseded_by INTEGER,"
            "conflict_of TEXT,"
            "note TEXT DEFAULT '',"
            "payload TEXT NOT NULL)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_identity "
            "ON bulletin_entries(bulletin_date, table_kind, category)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_seen "
            "ON bulletin_entries(first_seen_at)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_entry_latest "
            "ON bulletin_entries(logical_id, is_latest)")
        # One row per bulletin document revision; progress is append-only.
        c.execute(
            "CREATE TABLE IF NOT EXISTS bulletin_revisions("
            "bulletin_date TEXT NOT NULL,"
            "table_kind TEXT NOT NULL,"
            "revision INTEGER NOT NULL,"
            "observed_at TEXT NOT NULL,"
            "first_seen_at TEXT NOT NULL,"
            "complete INTEGER NOT NULL DEFAULT 1,"
            "source_batch TEXT,"
            "note TEXT DEFAULT '',"
            "PRIMARY KEY(bulletin_date, table_kind, revision))")
        # Crawl ledger: a row is inserted up-front and closed at the end.
        c.execute(
            "CREATE TABLE IF NOT EXISTS crawl_batches("
            "batch_id TEXT PRIMARY KEY,"
            "started_at TEXT NOT NULL,"
            "finished_at TEXT,"
            "mode TEXT,"
            "fetched_pages INTEGER DEFAULT 0,"
            "fetched_rows INTEGER DEFAULT 0,"
            "stored_rows INTEGER DEFAULT 0,"
            "duplicate_rows INTEGER DEFAULT 0,"
            "failed_pages TEXT DEFAULT '',"
            "status TEXT NOT NULL DEFAULT 'running',"
            "watermark_before TEXT,"
            "watermark_after TEXT,"
            "note TEXT DEFAULT '')")
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Hashing (legacy + shared helper).
    # ------------------------------------------------------------------ #
    @staticmethod
    def hash(record):
        return content_hash(record)

    # ------------------------------------------------------------------ #
    # Legacy record API.
    # ------------------------------------------------------------------ #
    def upsert(self, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO records(content_hash,id,ts,payload) VALUES(?,?,?,?)",
            (self.hash(record), str(record.get("id", "")),
             str(record.get("ts", "")), json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    def watermark(self):
        row = self.con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()
        return row[0] if row else None

    def set_watermark(self, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES('watermark',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(value),))
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Domain storage.
    # ------------------------------------------------------------------ #
    def observed_revision_map(self, bulletin_date, table_kind) -> dict[str, int]:
        """Existing observed_at -> revision map for one bulletin document.

        Used to align revision numbers of a continuation run with what was
        stored by earlier (possibly interrupted) runs.
        """
        rows = self.con.execute(
            "SELECT DISTINCT observed_at, revision FROM bulletin_entries "
            "WHERE bulletin_date=? AND table_kind=?",
            (str(bulletin_date), str(table_kind))).fetchall()
        return {obs: rev for obs, rev in rows}

    def store_entry(self, record: dict[str, Any], seen_at: str,
                    batch_id: Optional[str] = None,
                    revision_override: Optional[int] = None) -> bool:
        """Insert a newly observed entry, or refresh an existing one.

        Returns ``True`` when this content had never been stored before.
        Never deletes anything: a re-crawl of old content merely bumps
        ``last_seen_at`` and leaves the historical row intact.
        ``revision_override`` carries the document-level revision number
        computed from source observation order by the pipeline.
        """
        h = stable_content_hash(record)
        existing = self.con.execute(
            "SELECT content_hash, last_seen_at FROM bulletin_entries "
            "WHERE content_hash=?", (h,)).fetchone()
        if existing is not None:
            if str(seen_at) > existing[1]:
                self.con.execute(
                    "UPDATE bulletin_entries SET last_seen_at=? WHERE content_hash=?",
                    (str(seen_at), h))
                self.con.commit()
            return False

        logical_id = str(record.get("logical_id", ""))
        observed_at = str(record.get("ts", seen_at))
        if revision_override is not None:
            revision = revision_override
        else:
            revision = self._next_revision(
                record.get("bulletin_date"), record.get("table_kind"),
                seen_at, observed_at, logical_id)
        self.con.execute(
            "INSERT INTO bulletin_entries(content_hash, logical_id, source_id,"
            " bulletin_date, table_kind, category, target_month, cutoff,"
            " raw_status, state, state_explanation, revision, observed_at,"
            " first_seen_at, last_seen_at, is_latest, note, payload)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
            (h, logical_id, str(record.get("id", "")),
             str(record.get("bulletin_date", "")),
             str(record.get("table_kind", "")),
             str(record.get("category", record.get("name", ""))),
             record.get("target_month"), record.get("cutoff"),
             str(record.get("status", "")), record.get("state"),
             record.get("state_explanation", ""), revision,
             str(record.get("ts", seen_at)), str(seen_at), str(seen_at),
             str(record.get("note", "")),
             json.dumps(record, ensure_ascii=False)))
        self._retire_older_revisions(logical_id, revision, h)
        self._record_revision(record, revision, seen_at, batch_id)
        self.con.commit()
        return True

    def _next_revision(self, bulletin_date, table_kind, seen_at: str,
                       observed_at: str, logical_id: str) -> int:
        """Revision number for a brand-new content hash of a document.

        Revisions follow the *source* observation timeline (``observed_at``),
        not the local ingest time: a full re-run ingests everything in one
        batch (identical ``first_seen_at``) yet must still reconstruct the
        bulletin's revision chain from the timestamps the source assigned.

        * content first observed later than everything known -> new revision;
        * content whose observation time matches an existing revision -> that
          revision (the same bulletin re-crawled);
        * late/out-of-order content whose observation time predates the latest
          revision gets the next free number without touching existing rows,
          so visibility can never move backwards.
        """
        row = self.con.execute(
            "SELECT COALESCE(MAX(revision),0) FROM bulletin_entries "
            "WHERE bulletin_date=? AND table_kind=?",
            (str(bulletin_date), str(table_kind))).fetchone()
        max_rev = row[0] if row else 0
        if max_rev == 0:
            return 1
        match = self.con.execute(
            "SELECT revision FROM bulletin_entries "
            "WHERE bulletin_date=? AND table_kind=? AND observed_at=? LIMIT 1",
            (str(bulletin_date), str(table_kind), str(observed_at))).fetchone()
        if match is not None:
            return match[0]
        latest_obs = self.con.execute(
            "SELECT MAX(observed_at) FROM bulletin_entries "
            "WHERE bulletin_date=? AND table_kind=?",
            (str(bulletin_date), str(table_kind))).fetchone()[0]
        if latest_obs is not None and str(observed_at) > str(latest_obs):
            return max_rev + 1
        # Out-of-order arrival: keep existing revisions untouched and take the
        # next free number for this previously unknown older content.
        return max_rev + 1

    def _retire_older_revisions(self, logical_id: str, revision: int,
                                new_hash: str) -> None:
        self.con.execute(
            "UPDATE bulletin_entries SET is_latest=0, "
            "superseded_by=? WHERE logical_id=? AND revision<? AND is_latest=1",
            (revision, logical_id, revision))
        # Same-instant contradiction: distinct content, same logical row,
        # same observation timestamp.  Both stay visible; mark the loser.
        self.con.execute(
            "UPDATE bulletin_entries SET conflict_of=? "
            "WHERE logical_id=? AND observed_at="
            "(SELECT observed_at FROM bulletin_entries WHERE content_hash=?) "
            "AND content_hash<>? AND conflict_of IS NULL",
            (new_hash, logical_id, new_hash, new_hash))

    def _record_revision(self, record: dict, revision: int, seen_at: str,
                         batch_id: Optional[str]) -> None:
        self.con.execute(
            "INSERT INTO bulletin_revisions(bulletin_date, table_kind, revision,"
            " observed_at, first_seen_at, complete, source_batch, note)"
            " VALUES(?,?,?,?,?,1,?,?) "
            "ON CONFLICT(bulletin_date, table_kind, revision) DO UPDATE SET "
            "observed_at=excluded.observed_at, source_batch=excluded.source_batch",
            (str(record.get("bulletin_date", "")),
             str(record.get("table_kind", "")), revision,
             str(record.get("ts", seen_at)), str(seen_at),
             batch_id, str(record.get("note", ""))))

    def entry_count(self) -> int:
        return self.con.execute(
            "SELECT COUNT(*) FROM bulletin_entries").fetchone()[0]

    def all_entries(self) -> list[dict[str, Any]]:
        return [json.loads(r[0]) for r in self.con.execute(
            "SELECT payload FROM bulletin_entries ORDER BY first_seen_at, logical_id")]

    def get_entry(self, content_hash: str) -> Optional[dict[str, Any]]:
        row = self.con.execute(
            "SELECT payload FROM bulletin_entries WHERE content_hash=?",
            (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    # ------------------------------------------------------------------ #
    # Historical read models.
    # ------------------------------------------------------------------ #
    def snapshot(self, as_of: Optional[str] = None,
                 only_latest: bool = True) -> list[dict[str, Any]]:
        """Reconstruct the visible entries at system time ``as_of``.

        Rows whose ``first_seen_at`` is later than ``as_of`` are excluded, so
        information that had not been published/observed at that moment can
        never leak into a historical answer.  ``only_latest`` returns the
        revision that was current *at that moment* -- computed per as_of via
        a subquery rather than reading the global present-day ``is_latest``
        flag, which would wrongly hide rows that have since been superseded.
        Without ``as_of`` the full current view is returned.
        """
        where, params = [], []
        if as_of is not None:
            where.append("first_seen_at<=?")
            params.append(str(as_of))
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        sql = ("SELECT revision, conflict_of, content_hash, payload "
               "FROM bulletin_entries" + where_sql
               + " ORDER BY bulletin_date, table_kind, category, revision,"
                 " content_hash")
        rows = self.con.execute(sql, params).fetchall()
        if not only_latest:
            return [json.loads(r[3]) for r in rows]
        # Keep, per logical row, the greatest revision visible at as_of;
        # within a same-instant contradiction (same revision) pick the
        # deterministic content-hash winner and drop the conflict loser.
        latest: dict[str, tuple] = {}
        for revision, conflict_of, _h, payload in rows:
            item = json.loads(payload)
            lid = str(item.get("logical_id", ""))
            current = latest.get(lid)
            if current is None or revision > current[0]:
                latest[lid] = (revision, conflict_of, item)
            elif revision == current[0] and current[1] is not None and conflict_of is None:
                latest[lid] = (revision, conflict_of, item)
        return [v[2] for v in sorted(
            latest.values(),
            key=lambda v: (v[2].get("bulletin_date", ""),
                           v[2].get("table_kind", ""),
                           v[2].get("category", "")))]

    def bulletin_view(self, bulletin_date: str, table_kind: str,
                      as_of: Optional[str] = None) -> dict[str, Any]:
        """Full revision history of one bulletin document at a point in time."""
        params = [str(bulletin_date), str(table_kind)]
        sql = ("SELECT revision, observed_at, first_seen_at, is_latest,"
               " superseded_by, conflict_of, note, payload, logical_id "
               "FROM bulletin_entries WHERE bulletin_date=? AND table_kind=?")
        if as_of is not None:
            sql += " AND first_seen_at<=?"
            params.append(str(as_of))
        sql += " ORDER BY revision, category"
        rows = self.con.execute(sql, params).fetchall()
        # Point-in-time latest revision per logical row, computed from the
        # rows actually visible at as_of rather than the present-day flag.
        latest_visible: dict[str, int] = {}
        for row in rows:
            lid = row[8]
            latest_visible[lid] = max(latest_visible.get(lid, 0), row[0])
        # Within a same-instant contradiction (same logical id + revision),
        # exactly one row is latest: the winner is the one not marked as a
        # conflict loser; ties resolve deterministically by content hash.
        winners: set[tuple[str, int, str]] = set()
        groups: dict[tuple[str, int], list] = {}
        for row in rows:
            groups.setdefault((row[8], row[0]), []).append(row)
        for (lid, rev), grp in groups.items():
            if len(grp) == 1:
                winners.add((lid, rev, grp[0][7]))
            else:
                non_losers = [g for g in grp if g[5] is None]
                chosen = (non_losers or grp)[0]
                winners.add((lid, rev, chosen[7]))
        # The latest revision for a logical row is simply its greatest
        # visible revision; superseded rows of older revisions are not latest.
        greatest_rev: dict[str, int] = {}
        for row in rows:
            lid = row[8]
            greatest_rev[lid] = max(greatest_rev.get(lid, 0), row[0])
        entries = []
        revisions: dict[int, dict[str, Any]] = {}
        for (rev, obs, seen, latest, sup_by, conflict, note, payload,
             logical_id) in rows:
            item = json.loads(payload)
            is_point_latest = (
                rev == greatest_rev.get(logical_id)
                and (logical_id, rev, payload) in winners)
            item.update({"revision": rev,
                         "is_latest": int(is_point_latest),
                         "global_is_latest": latest,
                         "superseded_by": sup_by, "conflict_of": conflict,
                         "note": note, "first_seen_at": seen,
                         "observed_at": obs})
            entries.append(item)
            revisions.setdefault(rev, {"revision": rev, "observed_at": obs,
                                       "first_seen_at": seen,
                                       "category_count": 0,
                                       "complete": True})
            revisions[rev]["category_count"] += 1
        latest_rev = max((e["revision"] for e in entries), default=None)
        return {
            "bulletin_date": str(bulletin_date),
            "table_kind": str(table_kind),
            "as_of": as_of,
            "latest_revision_visible": latest_rev,
            "revisions": [revisions[k] for k in sorted(revisions)],
            "entries": entries,
        }

    def documents(self) -> list[dict[str, Any]]:
        """List every bulletin document ever observed, with progress info."""
        rows = self.con.execute(
            "SELECT bulletin_date, table_kind, "
            "COUNT(DISTINCT revision), MAX(revision), "
            "MIN(first_seen_at), MAX(last_seen_at), "
            "SUM(CASE WHEN is_latest=1 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN conflict_of IS NOT NULL THEN 1 ELSE 0 END) "
            "FROM bulletin_entries "
            "GROUP BY bulletin_date, table_kind "
            "ORDER BY bulletin_date, table_kind").fetchall()
        return [
            {"bulletin_date": bd, "table_kind": kind,
             "revision_count": rc, "latest_revision": latest,
             "first_seen_at": first, "last_seen_at": last,
             "latest_rows": cur or 0, "conflicted_rows": conf or 0}
            for (bd, kind, rc, latest, first, last, cur, conf) in rows
        ]

    # ------------------------------------------------------------------ #
    # Batch ledger.
    # ------------------------------------------------------------------ #
    def open_batch(self, batch_id: str, started_at: str, mode: str,
                   watermark_before: Optional[str]) -> None:
        self.con.execute(
            "INSERT OR IGNORE INTO crawl_batches(batch_id, started_at, mode,"
            " watermark_before, status) VALUES(?,?,?,?,'running')",
            (batch_id, str(started_at), mode, watermark_before))
        self.con.commit()

    def update_batch(self, batch_id: str, **fields: Any) -> None:
        if not fields:
            return
        keys = ", ".join("%s=?" % k for k in fields)
        self.con.execute(
            "UPDATE crawl_batches SET %s WHERE batch_id=?" % keys,
            tuple(fields.values()) + (batch_id,))
        self.con.commit()

    def close_batch(self, batch_id: str, finished_at: str, status: str,
                    watermark_after: Optional[str], note: str = "") -> None:
        self.con.execute(
            "UPDATE crawl_batches SET finished_at=?, status=?,"
            " watermark_after=?, note=? WHERE batch_id=?",
            (str(finished_at), status, watermark_after, note, batch_id))
        self.con.commit()

    def batches(self) -> list[dict[str, Any]]:
        cols = ("batch_id, started_at, finished_at, mode, fetched_pages,"
                " fetched_rows, stored_rows, duplicate_rows, failed_pages,"
                " status, watermark_before, watermark_after, note")
        return [dict(zip(cols.split(", "), row)) for row in self.con.execute(
            "SELECT %s FROM crawl_batches ORDER BY started_at" % cols)]

    # ------------------------------------------------------------------ #
    # Maintenance.
    # ------------------------------------------------------------------ #
    def reset(self):
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        self.con.execute("DELETE FROM bulletin_entries")
        self.con.execute("DELETE FROM bulletin_revisions")
        self.con.execute("DELETE FROM crawl_batches")
        self.con.commit()


def canonical(d: dict[str, Any]) -> str:
    return json.dumps(d, sort_keys=True, ensure_ascii=False, default=str)


def content_hash(d: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


# Fields that identify the *observed content* of a bulletin row.  Everything
# else (revision numbers, ingest timestamps, version chains, batch notes) is
# bookkeeping added locally, so it must not change a row's identity across
# re-crawls -- otherwise the same source row would be stored twice.
STABLE_FIELDS = (
    "source_id", "id", "bulletin_date", "table_kind", "category", "name",
    "target_month", "cutoff", "raw_status", "status", "state",
)


def stable_content_hash(record: dict[str, Any]) -> str:
    """Content identity independent of local bookkeeping fields."""
    material = {k: record.get(k) for k in STABLE_FIELDS if k in record}
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, ensure_ascii=False,
                   default=str).encode()).hexdigest()[:24]
