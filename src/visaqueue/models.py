"""Domain model for visa-bulletin scheduling data.

A bulletin is a published document identified by ``bulletin_date`` and
``table_kind`` (monthly table vs. dates table).  Each bulletin contains rows
for one or more categories; every row carries the cut-off that was visible at
publication time.  Re-publication of the same bulletin produces a new
``revision`` that supersedes the previous one without erasing it.

The model is *bi-temporal*:

* ``bulletin_date`` / ``target_month`` describe when information was published
  and which visa month it applies to (business time).
* ``first_seen_at`` / ``last_seen_at`` describe when the local store learned
  about the content (system time).  Historical queries use this to reconstruct
  exactly what a consumer could have observed at a past moment.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

# Canonical category ordering used for stable presentation and repeat checks.
CATEGORIES = ("EB1", "EB2", "EB3", "OtherWorkers", "EW", "CertainReligious")

# Lifecycle states stored per bulletin entry.
STATE_AVAILABLE = "available"        # a concrete cut-off is published
STATE_CURRENT = "current"            # "Current": no numerical limitation
STATE_UNAVAILABLE = "unavailable"    # "U": category not offered this month
STATE_REVOKED = "revoked"            # later revision removed the category
STATE_PENDING = "pending"            # announced but not yet usable
STATE_UNKNOWN = "unknown"            # unrecognised / unparsable status text

KNOWN_STATES = frozenset({
    STATE_AVAILABLE, STATE_CURRENT, STATE_UNAVAILABLE,
    STATE_REVOKED, STATE_PENDING, STATE_UNKNOWN,
})


@dataclass
class BulletinKey:
    """Identity of a bulletin document (independent of its revision)."""

    bulletin_date: str
    table_kind: str

    def __str__(self) -> str:
        return "%s|%s" % (self.bulletin_date, self.table_kind)


@dataclass
class Entry:
    """One category row inside one bulletin revision."""

    bulletin_date: str
    table_kind: str
    category: str
    target_month: str
    cutoff: Optional[str]
    raw_status: str
    state: str
    revision: int = 1
    first_seen_at: str = ""
    last_seen_at: str = ""
    superseded_by: Optional[int] = None
    conflict_of: Optional[int] = None
    note: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.bulletin_date, self.table_kind, self.category)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bulletin_date": self.bulletin_date,
            "table_kind": self.table_kind,
            "category": self.category,
            "target_month": self.target_month,
            "cutoff": self.cutoff,
            "raw_status": self.raw_status,
            "state": self.state,
            "revision": self.revision,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "superseded_by": self.superseded_by,
            "conflict_of": self.conflict_of,
            "note": self.note,
            "extra": dict(self.extra),
        }


@dataclass
class Bulletin:
    """A single revision of a bulletin document as observed in one crawl."""

    bulletin_date: str
    table_kind: str
    revision: int
    observed_at: str
    entries: list[Entry] = field(default_factory=list)
    complete: bool = True
    source_ref: str = ""
    note: str = ""

    @property
    def key(self) -> BulletinKey:
        return BulletinKey(self.bulletin_date, self.table_kind)


@dataclass
class CrawlBatch:
    """Bookkeeping for one fetch run; never overwritten afterwards."""

    batch_id: str
    started_at: str
    finished_at: str = ""
    mode: str = "full"
    fetched_pages: int = 0
    fetched_rows: int = 0
    stored_rows: int = 0
    duplicate_rows: int = 0
    failed_pages: str = ""
    status: str = "running"          # running | complete | partial | failed
    watermark_before: str = ""
    watermark_after: str = ""
    note: str = ""
