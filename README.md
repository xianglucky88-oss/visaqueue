# visaqueue

An offline visa-bulletin scheduling service with a local HTTP fixture and
SQLite storage. It is self-contained and does not require network access.

## What it models

Each visa bulletin contains two distinct table kinds, kept separate
throughout the pipeline:

- `monthly` — the month-based final-action-date table (cut-offs such as
  `01JAN26`);
- `dates` — the dates-for-filing table (ISO calendar cut-offs such as
  `2026-01-15`).

The store is **append-only and bi-temporal**:

- business time (`bulletin_date`, `target_month`, source observation `ts`)
  describes when information was published;
- system time (`first_seen_at` / `last_seen_at`) describes when the local
  store learned it.

Historical queries take an `as_of` timestamp and return exactly what was
visible then. Rows first seen later — including bulletin revisions that did
not exist yet — can never leak into an earlier answer.

### Traceability rules

- **Table-kind differences** — monthly and dates rows are stored and queried
  independently even when published together.
- **Bulletin revisions** — re-publication opens a new document revision; the
  older revision is marked `superseded_by` but stays readable, so the
  before/after cut-off difference is always explainable.
- **Availability states** — raw status text maps to canonical states
  (`available`, `current`, `unavailable`, `revoked`, `pending`, `unknown`)
  each with a human-readable explanation; unrecognised values are preserved,
  not dropped.
- **Same-instant contradictions** — two different contents observed at the
  same instant are both retained; one deterministic winner is marked latest
  and the loser is annotated via `conflict_of`.
- **No visibility regression** — crawls only add rows. Partial page failures
  stop the crawl, record the failed page in the batch ledger and freeze the
  watermark; a later continuation fills the gap without deleting anything.
- **Cross-month repeats** — the kernel judges whether a category repeats the
  previous visa month's date-bearing cut-off (per table kind).

The kernel owns category interpretation, cross-month repeat judgement and
bulletin progress; the framework owns transport, parsing, storage and
sessions.

## Usage

```text
make run       # full crawl of the bundled source
make run_inc   # incremental continuation (watermark filtered)
make test      # run the test suite
make mock      # start the local fixture service
```

Additional read-only commands:

```text
python -m visaqueue.cli history                     # current visible snapshot
python -m visaqueue.cli history --as-of 2026-03-01T00:00:00Z
python -m visaqueue.cli history document 2026-02-15 monthly
python -m visaqueue.cli progress                    # documents + crawl ledger
```

Re-running any command is safe for an existing database: identical content
collapses on a stable content hash (independent of local bookkeeping
fields), revision numbers stay aligned across interrupted/continuation
runs, and every crawl is recorded in the `crawl_batches` ledger with its
status, failed pages and watermark movement.

## Storage layout

- `bulletin_entries` — one row per distinct observed bulletin entry, with
  revision, lifecycle state, supersession and conflict markers;
- `bulletin_revisions` — document-level revision progress;
- `crawl_batches` — append-only ledger of every run;
- `records` / `meta` — legacy tables retained for the original record API.
