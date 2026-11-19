
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "visaqueue.db"

# Two table kinds coexist in every visa bulletin:
#   monthly -> the month-based cut-off table (final action dates by month)
#   dates   -> the dates-for-filing table (specific calendar cut-off dates)
TABLE_KINDS = ("monthly", "dates")

SPEC = SimpleNamespace(
    slug="visaqueue",
    fmt="json",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "category",
             "bulletin_date", "table_kind", "target_month", "cutoff"],
    page_size=8,
    int_fields=["value"],
    ts_field="ts",
    table_kinds=TABLE_KINDS,
)
