
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "visaqueue.db"

SPEC = SimpleNamespace(
    slug="visaqueue",
    fmt="json",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "category"],
    page_size=4,
    int_fields=["value"],
    ts_field="ts",
)
