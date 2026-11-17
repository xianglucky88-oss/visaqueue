
from collections import defaultdict
def process(record):
    return {"logical_id": str(record.get("name", "")).lower()}
def reduce(records):
    by = defaultdict(list)
    for r in records:
        by[r["logical_id"]].append(r)
    for rows in by.values():
        rows.sort(key=lambda x: str(x.get("ts", "")))
        prev = None
        for i, r in enumerate(rows):
            r["version_no"] = i + 1
            r["prev_version_id"] = prev
            r["is_latest"] = int(i == len(rows) - 1)
            prev = r.get("id")
    return records
