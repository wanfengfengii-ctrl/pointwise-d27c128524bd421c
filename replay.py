"""Business regression, incident assertion and raw replay entry points."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from collector import collect

def run(path, mode="recorded", limit=None):
    return asyncio.run(collect(json.loads(Path(path).read_text()), mode, limit))

def assert_normal():
    result = run("data/normal.json")
    expected = [("notice-01", 4096, "A"), ("notice-02", 98304, "B")]
    assert result["status"] == "complete", result["waiting"]
    for record, (task, size, fill) in zip(result["records"], expected):
        assert record["id"] == task and record["bytes"] == size
        assert record["sha256"] == hashlib.sha256(fill.encode() * size).hexdigest()
    assert len(result["records"]) == len(expected)
    print("NORMAL_REGRESSION_OK")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["normal", "incident", "replay"])
    parser.add_argument("pack", nargs="?", default="data/incident.json")
    parser.add_argument("--mode", choices=["recorded", "drain"], default="recorded")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.action == "normal":
        assert_normal()
        return
    result = run(args.pack, args.mode, args.limit)
    print(json.dumps(result, sort_keys=True))
    if args.action == "incident" and result["status"] != "complete":
        print("H2_COLLECTION_STALLED")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
