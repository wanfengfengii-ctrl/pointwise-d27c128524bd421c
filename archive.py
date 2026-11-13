"""Read or validate the sealed incident transcript, without interpreting it."""
import base64
import hashlib
import json
import sys
import zlib
from pathlib import Path
from replay import assert_normal, run

def archived():
    return zlib.decompress(base64.b64decode(Path("data/incident.log.z64").read_text())).decode()

def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "show"
    if action == "show":
        print(archived(), end="")
        return
    result = run("data/incident.json")
    if action == "verify":
        assert_normal()
        raw = json.dumps(result, sort_keys=True) + "\n"
        if result["status"] != "complete":
            raw += "H2_COLLECTION_STALLED\n"
        assert raw == archived(), "ARCHIVE_REPLAY_DIFFERENCE"
        receipts = json.loads(Path("data/reproductions.json").read_text())
        assert len(receipts) == 2
        for receipt in receipts:
            assert receipt["exit_code"] == 1
            assert receipt["marker"] == "H2_COLLECTION_STALLED"
            assert receipt["stdout_sha256"] == hashlib.sha256(raw.encode()).hexdigest()
        print("ARCHIVE_REPLAY_OK")
    elif action != "demo":
        raise SystemExit("expected show, verify or demo")
    print(json.dumps({"status": result["status"], "records": result["records"],
                      "waiting": result["waiting"], "event_count": len(result["trace"])}, sort_keys=True))

if __name__ == "__main__":
    main()
