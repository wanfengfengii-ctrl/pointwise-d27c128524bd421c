"""One origin, one connection, bounded application body slots."""
import asyncio
import hashlib
from portal import Portal
from transport import Transport

async def collect(pack, mode="recorded", limit=None):
    trace, records, finished = [], [], []
    transport = Transport(Portal(pack), trace)
    pending = list(pack["requests"])
    active = []
    quiet = 0
    for tick in range(pack["tick_limit"]):
        progress = False
        while pending and len(active) < pack["slots"]:
            request = pending.pop(0)
            body = transport.open(request["id"], tick)
            active.append({"request": request, "body": body, "bytes": 0,
                           "hash": hashlib.sha256()})
            progress = True
        progress = transport.pump(pack["burst"], tick) or progress
        for work in list(active):
            body, request = work["body"], work["request"]
            data = body.pull(tick)
            if data is not None:
                work["bytes"] += len(data)
                work["hash"].update(data)
                progress = True
            threshold = request["limit"] if limit is None else limit
            short = request["policy"] == "preview" and work["bytes"] >= threshold
            if short and mode == "drain":
                short = False
            if short or (body.ended and not body.queue):
                status = "preview" if request["policy"] == "preview" else "complete"
                record = {"id": request["id"], "status": status, "bytes": work["bytes"],
                          "sha256": work["hash"].hexdigest()}
                records.append(record)
                finished.append(request["id"])
                trace.append({"tick": tick, "event": "finish", **record})
                body.close(tick)
                active.remove(work)
                progress = True
        if not pending and not active:
            return {"status": "complete", "records": records, "waiting": [], "trace": trace}
        quiet = 0 if progress else quiet + 1
        if quiet >= 2:
            break
        await asyncio.sleep(0)
    waiting = [{"id": w["request"]["id"], "state": "body", "bytes": w["bytes"],
                "queued_frames": len(w["body"].queue), "ended": w["body"].ended}
               for w in active]
    waiting += [{"id": r["id"], "state": "slot", "bytes": 0} for r in pending]
    return {"status": "stalled", "records": records, "waiting": waiting, "trace": trace}
