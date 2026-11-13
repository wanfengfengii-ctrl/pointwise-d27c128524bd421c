"""Rebuild the HTTP/2 credit ledger from the recorded frames of one connection.

The ledger is reconstructed only from the trace's own frame records (nine-byte
headers and control-frame payloads) plus the application records (open, pull,
close, finish).  The h2 ``data`` events are used solely as a cross-check, never
as the source of the ledger.  No h2 private window field is read; the window
emission timing is reproduced by feeding the recorded pull sizes through a
local mirror of the locked h2/windows.py WindowManager (4.3.0) rules.

Usage:
  python3 analysis.py verify PACK   re-run the original entry, rebuild the
                                    ledger, run the drain / limit-16384
                                    counterparts and the normal regression,
                                    and diff everything against evidence.json
  python3 analysis.py demo PACK     show per-task progress and the key
                                    blocking evidence
  python3 analysis.py record PACK   regenerate evidence.json from current
                                    behavior (used to seal the facts)
"""
import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from collector import collect

ROOT = Path(__file__).resolve().parent
EVIDENCE_PATH = ROOT / "evidence.json"
NORMAL_PACK = ROOT / "data" / "normal.json"
REPRODUCTIONS_PATH = ROOT / "data" / "reproductions.json"

DEFAULT_WINDOW = 65535  # RFC 7540 connection-level initial window

FRAME_DATA = 0x0
FRAME_RST_STREAM = 0x3
FRAME_SETTINGS = 0x4
FRAME_WINDOW_UPDATE = 0x8

FLAG_END_STREAM = 0x1
FLAG_PADDED = 0x8

SETTINGS_INITIAL_WINDOW_SIZE = 0x4


def parse_frame_header(header_hex):
    raw = bytes.fromhex(header_hex)
    assert len(raw) == 9
    return {
        "length": int.from_bytes(raw[0:3], "big"),
        "type": raw[3],
        "flags": raw[4],
        "stream": int.from_bytes(raw[5:9], "big") & 0x7FFFFFFF,
    }


def parse_settings(payload_hex):
    payload = bytes.fromhex(payload_hex)
    return {int.from_bytes(payload[i:i + 2], "big"): int.from_bytes(payload[i + 2:i + 6], "big")
            for i in range(0, len(payload), 6)}


class WindowManagerModel:
    """Line-by-line mirror of the locked h2/windows.py WindowManager (4.3.0).

    Fed only with values visible in the trace (received frame lengths and
    recorded pull sizes) so the emission timing of WINDOW_UPDATE frames can
    be reconstructed without reading any h2 private field.
    """

    def __init__(self, max_window_size):
        self.max_window_size = max_window_size
        self.current_window_size = max_window_size
        self.bytes_processed = 0

    def window_consumed(self, size):
        # h2/windows.py:36-50
        self.current_window_size -= size
        assert self.current_window_size >= 0, "flow control window below 0"

    def process_bytes(self, size):
        # h2/windows.py:75-90 and 92-133 (_maybe_update_window)
        self.bytes_processed += size
        increment = 0
        if self.bytes_processed:
            max_increment = self.max_window_size - self.current_window_size
            if ((self.current_window_size == 0
                 and self.bytes_processed > min(1024, self.max_window_size // 4))
                    or self.bytes_processed >= self.max_window_size // 2):
                increment = min(self.bytes_processed, max_increment)
                self.bytes_processed = 0
            self.current_window_size += increment
        return increment


def run_pack(pack_path, mode="recorded", limit=None):
    import asyncio
    pack = json.loads(Path(pack_path).read_text())
    return pack, asyncio.run(collect(pack, mode, limit))


def build_ledger(result, pack):
    """Rebuild connection-level and stream-level credit from the trace frames."""
    trace = result["trace"]
    requests = {r["id"]: r for r in pack["requests"]}

    # Pre-scan: the client's first SETTINGS frame carries INITIAL_WINDOW_SIZE,
    # the initial per-stream inbound window for every stream on this connection.
    initial_stream_window = DEFAULT_WINDOW
    client_settings = None
    server_settings = None
    for event in trace:
        if event.get("event") == "frame":
            head = parse_frame_header(event["header"])
            if head["type"] == FRAME_SETTINGS and head["flags"] == 0:
                settings = parse_settings(event["payload"])
                if event["direction"] == "client" and client_settings is None:
                    client_settings = settings
                    initial_stream_window = settings.get(SETTINGS_INITIAL_WINDOW_SIZE,
                                                         DEFAULT_WINDOW)
                elif event["direction"] == "server" and server_settings is None:
                    server_settings = settings

    conn = {"initial": DEFAULT_WINDOW, "consumed": 0, "returned": 0,
            "window": DEFAULT_WINDOW}
    conn_mgr = WindowManagerModel(DEFAULT_WINDOW)

    streams = {}

    def stream_state(sid):
        if sid not in streams:
            streams[sid] = {
                "task": None, "window": initial_stream_window,
                "recv_flow": 0, "recv_body": 0, "padding": 0, "frames": 0,
                "pulled": 0, "dropped_body": 0, "dropped_flow": 0,
                "returned": 0, "closed_tick": None, "reset_tick": None,
                "app_closed_tick": None, "end_stream_tick": None,
                "frame_queue": [], "manager": WindowManagerModel(initial_stream_window),
            }
        return streams[sid]

    rows = []
    data_frames = []   # independent frame account
    data_events = []   # h2 events, cross-check only
    emulated_conn_wu = []    # (tick, increment) generated by the model
    actual_conn_wu = []      # (tick, increment) observed on the wire
    emulated_stream_wu = []  # (stream, tick, increment)
    actual_stream_wu = []
    unsent_conn_wu = []      # generated but not yet observed on the wire
    open_task_of = {}

    for index, event in enumerate(trace):
        tick = event["tick"]
        kind = event["event"]
        row = {"index": index, "tick": tick, "event": kind}
        if kind == "open":
            state = stream_state(event["stream"])
            state["task"] = event["task"]
            open_task_of[event["stream"]] = event["task"]
            row["stream"] = event["stream"]
            row["task"] = event["task"]
        elif kind == "frame":
            head = parse_frame_header(event["header"])
            row.update({"direction": event["direction"], "type": head["type"],
                        "stream": head["stream"], "length": head["length"]})
            if head["type"] == FRAME_DATA and event["direction"] == "server":
                padded = bool(head["flags"] & FLAG_PADDED)
                pad = event["pad_length"] if padded else 0
                overhead = 1 + pad if padded else 0
                flow = head["length"]
                body = flow - overhead
                end_stream = bool(head["flags"] & FLAG_END_STREAM)
                sid = head["stream"]
                state = stream_state(sid)
                frame_info = {"index": index, "tick": tick, "body": body,
                              "flow": flow, "end_stream": end_stream}
                data_frames.append({"stream": sid, **frame_info})
                state["frame_queue"].append(frame_info)
                state["recv_flow"] += flow
                state["recv_body"] += body
                state["padding"] += overhead
                state["frames"] += 1
                state["window"] -= flow
                state["manager"].window_consumed(flow)
                conn["consumed"] += flow
                conn["window"] -= flow
                conn_mgr.window_consumed(flow)
                if end_stream:
                    state["end_stream_tick"] = tick
                    state["closed_tick"] = tick
                row.update({"body": body, "flow": flow, "pad": pad,
                            "end_stream": end_stream})
                if state["app_closed_tick"] is not None:
                    # DATA for a stream the application already closed: h2
                    # returns the connection credit itself
                    # (h2/connection.py:1696-1728).
                    inc = conn_mgr.process_bytes(flow)
                    row["auto_ack_on_closed_stream"] = flow
                    if inc:
                        emulated_conn_wu.append((tick, inc))
                        unsent_conn_wu.append(inc)
            elif head["type"] == FRAME_WINDOW_UPDATE and event["direction"] == "client":
                increment = int.from_bytes(bytes.fromhex(event["payload"]), "big") & 0x7FFFFFFF
                sid = head["stream"]
                row["increment"] = increment
                if sid == 0:
                    conn["returned"] += increment
                    conn["window"] += increment
                    actual_conn_wu.append((tick, increment))
                    if unsent_conn_wu and unsent_conn_wu[0] == increment:
                        unsent_conn_wu.pop(0)
                else:
                    state = stream_state(sid)
                    state["window"] += increment
                    state["returned"] += increment
                    actual_stream_wu.append((sid, tick, increment))
            elif head["type"] == FRAME_RST_STREAM and event["direction"] == "client":
                sid = head["stream"]
                state = stream_state(sid)
                state["reset_tick"] = tick
                state["closed_tick"] = tick
        elif kind == "pull":
            sid = event["stream"]
            size = event["bytes"]
            state = stream_state(sid)
            state["pulled"] += size
            row.update({"stream": sid, "bytes": size})
            # Match the pull against the oldest unconsumed DATA frame of the
            # stream: Body.pull pops one queued frame per call (transport.py:32-39).
            assert state["frame_queue"], f"pull without frame at trace #{index}"
            frame_info = state["frame_queue"].pop(0)
            assert frame_info["body"] == size, (
                f"pull size {size} != frame body {frame_info['body']} at #{index}")
            row["frame_flow"] = frame_info["flow"]
            row["stranded_padding"] = frame_info["flow"] - frame_info["body"]
            # transport.py:36 acknowledges len(data) only, never the padding.
            inc = conn_mgr.process_bytes(size)
            if inc:
                emulated_conn_wu.append((tick, inc))
                unsent_conn_wu.append(inc)
            if state["closed_tick"] is None:
                # h2/connection.py:1384-1388: stream-level WINDOW_UPDATE only
                # while the stream is still open.
                sinc = state["manager"].process_bytes(size)
                if sinc:
                    emulated_stream_wu.append((sid, tick, sinc))
        elif kind == "close":
            sid = event["stream"]
            state = stream_state(sid)
            state["app_closed_tick"] = tick
            dropped_flow = sum(f["flow"] for f in state["frame_queue"])
            state["dropped_body"] = event["queued_bytes"]
            state["dropped_flow"] = dropped_flow
            # transport.py:47 clears the queue without acknowledge_received_data.
            assert sum(f["body"] for f in state["frame_queue"]) == event["queued_bytes"], (
                f"close queued_bytes mismatch on stream {sid}")
            state["frame_queue"] = []
            row.update({"stream": sid, "queued_bytes": event["queued_bytes"],
                        "dropped_flow": dropped_flow, "ended": event["ended"]})
        elif kind == "data":
            data_events.append({"stream": event["stream"], "bytes": event["bytes"],
                                "flow": event["flow_length"], "tick": tick})
            row.update({"stream": event["stream"], "bytes": event["bytes"],
                        "flow": event["flow_length"]})
        elif kind == "finish":
            row.update({"task": event["id"], "bytes": event["bytes"],
                        "status": event["status"]})
        row["conn_window"] = conn["window"]
        row["conn_consumed"] = conn["consumed"]
        row["conn_returned"] = conn["returned"]
        row["conn_processed_pending"] = conn_mgr.bytes_processed
        rows.append(row)

    # Cross-check: the independent DATA frame account must equal the h2 events.
    frame_check = [(f["stream"], f["body"], f["flow"]) for f in data_frames]
    event_check = [(d["stream"], d["bytes"], d["flow"]) for d in data_events]
    data_crosscheck_ok = frame_check == event_check

    # The model fed with recorded pulls must reproduce every WINDOW_UPDATE.
    wu_conn_model_ok = [i for _, i in emulated_conn_wu] == [i for _, i in actual_conn_wu]
    wu_stream_model_ok = ([(s, i) for s, _, i in emulated_stream_wu]
                          == [(s, i) for s, _, i in actual_stream_wu])

    pulled_total = sum(s["pulled"] for s in streams.values())
    dropped_body_total = sum(s["dropped_body"] for s in streams.values())
    padding_total = sum(s["padding"] for s in streams.values())
    buffered_end = sum(s["recv_body"] - s["pulled"] - s["dropped_body"]
                       for s in streams.values())
    lost_total = conn["consumed"] - pulled_total - buffered_end

    # End-of-tick states for the "cannot advance" analysis.
    tick_states = {}
    for row in rows:
        tick_states[row["tick"]] = row
    last_tick_with_events = max(tick_states) if tick_states else -1

    # Reconstruct per-tick-end: window, pending processed credit, unsent
    # WINDOW_UPDATEs and buffered body bytes.
    buffered_by_tick = {}
    recv_body_run = 0
    pulled_run = 0
    dropped_run = 0
    unsent_run = []
    per_tick = {}
    for row in rows:
        tick = row["tick"]
        if row["event"] == "frame" and row.get("type") == FRAME_DATA \
                and row.get("direction") == "server":
            recv_body_run += row["body"]
        elif row["event"] == "pull":
            pulled_run += row["bytes"]
        elif row["event"] == "close":
            dropped_run += row["queued_bytes"]
        state = per_tick.setdefault(tick, {})
        state["conn_window"] = row["conn_window"]
        state["pending"] = row["conn_processed_pending"]
        state["buffered"] = recv_body_run - pulled_run - dropped_run
    # Unsent connection WINDOW_UPDATEs at each tick end: increments the model
    # generated at tick t are written to the wire at tick t+1.
    generated_at = {}
    for tick, inc in emulated_conn_wu:
        generated_at.setdefault(tick, 0)
        generated_at[tick] += inc
    for tick, state in per_tick.items():
        state["unsent_wu"] = generated_at.get(tick, 0)

    emission_floor = min(1024, DEFAULT_WINDOW // 4)
    first_unadvanceable = None
    for tick in sorted(per_tick):
        state = per_tick[tick]
        if (state["conn_window"] == 0
                and state["pending"] <= emission_floor
                and state["unsent_wu"] == 0
                and state["buffered"] == 0
                and (result["waiting"] or tick < last_tick_with_events)):
            first_unadvanceable = {"tick": tick, **state}
            break

    first_zero_window = None
    for row in rows:
        if row["conn_window"] == 0:
            first_zero_window = {"index": row["index"], "tick": row["tick"]}
            break

    # First operation after which credit has no return path.
    first_unreturnable = None
    first_drop = None
    for row in rows:
        if first_unreturnable is None and row["event"] == "pull" \
                and row.get("stranded_padding"):
            first_unreturnable = {
                "index": row["index"], "tick": row["tick"], "event": "pull",
                "stream": row["stream"],
                "task": open_task_of.get(row["stream"]),
                "acknowledged": row["bytes"], "frame_flow": row["frame_flow"],
                "stranded": row["stranded_padding"],
                "cause": "transport.py:36 acknowledges len(data) only; the "
                         "padding of this DATA frame can never be returned"}
        if first_drop is None and row["event"] == "close" and row.get("dropped_flow"):
            first_drop = {
                "index": row["index"], "tick": row["tick"], "event": "close",
                "stream": row["stream"],
                "task": open_task_of.get(row["stream"]),
                "dropped_body": row["queued_bytes"],
                "dropped_flow": row["dropped_flow"],
                "cause": "transport.py:47 clears the queue without "
                         "acknowledge_received_data; collector.py:40 closes "
                         "the body right after the preview threshold"}

    min_window = min((r["conn_window"] for r in rows), default=DEFAULT_WINDOW)
    min_window_tick = next((r["tick"] for r in rows if r["conn_window"] == min_window),
                           None)

    stream_summary = {}
    for sid, state in sorted(streams.items()):
        stream_summary[str(sid)] = {
            "task": state["task"],
            "recv_flow": state["recv_flow"],
            "recv_body": state["recv_body"],
            "padding": state["padding"],
            "frames": state["frames"],
            "pulled": state["pulled"],
            "dropped_body": state["dropped_body"],
            "dropped_flow": state["dropped_flow"],
            "returned_window_updates": state["returned"],
            "window_end": state["window"],
            "end_stream_tick": state["end_stream_tick"],
            "reset_tick": state["reset_tick"],
            "app_closed_tick": state["app_closed_tick"],
        }

    return {
        "settings": {"client": ({str(k): v for k, v in client_settings.items()}
                                if client_settings else None),
                     "server": ({str(k): v for k, v in server_settings.items()}
                                if server_settings else None),
                     "initial_stream_window": initial_stream_window},
        "rows": rows,
        "connection": {
            "initial": conn["initial"],
            "consumed": conn["consumed"],
            "returned": conn["returned"],
            "window_end": conn["window"],
            "min_window": min_window,
            "min_window_tick": min_window_tick,
            "processed_total": pulled_total,
            "processed_pending_end": conn_mgr.bytes_processed,
            "buffered_end": buffered_end,
            "lost_total": lost_total,
            "lost_padding": padding_total,
            "lost_dropped_body": dropped_body_total,
            "emission_floor": emission_floor,
            "half_window_threshold": DEFAULT_WINDOW // 2,
        },
        "streams": stream_summary,
        "checks": {
            "data_crosscheck_ok": data_crosscheck_ok,
            "wu_conn_model_ok": wu_conn_model_ok,
            "wu_stream_model_ok": wu_stream_model_ok,
            "lost_identity_ok": lost_total == padding_total + dropped_body_total,
            "window_identity_ok": (conn["initial"] + conn["returned"] - conn["consumed"]
                                   == conn["window"]),
        },
        "firsts": {
            "first_unreturnable": first_unreturnable,
            "first_drop": first_drop,
            "first_zero_window": first_zero_window,
            "first_unadvanceable": first_unadvanceable,
            "last_tick_with_events": last_tick_with_events,
            "stall_declared_tick": last_tick_with_events + 2 if result["waiting"] else None,
        },
        "per_tick": {str(t): s for t, s in sorted(per_tick.items())},
        "emulated_conn_wu": [[t, i] for t, i in emulated_conn_wu],
        "actual_conn_wu": [[t, i] for t, i in actual_conn_wu],
    }


def classify_tasks(pack, result, ledger, limit=None):
    """Per-task business classification with direct / collateral blocking."""
    records = {r["id"]: r for r in result["records"]}
    waiting = {w["id"]: w for w in result["waiting"]}
    streams_by_task = {s["task"]: (sid, s) for sid, s in ledger["streams"].items()
                       if s["task"]}
    # Server send order at the stall: streams opened earlier are served first
    # (portal.py:23 iterates the live dict in insertion order).
    live_order = [sid for sid, s in sorted(
        ((sid, s) for sid, s in ledger["streams"].items()
         if s["task"] in waiting and waiting[s["task"]]["state"] == "body"),
        key=lambda item: int(item[0]))]
    rows = []
    for request in pack["requests"]:
        rid = request["id"]
        threshold = request["limit"] if limit is None else limit
        target = request["size"] if request["policy"] == "full" else threshold
        sid, stream = streams_by_task.get(rid, (None, None))
        recv_body = stream["recv_body"] if stream else 0
        dropped = stream["dropped_body"] if stream else 0
        row = {
            "id": rid,
            "policy": request["policy"],
            "target": target,
            "size": request["size"],
            "stream": sid,
            "received": recv_body,
            "consumed": records[rid]["bytes"] if rid in records else 0,
            "local_dropped": dropped,
            "remote_unsent": request["size"] - recv_body,
            "app_incomplete": None,
            "classification": None,
            "blocking": None,
        }
        if rid in records:
            record = records[rid]
            if record["status"] == "complete":
                row["classification"] = "complete"
                row["app_incomplete"] = 0
            else:
                row["classification"] = "preview"
                row["app_incomplete"] = max(0, target - record["bytes"])
        elif rid in waiting:
            state = waiting[rid]
            row["consumed"] = state["bytes"]
            row["app_incomplete"] = target - state["bytes"]
            if state["state"] == "slot":
                row["classification"] = "waiting_slot"
                holders = [w["id"] for w in result["waiting"] if w["state"] == "body"]
                row["blocking"] = ("collateral: no free slot (collector.py:15); "
                                   "slots held by " + ", ".join(holders))
            else:
                row["classification"] = "body_incomplete"
                direct = live_order and live_order[0] == sid
                conn_window = ledger["connection"]["window_end"]
                stream_window = stream["window_end"]
                if direct:
                    row["blocking"] = (
                        f"direct: connection window {conn_window} while its own "
                        f"stream window is {stream_window}; first in the server "
                        f"send order (portal.py:23)")
                else:
                    row["blocking"] = (
                        f"collateral: connection window {conn_window} and behind "
                        f"stream {live_order[0] if live_order else '?'} in the "
                        f"server send order (portal.py:23-25)")
        rows.append(row)
    return rows


def incident_stdout(result):
    raw = json.dumps(result, sort_keys=True) + "\n"
    if result["status"] != "complete":
        raw += "H2_COLLECTION_STALLED\n"
    return raw


def run_incident_entry_twice():
    """The original incident entry must fail twice in a row with the marker."""
    failures = []
    for _ in range(2):
        proc = subprocess.run(
            [sys.executable, "replay.py", "incident"],
            cwd=ROOT, capture_output=True, text=True)
        failures.append({
            "argv": ["python3", "replay.py", "incident"],
            "exit_code": proc.returncode,
            "marker": "H2_COLLECTION_STALLED" if "H2_COLLECTION_STALLED" in proc.stdout else None,
            "stdout": proc.stdout,
            "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
        })
    return failures


def run_normal_regression():
    proc = subprocess.run([sys.executable, "replay.py", "normal"],
                          cwd=ROOT, capture_output=True, text=True)
    return {"exit_code": proc.returncode, "stdout": proc.stdout,
            "ok": proc.returncode == 0 and "NORMAL_REGRESSION_OK" in proc.stdout}


def gather_evidence(pack_path):
    pack, incident = run_pack(pack_path)
    _, drain = run_pack(pack_path, mode="drain")
    _, limit_run = run_pack(pack_path, limit=16384)
    runs = {}
    for name, result, mode, limit in (
            ("incident", incident, "recorded", None),
            ("drain", drain, "drain", None),
            ("limit16384", limit_run, "recorded", 16384)):
        ledger = build_ledger(result, pack)
        runs[name] = {
            "mode": mode,
            "limit": limit,
            "result": result,
            "ledger": ledger,
            "tasks": classify_tasks(pack, result, ledger, limit),
        }
    failures = run_incident_entry_twice()
    normal = run_normal_regression()
    _, normal_result = run_pack(NORMAL_PACK)
    report_facts = {
        "task_classification": {t["id"]: t["classification"]
                                for t in runs["incident"]["tasks"]},
        "first_unreturnable": runs["incident"]["ledger"]["firsts"]["first_unreturnable"],
        "first_drop": runs["incident"]["ledger"]["firsts"]["first_drop"],
        "first_zero_window": runs["incident"]["ledger"]["firsts"]["first_zero_window"],
        "first_unadvanceable": runs["incident"]["ledger"]["firsts"]["first_unadvanceable"],
        "stall_declared_tick": runs["incident"]["ledger"]["firsts"]["stall_declared_tick"],
        "connection": runs["incident"]["ledger"]["connection"],
        "streams": runs["incident"]["ledger"]["streams"],
        "counterpart_outcomes": {
            "drain": {"status": drain["status"],
                      "lost_total": runs["drain"]["ledger"]["connection"]["lost_total"],
                      "lost_padding": runs["drain"]["ledger"]["connection"]["lost_padding"],
                      "lost_dropped_body": runs["drain"]["ledger"]["connection"]["lost_dropped_body"],
                      "min_window": runs["drain"]["ledger"]["connection"]["min_window"]},
            "limit16384": {"status": limit_run["status"],
                           "lost_total": runs["limit16384"]["ledger"]["connection"]["lost_total"],
                           "lost_padding": runs["limit16384"]["ledger"]["connection"]["lost_padding"],
                           "lost_dropped_body": runs["limit16384"]["ledger"]["connection"]["lost_dropped_body"],
                           "min_window": runs["limit16384"]["ledger"]["connection"]["min_window"]},
        },
    }
    return {
        "pack": str(pack_path),
        "pack_sha256": hashlib.sha256(Path(pack_path).read_bytes()).hexdigest(),
        "runs": runs,
        "failures": failures,
        "normal_regression": {**normal, "result_status": normal_result["status"],
                              "records": normal_result["records"]},
        "report_facts": report_facts,
    }


def diff_values(path, expected, actual, diffs):
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in expected:
            if key not in actual:
                diffs.append(f"{path}.{key}: missing in actual (expected {expected[key]!r})")
            else:
                diff_values(f"{path}.{key}", expected[key], actual[key], diffs)
        for key in actual:
            if key not in expected:
                diffs.append(f"{path}.{key}: unexpected in actual ({actual[key]!r})")
    elif isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            diffs.append(f"{path}: length {len(actual)} != expected {len(expected)}")
        for i, (e, a) in enumerate(zip(expected, actual)):
            diff_values(f"{path}[{i}]", e, a, diffs)
    elif expected != actual:
        diffs.append(f"{path}: actual {actual!r} != expected {expected!r}")


def verify(pack_path):
    evidence = json.loads(EVIDENCE_PATH.read_text())
    fresh = gather_evidence(pack_path)
    checks = []

    def check(name, ok, detail=""):
        checks.append((name, ok, detail))

    # 1. Full recompute vs sealed evidence (raw results, ledgers, tasks, facts).
    diffs = []
    for section in ("runs", "report_facts"):
        diff_values(section, evidence[section], fresh[section], diffs)
    check("recomputed runs and report facts match evidence.json", not diffs,
          "; ".join(diffs[:20]))

    # 2. Ledger internal identities and cross-checks, all three runs.
    ledger_problems = []
    for name, run in fresh["runs"].items():
        for key, ok in run["ledger"]["checks"].items():
            if not ok:
                ledger_problems.append(f"{name}.{key}")
    check("frame ledger identities and h2 cross-checks hold", not ledger_problems,
          ", ".join(ledger_problems))

    # 3. The original incident entry must fail twice with the marker.
    failure_problems = []
    receipts = json.loads(REPRODUCTIONS_PATH.read_text())
    for i, failure in enumerate(fresh["failures"]):
        if failure["exit_code"] != 1:
            failure_problems.append(f"run {i}: exit_code {failure['exit_code']} != 1")
        if failure["marker"] != "H2_COLLECTION_STALLED":
            failure_problems.append(f"run {i}: marker {failure['marker']!r}")
        expected_sha = receipts[i]["stdout_sha256"] if i < len(receipts) else None
        if failure["stdout_sha256"] != expected_sha:
            failure_problems.append(
                f"run {i}: stdout_sha256 {failure['stdout_sha256']} != {expected_sha}")
        sealed = evidence["failures"][i]
        if failure["stdout"] != sealed["stdout"]:
            failure_problems.append(f"run {i}: stdout differs from evidence.json")
    check("original incident entry fails twice with H2_COLLECTION_STALLED",
          not failure_problems, "; ".join(failure_problems))

    # 4. Normal business regression must still pass.
    check("normal regression passes", fresh["normal_regression"]["ok"],
          fresh["normal_regression"]["stdout"].strip())

    # 5. Counterpart outcomes: both control runs must complete.
    outcomes = fresh["report_facts"]["counterpart_outcomes"]
    check("drain counterpart completes", outcomes["drain"]["status"] == "complete",
          f"status={outcomes['drain']['status']}")
    check("limit=16384 counterpart completes",
          outcomes["limit16384"]["status"] == "complete",
          f"status={outcomes['limit16384']['status']}")

    width = max(len(name) for name, _, _ in checks)
    failed = 0
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name.ljust(width)}"
              + ("" if ok or not detail else f"\n      actual difference: {detail}"))
        failed += 0 if ok else 1
    if failed:
        print(f"ANALYSIS_VERIFY_FAILED ({failed} check(s))")
        return 1
    print("ANALYSIS_VERIFY_OK")
    return 0


def demo(pack_path):
    pack, result = run_pack(pack_path)
    ledger = build_ledger(result, pack)
    tasks = classify_tasks(pack, result, ledger)
    conn = ledger["connection"]
    firsts = ledger["firsts"]

    print(f"status: {result['status']}  (trace events: {len(result['trace'])})")
    print()
    print("per-task progress (business target / consumed / received / dropped / unsent):")
    for t in tasks:
        print(f"  {t['id']}  {t['classification']:<15} "
              f"target={t['target']:<7} consumed={t['consumed']:<7} "
              f"received={t['received']:<7} dropped={t['local_dropped']:<6} "
              f"unsent={t['remote_unsent']:<6} incomplete={t['app_incomplete']}")
        if t["blocking"]:
            print(f"      blocked {t['blocking']}")
    print()
    print("connection-level credit account (client inbound, from frames only):")
    print(f"  initial {conn['initial']}  consumed {conn['consumed']}  "
          f"returned {conn['returned']}  window now {conn['window_end']}")
    print(f"  processed by app {conn['processed_total']}  "
          f"pending emission {conn['processed_pending_end']}  "
          f"buffered {conn['buffered_end']}")
    print(f"  lost return path {conn['lost_total']} "
          f"(padding {conn['lost_padding']} + dropped bodies {conn['lost_dropped_body']})")
    print()
    fur = firsts["first_unreturnable"]
    if fur:
        print(f"first credit without a return path: trace #{fur['index']} tick "
              f"{fur['tick']} pull stream {fur['stream']} ({fur['task']}): "
              f"acknowledged {fur['acknowledged']} of flow {fur['frame_flow']}, "
              f"{fur['stranded']} stranded")
    fd = firsts["first_drop"]
    if fd:
        print(f"first queued-body drop: trace #{fd['index']} tick {fd['tick']} "
              f"close stream {fd['stream']} ({fd['task']}): "
              f"{fd['dropped_body']} body bytes (flow {fd['dropped_flow']}) discarded")
    fua = firsts["first_unadvanceable"]
    if fua:
        print(f"first end-of-tick that cannot advance: tick {fua['tick']} "
              f"(window {fua['conn_window']}, pending processed "
              f"{fua['pending']} <= floor {conn['emission_floor']}, "
              f"unsent WINDOW_UPDATE {fua['unsent_wu']}, buffered {fua['buffered']})")
        print(f"stall declared by the collector at tick {firsts['stall_declared_tick']} "
              f"(two quiet ticks, collector.py:45-47)")
    print()
    print("ledger checks:", json.dumps(ledger["checks"], sort_keys=True))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["verify", "demo", "record"])
    parser.add_argument("pack")
    args = parser.parse_args()
    if args.action == "verify":
        return verify(args.pack)
    if args.action == "demo":
        return demo(args.pack)
    evidence = gather_evidence(args.pack)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=1, sort_keys=True) + "\n")
    print(f"evidence.json written from {args.pack}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
