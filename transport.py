"""HTTP/2 receiver and application body objects."""
from collections import deque
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, ResponseReceived, StreamEnded, StreamReset

def frames(wire, direction, tick, trace):
    preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    if wire.startswith(preface):
        wire = wire[len(preface):]
    offset = 0
    while offset < len(wire):
        header = wire[offset:offset + 9]
        length = int.from_bytes(header[:3], "big")
        payload = wire[offset + 9:offset + 9 + length]
        assert len(header) == 9 and len(payload) == length
        frame = {"tick": tick, "event": "frame", "direction": direction, "header": header.hex()}
        if header[3] == 0:
            frame["pad_length"] = payload[0] if header[4] & 8 else None
        else:
            frame["payload"] = payload.hex()
        trace.append(frame)
        offset += 9 + length

class Body:
    def __init__(self, transport, sid):
        self.transport, self.sid = transport, sid
        self.queue = deque()
        self.ended = False
        self.closed = False

    def pull(self, tick):
        if not self.queue:
            return None
        data, charge = self.queue.popleft()
        self.transport.h2.acknowledge_received_data(len(data), self.sid)
        self.transport.trace.append({"tick": tick, "event": "pull", "stream": self.sid,
                                     "bytes": len(data)})
        return data

    def close(self, tick):
        if self.closed:
            return
        self.transport.trace.append({"tick": tick, "event": "close", "stream": self.sid,
                                     "queued_bytes": sum(len(x[0]) for x in self.queue),
                                     "ended": self.ended})
        self.queue.clear()
        if not self.ended:
            self.transport.h2.reset_stream(self.sid)
        self.closed = True
        self.transport.bodies.pop(self.sid, None)

class Transport:
    def __init__(self, portal, trace):
        self.portal, self.trace = portal, trace
        self.h2 = H2Connection(config=H2Configuration(client_side=True, header_encoding="utf-8"))
        self.h2.initiate_connection()
        self.bodies = {}

    def open(self, task, tick):
        sid = self.h2.get_next_available_stream_id()
        body = Body(self, sid)
        self.bodies[sid] = body
        self.h2.send_headers(sid, [(":method", "GET"), (":scheme", "https"),
                                  (":authority", "bulletin.example"), (":path", "/" + task)],
                             end_stream=True)
        self.trace.append({"tick": tick, "event": "open", "task": task, "stream": sid})
        return body

    def pump(self, burst, tick):
        outgoing = self.h2.data_to_send()
        frames(outgoing, "client", tick, self.trace)
        self.portal.receive(outgoing)
        incoming = self.portal.produce(burst)
        frames(incoming, "server", tick, self.trace)
        events = self.h2.receive_data(incoming)
        for event in events:
            if isinstance(event, DataReceived):
                body = self.bodies.get(event.stream_id)
                self.trace.append({"tick": tick, "event": "data", "stream": event.stream_id,
                                   "bytes": len(event.data), "flow_length": event.flow_controlled_length})
                if body is not None:
                    body.queue.append((event.data, event.flow_controlled_length))
                else:
                    self.h2.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
            elif isinstance(event, StreamEnded):
                if event.stream_id in self.bodies:
                    self.bodies[event.stream_id].ended = True
            elif isinstance(event, StreamReset):
                raise RuntimeError("origin reset stream")
        return bool(outgoing or incoming)
