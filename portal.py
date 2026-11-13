"""Finite recorded origin; all HTTP/2 framing is performed by h2."""
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import RequestReceived, StreamReset

class Portal:
    def __init__(self, pack):
        self.h2 = H2Connection(config=H2Configuration(client_side=False, header_encoding="utf-8"))
        self.h2.initiate_connection()
        self.requests = {"/" + r["id"]: r for r in pack["requests"]}
        self.live = {}

    def receive(self, wire):
        for event in self.h2.receive_data(wire):
            if isinstance(event, RequestReceived):
                request = self.requests[dict(event.headers)[":path"]]
                self.live[event.stream_id] = [request, request["size"]]
                self.h2.send_headers(event.stream_id, [(":status", "200"), ("content-type", "text/plain")])
            elif isinstance(event, StreamReset):
                self.live.pop(event.stream_id, None)

    def produce(self, burst):
        for sid, state in list(self.live.items()):
            request, remaining = state
            for _ in range(burst):
                window = self.h2.local_flow_control_window(sid)
                if window <= 0 or remaining == 0:
                    break
                padding = min(request["pad"], window - 2) if window >= 2 else None
                overhead = 0 if padding is None else padding + 1
                size = min(request["chunk"], remaining, window - overhead, 16384 - overhead)
                payload = request["fill"].encode("ascii") * size
                remaining -= size
                self.h2.send_data(sid, payload, end_stream=remaining == 0, pad_length=padding)
            state[1] = remaining
            if remaining == 0:
                del self.live[sid]
        return self.h2.data_to_send()
