"""Live feed plumbing: an in-process publish/subscribe bus for Server-Sent Events (SSE).

Every node publishes an event each time it APPLIES a WAL entry (whether it wrote it as
leader or received it by replication / catch-up), so a viewer connected to ANY node sees
every change.

Events are delivered to subscribers in WAL-seq order (concurrent writes to different
reports can finish applying slightly out of order; the bus puts them back in order),
which lets the SSE `id:` be the WAL seq: a viewer that reconnects, even to a different
node, sends Last-Event-ID and the node replays everything after it from its WAL.
"""
import queue
import threading


class Subscription:
    def __init__(self, size):
        self.q = queue.Queue(maxsize=size)
        self.dropped = False       # set if this viewer was too slow (queue full) or the node stopped


class EventBus:
    def __init__(self, ping_interval=5.0, queue_size=1000):
        self.ping_interval = ping_interval
        self.queue_size = queue_size
        self.closed = False
        self._subs = set()
        self._lock = threading.Lock()
        self._pending = {}         # seq -> entry waiting for earlier seqs to be published first
        self._next = 1

    def reset(self):
        """The WAL was thrown away and is being re-downloaded: restart ordering from seq 1."""
        with self._lock:
            self._pending.clear()
            self._next = 1

    def subscribe(self):
        sub = Subscription(self.queue_size)
        with self._lock:
            self._subs.add(sub)
        return sub

    def unsubscribe(self, sub):
        with self._lock:
            self._subs.discard(sub)

    def publish(self, entry):
        with self._lock:
            if entry["seq"] < self._next:
                return                                   # already published (duplicate)
            self._pending[entry["seq"]] = entry
            while self._next in self._pending:           # release in strict seq order
                e = self._pending.pop(self._next)
                self._next += 1
                for sub in list(self._subs):
                    try:
                        sub.q.put_nowait(e)
                    except queue.Full:                   # slow viewer: cut it loose; it will
                        sub.dropped = True               # reconnect and replay from its Last-Event-ID
                        self._subs.discard(sub)

    def close(self):
        """Node is stopping: end every open stream so viewers fail over."""
        self.closed = True
        with self._lock:
            for sub in self._subs:
                sub.dropped = True
            self._subs.clear()
