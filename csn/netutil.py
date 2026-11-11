"""Connection pool: reuse TCP connections between requests (HTTP/1.1 keep-alive).

Why: with one new TCP connection per request, a busy cluster burns through the OS's
ephemeral ports (closed sockets linger in TIME_WAIT) and requests start failing with
"Can't assign requested address". Reusing connections also skips the TCP handshake, so
it is faster. Used by the client library and by node-to-node calls (replication,
forwarding, heartbeats, catch-up).
"""
import http.client
import json
import threading


class ConnPool:
    def __init__(self, max_idle_per_host=64):
        self._idle = {}                       # "host:port" -> [idle HTTPConnection, ...]
        self._lock = threading.Lock()
        self.max_idle = max_idle_per_host

    def _take(self, addr, timeout):
        with self._lock:
            lst = self._idle.get(addr)
            conn = lst.pop() if lst else None
        if conn is not None:
            conn.timeout = timeout
            if conn.sock:
                conn.sock.settimeout(timeout)
            return conn, True                 # reused
        return http.client.HTTPConnection(addr, timeout=timeout), False

    def _give(self, addr, conn):
        with self._lock:
            lst = self._idle.setdefault(addr, [])
            if len(lst) < self.max_idle:
                lst.append(conn)
                return
        conn.close()

    def request(self, addr, method, path, body=None, timeout=1.0, headers=None):
        """Send one request; return (status, body_bytes). `body` is bytes, a dict (sent as
        JSON) or None. Raises OSError / http.client.HTTPException if the node is unreachable.
        If a REUSED connection turns out to be dead (server closed it while idle) we silently
        retry once on a fresh connection: that is not a real failure."""
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        hdrs = {"Content-Type": "application/json", **(headers or {})}
        for attempt in (0, 1):
            conn, reused = self._take(addr, timeout)
            try:
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                payload = resp.read()
            except (OSError, http.client.HTTPException):
                conn.close()
                if reused and attempt == 0:
                    continue
                raise
            if resp.will_close:
                conn.close()
            else:
                self._give(addr, conn)
            return resp.status, payload

    def json(self, addr, method, path, body=None, timeout=1.0):
        """Convenience: (status, parsed JSON)."""
        status, payload = self.request(addr, method, path, body, timeout)
        return status, (json.loads(payload) if payload else {})

    def close(self):
        with self._lock:
            for lst in self._idle.values():
                for c in lst:
                    c.close()
            self._idle.clear()
