"""Client library used by the resident, moderator and live-feed clients and the CLI.

Handles the three things a client must do to survive node failures:
  1. TIMEOUT    every HTTP call gives up after `timeout` seconds.
  2. RETRY      on a network error or HTTP 502/503/504 it tries again, at most
                `max_attempts` times in total, sleeping 0.25s, 0.5s, 1s, 2s, 2s... (exponential
                backoff, capped, with a little random jitter) so it does not hammer a struggling cluster.
  3. FAILOVER   every retry goes to the NEXT node in the list; the node that last worked is
                remembered and tried first next time.
400/404/409 are real answers, not failures: they are raised immediately as ApiError.

Retries are safe: a submit carries a request_id, so a retry of a POST that was already applied
returns the existing report instead of a duplicate; an update carries base_version, so its second
try is a 409.
"""
import http.client
import json
import random
import time
import urllib.error
import uuid
import urllib.request
from urllib.parse import urlencode, urlparse

from .netutil import ConnPool


class ApiError(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status, self.body = status, body


class Client:
    def __init__(self, nodes="http://127.0.0.1:8001", timeout=5.0, max_attempts=8,
                 backoff=0.25, backoff_cap=2.0):
        self.nodes = [n.rstrip("/") for n in ([nodes] if isinstance(nodes, str) else nodes)]
        self.timeout, self.max_attempts = timeout, max_attempts
        self.backoff, self.backoff_cap = backoff, backoff_cap
        self.pool = ConnPool()                          # keep-alive connections (per client)
        self.idx = 0                                    # node currently preferred
        self.stats = {"requests": 0, "retries": 0, "failovers": 0}
        self.retry_reasons = {}                         # why we retried -> count (for diagnosis)

    @property
    def current_node(self):
        return self.nodes[self.idx]

    def _call(self, method, path, body=None):
        self.stats["requests"] += 1
        last = None
        for attempt in range(self.max_attempts):
            addr = urlparse(self.nodes[self.idx]).netloc
            try:
                status, payload = self.pool.request(addr, method, path, body, timeout=self.timeout)
                data = json.loads(payload) if payload else {}
                if status < 400:
                    return data
                if status not in (502, 503, 504):
                    raise ApiError(status, data)         # a real answer (400/404/409): don't retry
                last = ApiError(status, data)
            except ApiError:
                raise
            except (OSError, http.client.HTTPException, ValueError) as e:   # refused, reset, timed out...
                last = e
            # ---- failure: back off, then fail over to the next node ----
            why = (f"HTTP {last.status} {last.body.get('error')}" if isinstance(last, ApiError)
                   else type(last).__name__ + ": " + str(last)[:60])
            self.retry_reasons[why] = self.retry_reasons.get(why, 0) + 1
            self.stats["retries"] += 1
            if attempt == self.max_attempts - 1:
                break
            self.idx = (self.idx + 1) % len(self.nodes)
            self.stats["failovers"] += 1
            time.sleep(min(self.backoff_cap, self.backoff * 2 ** attempt) * random.uniform(0.8, 1.0))
        if isinstance(last, ApiError):
            raise last
        raise ConnectionError(f"all {len(self.nodes)} nodes failed after "
                              f"{self.max_attempts} attempts: {last}")

    def health(self):
        return self._call("GET", "/health")

    def submit(self, type, location, description, user, request_id=None):
        # One request_id for all retries of this submit: if an earlier attempt was applied but its
        # reply got lost, the server returns that report instead of creating a duplicate.
        return self._call("POST", "/reports", {"type": type, "location": location,
                          "description": description, "updated_by": user,
                          "request_id": request_id or uuid.uuid4().hex})

    def history(self, report_id):
        """Every version of one report, oldest first."""
        return self._call("GET", f"/reports/{report_id}/history")

    def list(self, **filters):
        q = urlencode({k: v for k, v in filters.items() if v})
        return self._call("GET", "/reports" + (f"?{q}" if q else ""))

    def get(self, report_id):
        return self._call("GET", f"/reports/{report_id}")

    def update(self, report_id, user, base_version, role="resident", **fields):
        """base_version = the version the caller last saw. A stale one gives ApiError(409)."""
        return self._call("PATCH", f"/reports/{report_id}",
                          {"updated_by": user, "role": role,
                           "base_version": base_version, **fields})


class LiveFeed:
    """Live-feed viewer client. `for event in LiveFeed(nodes).events(): ...`

    Opens GET /events on one node and yields each change as a dict
    {"seq", "op": "created"|"updated", "report": {...}, "served_by"}.
    If the connection drops (node died, network error) it backs off, fails over to the next
    node and reconnects sending Last-Event-ID = the last seq it saw, so the new node replays
    whatever was missed. Events can repeat after a reconnect; use report["version"] to ignore old ones.
    Optional filters: type=, status=, location=.
    """
    def __init__(self, nodes, timeout=15.0, backoff=0.25, backoff_cap=2.0, **filters):
        self.nodes = [n.rstrip("/") for n in ([nodes] if isinstance(nodes, str) else nodes)]
        self.timeout, self.backoff, self.backoff_cap = timeout, backoff, backoff_cap
        self.filters = {k: v for k, v in filters.items() if v}
        self.idx, self.last_id, self.current_node = 0, None, None
        self.closed = False
        self.stats = {"connects": 0, "reconnects": 0}

    def close(self):
        self.closed = True            # takes effect at the next event or keep-alive ping

    def events(self):
        attempt = 0
        while not self.closed:
            url = self.nodes[self.idx] + "/events"
            if self.filters:
                url += "?" + urlencode(self.filters)
            headers = {"Accept": "text/event-stream"}
            if self.last_id is not None:
                headers["Last-Event-ID"] = str(self.last_id)
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers),
                                            timeout=self.timeout) as resp:
                    attempt = 0
                    self.current_node = self.nodes[self.idx]
                    self.stats["connects"] += 1
                    ev = {}
                    for raw in resp:                      # SSE is plain text lines
                        if self.closed:
                            return
                        line = raw.decode().rstrip("\r\n")
                        if line == "":                    # blank line = end of one event
                            if "data" in ev:
                                self.last_id = int(ev["id"])
                                yield json.loads(ev["data"])
                            ev = {}
                        elif not line.startswith(":"):    # ":" lines are comments / pings
                            key, _, val = line.partition(":")
                            ev[key] = val[1:] if val.startswith(" ") else val
            except (OSError, http.client.HTTPException):
                pass
            if self.closed:
                return
            # stream ended or failed: back off, fail over to the next node, resume from last_id
            self.stats["reconnects"] += 1
            self.idx = (self.idx + 1) % len(self.nodes)
            time.sleep(min(self.backoff_cap, self.backoff * 2 ** min(attempt, 4)) * random.uniform(0.8, 1.0))
            attempt += 1
