"""In-memory store of reports, backed by the WAL.

Key design point: every change is a WAL entry whose payload is the COMPLETE
resulting report. `apply()` is the only code that mutates memory, and it is used
both for live writes and for WAL replay. That keeps replay deterministic (and, in
Stage 3/4, lets followers apply the same entries the leader produced).
"""
import threading
import time

from .feed import EventBus
from .wal import WAL

REPORT_TYPES = {"power", "water", "road", "other"}
STATUSES = {"open", "verified", "resolved"}
MODERATOR_ONLY_STATUSES = {"verified", "resolved"}
# Input limits: a report is a short notice, and every byte is replicated and kept forever in the WAL.
MAX_LOCATION, MAX_DESCRIPTION, MAX_USER, MAX_REQUEST_ID = 120, 500, 40, 64


def matches(report, filters):
    """Does a report pass the optional type/status/location filters? (used by list and SSE)"""
    if filters.get("type") and report["type"] != filters["type"]:
        return False
    if filters.get("status") and report["status"] != filters["status"]:
        return False
    if filters.get("location") and filters["location"].lower() not in report["location"].lower():
        return False
    return True


class ValidationError(Exception):
    pass


def _check_lengths(**fields):
    limits = {"location": MAX_LOCATION, "description": MAX_DESCRIPTION, "updated_by": MAX_USER}
    for name, value in fields.items():
        if len(value) > limits[name]:
            raise ValidationError(f"{name} is too long (max {limits[name]} characters)")


class NotFound(Exception):
    pass


class ReplicationError(Exception):
    """The leader could not reach a majority of nodes (maps to HTTP 503)."""


class ReplicationGap(Exception):
    """A follower was sent an entry but is missing earlier ones (Stage 4 catch-up fixes this)."""
    def __init__(self, last_seq):
        super().__init__("gap")
        self.last_seq = last_seq


class VersionConflict(Exception):
    """The client edited from an out-of-date version (maps to HTTP 409)."""
    def __init__(self, current):
        super().__init__("version conflict")
        self.current = current   # the stored report, so the client can re-read and retry


class Store:
    def __init__(self, node_id, data_dir):
        self.node_id = node_id
        self.reports = {}          # report id -> report dict
        self.next_id = 1
        self._requests = {}        # request_id -> seq of the create entry (idempotent submits)
        # Stage 2 locking (always acquired in this order, so no deadlock):
        #   1. a per-record lock  (serialises edits to ONE report; other reports unaffected)
        #   2. _create_lock       (serialises id allocation for new reports)
        #   3. _index_lock        (very short: protects the dict itself while it is read/changed)
        #   4. the WAL's own lock (inside wal.append)
        self._record_locks = {}                 # report id -> Lock
        self._registry_lock = threading.Lock()  # protects _record_locks
        self._create_lock = threading.Lock()
        self._index_lock = threading.Lock()
        self.on_commit = None                   # leader sets this: replicate(entry) to followers
        self._repl_cond = threading.Condition() # follower: apply replicated entries in seq order
        self.critical_section_delay = 0         # test hook: sleep while holding a record lock
        self.wal = WAL(f"{data_dir}/{node_id}.wal", node_id)
        self.feed = EventBus()                  # live feed: one event per applied WAL entry
        self.recover()

    # ---- recovery -------------------------------------------------------
    def recover(self):
        """Rebuild memory by replaying every WAL entry in order."""
        for entry in self.wal.read_all():
            self.apply(entry)

    def apply(self, entry):
        """Apply one WAL entry to memory (the single place state changes)."""
        p = entry["payload"]
        if entry["op"] in ("create_report", "update_report"):
            with self._index_lock:
                self.reports[p["id"]] = dict(p)
                n = int(p["id"][1:])  # ids look like "R12"
                self.next_id = max(self.next_id, n + 1)
                if entry["op"] == "create_report" and p.get("request_id"):
                    self._requests[p["request_id"]] = entry["seq"]
        self.feed.publish(entry)                # tell live viewers (never blocks)

    def _record_lock(self, report_id):
        """Get (or lazily create) the lock that guards one report."""
        with self._registry_lock:
            return self._record_locks.setdefault(report_id, threading.Lock())

    # ---- writes ---------------------------------------------------------
    def create_report(self, data):
        rtype = data.get("type")
        location = (data.get("location") or "").strip()
        description = (data.get("description") or "").strip()
        user = (data.get("updated_by") or "").strip()
        if rtype not in REPORT_TYPES:
            raise ValidationError(f"type must be one of {sorted(REPORT_TYPES)}")
        if not location or not description or not user:
            raise ValidationError("location, description and updated_by are required")
        _check_lengths(location=location, description=description, updated_by=user)
        # Optional idempotency key: a client that retries a submit (timeout, 503, failover) sends
        # the same request_id, and gets the report it already created instead of a duplicate.
        request_id = data.get("request_id")
        if request_id is not None and (not isinstance(request_id, str) or not request_id.strip()
                                       or len(request_id) > MAX_REQUEST_ID):
            raise ValidationError(f"request_id must be a string of 1-{MAX_REQUEST_ID} characters")
        with self._create_lock:   # id allocation + WAL order must be atomic
            if request_id:
                with self._index_lock:
                    seq = self._requests.get(request_id)
                first = self.wal.entry(seq) if seq else None
                if first:
                    if self.on_commit:
                        self.on_commit(first)   # the first try may have failed to reach a majority:
                    with self._index_lock:      # replicate again (followers ignore duplicates)
                        return dict(self.reports[first["payload"]["id"]])
            report = {
                "id": f"R{self.next_id}", "type": rtype, "location": location,
                "description": description, "status": "open", "version": 1,
                "updated_by": user, "updated_at": time.time(),
            }
            if request_id:
                report["request_id"] = request_id
            entry = self.wal.append("create_report", report)   # WAL first...
            self.apply(entry)                                  # ...then memory
            if self.on_commit:
                self.on_commit(entry)       # ...then replicate (waits for a majority)
            return dict(report)

    def update_report(self, report_id, data):
        user = (data.get("updated_by") or "").strip()
        if not user:
            raise ValidationError("updated_by is required")
        _check_lengths(updated_by=user)
        if "description" in data:
            if not str(data["description"]).strip():
                raise ValidationError("description cannot be empty")
            _check_lengths(description=str(data["description"]).strip())
        base = data.get("base_version")
        if not isinstance(base, int) or isinstance(base, bool):
            raise ValidationError("base_version (the version you last read) is required")
        if "status" in data:
            if data["status"] not in STATUSES:
                raise ValidationError(f"status must be one of {sorted(STATUSES)}")
            # Moderator role is only *claimed* by the client (no authentication).
            if data["status"] in MODERATOR_ONLY_STATUSES and data.get("role") != "moderator":
                raise ValidationError("only a moderator can verify/resolve")

        # Only editors of THIS report wait here; other reports proceed in parallel.
        with self._record_lock(report_id):
            current = self.reports.get(report_id)
            if current is None:
                raise NotFound(report_id)
            if self.critical_section_delay:
                time.sleep(self.critical_section_delay)   # test hook only
            # Optimistic concurrency: the check and the write happen under the SAME lock,
            # so two edits based on version N can never both succeed.
            if base != current["version"]:
                raise VersionConflict(dict(current))
            new = dict(current)
            if "description" in data:
                new["description"] = str(data["description"]).strip()
            if "status" in data:
                new["status"] = data["status"]
            new["version"] = current["version"] + 1
            new["updated_by"] = user
            new["updated_at"] = time.time()
            entry = self.wal.append("update_report", new)
            self.apply(entry)
            if self.on_commit:
                self.on_commit(entry)       # still holding the record lock: the next edit of
            return dict(new)                # this report only starts after replication finished

    # ---- follower side of replication ---------------------------------
    def apply_replicated(self, entry, wait=1.5):
        """Apply an entry the leader sent. Entries must be applied in seq order, but
        the leader sends them concurrently, so seq N+1 may arrive before N: wait
        briefly for the gap to fill. Re-delivery of an old entry is harmless."""
        deadline = time.time() + wait
        with self._repl_cond:
            while True:
                last = self.wal.last_seq
                if entry["seq"] <= last:
                    return last                       # duplicate: already have it
                if entry["seq"] == last + 1:
                    break
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise ReplicationGap(last)        # genuinely missing entries
                self._repl_cond.wait(remaining)
            self.wal.append_entry(entry)              # WAL first, then memory (as on the leader)
            self.apply(entry)
            self._repl_cond.notify_all()
            return entry["seq"]

    def reset_for_resync(self):
        """This node's history disagrees with the leader's: forget everything and
        re-download the leader's log from the start."""
        with self._repl_cond, self._index_lock:
            self.wal.reset()
            self.feed.reset()
            self.reports.clear()
            self._requests.clear()
            self.next_id = 1

    # ---- reads ----------------------------------------------------------
    def get_report(self, report_id):
        with self._index_lock:
            r = self.reports.get(report_id)
            if r is None:
                raise NotFound(report_id)
            return dict(r)

    def history(self, report_id):
        """Every version of one report, oldest first. The WAL already is the history: each entry's
        payload is the complete report as it was after that change."""
        self.get_report(report_id)               # 404 if unknown
        return [{"seq": e["seq"], "op": "created" if e["op"] == "create_report" else "updated",
                 "report": e["payload"]}
                for e in self.wal.read_all() if e["payload"].get("id") == report_id]

    def list_reports(self, filters):
        """filters: dict with optional type/status/location (case-insensitive substring)."""
        with self._index_lock:
            snapshot = [dict(r) for r in self.reports.values()]
        out = [r for r in snapshot if matches(r, filters)]   # filter outside the lock
        out.sort(key=lambda r: int(r["id"][1:]))
        return out
