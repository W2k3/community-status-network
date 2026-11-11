"""Write-ahead log (WAL): an append-only file of JSON lines.

Each line is one LOG_ENTRY:  {"seq": 7, "op": "create_report", "payload": {...}, "node_id": "N1"}

Rule: a change is written to the WAL (and flushed to disk) BEFORE it is
acknowledged to the client. After a crash we replay the file to rebuild memory.
Entries are numbered 1,2,3... with no gaps, so entry number `seq` is entries[seq-1].
"""
import hashlib
import json
import os
import threading
import time


def entry_hash(entry):
    """Fingerprint of an entry. Used in catch-up to check two nodes agree on history."""
    return hashlib.sha1(json.dumps(entry, sort_keys=True).encode()).hexdigest()[:16]


class WAL:
    def __init__(self, path, node_id):
        self.path = path
        self.node_id = node_id
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.entries, valid_bytes = self._load()
        self.last_seq = self.entries[-1]["seq"] if self.entries else 0
        # A crash mid-write can leave a half line at the end. Cut it off, otherwise the
        # next append would be glued onto the broken line and be unreadable.
        if os.path.exists(path) and os.path.getsize(path) > valid_bytes:
            with open(path, "r+b") as f:
                f.truncate(valid_bytes)
        self._file = open(path, "a", encoding="utf-8")

    def _load(self):
        entries, valid = [], 0
        if not os.path.exists(self.path):
            return entries, 0
        with open(self.path, "rb") as f:
            for raw in f:
                try:
                    entries.append(json.loads(raw))
                except json.JSONDecodeError:
                    break  # torn tail: that write was never acknowledged to a client
                if not raw.endswith(b"\n"):
                    entries.pop()
                    break
                valid += len(raw)
        return entries, valid

    def read_all(self):
        """All entries (used to replay the log into memory at startup)."""
        return list(self.entries)

    def _write(self, entry):
        self._file.write(json.dumps(entry) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())  # force it onto disk, not just the OS cache
        self.entries.append(entry)
        self.last_seq = entry["seq"]

    def append(self, op, payload):
        """Leader side: assign the next seq, write the line, fsync, return the entry."""
        with self._lock:
            entry = {"seq": self.last_seq + 1, "op": op,
                     "payload": payload, "node_id": self.node_id}
            self._write(entry)
            return entry

    def append_entry(self, entry):
        """Follower side: store an entry exactly as the leader produced it (same seq)."""
        with self._lock:
            self._write(entry)

    def entry(self, seq):
        return self.entries[seq - 1] if 1 <= seq <= len(self.entries) else None

    def entries_after(self, seq, limit=None):
        out = self.entries[seq:]
        return out[:limit] if limit else out

    def reset(self):
        """Throw the log away (kept as a .diverged-<time> file for inspection).
        Used when this node's history disagrees with the leader's."""
        with self._lock:
            self._file.close()
            os.rename(self.path, f"{self.path}.diverged-{int(time.time())}")
            self.entries, self.last_seq = [], 0
            self._file = open(self.path, "a", encoding="utf-8")

    def close(self):
        self._file.close()
