"""A cluster node: peers, leader election, heartbeats, replication and catch-up.

TERMS.  Every leadership has a number, the TERM (like an epoch). An election makes
term+1. Heartbeats and replication carry (leader, term). A node always follows the
highest term it has seen, so an old leader that comes back (or was cut off) learns it
has been replaced and steps down, and followers refuse writes from a stale-term leader.
(This is a simplification, NOT Raft: no voting, no persisted terms.)

HEARTBEATS.  Every `hb_interval` (1s) each node POSTs its state to every peer. A peer
not heard from for `dead_after` (3s) is considered dead.

ELECTION.  If my leader is dead (or unknown), every live node independently picks the
LOWEST-ID live node (itself included). If that is me I first CATCH UP from the live peers
(so no acknowledged write is lost), then become leader. A restarted ex-leader does not
grab leadership back: at startup it asks its peers who leads and joins as a follower.

REPLICATION (leader -> followers) is synchronous to a majority; see replicate().
"""
import http.client
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .netutil import ConnPool
from .store import ReplicationError
from .wal import entry_hash

CATCHUP_BATCH = 2000


def load_config(path):
    """config.json: {"nodes": {"N1": "127.0.0.1:8001", ...}, "initial_leader": "N1"}"""
    with open(path) as f:
        return json.load(f)


NET_ERRORS = (OSError, http.client.HTTPException)


class Node:
    def __init__(self, node_id, store, nodes, initial_leader="N1",
                 hb_interval=1.0, dead_after=3.0, repl_timeout=3.0, quiet=False):
        self.node_id, self.store = node_id, store
        self.nodes = dict(nodes)               # node id -> "host:port"
        self.initial_leader = initial_leader
        self.hb_interval, self.dead_after, self.repl_timeout = hb_interval, dead_after, repl_timeout
        self.quiet = quiet
        self.leader_id, self.term = None, 0
        self.last_seen = {}                    # peer id -> time of last message from it
        self.peer_seq = {}                     # peer id -> its last WAL seq (as last reported)
        self._state_lock = threading.RLock()
        self._sync_lock = threading.Lock()     # one catch-up at a time
        self._elect_lock = threading.Lock()    # one election at a time
        self._need_sync = False                # set when the leader changes: verify my log vs theirs
        self._stop = threading.Event()
        self.pool = ConnPool()                 # keep-alive connections to peers
        self._pool = ThreadPoolExecutor(max_workers=64)    # replication sends
        self._hb_pool = ThreadPoolExecutor(max_workers=4)  # heartbeats (separate: must not queue behind writes)
        self._started_at = time.time()
        if len(self.nodes) == 1:               # single-node mode (Stages 1-2): always the leader
            self.set_leader(node_id, 1)

    # ---- small helpers --------------------------------------------------
    def log(self, msg):
        if not self.quiet:
            print(f"[{self.node_id}] {msg}", flush=True)

    @property
    def is_leader(self):
        return self.leader_id == self.node_id

    def rank(self, node_id):
        return sorted(self.nodes).index(node_id)       # N1 < N2 < N3

    def peers(self):
        return {i: a for i, a in self.nodes.items() if i != self.node_id}

    def majority(self):
        return len(self.nodes) // 2 + 1

    def info(self):
        """What I tell peers about myself (heartbeat / health body)."""
        return {"node_id": self.node_id, "leader": self.leader_id, "term": self.term,
                "last_seq": self.store.wal.last_seq,
                "role": "leader" if self.is_leader else "follower"}

    def live_peers(self):
        now = time.time()
        return {p for p in self.peers() if now - self.last_seen.get(p, 0) < self.dead_after}

    def set_leader(self, leader_id, term):
        with self._state_lock:
            changed = leader_id != self.leader_id
            self.leader_id, self.term = leader_id, term
            # Only the leader replicates; followers never do.
            self.store.on_commit = self.replicate if leader_id == self.node_id else None
            if changed and leader_id != self.node_id:
                self._need_sync = True
        if changed:
            self.log(f"leader is now {leader_id} (term {term})"
                     + ("  <-- I AM THE LEADER" if leader_id == self.node_id else ""))

    # ---- hearing from peers --------------------------------------------
    def observe(self, info):
        """Process a heartbeat/health/replication message: record liveness and follow the
        highest-term leader. This is also how a stale leader learns it must step down."""
        src, L, T = info.get("node_id"), info.get("leader"), info.get("term", 0)
        with self._state_lock:
            if src in self.nodes and src != self.node_id:
                self.last_seen[src] = time.time()
                if "last_seq" in info:
                    self.peer_seq[src] = info["last_seq"]
            if L not in self.nodes:
                return
            mine = self.leader_id
            adopt = (T > self.term or
                     (T == self.term and (mine is None or self.rank(L) < self.rank(mine))))
            adopt = adopt and (L != mine or T != self.term)
        if adopt:
            self.set_leader(L, T)

    def accept_replication(self, body):
        """Follower: should I accept an entry from this leader? Refuse stale terms."""
        T, L = body.get("term", 0), body.get("leader")
        if T < self.term:
            return False
        self.observe({"node_id": L, "leader": L, "term": T,
                      "last_seq": body["entry"]["seq"]})
        return True

    # ---- link B: leader -> followers -----------------------------------
    def _send(self, addr, entry):
        """POST one entry to one follower. True = the follower stored it (ack)."""
        body = {"entry": entry, "term": self.term, "leader": self.node_id}
        try:
            status, r = self.pool.json(addr, "POST", "/internal/replicate", body, self.repl_timeout)
        except NET_ERRORS:
            return False                           # follower down/slow: not an ack
        if status == 200:
            return bool(r.get("ack"))
        if status == 409 and r.get("error") == "stale term":   # a newer leader exists: step down
            self.observe({"node_id": r["leader"], "leader": r["leader"], "term": r["term"]})
        return False

    def replicate(self, entry):
        """Called by the store (on the leader) after the entry is in the leader's WAL.

        SYNCHRONOUS TO A MAJORITY: reply to the client only once the leader + at least one
        follower (2 of 3) have stored the entry. Trade-off: each write pays one network round
        trip, but an acknowledged write survives the loss of any single node. Waiting for ALL
        followers would block writes whenever one node is down; not waiting (asynchronous)
        would be fastest but could lose acknowledged writes if the leader died."""
        need = self.majority() - 1                 # follower acks needed (leader counts as 1)
        if need <= 0:
            return
        futures = [self._pool.submit(self._send, a, entry) for a in self.peers().values()]
        acks = 0
        for f in as_completed(futures):
            acks += 1 if f.result() else 0
            if acks >= need:
                return                              # majority reached; the rest finish in background
        raise ReplicationError(f"only {acks + 1}/{len(self.nodes)} nodes stored the write")

    # ---- catch-up -------------------------------------------------------
    def serve_log(self, after, check, as_leader):
        """Answer GET /internal/log: entries after `after`, provided my entry number `after`
        matches the caller's (`check` = its hash). If not, the histories diverged."""
        if as_leader and not self.is_leader:
            return 503, {"error": "not the leader"}
        wal = self.store.wal
        if after > 0:
            e = wal.entry(after)
            if e is None or entry_hash(e) != check:
                return 200, {"diverged": True}
        return 200, {"entries": wal.entries_after(after, CATCHUP_BATCH), "last_seq": wal.last_seq}

    def catch_up(self, addr, as_leader):
        """Ask the node at `addr` for the WAL entries after my last seq and apply them.
        If its history differs from mine at my last seq (e.g. I was an old leader holding
        a write that was never acknowledged), discard my log and re-download all of it."""
        first = True
        while True:
            last = self.store.wal.last_seq
            check = entry_hash(self.store.wal.entry(last)) if last else ""
            try:
                status, r = self.pool.json(addr, "GET", f"/internal/log?after={last}&check={check}"
                                           f"&as_leader={int(as_leader)}", timeout=5)
            except NET_ERRORS:
                return False
            if status != 200:
                return False                        # e.g. 503: that node is not (yet) the leader
            if r.get("diverged"):
                self.log("my log disagrees with the leader's: discarding it and resyncing from seq 0")
                self.store.reset_for_resync()
                continue
            for e in r["entries"]:
                self.store.apply_replicated(e, wait=0)
            if first and r["entries"]:
                self.log(f"caught up: applied {len(r['entries'])} entries, now at seq {self.store.wal.last_seq}")
            first = False
            if len(r["entries"]) < CATCHUP_BATCH:
                return True

    def sync_with_leader(self):
        if not self._sync_lock.acquire(blocking=False):
            return
        try:
            L = self.leader_id
            if L in (None, self.node_id):
                return
            if self.catch_up(self.nodes[L], as_leader=True):
                self._need_sync = False
        finally:
            self._sync_lock.release()

    # ---- startup, heartbeats, election ----------------------------------
    def start(self):
        """Replay already happened (Store.__init__). Now find out who leads, catch up, and
        start the heartbeat + failure-detector threads."""
        self._started_at = time.time()
        self._probe_peers()
        if self.leader_id is None and self.node_id == self.initial_leader:
            self.set_leader(self.node_id, self.term + 1)
        if self.leader_id not in (None, self.node_id):
            self.sync_with_leader()
        threading.Thread(target=self._hb_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    def stop(self):
        self._stop.set()
        self.store.feed.close()                  # end SSE streams so viewers fail over
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._hb_pool.shutdown(wait=False, cancel_futures=True)
        self.pool.close()

    def _probe_peers(self):
        """At startup, ask every peer 'who is leader?' (in parallel)."""
        def ask(addr):
            try:
                status, info = self.pool.json(addr, "GET", "/health", timeout=0.7)
                if status == 200:
                    self.observe(info)
            except NET_ERRORS:
                pass
        list(self._hb_pool.map(ask, self.peers().values()))

    def _heartbeat_to(self, addr):
        try:
            status, info = self.pool.json(addr, "POST", "/internal/heartbeat", self.info(),
                                          timeout=min(0.8, self.hb_interval))
            if status == 200:
                self.observe(info)
        except (*NET_ERRORS, ValueError):
            pass

    def _hb_loop(self):
        while not self._stop.is_set():
            for addr in self.peers().values():
                self._hb_pool.submit(self._heartbeat_to, addr)
            self._stop.wait(self.hb_interval)

    def _monitor_loop(self):
        while not self._stop.wait(self.hb_interval / 2):
            try:
                self._check()
            except Exception as e:                   # never let the detector thread die
                self.log(f"monitor error: {e!r}")

    def _check(self):
        L = self.leader_id
        if L == self.node_id:
            return
        if L is None:
            leader_dead = time.time() - self._started_at > self.dead_after   # grace after boot
        else:
            leader_dead = L not in self.live_peers()
        if leader_dead:
            self._elect()
        elif self._need_sync or self.peer_seq.get(L, 0) > self.store.wal.last_seq:
            self.sync_with_leader()                 # new leader, or I am behind it

    def _elect(self):
        if not self._elect_lock.acquire(blocking=False):
            return
        try:
            live = self.live_peers() | {self.node_id}
            winner = min(live, key=self.rank)       # lowest-ID live node
            new_term = self.term + 1
            if winner != self.node_id:
                self.log(f"leader {self.leader_id} silent for {self.dead_after:g}s -> electing {winner}")
                self.set_leader(winner, new_term)
                return
            self.log(f"leader {self.leader_id} silent for {self.dead_after:g}s -> I am the lowest live ID; "
                     f"catching up before taking over")
            # An acknowledged write is on a majority, so at least one live peer has it:
            # pull anything I'm missing BEFORE accepting writes.
            for addr in [self.nodes[p] for p in sorted(live - {self.node_id}, key=self.rank)]:
                self.catch_up(addr, as_leader=False)
            self.set_leader(self.node_id, new_term)
        finally:
            self._elect_lock.release()
