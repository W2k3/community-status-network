"""REST API + node-to-node endpoints for one node.

Public API (links A)
  GET   /health
  POST  /reports               body: type, location, description, updated_by
  GET   /reports?type=&status=&location=
  GET   /reports/<id>
  GET   /reports/<id>/history  every version of one report, oldest first (read from the WAL)
  PATCH /reports/<id>          body: updated_by, base_version, [description], [status], [role]
                               -> 409 + current version if base_version is stale
Live feed (SSE)
  GET   /events?type=&status=&location=&last_id=N   text/event-stream; also honours the
                               Last-Event-ID header. Events: id = WAL seq, event = "report",
                               data = {"seq","op":"created|updated","report":{...},"served_by"}
  GET   /nodes                 node addresses (lets the HTML viewer fail over)
  GET   /  or /live            the HTML viewer page (web/live.html)
Internal (links B and C)
  POST  /internal/replicate    body: {"entry", "term", "leader"}  (leader -> follower)
  POST  /internal/heartbeat    body: {"node_id", "leader", "term", "last_seq"}
  GET   /internal/log?after=N&check=H   WAL entries after seq N (catch-up)

WRITES on a follower are forwarded to the leader and the leader's answer is relayed
back unchanged. READS are answered from the local copy of whichever node was asked
(possibly stale by a few ms: see README "consistency").

Run:  python3 -m csn.server --id N1 --config config.json
"""
import argparse
import json
import os
import queue
import threading
import time
import http.client
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .node import Node, load_config
from .store import (matches, Store, ValidationError, NotFound, VersionConflict,
                    ReplicationError, ReplicationGap)


WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "web")
FONTS = {"inter.woff2", "space-grotesk.woff2", "jetbrains-mono.woff2"}   # the only static files served
MAX_BODY = 16 * 1024      # public request bodies are a few short strings


class Handler(BaseHTTPRequestHandler):
    node = None   # set per server in make_server()
    protocol_version = "HTTP/1.1"   # keep-alive: one TCP connection serves many requests
    timeout = 30                    # close a connection that has been idle this long (frees its thread)

    def log_error(self, *args):     # an idle keep-alive connection timing out is normal, not an error
        pass

    # -- helpers --
    def _send_raw(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Served-By", self.node.node_id)
        self.send_header("Access-Control-Allow-Origin", "*")   # lets the viewer page talk to any node
        self.end_headers()
        self.wfile.write(body)

    def _send(self, code, obj):
        self._send_raw(code, json.dumps(obj).encode())

    @staticmethod
    def _parse(raw):
        try:
            data = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            raise ValidationError("body is not valid JSON")
        if not isinstance(data, dict):
            raise ValidationError("body must be a JSON object")
        return data

    def _serve_file(self, *rel, ctype="text/html; charset=utf-8", cache="no-cache"):
        path = os.path.join(WEB_DIR, *rel)
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            return self._send(404, {"error": "no such file"})
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_page(self):
        self._serve_file("live.html")

    def do_OPTIONS(self):
        """CORS preflight. The browser page sends JSON POST/PATCH to OTHER nodes when its own node
        dies (write failover); browsers ask permission with OPTIONS first."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID")
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _stream_events(self, query):
        """Server-Sent Events: keep the connection open and write one event per change.
        1) subscribe FIRST (so nothing is missed), 2) replay WAL entries after Last-Event-ID,
        3) stream live events, skipping any the replay already covered."""
        n, feed = self.node, self.node.store.feed
        raw_id = self.headers.get("Last-Event-ID") or query.get("last_id")
        try:
            last_id = int(raw_id) if raw_id not in (None, "") else None
        except ValueError:
            last_id = None

        def send_event(e):
            if not matches(e["payload"], query):
                return
            data = {"seq": e["seq"], "op": "created" if e["op"] == "create_report" else "updated",
                    "report": e["payload"], "served_by": n.node_id}
            self.wfile.write(f"id: {e['seq']}\nevent: report\ndata: {json.dumps(data)}\n\n".encode())

        self.close_connection = True      # a stream has no Content-Length: it ends by closing
        sub = feed.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Served-By", n.node_id)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()            # no Content-Length: the stream stays open (HTTP/1.0 style)
            self.wfile.write(f": connected to {n.node_id}\nretry: 1000\n\n".encode())
            replayed = 0
            if last_id is not None:       # a reconnecting viewer: catch it up from the WAL
                for e in n.store.wal.entries_after(last_id):
                    send_event(e)
                    replayed = e["seq"]
            last_write = time.time()
            while not feed.closed and not sub.dropped:
                try:
                    e = sub.q.get(timeout=0.25)
                except queue.Empty:
                    if time.time() - last_write >= feed.ping_interval:
                        self.wfile.write(b": ping\n\n")      # keep-alive; also detects dead viewers
                        last_write = time.time()
                    continue
                if e["seq"] > replayed:
                    send_event(e)
                    last_write = time.time()
        except OSError:
            pass                          # viewer went away (broken pipe / reset)
        finally:
            feed.unsubscribe(sub)

    def _forward_to_leader(self, method, raw):
        """Follower: pass the write to the leader and relay its reply (status + body)."""
        n = self.node
        if self.headers.get("X-Forwarded-By"):      # already forwarded once: don't ping-pong
            return self._send(503, {"error": "not the leader", "node": n.node_id})
        if n.leader_id is None:
            return self._send(503, {"error": "no leader elected yet"})
        try:
            status, body = n.pool.request(n.nodes[n.leader_id], method, self.path, raw, timeout=10,
                                          headers={"X-Forwarded-By": n.node_id})
            self._send_raw(status, body)            # 200/201/400/404/409/503...: relay as is
        except (OSError, http.client.HTTPException):
            self._send(503, {"error": f"leader {n.leader_id} unreachable"})

    def _route(self, method):
        n, store = self.node, self.node.store
        parts = [p for p in urlparse(self.path).path.split("/") if p]
        query = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY and parts[:1] != ["internal"]:
            self.close_connection = True          # don't read (or keep alive past) an oversized body
            return self._send(413, {"error": f"body too large (max {MAX_BODY} bytes)"})
        raw = self.rfile.read(length) if length else b""
        try:
            if method == "GET" and parts in ([], ["live"]):
                return self._serve_page()
            if method == "GET" and len(parts) == 2 and parts[0] == "fonts" and parts[1] in FONTS:
                return self._serve_file("fonts", parts[1], ctype="font/woff2",
                                        cache="public, max-age=604800")
            if method == "GET" and parts == ["events"]:
                return self._stream_events(query)
            if method == "GET" and parts == ["nodes"]:
                return self._send(200, {"nodes": n.nodes, "leader": n.leader_id})
            if method == "GET" and parts == ["health"]:
                return self._send(200, dict(n.info(), ok=True))
            if parts == ["internal", "heartbeat"] and method == "POST":
                n.observe(self._parse(raw))
                return self._send(200, n.info())
            if parts == ["internal", "log"] and method == "GET":
                code, body = n.serve_log(int(query.get("after", 0)), query.get("check", ""),
                                         query.get("as_leader") == "1")
                return self._send(code, body)
            if parts == ["internal", "replicate"] and method == "POST":
                body = self._parse(raw)
                if not n.accept_replication(body):
                    return self._send(409, {"error": "stale term", "term": n.term,
                                            "leader": n.leader_id})
                seq = store.apply_replicated(body["entry"])
                return self._send(200, {"ack": True, "last_seq": seq})
            if parts[:1] == ["reports"]:
                is_write = (len(parts) == 1 and method == "POST") or \
                           (len(parts) == 2 and method == "PATCH")
                if is_write and not n.is_leader:
                    return self._forward_to_leader(method, raw)
                if len(parts) == 1 and method == "POST":
                    return self._send(201, store.create_report(self._parse(raw)))
                if len(parts) == 1 and method == "GET":
                    return self._send(200, store.list_reports(query))
                if len(parts) == 2 and method == "GET":
                    return self._send(200, store.get_report(parts[1]))
                if len(parts) == 2 and method == "PATCH":
                    return self._send(200, store.update_report(parts[1], self._parse(raw)))
                if len(parts) == 3 and parts[2] == "history" and method == "GET":
                    return self._send(200, store.history(parts[1]))
            self._send(404, {"error": "no such route"})
        except ValidationError as e:
            self._send(400, {"error": str(e)})
        except VersionConflict as e:
            self._send(409, {"error": "version conflict",
                             "current_version": e.current["version"], "current": e.current})
        except NotFound as e:
            self._send(404, {"error": f"report {e} not found"})
        except ReplicationGap as e:
            self._send(409, {"error": "gap", "last_seq": e.last_seq})
        except ReplicationError as e:
            self._send(503, {"error": str(e)})

    def do_GET(self): self._route("GET")
    def do_POST(self): self._route("POST")
    def do_PATCH(self): self._route("PATCH")

    def log_message(self, fmt, *args):
        if not self.node.access_log or "/internal/" in self.path or self.path in ("/health", "/nodes"):   # keep chatter out of the demo output
            return
        # Raw IP: address_string() does a slow reverse-DNS lookup.
        print(f"[{self.node.node_id}] {self.client_address[0]} {fmt % args}", flush=True)


class NodeHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a bigger accept backlog. The default is 5: under bursts of
    concurrent connections (load tests, replication fan-out) the OS silently drops the overflow
    and the sender stalls ~1s, which showed up as failed replications and client retries."""
    request_queue_size = 512

    def __init__(self, *args, **kw):
        self._conns, self._conns_lock = set(), threading.Lock()
        super().__init__(*args, **kw)

    # Remember open connections so server_close() can really cut them. With keep-alive, a
    # "stopped" node would otherwise keep answering on connections that are already open
    # (matters for in-process tests that simulate a crash).
    def get_request(self):
        sock, addr = super().get_request()
        with self._conns_lock:
            self._conns.add(sock)
        return sock, addr

    def shutdown_request(self, request):
        with self._conns_lock:
            self._conns.discard(request)
        super().shutdown_request(request)

    def server_close(self):
        super().server_close()
        with self._conns_lock:
            conns, self._conns = list(self._conns), set()
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def make_server(node_id, port, data_dir, nodes=None, initial_leader="N1", host="127.0.0.1",
                hb_interval=1.0, dead_after=3.0, quiet=False, access_log=True):
    """Build one node (not yet listening, and for clusters call node.start() afterwards).
    With nodes=None it is a single-node cluster that is its own leader."""
    store = Store(node_id, data_dir)       # <- WAL replay happens here
    node = Node(node_id, store, nodes or {node_id: f"{host}:{port}"}, initial_leader,
                hb_interval, dead_after, quiet=quiet)
    node.access_log = access_log           # False for benchmarks: printing every request skews timings
    handler = type("NodeHandler", (Handler,), {"node": node})   # one handler class per node
    srv = NodeHTTPServer((host, port), handler)
    srv.node = node
    return srv


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="N1, N2 or N3 (must exist in the config)")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--no-access-log", action="store_true", help="don't print one line per request")
    a = ap.parse_args()
    cfg = load_config(a.config)
    host, port = cfg["nodes"][a.id].rsplit(":", 1)
    srv = make_server(a.id, int(port), a.data_dir, cfg["nodes"],
                      cfg.get("initial_leader", "N1"), host,
                      cfg.get("heartbeat_interval", 1.0), cfg.get("dead_after", 3.0),
                      access_log=not a.no_access_log)
    print(f"[{a.id}] listening on {host}:{port}; replayed WAL up to seq {srv.node.store.wal.last_seq}",
          flush=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    srv.node.start()        # probe peers, join/lead, catch up, start heartbeats
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
