# Community Status Network

A fault-tolerant distributed service where residents post and update live status reports
(power cuts, water points, blocked roads), moderators verify and resolve them, and viewers watch
changes in real time. Built for the Distributed Systems project (BSE 4103 / BCS 3107).

* **Python 3 standard library only** for the system (`http.server`, `urllib`, `http.client`,
  `threading`, `json`, `unittest`, `concurrent.futures`).
* matplotlib is used **only** to draw benchmark charts (`bench/charts.py`).
* Three server processes (N1, N2, N3) on ports 8001-8003, or three laptops on one LAN.

## Set up from a fresh clone

You need **Python 3** (developed and tested on Python 3.14, macOS) and **git**. There is nothing to install:
no `pip install`, no virtual environment, no database. Ports 8001, 8002 and 8003 must be free.

**1. Get the code**

```bash
git clone https://github.com/W2k3/community-status-network.git
cd community-status-network
```

**2. Check Python**

```bash
python3 --version
```

On Windows the command is usually `python` instead of `python3`; use it in every step below.
(Windows has not been tested.)

**3. Check that everything works on your machine** (about 45 seconds, optional but recommended)

```bash
python3 run_tests.py
```

The last line should read `TOTAL          46/46 passed`.

**4. Start the system**

Easiest, one terminal: the guided demo starts all three nodes, walks through the features (including killing
and restarting the leader), then leaves the nodes running.

```bash
python3 demo.py
```

Or, to just start the three nodes without the walkthrough, open three terminals in the project folder and
run one command in each:

```bash
python3 -m csn.server --id N1
python3 -m csn.server --id N2
python3 -m csn.server --id N3
```

Each prints `listening on 127.0.0.1:800x`, and N1 prints `I AM THE LEADER`.

**5. Open the web app**

Go to <http://127.0.0.1:8002/> in a browser (8001 and 8003 show the same thing). Click the round badge at the
top right to set your name and role, then press **Report an issue**.

**6. Check it from a terminal** (optional, in a new terminal in the project folder)

```bash
python3 -m csn.cli submit --type water --location Kira --description "Borehole dry" --user alice
python3 -m csn.cli list
```

The report you submit here appears in the browser immediately.

**7. Stop and reset**

Press Ctrl-C in each terminal that runs a node (or once in the `demo.py` terminal). Reports are kept in the
`data/` folder and are still there next time you start. To start with an empty board, stop the nodes and
delete it:

```bash
rm -rf data
```

**If something goes wrong**

| Problem | Fix |
|---|---|
| `Address already in use` | Something else is using port 8001-8003, often a node you started earlier. Stop it, or change the ports in `config.json`. |
| `python3: command not found` | Install Python 3 from <https://www.python.org/downloads/>, or try `python`. |
| `No module named csn` | You are not in the project folder. `cd` into it and run the command again. |
| The page says "Reconnecting..." forever | No node is running. Start them (step 4). |
| The page says "Only 1 of 3 nodes is up" | Start a second node; changes work again straight away. |

A plain-language explanation of how the system works is in `docs/system_guide.tex`.

## Quick start (one command)

```bash
python3 demo.py
```

Starts the 3 nodes and walks through every requirement with narration: submit via a follower,
update, a conflicting edit (409), reads from every node, a live viewer, **killing the leader**,
automatic failover, and **restarting** it with WAL replay and catch-up. Afterwards the cluster keeps
running; open <http://127.0.0.1:8002/> for the browser live feed, or use the CLI. Ctrl-C stops everything.

`python3 demo.py --pause` waits for Enter between steps (good for the video), `--no-keep` exits when done.

## Run it by hand

Three terminals, from the project folder:

```bash
python3 -m csn.server --id N1      # initial leader
python3 -m csn.server --id N2
python3 -m csn.server --id N3
```

**On 3 laptops:** edit `config.json` so each node has its LAN address
(`"N1": "192.168.1.10:8001"`, ...), copy the project to each laptop, and run
`python3 -m csn.server --id N<k>` on laptop *k*. Nodes bind to the address in the config.
Optional keys: `"heartbeat_interval"` (default 1.0 s) and `"dead_after"` (default 3.0 s).

### Command-line client

The CLI reads all node addresses from `config.json` and fails over between them automatically.

```bash
python3 -m csn.cli submit --type water --location Kira --description "Borehole dry" --user alice
python3 -m csn.cli list [--type water] [--status open] [--location kira]
python3 -m csn.cli get R1
python3 -m csn.cli update R1 --user mod1 --role moderator --base-version 1 --status verified
python3 -m csn.cli watch [--type water]          # live feed viewer (Ctrl-C to stop)
```

`update` needs `--base-version`: the version you last read (see `get`). If someone else changed the
report meanwhile you get **HTTP 409** and the current version. `--node URL[,URL...]` targets specific nodes.

### Web app

Open `http://127.0.0.1:800x/` on any node (phone or laptop; light and dark themes). From the page you can:

* **Report an issue** (type, location, description, your name) and **edit** a description.
* **Verify / resolve / reopen** as a moderator. Click your name at the top right to set your name and role
  (remembered in the browser; like the CLI, the role is only claimed).
* **Open a report** to see its status and its full **history** (every version, read from the WAL).
* Watch the **live feed** (`EventSource`), the **cluster card** (leader, followers, lag, dead nodes, polled from
  each node's `/health`) and the **live activity** log.

What it does when things go wrong:

* **Its node dies:** the feed moves to the next node, resumes from the last event id and shows
  "Reconnected through N3 ... you missed nothing".
* **A write hits a dead node or an election:** the page retries with backoff on the next node (same rules as
  `csn/client.py`) and tells the user the change will go through by itself. Submits carry a `request_id`, so a
  retry can never create a duplicate.
* **Someone else edited first (409):** a dialog shows the latest version next to your unsaved edit; you choose to
  apply your edit on top of the latest or discard it. Nothing is overwritten silently.

Everything is one file, `web/live.html` (no build step, no dependencies). Fonts (Inter, Space Grotesk, JetBrains
Mono; SIL Open Font License) are served by the node from `web/fonts/`, so the page works on a LAN without internet.

## REST API

| Method & path | Purpose |
|---|---|
| `POST /reports` | submit: `type`, `location`, `description`, `updated_by`, optional `request_id` (idempotency key) -> 201 + report |
| `GET /reports?type=&status=&location=` | list / filter (served by any node) |
| `GET /reports/<id>` | one report |
| `GET /reports/<id>/history` | every version of one report, oldest first (from the WAL) |
| `PATCH /reports/<id>` | update: `updated_by`, `base_version`, optional `description`, `status`, `role`. 409 if stale |
| `GET /events?type=&status=&location=` | Server-Sent Events live feed (`Last-Event-ID` / `?last_id=` to resume) |
| `GET /health`, `GET /nodes` | node state (role, leader, term, last_seq) / node addresses |
| `POST /internal/replicate`, `POST /internal/heartbeat`, `GET /internal/log` | node-to-node |

Status codes: 400 invalid input, 404 unknown report, **409 version conflict**, 413 body over 16 KB,
503 no leader / no majority. Limits: location 120, description 500, name 40 characters.
`OPTIONS` on any path answers the browser's CORS preflight (the web app writes to other nodes on failover).
Report types: `power`, `water`, `road`, `other`. Statuses: `open`, `verified`, `resolved`
(only a client claiming `role: moderator` may set the last two).

## Architecture

```
 Resident CLI    Moderator CLI    Live-feed viewer (CLI / browser)
        \              |              /
         +--- Client library: timeout, retry + backoff, failover ---+
                              | A) HTTP / REST / JSON  (+ SSE for the feed)
      +-----------------------+-----------------------+
      |                       |                       |
 +----v-----+  B) replicate +-v--------+ B)       +---v------+
 | N1       |-------------->| N2       |<-------->| N3       |
 | (leader) |  C) heartbeat | follower | C) hb &  | follower |
 | API  SSE |<------------->|          | election |          |
 | Coordinator (locks+versions)  Store + WAL on each node     |
 +----------+               +----------+          +----------+
```

Each node has: **API layer** (REST + SSE) -> **Coordinator** (per-record locks and version checks, in
`store.py`) -> **Store + write-ahead log** (`store.py`, `wal.py`). `node.py` holds cluster behaviour
(replication, heartbeats, election, catch-up). Links: **A** client to node, **B** leader to followers
(each write), **C** node to node (heartbeat and election).

**Data model:** USER (a name string plus a claimed role), REPORT (`id, type, location, description, status,
version, updated_by, updated_at`), REPORT_UPDATE (every edit is a WAL entry, so the log is the history),
LOG_ENTRY (`seq, op, payload, node_id`), NODE (id, address, role, term).

## Coordination (how writes stay correct)

1. **Followers forward writes to the leader** and relay its reply unchanged (including 409).
2. The leader takes a **per-record lock**, compares the client's `base_version` with the stored
   `version`. If stale: **409** with the current version, nothing changes. If current: apply, `version + 1`,
   append to the **WAL** (fsync), **replicate**, then acknowledge. The check and the write happen under the
   same lock, so two edits based on version N can never both succeed. Edits to different reports run in parallel.
3. **Replication is synchronous to a majority.** The leader replies only after itself plus at least one
   follower (2 of 3) have stored the entry; the third finishes in the background.
   *Trade-off:* every write pays one network round trip, but an acknowledged write survives the loss of any
   one node. Waiting for all followers would block writes whenever a node is down; not waiting (asynchronous)
   is fastest but could lose acknowledged writes if the leader dies.
4. **Reads are served by whichever node is asked, from its local copy.** Staleness risk: the follower that was
   not part of the majority may lag by milliseconds, so a read there can briefly miss a write you just got an
   acknowledgement for. Read from the leader (or retry) if you need the latest.

## Fault handling

* **Heartbeats** every 1 s to all peers; a peer silent for 3 s is dead. If the leader is dead, every live node
  picks the **lowest-ID live node** as the new leader. Measured: writes are back after **about 3.3 s**.
* **Terms.** Each election increments a term number carried in heartbeats and replication. Nodes follow the
  highest term, so an old or cut-off leader learns it was replaced and steps down, and followers refuse
  replication from a stale-term leader.
* **No acknowledged write is lost.** A new leader first catches up from the live peers before accepting writes.
* **Client library** (`csn/client.py`): per-request timeout, at most 8 attempts, exponential backoff
  (0.25 s, 0.5 s, 1 s, 2 s ...) with jitter, failover to the next node on connection errors and 502/503/504.
  400/404/409 are real answers and are not retried.
* **Restart.** A node replays its WAL at startup, asks its peers who leads, joins as a **follower** (a returning
  ex-leader does not take leadership back) and fetches the entries after its last `seq`. If its log disagrees with
  the leader's at that point (it held a write that was never acknowledged), it discards the log (kept as
  `*.wal.diverged-<time>`) and re-downloads. A half-written last WAL line from a crash is ignored and trimmed.
* **Live feed.** Every node publishes an event whenever it applies a WAL entry, so a viewer on any node sees
  every change. Event ids are WAL sequence numbers; a reconnecting viewer sends `Last-Event-ID` and the node
  replays what it missed.

## Limits (be honest in the demo and the report)

* **Tolerates the loss of 1 of 3 nodes.** With 2 nodes down there is no majority and writes fail with 503
  (reads from the survivor still work).
* **Possible split-brain during a network partition.** A cut-off leader keeps believing it leads. It cannot reach
  a majority, so its writes fail with 503, but it has already applied them locally, so reads from it can show
  writes that never committed. When the partition heals the higher term wins and the loser resyncs.
* **Not Raft or Paxos.** There are no votes, terms are not persisted, and the lowest-ID rule is a simplification.
* **No authentication.** User names and the moderator role are claimed by the client; CORS is open (`*`).
* **A write that fails with 503 is applied on the leader but not replicated** until catch-up (or discarded by
  the divergence resync). A client that sees 503 should re-read before retrying.
* **Retries are safe only when the client sends a `request_id`** (the web app, the client library and the CLI do):
  a retried submit then returns the report it already created. A raw `POST /reports` without one can still
  duplicate. Updates are safe to retry because `base_version` turns the second try into a 409.
* **Failover takes about 3.3 s** (3 s dead-leader timeout plus election); writes are unavailable during it,
  reads keep working.
* **Creates serialize** (id allocation and replication share one lock), so submits are slower than updates under
  heavy load (see benchmarks). No snapshots or log compaction; catch-up sends the log in batches of 2,000 entries.

## Tests and evidence

```bash
python3 run_tests.py                 # all 46 tests; writes results/test_results.txt
python3 -m unittest tests.test_stage4 -v     # one stage
```

| Stage | Tests | What they prove |
|---|---:|---|
| 1 | 6 | REST API, filtering, validation, WAL replay after restart |
| 2 | 6 | two threads edit the same report: exactly one 200 and one 409; 10 threads: one winner; locks are per record |
| 3 | 7 | replication to all nodes, follower forwarding, 409 relayed, identical WALs under concurrent writes, majority rules |
| 4 | 8 | election, client failover, no loss of acknowledged writes, restart replay + catch-up, divergence resync, stale-term refusal |
| 5 | 9 | SSE on any node, filters, `Last-Event-ID` replay, viewer failover, ordering, HTML page |
| 6 | 10 | report history, idempotent submit (across nodes, failover and restart), input limits, CORS preflight, fonts |

Latest log: `results/test_results.txt` (46/46 passed).

## Benchmarks

```bash
python3 bench/load_test.py --clients 10,50,100 --duration 15     # load test
python3 bench/failure_test.py --trials 3                         # kill the leader under load, restart it
python3 -m venv .venv-charts && .venv-charts/bin/pip install matplotlib   # once
.venv-charts/bin/python bench/charts.py results                  # charts
```

Real node processes, one laptop (8 CPUs). The load generator shares the CPU with the nodes, so compare levels
with each other rather than reading the numbers as absolute capacity.

| Clients | Throughput | Avg | p95 | p99 | Success rate | 409 (correct rejections) | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 4,681 req/s | 2.1 ms | 9.6 ms | 17 ms | 100% | 2.5% | 0 |
| 50 | 4,775 req/s | 10.5 ms | 76 ms | 116 ms | 100% | 3.5% | 0 |
| 100 | 4,302 req/s | 23.1 ms | 187 ms | 262 ms | 100% | 3.7% | 0 |

After every run all three nodes hold identical data and the WAL length equals the number of acknowledged writes.

Leader killed with SIGKILL under load (3 trials): election **3.3 s**, writes available again **3.3 s**, a client
write issued during the outage completed in **3.4-5.1 s** (mean 4.0 s; retry + failover), **0 failed reads**,
**0 lost acknowledged writes** (0 of 3,982), restarted node serving again in **0.08 s** and caught up
(963-1,202 entries behind) in **0.13 s**, rejoining as a follower with WALs identical.

Charts and raw data: `results/` (`load_latency.png`, `load_throughput.png`, `load_success.png`,
`failover_timeline.png`, `failover_trials.png`, `*.json`, `*.csv`, `*.md`, node logs in `results/logs/`).

Benchmarking found a real bug: with one new TCP connection per request, a busy laptop ran out of ephemeral ports
(`Can't assign requested address`), which showed up as failed replication and client retries. Fixed with HTTP/1.1
keep-alive and a connection pool (`csn/netutil.py`).

## Project layout

```
csn/wal.py       write-ahead log (JSON lines, fsync, torn-tail repair, in-memory index)
csn/store.py     in-memory reports, per-record locks, version checks, follower-side apply
csn/node.py      replication, heartbeats, terms, election, catch-up, divergence resync
csn/server.py    HTTP server: REST + SSE + internal endpoints + HTML page
csn/feed.py      publish/subscribe bus behind the live feed
csn/netutil.py   keep-alive connection pool
csn/client.py    client library (timeout, retry, backoff, failover) + LiveFeed viewer
csn/cli.py       command-line client
web/live.html    web app (reports, submit, moderation, live feed, cluster view)      web/fonts/   its fonts
config.json      node addresses
demo.py          one-command demo      run_tests.py   test runner + log
tests/           unit and integration tests (stages 1-6)
bench/           load test, failure test, charts       results/   evidence
docs/REPORT_OUTLINE.md   skeleton and split of work for the written report and video
```
