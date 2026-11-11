"""Command-line client.   python3 -m csn.cli --help"""
import argparse
import json
import os
import sys

from .client import Client, ApiError, LiveFeed


def main():
    ap = argparse.ArgumentParser(prog="csn.cli")
    ap.add_argument("--node", help="node URL(s), comma separated; default: all nodes in --config")
    ap.add_argument("--config", default="config.json")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit", help="resident: post a new report")
    s.add_argument("--type", required=True, choices=["power", "water", "road", "other"])
    s.add_argument("--location", required=True)
    s.add_argument("--description", required=True)
    s.add_argument("--user", required=True)

    l = sub.add_parser("list", help="list/filter reports")
    l.add_argument("--type"); l.add_argument("--status"); l.add_argument("--location")

    g = sub.add_parser("get", help="show one report")
    g.add_argument("id")

    u = sub.add_parser("update", help="update a report (moderators set status)")
    u.add_argument("id")
    u.add_argument("--user", required=True)
    u.add_argument("--base-version", type=int, required=True,
                   help="the version you last read (see `get`); stale => 409")
    u.add_argument("--role", default="resident", choices=["resident", "moderator"])
    u.add_argument("--description")
    u.add_argument("--status", choices=["open", "verified", "resolved"])

    w = sub.add_parser("watch", help="live feed viewer: print changes as they happen (Ctrl-C to stop)")
    w.add_argument("--type"); w.add_argument("--status"); w.add_argument("--location")

    a = ap.parse_args()
    if a.node:
        nodes = a.node.split(",")
    elif os.path.exists(a.config):
        with open(a.config) as f:
            nodes = ["http://" + addr for addr in json.load(f)["nodes"].values()]
    else:
        nodes = ["http://127.0.0.1:8001"]
    c = Client(nodes)
    if a.cmd == "watch":
        feed = LiveFeed(nodes, type=a.type, status=a.status, location=a.location)
        try:
            for ev in feed.events():
                r = ev["report"]
                print(f"[seq {ev['seq']:>3} via {ev['served_by']}] {ev['op']:<7} {r['id']} "
                      f"{r['type']:<5} {r['location']}: {r['description']} "
                      f"(status={r['status']}, v{r['version']}, by {r['updated_by']})", flush=True)
        except KeyboardInterrupt:
            pass
        return
    try:
        if a.cmd == "submit":
            out = c.submit(a.type, a.location, a.description, a.user)
        elif a.cmd == "list":
            out = c.list(type=a.type, status=a.status, location=a.location)
        elif a.cmd == "get":
            out = c.get(a.id)
        else:
            fields = {k: v for k, v in (("description", a.description), ("status", a.status)) if v}
            out = c.update(a.id, a.user, a.base_version, a.role, **fields)
        print(json.dumps(out, indent=2))
    except ApiError as e:
        print(e, file=sys.stderr); sys.exit(1)
    except ConnectionError as e:
        print(e, file=sys.stderr); sys.exit(2)


if __name__ == "__main__":
    main()
