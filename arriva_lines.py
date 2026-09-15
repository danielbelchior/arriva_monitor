#!/usr/bin/env python3
"""Extract live vehicle positions for Arriva bus lines from viewer.arriva.nl.

The viewer (viewer.arriva.nl) is a Meteor app whose live data comes from a
DDP websocket at wss://service.arriva.nl/websocket (subscription
"actual_journeys"). Route shapes are plain REST:
https://service.arriva.nl/api/route/{line_id}

Usage:
    pip install websocket-client
    python arriva_lines.py                 # lines 154, 156, 157 -> JSON on stdout
    python arriva_lines.py --lines 156 --routes   # include route geometry
    python arriva_lines.py --out buses.json
"""

import argparse
import json
import sys
import time
import urllib.request

import websocket

DDP_URL = "wss://service.arriva.nl/websocket"
ROUTE_URL = "https://service.arriva.nl/api/route/{line_id}"
DEFAULT_LINES = ["154", "156", "157"]
READY_TIMEOUT_S = 30


def fetch_actual_journeys(timeout=READY_TIMEOUT_S):
    """Subscribe to the live feed and return all current journey documents."""
    ws = websocket.create_connection(DDP_URL, timeout=15, suppress_origin=True)
    try:
        ws.send(json.dumps(
            {"msg": "connect", "version": "1", "support": ["1", "pre2", "pre1"]}))
        journeys = {}
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = json.loads(ws.recv())
            except websocket.WebSocketTimeoutException:
                continue
            kind = msg.get("msg")
            if kind == "connected":
                ws.send(json.dumps({
                    "msg": "sub", "id": "sub1", "name": "actual_journeys",
                    "params": [{"transport_type": {"$in": ["TRAIN", "BUS"]}}],
                }))
            elif kind == "ping":
                ws.send(json.dumps({"msg": "pong", "id": msg.get("id")}))
            elif kind == "added" and msg.get("collection") == "actual_journeys":
                journeys[msg["id"]] = msg.get("fields", {})
            elif kind == "changed" and msg.get("collection") == "actual_journeys":
                journeys.setdefault(msg["id"], {}).update(msg.get("fields", {}))
            elif kind == "ready" and "sub1" in msg.get("subs", []):
                break  # initial snapshot complete
        return journeys
    finally:
        ws.close()


def vehicles_for_lines(journeys, line_numbers):
    """Filter journey docs to live buses of the requested line numbers."""
    wanted = {str(n) for n in line_numbers}
    vehicles = []
    for doc_id, fields in journeys.items():
        line = fields.get("line") or {}
        if fields.get("transport_type") != "BUS" or str(line.get("number")) not in wanted:
            continue
        coords = (fields.get("geoJson") or {}).get("coordinates") or [None, None]
        vehicles.append({
            "line_number": line.get("number"),
            "line_name": line.get("name"),
            "line_id": fields.get("line_id"),
            "journey_id": fields.get("journey_id"),
            "vehicle_id": fields.get("vehicle_id"),
            "longitude": coords[0],
            "latitude": coords[1],
            "punctuality_seconds": fields.get("punctuality"),
            "user_stop_id": fields.get("user_stop_id"),
            "updated_at": fields.get("updated_at"),
            "_id": doc_id,
        })
    return vehicles


def fetch_route(line_id):
    """Fetch the GeoJSON route shape for a line_id (REST, no websocket)."""
    with urllib.request.urlopen(ROUTE_URL.format(line_id=line_id), timeout=15) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lines", nargs="+", default=DEFAULT_LINES,
                    help="line numbers (default: %(default)s)")
    ap.add_argument("--routes", action="store_true",
                    help="also fetch route geometry per line via the REST API")
    ap.add_argument("--out", help="write JSON to this file instead of stdout")
    args = ap.parse_args()

    journeys = fetch_actual_journeys()
    vehicles = vehicles_for_lines(journeys, args.lines)

    result = {"lines": {}, "vehicle_count": len(vehicles)}
    for ln in args.lines:
        result["lines"][str(ln)] = [v for v in vehicles if str(v["line_number"]) == str(ln)]

    if args.routes:
        for ln, vs in result["lines"].items():
            line_ids = {v["line_id"] for v in vs if v.get("line_id")}
            result["lines"][ln] = {
                "vehicles": vs,
                "routes": {str(i): fetch_route(i) for i in sorted(line_ids)},
            }

    payload = json.dumps(result, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w") as f:
            f.write(payload + "\n")
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        print(payload)

    for ln in args.lines:
        n = len(result["lines"][str(ln)]["vehicles"] if args.routes
                  else result["lines"][str(ln)])
        print(f"line {ln}: {n} live vehicle(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
