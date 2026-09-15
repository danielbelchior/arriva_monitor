#!/usr/bin/env python3
"""Show the next ETA(s) per Arriva bus line at one or more stops.

Data comes from the same Meteor DDP feed behind https://viewer.arriva.nl:
  - subscription "public_journey_passing_time" (params: user_stop_id, null)
    gives scheduled passing times per stop, in seconds since local midnight;
  - subscription "actual_journeys" gives live vehicles, including the
    "punctuality" offset (seconds) used to turn scheduled times into ETAs.

Usage:
    pip install websocket-client
    python arriva_eta.py                                # Son en Breugel + Eindhoven CS
    python arriva_eta.py --stops 64150080 --lines 156 --count 2
"""

import argparse
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import websocket

DDP_URL = "wss://service.arriva.nl/websocket"
LOCAL_TZ = ZoneInfo("Europe/Amsterdam")
DEFAULT_STOPS = ["64150080", "64004020"]
DEFAULT_LINES = ["154", "156", "157"]
READY_TIMEOUT_S = 30

# Friendly names for known stops, used in the output.
STOP_NAMES = {
    "64150080": "Son en Breugel",
    "64004020": "Eindhoven Central Station",
}

# Internal line_id -> public line number. The DDP feed only exposes the public
# number on *live* journeys, so this seeds the mapping for lines that have no
# bus on the road right now. 22156=156 was verified against the live feed;
# 22154/22157 follow by elimination (they are the only other lines serving
# stop 64150080). Entries learned from live data are merged on top at runtime.
KNOWN_LINE_IDS = {22154: "154", 22156: "156", 22157: "157"}


class ArrivaDDP:
    """Minimal DDP client: connect, subscribe, collect the initial snapshot."""

    def __init__(self, stop_ids):
        self.stop_ids = list(stop_ids)
        self.journeys = {}  # actual_journeys docs by id
        self.passing_times = {s: {} for s in self.stop_ids}  # per stop, by id

    def fetch(self, timeout=READY_TIMEOUT_S):
        ws = websocket.create_connection(DDP_URL, timeout=15, suppress_origin=True)
        wanted_ready = {"journeys"} | {self._sub_id(s) for s in self.stop_ids}
        try:
            ws.send(
                json.dumps(
                    {"msg": "connect", "version": "1", "support": ["1", "pre2", "pre1"]}
                )
            )
            ready = set()
            deadline = time.time() + timeout
            while time.time() < deadline and not wanted_ready <= ready:
                try:
                    msg = json.loads(ws.recv())
                except websocket.WebSocketTimeoutException:
                    continue
                kind = msg.get("msg")
                if kind == "connected":
                    ws.send(
                        json.dumps(
                            {
                                "msg": "sub",
                                "id": "journeys",
                                "name": "actual_journeys",
                                "params": [
                                    {"transport_type": {"$in": ["TRAIN", "BUS"]}}
                                ],
                            }
                        )
                    )
                    for stop in self.stop_ids:
                        ws.send(
                            json.dumps(
                                {
                                    "msg": "sub",
                                    "id": self._sub_id(stop),
                                    "name": "public_journey_passing_time",
                                    "params": [stop, None],
                                }
                            )
                        )
                elif kind == "ping":
                    ws.send(json.dumps({"msg": "pong", "id": msg.get("id")}))
                elif kind in ("added", "changed"):
                    self._store(
                        msg.get("collection"),
                        msg["id"],
                        msg.get("fields", {}),
                        update=(kind == "changed"),
                    )
                elif kind == "ready":
                    ready.update(msg.get("subs", []))
            missing = wanted_ready - ready
            if missing:
                raise TimeoutError(f"subscriptions not ready in time: {missing}")
            return self
        finally:
            ws.close()

    @staticmethod
    def _sub_id(stop_id):
        return f"passing:{stop_id}"

    def _store(self, collection, doc_id, fields, update=False):
        if collection == "actual_journeys":
            target = self.journeys
        elif collection == "public_journey_passing_time":
            stop = fields.get("user_stop_id")
            target = self.passing_times.get(stop)
            if target is None:
                return
        else:
            return
        if update:
            target.setdefault(doc_id, {}).update(fields)
        else:
            target[doc_id] = fields


def build_eta_list(ddp, line_numbers, stop_id):
    """Join scheduled passing times with live punctuality; return sorted ETAs."""
    wanted = {str(n) for n in line_numbers}

    # line_id -> public line number: known mapping first, live feed on top
    line_number_by_id = dict(KNOWN_LINE_IDS)
    for j in ddp.journeys.values():
        line = j.get("line") or {}
        if j.get("line_id") is not None and line.get("number") is not None:
            line_number_by_id[j["line_id"]] = str(line["number"])

    # live punctuality (seconds) keyed by (line_id, journey_id).
    # The feed encodes NaN/Infinity as EJSON {"$InfNaN": ...} dicts; skip those.
    punctuality = {}
    for j in ddp.journeys.values():
        p = j.get("punctuality")
        if isinstance(p, (int, float)):
            punctuality[(j.get("line_id"), j.get("journey_id"))] = p

    now = datetime.now(LOCAL_TZ)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

    etas = []
    for pt in ddp.passing_times.get(stop_id, {}).values():
        number = line_number_by_id.get(pt.get("line_id"))
        if number not in wanted:
            continue
        delay = punctuality.get((pt.get("line_id"), pt.get("journey_id")))
        target_dep = pt.get("target_departure_time")
        target_arr = pt.get("target_arrival_time")
        if target_dep is None and target_arr is None:
            continue
        scheduled = midnight + timedelta(seconds=target_dep or target_arr)
        expected = (
            scheduled + timedelta(seconds=delay) if delay is not None else scheduled
        )
        if expected <= now:
            continue  # already passed
        etas.append(
            {
                "stop": stop_id,
                "stop_name": STOP_NAMES.get(stop_id),
                "line": number,
                "journey_id": pt.get("journey_id"),
                "scheduled": scheduled.isoformat(),
                "expected": expected.isoformat(),
                "delay_seconds": delay,
                "live": delay is not None,
                "in_minutes": round((expected - now).total_seconds() / 60),
            }
        )
    etas.sort(key=lambda e: e["expected"])
    return etas


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--stops",
        nargs="+",
        default=DEFAULT_STOPS,
        help="user_stop_id(s) of the bus stop(s) (default: %(default)s)",
    )
    ap.add_argument(
        "--lines",
        nargs="+",
        default=DEFAULT_LINES,
        help="line numbers (default: %(default)s)",
    )
    ap.add_argument(
        "--count",
        type=int,
        default=2,
        help="number of upcoming ETAs per line (default: %(default)s)",
    )
    ap.add_argument("--json", action="store_true", help="print raw JSON")
    args = ap.parse_args()

    ddp = ArrivaDDP(args.stops).fetch()
    etas_by_stop = {s: build_eta_list(ddp, args.lines, s) for s in args.stops}

    if args.json:
        print(json.dumps(etas_by_stop, indent=2, ensure_ascii=False))
        return

    now = datetime.now(LOCAL_TZ)
    print(f"{now:%H:%M} local time")
    for stop in args.stops:
        name = STOP_NAMES.get(stop)
        print(f"\nStop {stop}" + (f" ({name}):" if name else ":"))
        if not ddp.passing_times.get(stop):
            print(
                "  (no passing-time data at all for this stop - "
                "check the stop ID or whether it has Arriva service today)"
            )
            continue
        for ln in args.lines:
            upcoming = [e for e in etas_by_stop[stop] if e["line"] == str(ln)][
                : args.count
            ]
            if not upcoming:
                print(f"  Line {ln}: no upcoming buses in the feed")
                continue
            print(f"  Line {ln}:")
            for e in upcoming:
                exp = datetime.fromisoformat(e["expected"])
                sch = datetime.fromisoformat(e["scheduled"])
                live = (
                    f"live, delay {e['delay_seconds'] // 60:+d} min"
                    if e["live"]
                    else "scheduled only"
                )
                print(
                    f"    {exp:%H:%M} (in {e['in_minutes']} min"
                    f" | planned {sch:%H:%M} | {live})"
                )


if __name__ == "__main__":
    main()
