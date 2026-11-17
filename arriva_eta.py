#!/usr/bin/env python3
"""Show the next ETA(s) per Arriva bus line at one or more stops.

Data comes from the same Meteor DDP feed behind https://viewer.arriva.nl:
  - subscription "public_journey_passing_time" (params: user_stop_id, null)
    gives scheduled passing times per stop, in seconds since local midnight;
  - subscription "actual_journeys" gives live vehicles, including the
    "punctuality" offset (seconds) used to turn scheduled times into ETAs.

The next buses per stop are also pushed to Home Assistant through the local
(gitignored) ha.py module, unless --no-ha is given.

Usage:
    pip install -r requirements.txt
    python arriva_eta.py                                # Son en Breugel + Eindhoven CS
    python arriva_eta.py --stops 64150080 --lines 156 --count 2 --no-ha
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import websocket

DDP_URL = "wss://service.arriva.nl/websocket"
LOCAL_TZ = ZoneInfo("Europe/Amsterdam")
DEFAULT_LINES = ["154", "156", "157"]
READY_TIMEOUT_S = 30

# Every stop ID serves a single direction. Lines 154/156/157 run odd journey
# numbers towards Eindhoven and even ones away from it:
#   64150080 Son en Breugel, platform towards Eindhoven
#   64004020 Eindhoven CS, platform where buses *from* Son arrive
#   64004010 Eindhoven CS, platform where buses *towards* Son depart
#   64150090 Son en Breugel, platform where buses from Eindhoven arrive
STOP_NAMES = {
    "64150080": "Son en Breugel, towards Eindhoven",
    "64004010": "Eindhoven Central Station, towards Son",
    "64004020": "Eindhoven Central Station, arriving from Son",
    "64150090": "Son en Breugel, arriving from Eindhoven",
}
DEFAULT_STOPS = ["64150080", "64004010"]

# Home Assistant entities per stop: text summary + datetimes of the next two buses.
HA_ENTITIES = {
    "64150080": (
        "input_text.bus_from_son",
        ["input_datetime.bus_next_from_son", "input_datetime.bus_next_from_son_2"],
    ),
    "64004010": (
        "input_text.bus_from_eindhoven",
        [
            "input_datetime.bus_next_from_eindhoven",
            "input_datetime.bus_next_from_eindhoven_2",
        ],
    ),
}

# Internal line_id -> public line number. The DDP feed only exposes the public
# number on *live* journeys, so this seeds the mapping for lines that have no
# bus on the road right now. 22154=154 and 22156=156 were verified against the
# live feed; 22157=157 follows by elimination (the only other line serving
# these stops). Entries learned from live data are merged on top at runtime.
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


def service_day_start(day):
    """Reference point for 'seconds since midnight' timetable values.

    Like GTFS, this is noon minus 12 hours, so the offsets stay correct on the
    days the clocks change (where real midnight is 11 or 13 hours before noon).
    """
    noon = datetime(day.year, day.month, day.day, 12, tzinfo=LOCAL_TZ)
    return noon - timedelta(hours=12)


def scheduled_time(seconds, now):
    """Turn a timetable offset into an aware datetime.

    Offsets of 24h or more belong to the previous service day's late-night
    trips (e.g. 24:30 = 00:30 tonight); use yesterday as base while that
    still lies in the future.
    """
    today = service_day_start(now.date())
    if seconds >= 86400:
        yesterday = service_day_start(now.date() - timedelta(days=1))
        candidate = yesterday + timedelta(seconds=seconds)
        if candidate > now:
            return candidate
    return today + timedelta(seconds=seconds)


def build_eta_list(ddp, line_numbers, stop_id, now=None):
    """Join scheduled passing times with live punctuality; return sorted ETAs."""
    wanted = {str(n) for n in line_numbers}
    now = now or datetime.now(LOCAL_TZ)

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
        if isinstance(p, (int, float)) and not isinstance(p, bool):
            punctuality[(j.get("line_id"), j.get("journey_id"))] = round(p)

    etas = []
    for pt in ddp.passing_times.get(stop_id, {}).values():
        number = line_number_by_id.get(pt.get("line_id"))
        if number not in wanted:
            continue
        offset = pt.get("target_departure_time")
        if offset is None:
            offset = pt.get("target_arrival_time")
        if offset is None:
            continue
        delay = punctuality.get((pt.get("line_id"), pt.get("journey_id")))
        scheduled = scheduled_time(offset, now)
        expected = scheduled + timedelta(seconds=delay or 0)
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


def delay_label(e):
    if not e["live"]:
        return "scheduled only"
    return f"live, delay {round(e['delay_seconds'] / 60):+d} min"


def format_eta(e):
    """Format an ETA as 'line 156 at 14:32 in 5 min (live, delay +1 min)'."""
    exp = datetime.fromisoformat(e["expected"])
    return f"{e['line']} at {exp:%H:%M} in {e['in_minutes']} min ({delay_label(e)})"


def print_report(ddp, etas_by_stop, lines, count):
    now = datetime.now(LOCAL_TZ)
    print(f"{now:%H:%M} local time")
    for stop, etas in etas_by_stop.items():
        name = STOP_NAMES.get(stop)
        print(f"\nStop {stop}" + (f" ({name}):" if name else ":"))
        if not ddp.passing_times.get(stop):
            print(
                "  (no passing-time data at all for this stop - "
                "check the stop ID or whether it has Arriva service today)"
            )
            continue
        for ln in lines:
            upcoming = [e for e in etas if e["line"] == str(ln)][:count]
            if not upcoming:
                print(f"  Line {ln}: no upcoming buses in the feed")
                continue
            print(f"  Line {ln}:")
            for e in upcoming:
                exp = datetime.fromisoformat(e["expected"])
                sch = datetime.fromisoformat(e["scheduled"])
                print(
                    f"    {exp:%H:%M} (in {e['in_minutes']} min"
                    f" | planned {sch:%H:%M} | {delay_label(e)})"
                )


def push_to_home_assistant(etas_by_stop):
    """Send the next two buses (any of the requested lines) per stop to HA."""
    from ha import update_datetime, update_state  # local, gitignored module

    for stop, (text_entity, datetime_entities) in HA_ENTITIES.items():
        if stop not in etas_by_stop:
            continue
        upcoming = etas_by_stop[stop][: len(datetime_entities)]
        update_state(text_entity, "\n".join(format_eta(e) for e in upcoming))
        for i, entity in enumerate(datetime_entities):
            value = (
                datetime.fromisoformat(upcoming[i]["expected"])
                if i < len(upcoming)
                else None
            )
            update_datetime(entity, value)


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
    ap.add_argument(
        "--no-ha", action="store_true", help="do not push results to Home Assistant"
    )
    args = ap.parse_args()

    ddp = ArrivaDDP(args.stops).fetch()
    etas_by_stop = {s: build_eta_list(ddp, args.lines, s) for s in args.stops}

    if args.json:
        print(json.dumps(etas_by_stop, indent=2, ensure_ascii=False))
    else:
        print_report(ddp, etas_by_stop, args.lines, args.count)

    if not args.no_ha:
        try:
            push_to_home_assistant(etas_by_stop)
        except Exception as e:
            print(f"Home Assistant update failed: {e!r}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
