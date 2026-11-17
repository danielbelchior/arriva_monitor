#!/usr/bin/env python3
"""Live positions of Arriva buses between Eindhoven and Sint-Oedenrode.

Uses the same live feed as arriva_lines.py (subscription "actual_journeys" on
wss://service.arriva.nl/websocket) and keeps only buses of the requested lines
that are inside the Eindhoven - Sint-Oedenrode corridor, with their direction.

Usage:
    pip install -r requirements.txt
    python arriva_positions.py                 # lines 154, 156, 157
    python arriva_positions.py --lines 156 --json
"""

import argparse
import json
from datetime import datetime, timezone

from arriva_eta import KNOWN_LINE_IDS
from arriva_lines import fetch_actual_journeys

DEFAULT_LINES = ["154", "156", "157"]

# Corridor box. South edge is Eindhoven Central Station, north edge is the
# centre of Sint-Oedenrode. Between those latitudes the routes of 154/156/157
# stay within lon 5.462-5.497, so the longitude bounds only add a margin.
SOUTH_LAT, NORTH_LAT = 51.4430, 51.5675
WEST_LON, EAST_LON = 5.44, 5.52

# Journey numbers encode direction: odd towards Eindhoven, even away from it
# (towards Son and further north). Verified on the timetables of all three lines.
DIRECTION_BY_PARITY = {1: "to-eindhoven", 0: "to-son"}

STATUS_FINISHED = 4  # trip ended; the doc lingers with its last position
DEFAULT_MAX_AGE_S = 300


def in_corridor(lat, lon):
    return SOUTH_LAT <= lat <= NORTH_LAT and WEST_LON <= lon <= EAST_LON


def corridor_buses(journeys, line_numbers, max_age_s=DEFAULT_MAX_AGE_S, now=None):
    """Return live buses of the given lines inside the corridor, south to north."""
    wanted = {str(n) for n in line_numbers}
    now = now or datetime.now(timezone.utc)

    # The feed leaves "line" empty for some journeys, so map line_id too.
    line_number_by_id = dict(KNOWN_LINE_IDS)
    for f in journeys.values():
        number = (f.get("line") or {}).get("number")
        if f.get("line_id") is not None and number is not None:
            line_number_by_id[f["line_id"]] = str(number)

    buses = []
    for f in journeys.values():
        if f.get("transport_type") != "BUS" or f.get("status") == STATUS_FINISHED:
            continue
        number = line_number_by_id.get(f.get("line_id"))
        journey_id = f.get("journey_id")
        if number not in wanted or not isinstance(journey_id, int):
            continue
        coords = (f.get("geoJson") or {}).get("coordinates") or []
        if len(coords) != 2 or not all(isinstance(c, (int, float)) for c in coords):
            continue
        lon, lat = coords
        if not in_corridor(lat, lon):
            continue
        updated = f.get("updated_at")
        age_s = (now - datetime.fromisoformat(updated)).total_seconds() if updated else None
        if age_s is None or age_s > max_age_s:
            continue
        delay = f.get("punctuality")
        buses.append(
            {
                "line": number,
                "direction": DIRECTION_BY_PARITY[journey_id % 2],
                "latitude": lat,
                "longitude": lon,
                "journey_id": journey_id,
                "vehicle_id": f.get("vehicle_id"),
                "delay_seconds": round(delay) if isinstance(delay, (int, float)) else None,
                "last_stop_id": f.get("user_stop_id"),
                "updated_at": updated,
                "age_seconds": round(age_s),
            }
        )
    buses.sort(key=lambda b: b["latitude"])
    return buses


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lines", nargs="+", default=DEFAULT_LINES,
                    help="line numbers (default: %(default)s)")
    ap.add_argument("--max-age", type=int, default=DEFAULT_MAX_AGE_S,
                    help="ignore positions older than this many seconds "
                         "(default: %(default)s)")
    ap.add_argument("--json", action="store_true", help="print JSON")
    args = ap.parse_args()

    buses = corridor_buses(fetch_actual_journeys(), args.lines, args.max_age)

    if args.json:
        print(json.dumps(buses, indent=2, ensure_ascii=False))
        return
    if not buses:
        print("No buses between Eindhoven and Sint-Oedenrode right now.")
        return
    for b in buses:
        delay = (f"{round(b['delay_seconds'] / 60):+d} min"
                 if b["delay_seconds"] is not None else "n/a")
        print(f"Line {b['line']:>3}  {b['direction']:<12}  "
              f"{b['latitude']:.5f}, {b['longitude']:.5f}  "
              f"delay {delay}  ({b['age_seconds']}s ago)")


if __name__ == "__main__":
    main()
