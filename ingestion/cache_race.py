#!/usr/bin/env python3
"""
cache_race.py - OpenF1 race data cache for Pit Wall Race Engineer.

Pulls one full F1 race weekend's race session from the OpenF1 API and writes
each endpoint to a local JSON-lines file. Decouples the build from API
availability and rate limits, and makes the demo deterministic.

Usage:
    python cache_race.py                      # 2024 Belgian GP (default)
    python cache_race.py --year 2023 --country Hungary
    python cache_race.py --session-key 9999   # skip lookup, use a known key

Output:
    demo-data/<year>_<country>_race/
        sessions.jsonl
        laps.jsonl
        stints.jsonl
        pit.jsonl
        intervals.jsonl
        position.jsonl
        weather.jsonl
        race_control.jsonl
        car_data.jsonl
        _manifest.json        <- row counts, timings, payload sizes per endpoint

Dependencies:
    pip install requests
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

BASE = "https://api.openf1.org/v1"

# Endpoints that can be fetched in one request filtered by session_key.
# car_data is NOT here - it is too large to fetch whole and returns HTTP 422.
# It is handled separately by cache_car_data(), one driver at a time.
SESSION_ENDPOINTS = [
    "laps",
    "stints",
    "pit",
    "intervals",
    "position",
    "weather",
    "race_control",
]

# Polite pause between requests so we do not hammer a free community API.
REQUEST_DELAY_SECONDS = 1.0
# Retry settings for transient failures (timeouts, 429s, 5xx).
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 3.0
REQUEST_TIMEOUT_SECONDS = 120


def get(endpoint, **params):
    """GET an OpenF1 endpoint with retry/backoff. Returns parsed JSON list."""
    url = f"{BASE}/{endpoint}"
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params,
                                timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"    rate limited (429), waiting {wait:.0f}s "
                      f"(attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json(), len(resp.content)
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            print(f"    request failed ({exc}), retrying in {wait:.0f}s "
                  f"(attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {endpoint} after {MAX_RETRIES} attempts: "
                       f"{last_error}")


def resolve_race_session(year, country):
    """Find the Race session_key for a given year and country."""
    print(f"Resolving {country} {year} race session...")
    sessions, _ = get("sessions", year=year, country_name=country,
                      session_name="Race")
    if not sessions:
        sys.exit(f"ERROR: no Race session found for {country} {year}. "
                 f"Check the country name and year.")
    race = sessions[0]
    print(f"  found: {race['circuit_short_name']} - "
          f"session_key={race['session_key']}, "
          f"meeting_key={race['meeting_key']}")
    return race


def write_jsonl(path, rows):
    """Write a list of dicts to a JSON-lines file (one JSON object per line)."""
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def human_size(num_bytes):
    """Format a byte count as a readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TB"


def cache_car_data(session_key, out):
    """Fetch car_data one driver at a time and append to a single file.

    The whole-race car_data query returns HTTP 422 (too large), so we get
    the list of drivers in the session and pull each driver's telemetry
    separately, writing all of it into one car_data.jsonl file.
    """
    print("Fetching car_data (per driver)...")
    start = time.time()

    # Find which drivers took part in this session.
    try:
        drivers, _ = get("drivers", session_key=session_key)
    except RuntimeError as exc:
        print(f"  FAILED to list drivers: {exc}")
        return {"error": f"could not list drivers: {exc}"}

    driver_numbers = sorted({d["driver_number"] for d in drivers})
    print(f"  {len(driver_numbers)} drivers to fetch")

    path = out / "car_data.jsonl"
    total_rows = 0
    total_bytes = 0
    failed_drivers = []

    # Open once, append each driver's rows as we go.
    with open(path, "w", encoding="utf-8") as fh:
        for num in driver_numbers:
            try:
                rows, payload_bytes = get("car_data",
                                          session_key=session_key,
                                          driver_number=num)
            except RuntimeError as exc:
                print(f"    driver {num}: FAILED ({exc})")
                failed_drivers.append(num)
                continue

            for row in rows:
                fh.write(json.dumps(row) + "\n")
            total_rows += len(rows)
            total_bytes += payload_bytes
            print(f"    driver {num}: {len(rows)} rows  "
                  f"{human_size(payload_bytes)}")
            time.sleep(REQUEST_DELAY_SECONDS)

    elapsed = time.time() - start
    info = {
        "rows": total_rows,
        "seconds": round(elapsed, 1),
        "payload": human_size(total_bytes),
        "payload_bytes": total_bytes,
        "file": path.name,
        "drivers_fetched": len(driver_numbers) - len(failed_drivers),
    }
    if failed_drivers:
        info["failed_drivers"] = failed_drivers
    print(f"  car_data total: {total_rows} rows  "
          f"{human_size(total_bytes)}  {elapsed:.1f}s")
    return info


def cache_race(year, country, session_key, outdir):
    """Pull all endpoints for one race session and write them to disk."""
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    manifest = {
        "year": year,
        "country": country,
        "session_key": session_key,
        "cached_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "endpoints": {},
    }

    # Always save the session record itself for reference.
    sessions, _ = get("sessions", session_key=session_key)
    write_jsonl(out / "sessions.jsonl", sessions)
    if sessions:
        manifest["session_info"] = sessions[0]

    for endpoint in SESSION_ENDPOINTS:
        print(f"Fetching {endpoint}...")
        start = time.time()
        try:
            rows, payload_bytes = get(endpoint, session_key=session_key)
        except RuntimeError as exc:
            print(f"  FAILED: {exc}")
            manifest["endpoints"][endpoint] = {"error": str(exc)}
            continue
        elapsed = time.time() - start

        path = out / f"{endpoint}.jsonl"
        write_jsonl(path, rows)

        info = {
            "rows": len(rows),
            "seconds": round(elapsed, 1),
            "payload": human_size(payload_bytes),
            "payload_bytes": payload_bytes,
            "file": path.name,
        }
        manifest["endpoints"][endpoint] = info
        status = "EMPTY - investigate" if len(rows) == 0 else "ok"
        print(f"  {info['rows']} rows  {info['payload']}  "
              f"{info['seconds']}s  [{status}]")

        time.sleep(REQUEST_DELAY_SECONDS)

    # car_data is handled separately - fetched per driver to avoid HTTP 422.
    manifest["endpoints"]["car_data"] = cache_car_data(session_key, out)

    with open(out / "_manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    return manifest


def print_summary(manifest, outdir):
    """Print a readable end-of-run summary for the day-2 end-of-day check."""
    print("\n" + "=" * 60)
    print("CACHE COMPLETE")
    print("=" * 60)
    print(f"Output directory: {outdir}")
    print(f"Session key:      {manifest['session_key']}")
    print()
    print(f"{'endpoint':<16}{'rows':>10}{'size':>12}{'time':>10}")
    print("-" * 48)
    total_bytes = 0
    empties = []
    for name, info in manifest["endpoints"].items():
        if "error" in info:
            print(f"{name:<16}{'ERROR':>10}{'-':>12}{'-':>10}")
            empties.append(name)
            continue
        total_bytes += info["payload_bytes"]
        print(f"{name:<16}{info['rows']:>10}{info['payload']:>12}"
              f"{info['seconds']:>9}s")
        if info["rows"] == 0:
            empties.append(name)
    print("-" * 48)
    print(f"{'TOTAL':<16}{'':<10}{human_size(total_bytes):>12}")
    print()
    if empties:
        print(f"WARNING: these endpoints returned no data or errored: "
              f"{', '.join(empties)}")
        print("Check whether that is expected for this session before Day 3.")
    else:
        print("All endpoints returned data. Ready for Day 3 ingestion work.")


def main():
    parser = argparse.ArgumentParser(
        description="Cache one OpenF1 race session to local JSON-lines files.")
    parser.add_argument("--year", type=int, default=2024,
                        help="Race year (default: 2024)")
    parser.add_argument("--country", default="Belgium",
                        help="Country name as used by OpenF1 (default: Belgium)")
    parser.add_argument("--session-key", type=int, default=None,
                        help="Use a known session_key directly, skip lookup")
    parser.add_argument("--outdir", default=None,
                        help="Output directory (default: demo-data/<year>_<country>_race)")
    args = parser.parse_args()

    if args.session_key is not None:
        session_key = args.session_key
        year, country = args.year, args.country
    else:
        race = resolve_race_session(args.year, args.country)
        session_key = race["session_key"]
        year, country = args.year, args.country

    outdir = args.outdir or f"demo-data/{year}_{country.lower()}_race"

    print(f"\nCaching {country} {year} race -> {outdir}\n")
    manifest = cache_race(year, country, session_key, outdir)
    print_summary(manifest, outdir)


if __name__ == "__main__":
    main()
