#!/usr/bin/env python3
"""
hec_ingest.py  --  Day 3 deliverable

Streams a cached race (the .jsonl files written by cache_race.py) into Splunk
via the HTTP Event Collector.

Written against the REAL cache_race.py output for the 2024 Belgian GP:
  demo-data/spa_2024/
    car_data.jsonl      607,120 rows  -> index f1_telemetry  (kept WIDE)
    laps.jsonl              842 rows  -> index f1_timing
    intervals.jsonl      21,488 rows  -> index f1_race_state  (record_kind=interval)
    position.jsonl          539 rows  -> index f1_race_state  (record_kind=position)
    stints.jsonl             58 rows  -> index f1_race_state  (record_kind=stint)
    pit.jsonl                34 rows  -> index f1_race_state  (record_kind=pit)
    weather.jsonl           137 rows  -> index f1_race_state  (record_kind=weather)
    race_control.jsonl       23 rows  -> index f1_race_state  (record_kind=race_control)
    sessions.jsonl            1 row   -> SKIPPED (session metadata, not time-series)
    _manifest.json                    -> SKIPPED (not event data)

car_data is kept WIDE: each event carries all six channels
(rpm, brake, speed, drs, throttle, n_gear) as written by OpenF1. No
metric_name/metric_value explosion. Six known channels, queried directly.

Setup (do once, in Splunk Enterprise):
  Settings > Data inputs > HTTP Event Collector > New Token
  Name: pit-wall-hec ; allowed indexes: f1_telemetry, f1_timing,
  f1_race_state, f1_agent_output. Then enable HEC globally.

Usage (run from the ingestion/ directory):
  export SPLUNK_HEC_URL="https://<host>:8088/services/collector/event"
  export SPLUNK_HEC_TOKEN="<token>"
  python hec_ingest.py --smoke --driver 44      # Day 3 smoke test
  python hec_ingest.py                          # full race ingest, fast
  python hec_ingest.py --insecure               # self-signed dev cert

Day 4: controlled-speed deterministic replayer (opt-in, additive).
  python hec_ingest.py --speed 1.0              # real-time replay
  python hec_ingest.py --speed 10               # 10x faster than real-time
  python hec_ingest.py --speed 5 --start-lap 30 # start at lap 30, 5x
  python hec_ingest.py --speed 1 \
      --start-time "2024-07-28T13:30:00+00:00"  # start at this wall-clock time

  --speed 0 (default) means "as fast as HEC accepts" -- Day 3 behaviour,
  unchanged. Any --speed > 0 schedules every event by its envelope["time"].
  --start-lap and --start-time are mutually exclusive.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime

# Cache file -> (target index, sourcetype, record_kind).
# record_kind lets the single f1_race_state index carry six OpenF1 endpoints
# while staying filterable in SPL (record_kind=interval, record_kind=stint...).
ROUTING = {
    "car_data":     ("f1_telemetry",  "f1:telemetry",   None),
    "laps":         ("f1_timing",     "f1:timing",      None),
    "intervals":    ("f1_race_state", "f1:racestate",   "interval"),
    "position":     ("f1_race_state", "f1:racestate",   "position"),
    "stints":       ("f1_race_state", "f1:racestate",   "stint"),
    "pit":          ("f1_race_state", "f1:racestate",   "pit"),
    "weather":      ("f1_race_state", "f1:racestate",   "weather"),
    "race_control": ("f1_race_state", "f1:racestate",   "race_control"),
    # sessions.jsonl deliberately omitted - it is session metadata, one row,
    # not time-series, and nothing downstream queries it as events.
}

# car_data telemetry channels. Used to compute the coarse car_state label.
TELEMETRY_CHANNELS = ["rpm", "brake", "speed", "drs", "throttle", "n_gear"]


def iso_to_epoch(iso):
    """OpenF1 ISO8601 timestamp -> epoch seconds. Tolerant of minor variants."""
    if not iso:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(iso, fmt).timestamp()
        except (ValueError, TypeError):
            continue
    return None


def car_state_label(row):
    """
    Coarse car_state label from throttle/brake - satisfies the Day 3 plan's
    'car_state' field on telemetry events.
    """
    thr = row.get("throttle") or 0
    brk = row.get("brake") or 0
    if brk and brk > 0:
        return "braking"
    if thr >= 95:
        return "push"
    if thr < 40:
        return "lift_coast"
    return "partial"


def hec_post(url, token, batch, verify_tls=True):
    """POST a batch of HEC event envelopes (newline-delimited JSON)."""
    body = "\n".join(json.dumps(e) for e in batch).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Authorization": f"Splunk {token}",
                 "Content-Type": "application/json"},
    )
    ctx = None
    if not verify_tls:
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def to_hec_event(record, src_name):
    """
    Convert one cached record into one HEC event envelope.
    car_data stays wide: all six channels in a single event. driver_number is
    renamed to driver_id so every index shares one driver field name.
    """
    index, sourcetype, kind = ROUTING[src_name]
    ts = record.get("date") or record.get("date_start")
    envelope = {"index": index, "sourcetype": sourcetype}
    epoch = iso_to_epoch(ts)
    if epoch is not None:
        envelope["time"] = epoch

    evt = dict(record)
    # Normalise the driver field name across every data class.
    if "driver_number" in evt:
        evt["driver_id"] = evt["driver_number"]

    if src_name == "car_data":
        evt["car_state"] = car_state_label(record)
    if kind:
        evt["record_kind"] = kind

    envelope["event"] = evt
    return envelope


def driver_of(record):
    """Driver number from a record, if it has one."""
    return record.get("driver_number")


def iter_records(src_dir, only_driver=None, smoke=False):
    """
    Yield (src_name, record) across all routed cache files.
    --smoke caps car_data to keep the test quick (~one driver, first slice).
    """
    smoke_cap = 4000  # ~4000 car_data rows for one driver ~= a few minutes
    for src_name in ROUTING:
        path = os.path.join(src_dir, f"{src_name}.jsonl")
        if not os.path.exists(path):
            print(f"  (skip, not found: {src_name}.jsonl)", flush=True)
            continue
        print(f"reading {src_name}.jsonl ...", flush=True)
        count = 0
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if only_driver is not None and driver_of(rec) != only_driver:
                    continue
                yield src_name, rec
                count += 1
                if smoke and src_name == "car_data" and count >= smoke_cap:
                    print(f"  (smoke: capped car_data at {smoke_cap} rows)")
                    break


# ---------------------------------------------------------------------------
# Day 4: controlled-speed replay helpers.
# ---------------------------------------------------------------------------

def parse_start_time(value):
    """--start-time accepts either epoch seconds (e.g. 1722171600) or
    ISO8601 (e.g. 2024-07-28T13:30:00+00:00). Returns epoch seconds, or None
    when value is None. Exits cleanly on a string we cannot parse."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    epoch = iso_to_epoch(value)
    if epoch is None:
        sys.exit(f"--start-time: cannot parse {value!r} as epoch seconds "
                 f"or ISO8601.")
    return epoch


def compute_lap_cutoff(src_dir, only_driver, smoke, start_lap):
    """Earliest event time at which the chosen lap_number begins.

    laps.jsonl is the only file whose rows carry both lap_number and a
    timestamp (date_start), so we scan only that file. The cutoff is the
    minimum date_start across rows matching start_lap (and --driver, if set).
    Other files that lack lap_number are filtered later by epoch >= cutoff.
    """
    path = os.path.join(src_dir, "laps.jsonl")
    if not os.path.exists(path):
        sys.exit(f"--start-lap: laps.jsonl not found in {src_dir}.")
    cutoff = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("lap_number") != start_lap:
                continue
            if only_driver is not None and driver_of(rec) != only_driver:
                continue
            e = iso_to_epoch(rec.get("date_start"))
            if e is not None and (cutoff is None or e < cutoff):
                cutoff = e
    if cutoff is None:
        sys.exit(f"--start-lap {start_lap}: no laps.jsonl rows match "
                 f"(driver={only_driver}). Pick a lap that exists.")
    # smoke is accepted for API symmetry; the cutoff itself is independent
    # of smoke caps (laps.jsonl is small and never capped).
    _ = smoke
    return cutoff


def iter_filtered_records(src_dir, only_driver, smoke,
                          start_lap, lap_cutoff_epoch, start_time_epoch):
    """iter_records wrapped with Day 4 --start-lap / --start-time filters.

    Behaviour:
      - With lap_number present on the record: keep when lap_number >= start_lap.
      - Without lap_number: keep when event time >= lap_cutoff_epoch.
        Records that are themselves untimed get dropped under --start-lap
        (we have no way to place them temporally against the cutoff).
      - --start-time: drop records whose event time is < start_time_epoch.
        Untimed records can't be compared and are dropped under --start-time.
    """
    for src_name, rec in iter_records(src_dir, only_driver, smoke):
        if start_lap is not None:
            lap = rec.get("lap_number")
            if lap is not None:
                if lap < start_lap:
                    continue
            else:
                ts = rec.get("date") or rec.get("date_start")
                e = iso_to_epoch(ts)
                if e is None or e < lap_cutoff_epoch:
                    continue
        if start_time_epoch is not None:
            ts = rec.get("date") or rec.get("date_start")
            e = iso_to_epoch(ts)
            if e is None or e < start_time_epoch:
                continue
        yield src_name, rec


def collect_timed(records_iter):
    """Materialise a filtered record stream into two lists ready for replay.

    Returns (timed, untimed). timed is sorted ascending by event epoch.
    timed entries are envelopes; untimed entries are envelopes too. Splitting
    here means the timed loop never has to special-case None-time events.
    """
    timed, untimed = [], []
    for src_name, rec in records_iter:
        env = to_hec_event(rec, src_name)
        epoch = env.get("time")
        if epoch is None:
            untimed.append(env)
        else:
            timed.append((epoch, env))
    timed.sort(key=lambda x: x[0])
    return timed, untimed


def _post_batch(url, token, batch, verify_tls, label="batch"):
    """One HEC post wrapped with error capture. Returns (ok, count)."""
    try:
        hec_post(url, token, batch, verify_tls=verify_tls)
        return True, len(batch)
    except Exception as e:  # noqa: BLE001
        print(f"  ! {label} failed: {e}", file=sys.stderr)
        return False, len(batch)


def play_timed(url, token, timed, untimed, batch_size, speed, verify_tls):
    """
    Wall-clock-paced replay.

    Scheduling design (why this does NOT drift over a 50-lap race)
    ---------------------------------------------------------------
    Naive replayers do:   time.sleep(next_event.epoch - prev_event.epoch)
    That accumulates error: every sleep() returns slightly late, every HEC
    POST eats wall time, and those errors compound. A 90-minute replay can
    end up tens of seconds late.

    We instead anchor every event to an absolute wall-clock target:
        wall_start    = monotonic() before the first scheduled event
        earliest      = epoch of that first event after filtering
        target(event) = wall_start + (event.epoch - earliest) / speed
    Each iteration computes target from the event's own epoch, NEVER from
    a running sum of sleeps. If we fell 200 ms behind on the previous
    cycle, the next event's target is already 200 ms in the past, so we
    don't sleep -- we just emit. Error never accumulates.

    Batching: events whose target has already passed are buffered into the
    current batch. A flush happens (a) mid-burst when --batch is hit, or
    (b) just before we sleep because the next event is still in the future.
    """
    sent, failed = 0, 0

    # Prologue: untimed events have no schedule slot. Send them up front so
    # they aren't dropped and don't collapse to wall_start (offset zero).
    if untimed:
        print(f"  prologue: posting {len(untimed)} untimed events", flush=True)
        for i in range(0, len(untimed), batch_size):
            chunk = untimed[i:i + batch_size]
            ok, n = _post_batch(url, token, chunk, verify_tls,
                                label="prologue batch")
            if ok:
                sent += n
            else:
                failed += n

    if not timed:
        return sent, failed

    earliest = timed[0][0]
    wall_start = time.monotonic()
    span_seconds = timed[-1][0] - earliest
    print(f"  timed replay: {len(timed)} events over {span_seconds:.1f}s of "
          f"sim time at {speed}x  (wall time ~= {span_seconds/speed:.1f}s)",
          flush=True)

    batch = []
    last_progress = 0
    i, n = 0, len(timed)
    while i < n:
        epoch, env = timed[i]
        target = wall_start + (epoch - earliest) / speed
        now = time.monotonic()

        if now < target:
            # Next event is in the future. Flush whatever has accumulated
            # so events that ARE due get on the wire promptly, then sleep
            # until the next target. The sleep is anchored to target, not
            # to "delta from now", so it is self-correcting on next pass.
            if batch:
                ok, count = _post_batch(url, token, batch, verify_tls)
                if ok:
                    sent += count
                else:
                    failed += count
                batch = []
            time.sleep(target - now)
            continue  # re-enter loop in case sleep overshot one event

        # Event is due. Queue it; advance index.
        batch.append(env)
        i += 1

        # Mid-burst size cap. Splunk HEC dislikes huge single requests.
        if len(batch) >= batch_size:
            ok, count = _post_batch(url, token, batch, verify_tls)
            if ok:
                sent += count
            else:
                failed += count
            batch = []
            if sent - last_progress >= 5000:
                last_progress = sent
                sim_t = epoch - earliest
                print(f"  ... {sent} events sent (sim t+{sim_t:.1f}s)",
                      flush=True)

    if batch:
        ok, count = _post_batch(url, token, batch, verify_tls,
                                label="final batch")
        if ok:
            sent += count
        else:
            failed += count

    return sent, failed


def main():
    ap = argparse.ArgumentParser(
        description="Stream a cached F1 race into Splunk via HEC.")
    ap.add_argument("--src", default="demo-data/spa_2024",
                    help="Cache directory (default: demo-data/spa_2024)")
    ap.add_argument("--driver", type=int,
                    help="Limit ingest to one driver_number")
    ap.add_argument("--smoke", action="store_true",
                    help="Day 3 smoke test: one driver, capped car_data")
    ap.add_argument("--batch", type=int, default=200, help="HEC batch size")
    ap.add_argument("--insecure", action="store_true",
                    help="Skip TLS verification (self-signed dev certs)")
    ap.add_argument("--speed", type=float, default=0.0,
                    help="Day 4: wall-clock playback multiplier. 0 = as fast "
                         "as possible (Day 3 default). 1.0 = real time. "
                         "10.0 = 10x faster than real-time.")
    ap.add_argument("--start-lap", type=int, default=None,
                    help="Day 4: begin playback at this lap_number. "
                         "Mutually exclusive with --start-time.")
    ap.add_argument("--start-time", default=None,
                    help="Day 4: begin playback at this wall-clock time "
                         "(epoch seconds or ISO8601). Mutually exclusive "
                         "with --start-lap.")
    args = ap.parse_args()

    if args.start_lap is not None and args.start_time is not None:
        sys.exit("--start-lap and --start-time are mutually exclusive.")

    url = os.environ.get("SPLUNK_HEC_URL")
    token = os.environ.get("SPLUNK_HEC_TOKEN")
    print(f"hec_ingest: src={args.src} driver={args.driver} smoke={args.smoke} "
          f"batch={args.batch} insecure={args.insecure}", flush=True)
    print(f"hec_ingest: speed={args.speed} start_lap={args.start_lap} "
          f"start_time={args.start_time}", flush=True)
    print(f"hec_ingest: url={url or '<UNSET>'} "
          f"token={'<set>' if token else '<UNSET>'}", flush=True)
    if not url or not token:
        print("ERROR: Set SPLUNK_HEC_URL and SPLUNK_HEC_TOKEN environment "
              "variables.", flush=True)
        sys.exit(1)

    if not os.path.isdir(args.src):
        sys.exit(f"Cache directory not found: {args.src}\n"
                 f"Run this from the ingestion/ directory, or pass --src.")

    if args.smoke and args.driver is None:
        args.driver = 44  # Hamilton - sensible default for the smoke test
        print("Smoke mode: defaulting to driver 44 (Hamilton).")

    start_time_epoch = parse_start_time(args.start_time)
    lap_cutoff_epoch = None
    if args.start_lap is not None:
        lap_cutoff_epoch = compute_lap_cutoff(
            args.src, args.driver, args.smoke, args.start_lap)
        print(f"  --start-lap {args.start_lap}: cutoff epoch = "
              f"{lap_cutoff_epoch:.3f}", flush=True)

    # Day 4 is opt-in. If none of the new flags are used, fall through to the
    # original Day 3 loop verbatim so behaviour is bit-for-bit unchanged.
    day4_active = (args.speed > 0 or args.start_lap is not None
                   or args.start_time is not None)

    sent, failed = 0, 0
    if not day4_active:
        # ----- Day 3 path (UNCHANGED) -----
        batch = []
        for src_name, rec in iter_records(args.src, args.driver, args.smoke):
            batch.append(to_hec_event(rec, src_name))
            if len(batch) >= args.batch:
                try:
                    hec_post(url, token, batch, verify_tls=not args.insecure)
                    sent += len(batch)
                except Exception as e:  # noqa: BLE001
                    failed += len(batch)
                    print(f"  ! batch failed: {e}", file=sys.stderr)
                batch = []
                if sent and sent % 5000 == 0:
                    print(f"  ... {sent} events sent")
        if batch:
            try:
                hec_post(url, token, batch, verify_tls=not args.insecure)
                sent += len(batch)
            except Exception as e:  # noqa: BLE001
                failed += len(batch)
                print(f"  ! final batch failed: {e}", file=sys.stderr)
    else:
        # ----- Day 4 path: filtered + optionally timed -----
        filtered = iter_filtered_records(
            args.src, args.driver, args.smoke,
            args.start_lap, lap_cutoff_epoch, start_time_epoch)

        if args.speed > 0:
            timed, untimed = collect_timed(filtered)
            print(f"  collected {len(timed)} timed + {len(untimed)} untimed "
                  f"events for replay", flush=True)
            sent, failed = play_timed(
                url, token, timed, untimed,
                batch_size=args.batch, speed=args.speed,
                verify_tls=not args.insecure)
        else:
            # Filtered but fast-as-possible. Same shape as Day 3 loop, just
            # consuming the filtered iterator. Untimed events flow through
            # in stream order (spec: "In speed=0 mode, behaviour is unchanged
            # -- they just flow through").
            batch = []
            for src_name, rec in filtered:
                batch.append(to_hec_event(rec, src_name))
                if len(batch) >= args.batch:
                    try:
                        hec_post(url, token, batch,
                                 verify_tls=not args.insecure)
                        sent += len(batch)
                    except Exception as e:  # noqa: BLE001
                        failed += len(batch)
                        print(f"  ! batch failed: {e}", file=sys.stderr)
                    batch = []
                    if sent and sent % 5000 == 0:
                        print(f"  ... {sent} events sent")
            if batch:
                try:
                    hec_post(url, token, batch, verify_tls=not args.insecure)
                    sent += len(batch)
                except Exception as e:  # noqa: BLE001
                    failed += len(batch)
                    print(f"  ! final batch failed: {e}", file=sys.stderr)

    print(f"\nDone. {sent} events sent, {failed} failed.")
    if failed:
        print("Some batches failed - check HEC token, URL, and that the four "
              "indexes exist and are allowed on the token.")
    print("\nDay 3 smoke check - run this SPL in Splunk:")
    print("  index=f1_telemetry driver_id=44 | stats count")
    print("  index=f1_telemetry driver_id=44 | head 1")
    print("  Expect a non-zero count and an event with rpm/speed/throttle/"
          "brake/drs/n_gear/car_state.")


if __name__ == "__main__":
    main()
