#!/usr/bin/env python3
"""
fr24_kepler_export.py

Pulls historical flight-track data from the Flightradar24 (FR24) API for a
list of aircraft registrations and writes it to a CSV that can be loaded
straight into Kepler.gl (https://kepler.gl/demo).

Output columns:
    lat, lon, time_filter, datetime_display, altitude, airline, registration, flight_id

How it works
  1. Discovery: finds every flight for your registrations in the date range
     (FR24 "flight summary" endpoint, billed per flight returned).
  2. Tracks: downloads the full position track for each flight found
     (FR24 "flight tracks" endpoint, billed per call), and appends the points
     to the output CSV.

REQUIREMENTS
  Python 3.9+, the `requests` package, and an FR24 API token with credits.
  Set the token as an environment variable (never write it into a file):
    macOS / Linux:        export FR24_API_TOKEN="your_token_here"
    Windows PowerShell:   $env:FR24_API_TOKEN="your_token_here"
    Windows cmd:          set FR24_API_TOKEN=your_token_here

AIRCRAFT LIST
  A CSV with two columns, `registration` and `airline`. The airline value is
  a free-text label written into the output so you can colour by it in Kepler.
  Lines starting with # in the registration column are ignored.

USAGE

  Fixed date range (one-off backfill):
    python fr24_kepler_export.py --aircraft-csv aircraft_list.csv --mode fixed --from 2024-11-01 --output kepler_flights.csv

  Rolling window (repeat runs, e.g. a daily scheduled job; each run only
  pulls flights since the last successful run for each aircraft):
    python fr24_kepler_export.py --aircraft-csv aircraft_list.csv --mode rolling --rolling-hours 24 --output kepler_flights.csv

  Discovery only (runs step 1, reports how many flights would need tracks and
  the estimated credit cost, then stops before step 2):
    python fr24_kepler_export.py --aircraft-csv aircraft_list.csv --mode fixed --from 2024-11-01 --dry-run

  Note: --dry-run is not free. Discovery is billed per flight found.

  Cap spending:
    Add --max-credits N to stop before the estimated spend exceeds N credits.
    Set N to your real credit balance.

  Pull specific aircraft first (useful if credits might run out):
    Add --priority-regs REG1,REG2

DATE LIMITS
  Your FR24 plan limits how far back you can query (for example a rolling 24
  months). A start date exactly on that limit is rejected with a 400 error, so
  start a few days inside it.

COSTS
  The credit constants near the top of this file (CREDIT_COST_*) are
  estimates used only for --max-credits and the discovery summary. Check them
  against your FR24 pricing and your own credit balance and adjust if needed.
  The tracks cost in particular is an average and may vary with how many
  points each flight returns.

STATE / CHECKPOINTING
  Progress is saved in a local SQLite file (--state-db, default fr24_state.db):
    - which (registration, date window) discovery calls have been made
    - which flights have had their tracks downloaded
    - the last covered timestamp per registration (rolling mode)
  If the script stops (error, Ctrl-C, out of credits), run the same command
  again and it resumes without re-spending credits on finished work. Delete
  the state file, or use a new --state-db, to start from scratch. Use a
  separate --state-db and --output for each separate aircraft list.
"""

import argparse
import csv
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------
# CONFIG
# Endpoint paths, field names and credit costs are collected here so they are
# easy to adjust if the FR24 API changes. See https://fr24api.flightradar24.com/docs
# --------------------------------------------------------------------------

FR24_BASE_URL = "https://fr24api.flightradar24.com"

FR24_ENDPOINTS = {
    # Discovery: list flights matching registrations within a date window.
    # "light" is the cheaper variant: it returns the flight ID and basic
    # timing, not the full route/aircraft-type payload. Use
    # /api/flight-summary/full if you need more fields and accept the higher cost.
    "flight_summary": "/api/flight-summary/light",
    # Full point-by-point track for a single flight.
    "flight_tracks": "/api/flight-tracks",
}

FR24_API_VERSION = "v1"  # sent as the Accept-Version header

# Field names expected in API responses. Adjust here if the schema changes.
SUMMARY_FIELDS = {
    "flight_id": "fr24_id",
    "registration": "reg",
    "datetime_takeoff": "datetime_takeoff",
    "datetime_landed": "datetime_landed",
}
TRACK_FIELDS = {
    "timestamp": "timestamp",   # expected epoch seconds or ISO8601 string
    "lat": "lat",
    "lon": "lon",
    "altitude": "alt",
}

# Estimated credit costs, used for --max-credits and the cost summary.
# Check them against your FR24 pricing and your own credit balance.
CREDIT_COST_SUMMARY_PER_FLIGHT = 3  # flight-summary/light for flights older than 30 days, per flight returned
CREDIT_COST_TRACK_CALL = 40  # flight-tracks, per call. An average: the real cost may vary with the number of points returned

# Max registrations to batch into a single flight-summary call (FR24 accepts
# comma-separated registrations). Lower this if you hit request-size limits.
SUMMARY_BATCH_SIZE = 15

# Max days per flight-summary date window. Long ranges are split into windows
# of this size so a single response is not truncated.
SUMMARY_WINDOW_DAYS = 7

# Seconds to wait between API calls (basic rate limiting).
REQUEST_DELAY_SECONDS = 0.5

CSV_COLUMNS = [
    "lat", "lon", "time_filter", "datetime_display",
    "altitude", "airline", "registration", "flight_id",
]

# --------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fr24_kepler_export")


class CreditGuard:
    """Tracks estimated credit spend and stops the run before exceeding budget."""

    def __init__(self, max_credits):
        self.max_credits = max_credits
        self.spent = 0

    def charge(self, amount, reason=""):
        if self.max_credits is not None and self.spent + amount > self.max_credits:
            raise SystemExit(
                f"Aborting: next call would spend {amount} credits "
                f"({reason}), pushing total to {self.spent + amount}, "
                f"over --max-credits={self.max_credits}. "
                f"Spent {self.spent} credits before stopping."
            )
        self.spent += amount


class Fr24Client:
    def __init__(self, token, credit_guard):
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Accept-Version": FR24_API_VERSION,
        })
        self.credit_guard = credit_guard

    def _get(self, path, params):
        url = FR24_BASE_URL + path
        resp = self.session.get(url, params=params, timeout=30)
        if resp.status_code == 401:
            raise SystemExit(
                "401 Unauthorized -- check FR24_API_TOKEN is set and valid."
            )
        if resp.status_code == 402:
            raise SystemExit(
                "402 / credit-exhausted response from FR24 -- you're out of "
                "API credits."
            )
        if not resp.ok:
            log.error("FR24 API error %s: %s", resp.status_code, resp.text[:1000])
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return resp.json()

    def flight_summary(self, registrations, dt_from, dt_to):
        """registrations: list of tail numbers (batched into one call).
        Billed per flight returned, not per call, so the credit guard is
        charged after the response arrives."""
        params = {
            "registrations": ",".join(registrations),
            "flight_datetime_from": dt_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flight_datetime_to": dt_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        data = self._get(FR24_ENDPOINTS["flight_summary"], params)
        results = data.get("data", data if isinstance(data, list) else [])
        self.credit_guard.charge(
            len(results) * CREDIT_COST_SUMMARY_PER_FLIGHT,
            f"flight-summary x{len(registrations)} regs -> {len(results)} flights returned",
        )
        return results

    def flight_tracks(self, flight_id):
        # Charged per call using the CREDIT_COST_TRACK_CALL estimate.
        self.credit_guard.charge(CREDIT_COST_TRACK_CALL, f"flight-tracks {flight_id}")
        params = {"flight_id": flight_id}
        data = self._get(FR24_ENDPOINTS["flight_tracks"], params)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if isinstance(data, list) and data:
            # Some FR24 endpoints wrap a single flight's tracks in a list.
            entry = data[0]
            return entry.get("tracks", entry.get("track", []))
        return []


# --------------------------------------------------------------------------
# State / checkpointing
# --------------------------------------------------------------------------

def init_state_db(path):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS summary_windows (
            registration TEXT NOT NULL,
            window_from TEXT NOT NULL,
            window_to TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (registration, window_from, window_to)
        );
        CREATE TABLE IF NOT EXISTS flights (
            flight_id TEXT PRIMARY KEY,
            registration TEXT NOT NULL,
            airline TEXT,
            datetime_takeoff TEXT,
            datetime_landed TEXT
        );
        CREATE TABLE IF NOT EXISTS tracks_fetched (
            flight_id TEXT PRIMARY KEY,
            fetched_at TEXT NOT NULL,
            point_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS rolling_progress (
            registration TEXT PRIMARY KEY,
            last_covered_to TEXT NOT NULL
        );
    """)
    con.commit()
    return con


def window_already_fetched(con, registration, dt_from, dt_to):
    row = con.execute(
        "SELECT 1 FROM summary_windows WHERE registration=? AND window_from=? AND window_to=?",
        (registration, dt_from.isoformat(), dt_to.isoformat()),
    ).fetchone()
    return row is not None


def mark_window_fetched(con, registration, dt_from, dt_to):
    con.execute(
        "INSERT OR REPLACE INTO summary_windows VALUES (?, ?, ?, ?)",
        (registration, dt_from.isoformat(), dt_to.isoformat(), datetime.now(timezone.utc).isoformat()),
    )
    con.commit()


def flight_known(con, flight_id):
    return con.execute(
        "SELECT 1 FROM flights WHERE flight_id=?", (flight_id,)
    ).fetchone() is not None


def store_flight(con, flight_id, registration, airline, takeoff, landed):
    con.execute(
        "INSERT OR REPLACE INTO flights VALUES (?, ?, ?, ?, ?)",
        (flight_id, registration, airline, takeoff, landed),
    )
    con.commit()


def track_already_fetched(con, flight_id):
    return con.execute(
        "SELECT 1 FROM tracks_fetched WHERE flight_id=?", (flight_id,)
    ).fetchone() is not None


def mark_track_fetched(con, flight_id, point_count):
    con.execute(
        "INSERT OR REPLACE INTO tracks_fetched VALUES (?, ?, ?)",
        (flight_id, datetime.now(timezone.utc).isoformat(), point_count),
    )
    con.commit()


def get_rolling_start(con, registration, default_start):
    row = con.execute(
        "SELECT last_covered_to FROM rolling_progress WHERE registration=?",
        (registration,),
    ).fetchone()
    if row:
        return datetime.fromisoformat(row[0])
    return default_start


def set_rolling_progress(con, registration, covered_to):
    con.execute(
        "INSERT OR REPLACE INTO rolling_progress VALUES (?, ?)",
        (registration, covered_to.isoformat()),
    )
    con.commit()


# --------------------------------------------------------------------------
# Core logic
# --------------------------------------------------------------------------

def load_aircraft_list(path):
    aircraft = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            reg = (row.get("registration") or "").strip()
            airline = (row.get("airline") or "").strip()
            if not reg or reg.startswith("#"):
                continue
            aircraft.append((reg, airline))
    return aircraft


def daterange_windows(dt_from, dt_to, window_days):
    cur = dt_from
    while cur < dt_to:
        nxt = min(cur + timedelta(days=window_days), dt_to)
        yield cur, nxt
        cur = nxt


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def discover_flights(client, con, aircraft, dt_from, dt_to, dry_run):
    """
    Runs flight-summary calls (batched, windowed, checkpointed) and returns
    the set of (flight_id, registration, airline) not yet track-fetched.
    """
    reg_to_airline = {reg: airline for reg, airline in aircraft}
    all_regs = list(reg_to_airline.keys())
    windows = list(daterange_windows(dt_from, dt_to, SUMMARY_WINDOW_DAYS))
    log.info(
        "Discovery: %d aircraft x %d date windows (%d-day chunks)",
        len(all_regs), len(windows), SUMMARY_WINDOW_DAYS,
    )

    pending = []
    for w_from, w_to in windows:
        for batch in chunked(all_regs, SUMMARY_BATCH_SIZE):
            # Checkpointing is per registration, so only query the ones not
            # yet covered for this exact window.
            todo = [r for r in batch if not window_already_fetched(con, r, w_from, w_to)]
            if not todo:
                continue
            log.info(
                "  summary: %s .. %s  regs=%s",
                w_from.date(), w_to.date(), ",".join(todo),
            )
            results = client.flight_summary(todo, w_from, w_to)
            for entry in results:
                fid = entry.get(SUMMARY_FIELDS["flight_id"])
                reg = entry.get(SUMMARY_FIELDS["registration"], "").upper()
                if not fid or not reg:
                    continue
                airline = reg_to_airline.get(reg, "")
                takeoff = entry.get(SUMMARY_FIELDS["datetime_takeoff"])
                landed = entry.get(SUMMARY_FIELDS["datetime_landed"])
                if not flight_known(con, fid):
                    store_flight(con, fid, reg, airline, takeoff, landed)
                if not track_already_fetched(con, fid):
                    pending.append((fid, reg, airline))
            for r in todo:
                mark_window_fetched(con, r, w_from, w_to)

    # de-dupe flights newly found in this run's fresh summary calls
    seen = set()
    unique_pending = []
    for fid, reg, airline in pending:
        if fid not in seen:
            seen.add(fid)
            unique_pending.append((fid, reg, airline))

    # Also queue flights discovered in an earlier run (their windows are
    # checkpointed and skipped above) whose tracks have not been downloaded
    # yet, so a resumed or post-dry-run command still pulls them.
    placeholders = ",".join("?" * len(all_regs))
    rows = con.execute(
        f"""
        SELECT f.flight_id, f.registration, f.airline
        FROM flights f
        LEFT JOIN tracks_fetched t ON f.flight_id = t.flight_id
        WHERE f.registration IN ({placeholders}) AND t.flight_id IS NULL
        """,
        all_regs,
    ).fetchall()
    for fid, reg, airline in rows:
        if fid not in seen:
            seen.add(fid)
            unique_pending.append((fid, reg, airline))

    return unique_pending


def parse_track_timestamp(raw):
    """Returns (unix_ms, display_string) from an epoch-seconds number or ISO8601 string."""
    if isinstance(raw, (int, float)):
        # assume epoch seconds
        dt = datetime.fromtimestamp(raw, tz=timezone.utc)
    else:
        # assume ISO8601 string
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        dt = dt.astimezone(timezone.utc)
    unix_ms = int(dt.timestamp() * 1000)
    display = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    return unix_ms, display


def pull_tracks_and_write(client, con, pending, csv_writer, output_flush_every=200):
    total_points = 0
    for i, (fid, reg, airline) in enumerate(pending, 1):
        log.info("  [%d/%d] flight-tracks for %s (%s)", i, len(pending), fid, reg)
        try:
            points = client.flight_tracks(fid)
        except requests.HTTPError as e:
            log.warning("    failed to fetch track for %s: %s -- skipping", fid, e)
            continue

        rows_written = 0
        for pt in points:
            try:
                ts_raw = pt.get(TRACK_FIELDS["timestamp"])
                lat = pt.get(TRACK_FIELDS["lat"])
                lon = pt.get(TRACK_FIELDS["lon"])
                alt = pt.get(TRACK_FIELDS["altitude"])
                if lat is None or lon is None or ts_raw is None:
                    continue
                unix_ms, display = parse_track_timestamp(ts_raw)
            except (ValueError, TypeError) as e:
                log.debug("    skipping malformed point in %s: %s", fid, e)
                continue

            csv_writer.writerow({
                "lat": lat,
                "lon": lon,
                "time_filter": unix_ms,
                "datetime_display": display,
                "altitude": alt if alt is not None else "",
                "airline": airline,
                "registration": reg,
                "flight_id": fid,
            })
            rows_written += 1

        mark_track_fetched(con, fid, rows_written)
        total_points += rows_written

        if i % output_flush_every == 0:
            log.info("  ...%d flights done, %d points written so far", i, total_points)

    return total_points


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--aircraft-csv", required=True, help="CSV with registration,airline columns")
    p.add_argument("--output", default="kepler_flights.csv", help="Output CSV path")
    p.add_argument("--state-db", default="fr24_state.db", help="SQLite checkpoint DB path")

    p.add_argument("--mode", choices=["fixed", "rolling"], required=True)
    p.add_argument("--from", dest="date_from", help="Fixed mode: start date, YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", help="Fixed mode: end date, YYYY-MM-DD (default: now)")
    p.add_argument("--rolling-hours", type=int, default=24,
                    help="Rolling mode: lookback window on first run per aircraft")

    p.add_argument("--max-credits", type=int, default=None,
                    help="Stop before the estimated spend exceeds this many credits (see CREDIT_COST_* constants). Set to your real balance")
    p.add_argument("--dry-run", action="store_true",
                    help="Run discovery only, report the estimated cost of pulling tracks, and exit. Discovery still uses credits")

    p.add_argument("--priority-regs",
                    help="Comma-separated registrations to pull tracks for FIRST (so they're covered if credits run out)")
    return p.parse_args()


def main():
    args = parse_args()

    token = os.environ.get("FR24_API_TOKEN")
    if not token:
        sys.exit("Set FR24_API_TOKEN in your environment before running.")

    aircraft = load_aircraft_list(args.aircraft_csv)
    log.info("Loaded %d aircraft from %s", len(aircraft), args.aircraft_csv)
    if not aircraft:
        sys.exit("No aircraft loaded -- check --aircraft-csv")

    con = init_state_db(args.state_db)
    guard = CreditGuard(args.max_credits)
    client = Fr24Client(token, guard)

    now = datetime.now(timezone.utc)

    if args.mode == "fixed":
        if not args.date_from:
            sys.exit("--from is required in fixed mode")
        dt_from = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dt_to = (
            datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if args.date_to else now
        )
        pending = discover_flights(client, con, aircraft, dt_from, dt_to, args.dry_run)

    else:  # rolling
        default_start = now - timedelta(hours=args.rolling_hours)
        pending = []
        for reg, airline in aircraft:
            reg_from = get_rolling_start(con, reg, default_start)
            if reg_from >= now:
                continue
            batch_pending = discover_flights(client, con, [(reg, airline)], reg_from, now, args.dry_run)
            pending.extend(batch_pending)
            set_rolling_progress(con, reg, now)

    if args.priority_regs:
        prio = set(r.strip() for r in args.priority_regs.split(","))
        pending.sort(key=lambda x: 0 if x[1] in prio else 1)
        log.info("Priority ordering applied: %d flights for priority regs go first",
                 sum(1 for x in pending if x[1] in prio))

    n_flights = len(pending)
    est_credits = n_flights * CREDIT_COST_TRACK_CALL
    log.info(
        "Discovery complete: %d new flights found needing track data "
        "(estimated %d credits at %d/call for flight-tracks; "
        "%d credits already spent on flight-summary calls)",
        n_flights, est_credits, CREDIT_COST_TRACK_CALL, guard.spent,
    )

    if args.dry_run:
        log.info("--dry-run set: stopping before any flight-tracks calls.")
        return

    if n_flights == 0:
        log.info("Nothing new to pull.")
        return

    write_header = not Path(args.output).exists() or Path(args.output).stat().st_size == 0
    with open(args.output, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        total_points = pull_tracks_and_write(client, con, pending, writer)

    log.info(
        "Done. %d flights processed, %d track points written to %s. "
        "Total estimated credits spent this run: %d",
        n_flights, total_points, args.output, guard.spent,
    )


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
