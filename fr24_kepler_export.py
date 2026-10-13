#!/usr/bin/env python3
"""
fr24_kepler_export.py

Pulls historical flight-track data from the Flightradar24 API for a list of
aircraft and exports it to a CSV formatted for import into Kepler.gl.

Output schema (matches kepler_flights.csv):
    lat, lon, time_filter, datetime_display, altitude, airline, registration, flight_id

USAGE

  Fixed date-range backfill (one-off):
    python fr24_kepler_export.py \
        --aircraft-csv aircraft_list.csv \
        --mode fixed --from 2023-01-01 --to 2026-01-31 \
        --output kepler_flights.csv

  Rolling window (repeat runs, e.g. daily cron -- only pulls flights since
  the last successful run per aircraft):
    python fr24_kepler_export.py \
        --aircraft-csv aircraft_list.csv \
        --mode rolling --rolling-hours 24 \
        --output kepler_flights.csv

  Estimate cost before spending any credits on flight-tracks:
    python fr24_kepler_export.py \
        --aircraft-csv aircraft_list.csv \
        --mode fixed --from 2023-01-01 --to 2026-01-31 \
        --dry-run

CONFIGURATION

  Set your FR24 API token:
    export FR24_API_TOKEN="your_token_here"

  !! VERIFY BEFORE A REAL RUN !!
  This targets the official FR24 API (fr24api.flightradar24.com). Endpoint
  paths, param names, response field names, and credit costs below are
  built from the documented structure as of my knowledge cutoff (Jan 2026)
  and may have drifted. Before spending real credits:
    1. Check https://fr24api.flightradar24.com/docs (or your dashboard's
       API reference) against the FR24_ENDPOINTS / field-name constants
       just below the imports.
    2. Confirm the actual credit cost per call in your dashboard and update
       CREDIT_COST_SUMMARY_CALL / CREDIT_COST_TRACK_CALL accordingly --
       the --max-credits safety guard is only as good as these numbers.
    3. Run once with --dry-run first. It only calls flight-summary (cheap,
       discovery-only) and reports how many individual flight-tracks calls
       the full run would make and what that would cost at your configured
       per-call credit rates -- before any track data (the expensive part)
       is pulled.

STATE / CHECKPOINTING
  Progress is recorded in a local SQLite file (--state-db, default
  fr24_state.db) next to the output CSV:
    - which (registration, date-window) summary calls have been made
    - which flight_ids have had their tracks pulled already
    - the last successfully-covered timestamp per registration (rolling mode)
  Re-running the script (after a crash, Ctrl-C, or on a schedule) will skip
  work already recorded here rather than re-spending credits on it. Delete
  the state DB (or use a fresh --state-db) to force a full re-pull.
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
# CONFIG -- verify against current FR24 API docs before a real run
# --------------------------------------------------------------------------

FR24_BASE_URL = "https://fr24api.flightradar24.com"

FR24_ENDPOINTS = {
    # Discovery: list flights matching registrations within a date window.
    # "light" is the cheaper variant -- returns fr24_id + basic timing, not
    # the full route/aircraft-type payload. Swap to /api/flight-summary/full
    # if you need more fields and don't mind the higher credit cost.
    "flight_summary": "/api/flight-summary/light",
    # Full point-by-point track for a single flight.
    "flight_tracks": "/api/flight-tracks",
}

FR24_API_VERSION = "v1"  # sent as the Accept-Version header (confirmed working in a prior script)

# Field names expected in API responses -- adjust here if the schema has
# changed rather than hunting through the request functions below.
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

# Credit costs per call -- PLACEHOLDERS. Check your FR24 dashboard for the
# real per-call/per-record cost and update these before trusting
# --max-credits or the --dry-run estimate.
CREDIT_COST_SUMMARY_PER_FLIGHT = 3  # flight-summary/light, historic >30 days old (confirmed from FR24 pricing docs)
CREDIT_COST_TRACK_CALL = 40  # observed: ~600k credits bought ~15,000 flight-tracks calls (~416 points/flight avg). Likely scales with points returned, so treat as an average, not a fixed rate

# Max registrations to batch into a single flight-summary call. FR24 allows
# comma-separated registrations in one request -- batching cuts credit
# spend vs one call per aircraft. Lower this if you hit request-size limits.
SUMMARY_BATCH_SIZE = 15

# Max days per flight-summary date window. Large ranges are chunked into
# windows this wide so a single call doesn't get truncated server-side.
SUMMARY_WINDOW_DAYS = 7

# Seconds to sleep between API calls (basic rate limiting).
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
            "Accept-Version": FR24_API_VERSION,  # confirmed working header name from a prior working script — reverted from an "API-Version" guess based on ambiguous docs phrasing
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
        Billed per flight RETURNED (confirmed from FR24 pricing docs: 3
        credits/flight for light+historic>30days), not per call -- so we
        charge after seeing the response, not before."""
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
        # Charged per call as a conservative stand-in -- the real FR24
        # pricing model bills per result, same shape as flight-summary, but
        # no official per-flight-tracks number has been confirmed. Update
        # this to match once you've checked a real credit-balance delta.
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
            # Skip whole-batch shortcut isn't safe (per-registration
            # checkpointing), so filter down to regs not yet covered for
            # this exact window.
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

    # Also pick up flights already known from a PREVIOUS run's discovery
    # (checkpointed windows we skipped above) that haven't had their tracks
    # pulled yet. Without this, re-running after a dry-run or an interrupted
    # pull only processes whatever's newly discovered this run, silently
    # leaving previously-discovered flights stuck in "known but never
    # pulled" limbo.
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
    """Returns (unix_ms, display_string) from whatever FR24 gives us."""
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
                    help="Abort before spending more than this many credits (see CREDIT_COST_* constants)")
    p.add_argument("--dry-run", action="store_true",
                    help="Run discovery (flight-summary) only, report the estimated cost of pulling tracks, and exit without spending credits on flight-tracks")

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
