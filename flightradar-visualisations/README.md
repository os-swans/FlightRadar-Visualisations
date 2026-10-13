# FlightRadar Visualisations

Pull the flight history of a list of aircraft from the Flightradar24 (FR24) API, then map every recorded position in [Kepler.gl](https://kepler.gl/demo).

Works on Windows, macOS and Linux. Requires Python 3.9+ and an FR24 API token with credits.

> Unofficial tool. Not affiliated with or endorsed by Flightradar24. "Flightradar24" is a trademark of its owner.

## Quick start

```bash
python3 -m venv fr24-env            # Windows: py -m venv fr24-env
source fr24-env/bin/activate        # Windows: fr24-env\Scripts\Activate.ps1
pip install -r requirements.txt
export FR24_API_TOKEN="your_token"  # PowerShell: $env:FR24_API_TOKEN="your_token"
python fr24_kepler_export.py --aircraft-csv aircraft_list_example.csv --mode fixed --from 2024-11-01 --output kepler_flights.csv --max-credits 10000
```

Re-running the same command resumes from the checkpoint (`fr24_state.db`). Never put your token in a file.

## Filter by region

```bash
python filter_flights_by_region.py --input kepler_flights.csv --output africa.csv --continent Africa
```

Also supports `--countries "Sudan, Ethiopia, Venezuela, etc"` and `--origin-only`.

## Load into Kepler.gl

Open kepler.gl/demo, drag in the CSV, and immediately change the points size to a small number (1-3). Play around with the filters try colouring by altitude, airline, or registration number.

## Notes

- Credit costs are estimates.
- History is limited by your plan; start a few days inside the limit.
