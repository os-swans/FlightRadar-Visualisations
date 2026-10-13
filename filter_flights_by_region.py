#!/usr/bin/env python3
"""
filter_flights_by_region.py

Keeps entire flight paths for any flight_id that had AT LEAST ONE point
inside the target region, not just the points that fall inside it. This
matters because journeys can be composed of multiple legs, some of which can fall outside the zone. For example, a flight from Moscow to Bamako might first fly to Syria. 

Region boundaries comes from Natural Earth's public country dataset.

USAGE
    python filter_flights_by_region.py --input kepler_flights.csv \
        --output kepler_flights_africa.csv --continent "insert continent".

Valid continent values include: Asia, Europe, Africa,
    "North America", "South America", Oceania, Antarctica.

    For something other than a whole continent (e.g. a few named
    countries), use --countries instead:
        python filter_flights_by_region.py --input kepler_flights.csv \
            --output kepler_flights_selected.csv \
            --countries "France,Germany,Spain"
"""

import argparse
import sys
 
import pandas as pd
import requests
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from shapely.prepared import prep
 
NE_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_10m_admin_0_countries.geojson"
)
 
# Natural Earth files these under CONTINENT="Seven seas (open ocean)" rather
# than "Africa", even though they're African island nations -- so a plain
# --continent Africa filter silently drops them. Always included alongside
# whatever --continent/--countries selects.
AFRICAN_ISLAND_NATIONS_MISFILED_BY_NATURAL_EARTH = ["Seychelles", "Mauritius"]
 
 
def load_region_shape(continent=None, countries=None):
    resp = requests.get(NE_COUNTRIES_URL, timeout=30)
    resp.raise_for_status()
    geo = resp.json()
 
    if continent:
        matched = [f for f in geo["features"] if f["properties"].get("CONTINENT") == continent]
        if not matched:
            available = sorted(set(f["properties"].get("CONTINENT", "") for f in geo["features"]))
            sys.exit(f"No countries found for continent '{continent}'. Available: {available}")
        if continent == "Africa":
            extra = [f for f in geo["features"]
                     if f["properties"].get("NAME") in AFRICAN_ISLAND_NATIONS_MISFILED_BY_NATURAL_EARTH]
            print(f"Also including {len(extra)} African island nation(s) Natural Earth "
                  f"files outside the 'Africa' continent label: "
                  f"{[f['properties'].get('NAME') for f in extra]}")
            matched += extra
    else:
        wanted = set(c.strip() for c in countries.split(","))
        matched = [f for f in geo["features"] if f["properties"].get("NAME") in wanted]
        found_names = set(f["properties"].get("NAME") for f in matched)
        missing = wanted - found_names
        if missing:
            print(f"Warning: no match found for: {', '.join(missing)} "
                  f"(country names in this dataset may differ slightly -- "
                  f"e.g. check for 'Ivory Coast' vs \"C\u00f4te d'Ivoire\")")
        if not matched:
            sys.exit("No countries matched -- check spelling against Natural Earth's NAME field.")
 
    shapes = [shape(f["geometry"]) for f in matched]
    print(f"Matched {len(shapes)} countries/territories")
    return unary_union(shapes)
 
 
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--continent", help="e.g. Africa, Asia, Europe")
    p.add_argument("--countries", help='Comma-separated country names, e.g. "France,Germany,Spain"')
    p.add_argument("--origin-only", action="store_true",
                    help="Only keep flights whose FIRST tracked point (by time_filter) falls in the region, "
                         "instead of any point. Use this for 'flights that originated in X' rather than "
                         "'flights that entered X'.")
    args = p.parse_args()
 
    if not args.continent and not args.countries:
        sys.exit("Specify either --continent or --countries")
    if args.continent and args.countries:
        sys.exit("Specify only one of --continent or --countries, not both")
 
    print("Loading region boundary...")
    region_shape = load_region_shape(continent=args.continent, countries=args.countries)
    region_prepared = prep(region_shape)  # prepared geometry -- much faster for repeated .contains() calls
 
    print(f"Loading {args.input}...")
    df = pd.read_csv(args.input)
    print(f"{len(df)} points, {df['flight_id'].nunique()} unique flights loaded")
 
    if args.origin_only:
        print("Checking each flight's FIRST tracked point against the region boundary...")
        first_points = df.sort_values("time_filter").groupby("flight_id", as_index=False).first()
        first_points["_inside_region"] = first_points.apply(
            lambda row: region_prepared.contains(Point(row["lon"], row["lat"])), axis=1
        )
        matching_flight_ids = set(first_points.loc[first_points["_inside_region"], "flight_id"])
 
        # Flag flights whose first point looks like it's already airborne
        # (a sign the true origin may be earlier than what FR24 captured,
        # common with patchy transponder coverage) rather than near ground.
        matched_first = first_points[first_points["flight_id"].isin(matching_flight_ids)]
        likely_midflight_start = matched_first[matched_first["altitude"] > 5000]
        if len(likely_midflight_start) > 0:
            print(f"\nWarning: {len(likely_midflight_start)} of {len(matching_flight_ids)} matched flights "
                  f"have a first-tracked-point altitude above 5000 -- their real origin may be earlier than "
                  f"what's captured here (patchy tracking coverage), not necessarily inside the region. "
                  f"Flight IDs: {list(likely_midflight_start['flight_id'])[:20]}"
                  + (" ..." if len(likely_midflight_start) > 20 else ""))
    else:
        print("Checking each point against the region boundary (this may take a while for large files)...")
        inside = df.apply(lambda row: region_prepared.contains(Point(row["lon"], row["lat"])), axis=1)
        df["_inside_region"] = inside
        matching_flight_ids = set(df.loc[df["_inside_region"], "flight_id"])
 
    print(f"{len(matching_flight_ids)} flights matched")
 
    result = df[df["flight_id"].isin(matching_flight_ids)]
    result.to_csv(args.output, index=False)
    print(f"Wrote {len(result)} points ({result['flight_id'].nunique()} full flight paths) to {args.output}")
 
    print("\nBy registration:")
    print(result.groupby("registration")["flight_id"].nunique().sort_values(ascending=False).to_string())
 
 
if __name__ == "__main__":
    main()
 



if __name__ == "__main__":
    main()
