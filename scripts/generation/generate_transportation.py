"""
generate_transportation.py
--------------------------
Generates tms_transportation.csv — one row per truck run.

Simulation period: July 1 – December 31 2025 (184 days)

Columns:
  truck_id, origin_node_id, destination_node_id, carrier,
  departure_datetime, transit_time_hours, arrival_datetime,
  container_count, package_count, cbm

Container count distribution (per truck):
  15% — 1  to 9  containers (light)
  25% — 10 to 25 containers (mid)
  50% — 26 to 29 containers (heavy)
  10% — 30 to 32 containers (peak)

Timing logic per lane type:

  indirect_leg1 (origin -> consolidation hub):
    Arrival generated first, target 06:00-20:59 (soft).
    Departure back-calculated as arrival minus transit time.

  indirect_leg2 (consolidation hub -> delivery node):
    Hard departure window 22:00-05:00 spanning midnight.
    25% depart 22:00-23:59 on sim_date.
    75% depart 00:00-05:00 on sim_date+1.
    Arrival forward-calculated as departure plus transit time.
    Late arrivals emerge naturally from transit variance.

  direct (origin -> delivery node):
    Explicit 90/10 SLA split on arrival.
    90% arrive 06:00-09:59 (on time).
    10% arrive 10:00-14:00 (late).
    Departure back-calculated as arrival minus transit time.

Seasonality multipliers (applied to daily truck count per lane):
  July-August:    0.85
  September-Oct:  1.00
  November:       1.20
  December:       1.40

Carrier assignment:
  Each active lane gets a fixed set of 2-3 carriers drawn at network
  build time. Per truck run, carrier is drawn randomly from that set.
  Guarantees same-lane carrier overlap for benchmarking.

Data quality issues injected:
  - Negative transit time in 1 row
  - Arrival before departure in 1 row
  - NULL carrier in ~1% of rows
  - CBM exceeding container capacity in ~0.5% of rows
  - Truck ID with wrong format in 1 row

Output: data/tms_transportation.csv
"""

import csv
import math
import os
import random
import sys
from datetime import date, datetime, timedelta

random.seed(42)

# ── PATH SETUP ───────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(__file__)
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "..", "data")
sys.path.insert(0, SCRIPT_DIR)
from lanes import build_network

# ── CONSTANTS ────────────────────────────────────────────────────────────────

SIM_START     = date(2025, 7, 1)
SIM_END       = date(2025, 12, 31)
AVG_SPEED_KMH = 80.0

CARRIERS = ["FastFreight EU", "EuroHaul", "TransCargo", "QuickMove", "DirectLog"]

SEASONALITY = {
    7:  0.85,
    8:  0.85,
    9:  1.00,
    10: 1.00,
    11: 1.20,
    12: 1.40,
}

CONTAINER_BANDS = [
    (1,  9,  0.15),
    (10, 25, 0.25),
    (26, 29, 0.50),
    (30, 32, 0.10),
]

# ── HELPERS ──────────────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi/2)**2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def expected_transit_hours(distance_km):
    return distance_km / AVG_SPEED_KMH


def actual_transit_hours(expected_h):
    """
    Right-skewed variance:
      70% — +0.0 to +0.5h
      20% — +0.5 to +1.5h
      10% — +1.5 to +3.0h
    """
    r = random.random()
    if r < 0.70:
        variance = random.uniform(0.0, 0.5)
    elif r < 0.90:
        variance = random.uniform(0.5, 1.5)
    else:
        variance = random.uniform(1.5, 3.0)
    return round(expected_h + variance, 4)


def draw_container_count():
    r = random.random()
    cumulative = 0.0
    for band_min, band_max, weight in CONTAINER_BANDS:
        cumulative += weight
        if r <= cumulative:
            return random.randint(band_min, band_max)
    return random.randint(26, 29)


def draw_cbm(container_count):
    fill_rate = random.uniform(0.60, 1.00)
    return round(container_count * fill_rate, 2)


def draw_package_count(cbm):
    packages_per_cbm = random.uniform(8, 15)
    return max(1, round(cbm * packages_per_cbm))


def daily_truck_count(month):
    multiplier   = SEASONALITY.get(month, 1.0)
    adjusted_max = max(1, round(10 * multiplier))
    return random.randint(1, min(adjusted_max, 10))


def load_node_lookup(network):
    lookup = {}
    for node in network["origins"] + network["hubs"] + network["deliveries"]:
        lookup[node["node_id"]] = node
    return lookup


# ── TIMING FUNCTIONS ─────────────────────────────────────────────────────────

def timing_direct(sim_date, transit_h):
    """
    Direct lane: arrival-first with explicit 90/10 SLA.
    90% arrive 06:00-09:59, 10% arrive 10:00-14:00.
    Departure = arrival - transit.
    """
    minute = random.randint(0, 59)
    if random.random() < 0.90:
        hour = random.randint(6, 9)
    else:
        hour = random.randint(10, 14)
    arrival_dt   = datetime(sim_date.year, sim_date.month, sim_date.day, hour, minute)
    departure_dt = arrival_dt - timedelta(hours=transit_h)
    return departure_dt, arrival_dt


def timing_leg1(sim_date, transit_h):
    """
    Origin -> consolidation hub: arrival-first.
    Target arrival 06:00-20:59 (soft constraint).
    Departure = arrival - transit.
    """
    hour         = random.randint(6, 20)
    minute       = random.randint(0, 59)
    arrival_dt   = datetime(sim_date.year, sim_date.month, sim_date.day, hour, minute)
    departure_dt = arrival_dt - timedelta(hours=transit_h)
    return departure_dt, arrival_dt


def timing_leg2(sim_date, transit_h):
    """
    Consolidation hub -> delivery node: departure-first.
    Hard departure window 22:00-05:00 spanning midnight.
      25% depart 22:00-23:59 on sim_date
      75% depart 00:00-05:00 on sim_date+1
    Arrival = departure + transit.
    """
    minute = random.randint(0, 59)
    if random.random() < 0.25:
        hour         = random.randint(22, 23)
        departure_dt = datetime(sim_date.year, sim_date.month, sim_date.day, hour, minute)
    else:
        hour         = random.randint(0, 5)
        next_date    = sim_date + timedelta(days=1)
        departure_dt = datetime(next_date.year, next_date.month, next_date.day, hour, minute)
    arrival_dt = departure_dt + timedelta(hours=transit_h)
    return departure_dt, arrival_dt


# ── CARRIER ASSIGNMENT ────────────────────────────────────────────────────────

def build_lane_carrier_map(all_lanes):
    """
    Assigns a fixed set of 2-3 carriers to each active lane.
    Same carrier set applies for all simulation days.
    """
    lane_carriers = {}
    for lane in all_lanes:
        if not lane["is_active"]:
            continue
        key = (lane["origin_id"], lane["destination_id"])
        if key not in lane_carriers:
            n                  = random.randint(2, 3)
            lane_carriers[key] = random.sample(CARRIERS, n)
    return lane_carriers


# ── MAIN GENERATION ───────────────────────────────────────────────────────────

def generate_transportation(network, lane_carriers, node_lookup):
    rows          = []
    truck_counter = 1

    current_date = SIM_START
    while current_date <= SIM_END:
        month = current_date.month

        for lane in network["all_lanes"]:
            if not lane["is_active"]:
                continue

            origin_id = lane["origin_id"]
            dest_id   = lane["destination_id"]
            lane_type = lane["lane_type"]

            origin_node = node_lookup.get(origin_id)
            dest_node   = node_lookup.get(dest_id)
            if not origin_node or not dest_node:
                continue

            distance_km = haversine_km(
                origin_node["latitude"], origin_node["longitude"],
                dest_node["latitude"],   dest_node["longitude"]
            )
            expected_h = expected_transit_hours(distance_km)
            n_trucks   = daily_truck_count(month)
            lane_key   = (origin_id, dest_id)
            carriers   = lane_carriers.get(lane_key, ["EuroHaul", "TransCargo"])

            for _ in range(n_trucks):
                transit_h = actual_transit_hours(expected_h)

                if lane_type == "indirect_leg2":
                    departure_dt, arrival_dt = timing_leg2(current_date, transit_h)
                elif lane_type == "indirect_leg1":
                    departure_dt, arrival_dt = timing_leg1(current_date, transit_h)
                else:
                    departure_dt, arrival_dt = timing_direct(current_date, transit_h)

                container_count = draw_container_count()
                cbm             = draw_cbm(container_count)
                package_count   = draw_package_count(cbm)
                carrier         = random.choice(carriers)
                truck_id        = f"TRK_{truck_counter:07d}"
                truck_counter  += 1

                rows.append({
                    "truck_id":            truck_id,
                    "origin_node_id":      origin_id,
                    "destination_node_id": dest_id,
                    "carrier":             carrier,
                    "departure_datetime":  departure_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "transit_time_hours":  transit_h,
                    "arrival_datetime":    arrival_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "container_count":     container_count,
                    "package_count":       package_count,
                    "cbm":                 cbm,
                })

        current_date += timedelta(days=1)

    return rows


# ── DATA QUALITY INJECTION ────────────────────────────────────────────────────

def inject_quality_issues(rows):
    result = [row.copy() for row in rows]
    total  = len(result)
    used   = set()

    # Issue 1: Negative transit time
    idx1 = random.randint(0, total - 1)
    result[idx1]["transit_time_hours"] = -abs(result[idx1]["transit_time_hours"])
    used.add(idx1)

    # Issue 2: Arrival before departure
    idx2 = random.choice([i for i in range(total) if i not in used])
    dep_dt = datetime.strptime(result[idx2]["departure_datetime"], "%Y-%m-%d %H:%M:%S")
    result[idx2]["arrival_datetime"]   = result[idx2]["departure_datetime"]
    result[idx2]["departure_datetime"] = (dep_dt + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    used.add(idx2)

    # Issue 3: NULL carrier ~1%
    null_count   = max(1, round(total * 0.01))
    null_indices = random.sample([i for i in range(total) if i not in used], null_count)
    for i in null_indices:
        result[i]["carrier"] = ""
    used.update(null_indices)

    # Issue 4: CBM over capacity ~0.5%
    cbm_count   = max(1, round(total * 0.005))
    cbm_indices = random.sample([i for i in range(total) if i not in used], cbm_count)
    for i in cbm_indices:
        result[i]["cbm"] = result[i]["container_count"] + round(random.uniform(0.5, 3.0), 2)
    used.update(cbm_indices)

    # Issue 5: Wrong truck ID format
    idx5 = random.choice([i for i in range(total) if i not in used])
    result[idx5]["truck_id"] = result[idx5]["truck_id"].replace("TRK_", "TRUCK-")

    return result


# ── WRITE CSV ─────────────────────────────────────────────────────────────────

def write_csv(rows, output_path):
    fieldnames = [
        "truck_id", "origin_node_id", "destination_node_id", "carrier",
        "departure_datetime", "transit_time_hours", "arrival_datetime",
        "container_count", "package_count", "cbm",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Building network connectivity...")
    network     = build_network()
    node_lookup = load_node_lookup(network)

    print("Assigning carriers to lanes...")
    lane_carriers = build_lane_carrier_map(network["all_lanes"])

    print("Generating truck runs...")
    rows = generate_transportation(network, lane_carriers, node_lookup)

    print("Injecting data quality issues...")
    rows_with_issues = inject_quality_issues(rows)

    output_path = os.path.join(DATA_DIR, "tms_transportation.csv")
    print("Writing CSV...")
    write_csv(rows_with_issues, output_path)

    total = len(rows_with_issues)
    print()
    print(f"Transportation generation complete:")
    print(f"  Simulation period:  {SIM_START} to {SIM_END}")
    print(f"  Total truck runs:   {total:,}")
    print(f"  Output:             {output_path}")
    print()

    # Container distribution
    bands = {
        "light  (1-9)":   0,
        "mid    (10-25)": 0,
        "heavy  (26-29)": 0,
        "peak   (30-32)": 0,
    }
    for row in rows:
        c = row["container_count"]
        if c <= 9:
            bands["light  (1-9)"] += 1
        elif c <= 25:
            bands["mid    (10-25)"] += 1
        elif c <= 29:
            bands["heavy  (26-29)"] += 1
        else:
            bands["peak   (30-32)"] += 1
    print("Container distribution (clean data):")
    for band, count in bands.items():
        pct = count / len(rows) * 100
        print(f"  {band}  {count:>8,}  ({pct:.1f}%)")
    print()

    # SLA — direct lanes
    direct_rows  = [
        r for r in rows
        if r["origin_node_id"].startswith("ON_")
        and r["destination_node_id"].startswith("DN_")
    ]
    on_time_d    = sum(
        1 for r in direct_rows
        if datetime.strptime(r["arrival_datetime"], "%Y-%m-%d %H:%M:%S").hour < 10
    )
    total_direct = len(direct_rows)
    if total_direct:
        print(f"Direct lane SLA (arrival before 10:00):")
        print(f"  On time:  {on_time_d:,} / {total_direct:,}  ({on_time_d/total_direct*100:.1f}%)")
        print(f"  Late:     {total_direct-on_time_d:,}  ({(total_direct-on_time_d)/total_direct*100:.1f}%)")
    print()

    # SLA — indirect leg2
    leg2_rows  = [
        r for r in rows
        if r["origin_node_id"].startswith("CH_")
        and r["destination_node_id"].startswith("DN_")
    ]
    on_time_l2 = sum(
        1 for r in leg2_rows
        if datetime.strptime(r["arrival_datetime"], "%Y-%m-%d %H:%M:%S").hour < 10
    )
    total_leg2 = len(leg2_rows)
    if total_leg2:
        print(f"Indirect leg2 SLA (arrival before 10:00):")
        print(f"  On time:  {on_time_l2:,} / {total_leg2:,}  ({on_time_l2/total_leg2*100:.1f}%)")
        print(f"  Late:     {total_leg2-on_time_l2:,}  ({(total_leg2-on_time_l2)/total_leg2*100:.1f}%)")
    print()

    # Hub departure window check
    hub_departs = leg2_rows
    in_window   = sum(
        1 for r in hub_departs
        if datetime.strptime(r["departure_datetime"], "%Y-%m-%d %H:%M:%S").hour >= 22
        or datetime.strptime(r["departure_datetime"], "%Y-%m-%d %H:%M:%S").hour <= 5
    )
    print(f"Hub departure window check (22:00-05:00):")
    print(f"  In window: {in_window:,} / {len(hub_departs):,}  ({in_window/len(hub_departs)*100:.1f}%)")
    print()

    print("Quality issues injected:")
    print(f"  Negative transit time:  1 row")
    print(f"  Arrival before depart:  1 row")
    print(f"  NULL carrier:           ~{max(1, round(len(rows)*0.01)):,} rows")
    print(f"  CBM over capacity:      ~{max(1, round(len(rows)*0.005)):,} rows")
    print(f"  Wrong truck ID format:  1 row")


if __name__ == "__main__":
    main()