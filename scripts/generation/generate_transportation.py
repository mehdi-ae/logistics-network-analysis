"""
generate_transportation.py
--------------------------
Generates tms_transportation.csv — one row per truck run.

Simulation period: July 1 – December 31 2025 (184 days)

Changes from previous version:
  - packages_per_cbm reduced to 3-5 (was 8-15) for BigQuery storage
  - Fill rate driven by lane volume profile (heavy/medium/thin)
  - Truck count per day driven by lane volume profile
  - Thin lanes: 80% chance of running on any given day

Lane volume profiles:
  Heavy  (25%) — 5-10 trucks/day, fill rate 0.75-1.00
  Medium (50%) — 2-5  trucks/day, fill rate 0.45-0.74
  Thin   (25%) — 0-1  trucks/day (80% run probability), fill rate 0.15-0.44

Seasonality (applied to fill rate, capped at 1.0):
  July-August:    0.85
  September-Oct:  1.00
  November:       1.15
  December:       1.30

Timing:
  indirect_leg1: arrival-first, target 06:00-20:59
  indirect_leg2: departure-first, hard window 22:00-05:00
  direct:        arrival-first, explicit 90/10 SLA split

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
    11: 1.15,
    12: 1.30,
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
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = (math.sin(dphi/2)**2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def expected_transit_hours(distance_km):
    return distance_km / AVG_SPEED_KMH


def actual_transit_hours(expected_h):
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


def draw_cbm(container_count, fill_min, fill_max):
    fill_rate = min(random.uniform(fill_min, fill_max), 1.0)
    return round(container_count * fill_rate, 2)


def draw_package_count(cbm):
    packages_per_cbm = random.uniform(3, 5)
    return max(1, round(cbm * packages_per_cbm))


def load_node_lookup(network):
    lookup = {}
    for node in network["origins"] + network["hubs"] + network["deliveries"]:
        lookup[node["node_id"]] = node
    return lookup


# ── TIMING FUNCTIONS ─────────────────────────────────────────────────────────

def timing_direct(sim_date, transit_h):
    minute = random.randint(0, 59)
    hour   = random.randint(6, 9) if random.random() < 0.90 else random.randint(10, 14)
    arrival_dt   = datetime(sim_date.year, sim_date.month, sim_date.day, hour, minute)
    departure_dt = arrival_dt - timedelta(hours=transit_h)
    return departure_dt, arrival_dt


def timing_leg1(sim_date, transit_h):
    hour         = random.randint(6, 20)
    minute       = random.randint(0, 59)
    arrival_dt   = datetime(sim_date.year, sim_date.month, sim_date.day, hour, minute)
    departure_dt = arrival_dt - timedelta(hours=transit_h)
    return departure_dt, arrival_dt


def timing_leg2(sim_date, transit_h):
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


# ── LANE TRUCK COUNT ──────────────────────────────────────────────────────────

def daily_truck_count(profile, month):
    multiplier = SEASONALITY.get(month, 1.0)
    if profile["profile"] == "thin":
        if random.random() > profile["thin_prob"]:
            return 0
        return 1
    adjusted_max = min(
        max(profile["truck_min"], round(profile["truck_max"] * multiplier)),
        profile["truck_max"]
    )
    return random.randint(profile["truck_min"], adjusted_max)


# ── CARRIER ASSIGNMENT ────────────────────────────────────────────────────────

def build_lane_carrier_map(all_lanes):
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
    profiles      = network["lane_volume_profiles"]

    current_date = SIM_START
    while current_date <= SIM_END:
        month = current_date.month

        for lane in network["all_lanes"]:
            if not lane["is_active"]:
                continue

            origin_id = lane["origin_id"]
            dest_id   = lane["destination_id"]
            lane_type = lane["lane_type"]
            lane_key  = (origin_id, dest_id)

            origin_node = node_lookup.get(origin_id)
            dest_node   = node_lookup.get(dest_id)
            if not origin_node or not dest_node:
                continue

            profile = profiles.get(lane_key, {
                "profile": "medium", "fill_min": 0.45, "fill_max": 0.74,
                "truck_min": 2, "truck_max": 5, "thin_prob": 1.0
            })

            n_trucks = daily_truck_count(profile, month)
            if n_trucks == 0:
                continue

            distance_km = haversine_km(
                origin_node["latitude"], origin_node["longitude"],
                dest_node["latitude"],   dest_node["longitude"]
            )
            expected_h = expected_transit_hours(distance_km)
            carriers   = lane_carriers.get(lane_key, ["EuroHaul", "TransCargo"])

            multiplier = SEASONALITY.get(month, 1.0)
            fill_min   = max(min(profile["fill_min"] * multiplier, 1.0), 0.10)
            fill_max   = min(profile["fill_max"] * multiplier, 1.0)

            for _ in range(n_trucks):
                transit_h = actual_transit_hours(expected_h)

                if lane_type == "indirect_leg2":
                    departure_dt, arrival_dt = timing_leg2(current_date, transit_h)
                elif lane_type == "indirect_leg1":
                    departure_dt, arrival_dt = timing_leg1(current_date, transit_h)
                else:
                    departure_dt, arrival_dt = timing_direct(current_date, transit_h)

                container_count = draw_container_count()
                cbm             = draw_cbm(container_count, fill_min, fill_max)
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

    idx1 = random.randint(0, total - 1)
    result[idx1]["transit_time_hours"] = -abs(result[idx1]["transit_time_hours"])
    used.add(idx1)

    idx2   = random.choice([i for i in range(total) if i not in used])
    dep_dt = datetime.strptime(result[idx2]["departure_datetime"], "%Y-%m-%d %H:%M:%S")
    result[idx2]["arrival_datetime"]   = result[idx2]["departure_datetime"]
    result[idx2]["departure_datetime"] = (dep_dt + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    used.add(idx2)

    null_count   = max(1, round(total * 0.01))
    null_indices = random.sample([i for i in range(total) if i not in used], null_count)
    for i in null_indices:
        result[i]["carrier"] = ""
    used.update(null_indices)

    cbm_count   = max(1, round(total * 0.005))
    cbm_indices = random.sample([i for i in range(total) if i not in used], cbm_count)
    for i in cbm_indices:
        result[i]["cbm"] = result[i]["container_count"] + round(random.uniform(0.5, 3.0), 2)
    used.update(cbm_indices)

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

    bands = {"light  (1-9)": 0, "mid    (10-25)": 0, "heavy  (26-29)": 0, "peak   (30-32)": 0}
    for row in rows:
        c = row["container_count"]
        if c <= 9:        bands["light  (1-9)"]   += 1
        elif c <= 25:     bands["mid    (10-25)"]  += 1
        elif c <= 29:     bands["heavy  (26-29)"]  += 1
        else:             bands["peak   (30-32)"]  += 1
    print("Container distribution (clean data):")
    for band, count in bands.items():
        pct = count / len(rows) * 100
        print(f"  {band}  {count:>8,}  ({pct:.1f}%)")
    print()

    profiles = network["lane_volume_profiles"]
    cbm_by_profile = {"heavy": [], "medium": [], "thin": []}
    for row in rows:
        key = (row["origin_node_id"], row["destination_node_id"])
        p   = profiles.get(key, {}).get("profile", "medium")
        cbm_by_profile[p].append(row["cbm"] / row["container_count"])
    print("Average fill rate by lane profile (clean data):")
    for p, rates in cbm_by_profile.items():
        if rates:
            print(f"  {p:<8}  avg fill rate: {sum(rates)/len(rates):.2f}")
    print()

    direct_rows = [r for r in rows
                   if r["origin_node_id"].startswith("ON_")
                   and r["destination_node_id"].startswith("DN_")]
    on_time_d   = sum(1 for r in direct_rows
                      if datetime.strptime(r["arrival_datetime"], "%Y-%m-%d %H:%M:%S").hour < 10)
    if direct_rows:
        print(f"Direct lane SLA (before 10:00):")
        print(f"  On time: {on_time_d:,} / {len(direct_rows):,}  ({on_time_d/len(direct_rows)*100:.1f}%)")
    print()

    leg2_rows  = [r for r in rows
                  if r["origin_node_id"].startswith("CH_")
                  and r["destination_node_id"].startswith("DN_")]
    on_time_l2 = sum(1 for r in leg2_rows
                     if datetime.strptime(r["arrival_datetime"], "%Y-%m-%d %H:%M:%S").hour < 10)
    in_window  = sum(1 for r in leg2_rows
                     if datetime.strptime(r["departure_datetime"], "%Y-%m-%d %H:%M:%S").hour >= 22
                     or datetime.strptime(r["departure_datetime"], "%Y-%m-%d %H:%M:%S").hour <= 5)
    if leg2_rows:
        print(f"Indirect leg2 SLA (before 10:00):")
        print(f"  On time:   {on_time_l2:,} / {len(leg2_rows):,}  ({on_time_l2/len(leg2_rows)*100:.1f}%)")
        print(f"  Hub depart window check: {in_window:,} / {len(leg2_rows):,}  ({in_window/len(leg2_rows)*100:.1f}%)")
    print()

    print("Quality issues injected:")
    print(f"  Negative transit time:  1 row")
    print(f"  Arrival before depart:  1 row")
    print(f"  NULL carrier:           ~{max(1, round(len(rows)*0.01)):,} rows")
    print(f"  CBM over capacity:      ~{max(1, round(len(rows)*0.005)):,} rows")
    print(f"  Wrong truck ID format:  1 row")


if __name__ == "__main__":
    main()