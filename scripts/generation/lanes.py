"""
lanes.py
--------
Internal module — no CSV output.

Builds the complete network connectivity map including:
  - Lane connectivity (same-country priority, cross-border rules)
  - Lane volume profiles (heavy/medium/thin)
  - Origin container type tiers (controls direct lane container correctness)
  - Hub container type tiers (controls leg2 lane container correctness)
  - Node capability map (MTL/BOX/ALL)
  - 10% deactivation on same-country lanes only

Lane volume profiles:
  Heavy  (25%) — 5-10 trucks/day, fill rate 0.75-1.00
  Medium (50%) — 2-5  trucks/day, fill rate 0.45-0.74
  Thin   (25%) — 0-1  trucks/day (80% run probability), fill rate 0.15-0.44

Origin container type tiers (direct lanes only):
  High   (50% of origins) — correct container type 75-80% of the time
  Medium (30% of origins) — correct container type 60-74% of the time
  Low    (20% of origins) — correct container type 40-59% of the time

Hub container type tiers (leg2 lanes):
  All hubs — correct container type 85-95% of the time
  Per-lane variation of +-3% within hub tier range

Cross-border rules:
  - Always indirect (origin -> foreign CH -> DN)
  - Always active (no deactivation)
  - Origin always sends BOX to consolidation hub (leg1)
"""

import csv
import math
import os
import random
from collections import defaultdict

random.seed(42)

# ── HELPERS ──────────────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = (math.sin(dphi/2)**2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_network(csv_path):
    """
    Loads mdm_network.csv.
    Skips duplicates, invalid node_type, missing coordinates.
    Normalises country to uppercase.
    Returns (origins, hubs, deliveries, capability_map).
    """
    origins, hubs, deliveries = [], [], []
    capability_map = {}
    seen_ids       = set()
    valid_types    = {"origin_node", "consolidation_hub", "delivery_node"}
    valid_caps     = {"MTL", "BOX", "ALL"}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_id = row["node_id"].strip()
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)

            node_type = row["node_type"].strip()
            if node_type not in valid_types:
                continue

            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (ValueError, TypeError):
                continue

            country = row["country"].strip().upper()
            cap     = row.get("node_capability", "").strip().upper()
            if cap not in valid_caps:
                cap = "ALL"

            node = {
                "node_id":   node_id,
                "node_type": node_type,
                "city":      row["city"].strip(),
                "country":   country,
                "latitude":  lat,
                "longitude": lon,
            }
            capability_map[node_id] = cap

            if node_type == "origin_node":
                origins.append(node)
            elif node_type == "consolidation_hub":
                hubs.append(node)
            elif node_type == "delivery_node":
                deliveries.append(node)

    return origins, hubs, deliveries, capability_map


def nearest_nodes(source, candidates, n=None):
    scored = sorted(
        candidates,
        key=lambda c: haversine_km(
            source["latitude"], source["longitude"],
            c["latitude"],      c["longitude"]
        )
    )
    return scored if n is None else scored[:n]


# ── STEP 1: HUB COVERAGE ─────────────────────────────────────────────────────

def assign_hub_coverage(hubs, deliveries):
    hub_coverage = {h["node_id"]: [] for h in hubs}

    for hub in hubs:
        capacity     = random.randint(10, 20)
        same_country = [d for d in deliveries if d["country"] == hub["country"]]
        other        = [d for d in deliveries if d["country"] != hub["country"]]

        candidates = nearest_nodes(hub, same_country)
        assigned   = [d["node_id"] for d in candidates[:capacity]]

        remaining = capacity - len(assigned)
        if remaining > 0:
            cross    = nearest_nodes(hub, other)
            assigned += [d["node_id"] for d in cross[:remaining]]

        hub_coverage[hub["node_id"]] = assigned

    all_dn_ids = {d["node_id"] for d in deliveries}
    covered    = {dn for dns in hub_coverage.values() for dn in dns}
    uncovered  = all_dn_ids - covered

    for dn_id in uncovered:
        dn = next(d for d in deliveries if d["node_id"] == dn_id)
        for hub in nearest_nodes(dn, hubs):
            if len(hub_coverage[hub["node_id"]]) < 20:
                hub_coverage[hub["node_id"]].append(dn_id)
                break

    return hub_coverage


# ── STEP 2: ORIGIN LANE ASSIGNMENT ───────────────────────────────────────────

def assign_origin_lanes(origins, hubs, deliveries, hub_coverage):
    dn_by_country  = defaultdict(list)
    hub_by_country = defaultdict(list)
    for d in deliveries:
        dn_by_country[d["country"]].append(d)
    for h in hubs:
        hub_by_country[h["country"]].append(h)

    origin_lanes = {}

    for origin in origins:
        oc  = origin["country"]
        oid = origin["node_id"]

        capacity  = random.randint(5, 15)
        n_hubs    = max(1, round(capacity * 0.33))
        n_direct  = capacity - n_hubs

        same_dns     = nearest_nodes(origin, dn_by_country[oc])
        direct_dns   = [d["node_id"] for d in same_dns[:n_direct]]

        same_chs     = nearest_nodes(origin, hub_by_country[oc])
        indirect_chs = [h["node_id"] for h in same_chs[:n_hubs]]

        foreign_hubs        = [h for h in hubs if h["country"] != oc]
        foreign_hubs_sorted = nearest_nodes(origin, foreign_hubs)

        cross_border_chs = []
        if foreign_hubs_sorted:
            cross_border_chs.append(foreign_hubs_sorted[0]["node_id"])

        remaining_cap = capacity - len(direct_dns) - len(indirect_chs)
        if remaining_cap > 0 and len(foreign_hubs_sorted) > 1:
            for h in foreign_hubs_sorted[1:remaining_cap + 1]:
                cross_border_chs.append(h["node_id"])

        direct_dn_set = set(direct_dns)
        indirect_map  = {}
        for hub_id in indirect_chs + cross_border_chs:
            covered   = hub_coverage.get(hub_id, [])
            available = [dn for dn in covered if dn not in direct_dn_set]
            if available:
                indirect_map[hub_id] = available

        origin_lanes[oid] = {
            "direct":            direct_dns,
            "indirect":          indirect_map,
            "cross_border_hubs": cross_border_chs,
        }

    return origin_lanes


# ── STEP 3: LANE VOLUME PROFILES ─────────────────────────────────────────────

def assign_lane_volume_profiles(all_lanes):
    PROFILES = {
        "heavy":  {"fill_min": 0.75, "fill_max": 1.00, "truck_min": 5, "truck_max": 10, "thin_prob": 1.0},
        "medium": {"fill_min": 0.45, "fill_max": 0.74, "truck_min": 2, "truck_max": 5,  "thin_prob": 1.0},
        "thin":   {"fill_min": 0.15, "fill_max": 0.44, "truck_min": 1, "truck_max": 1,  "thin_prob": 0.80},
    }

    active = list(dict.fromkeys(
        (l["origin_id"], l["destination_id"])
        for l in all_lanes if l["is_active"]
    ))
    random.shuffle(active)

    n        = len(active)
    n_heavy  = round(n * 0.25)
    n_medium = round(n * 0.50)

    profile_map = {}
    for i, key in enumerate(active):
        if i < n_heavy:
            p = "heavy"
        elif i < n_heavy + n_medium:
            p = "medium"
        else:
            p = "thin"
        profile_map[key] = {"profile": p, **PROFILES[p]}

    return profile_map


# ── STEP 4: ORIGIN CONTAINER TYPE TIERS ──────────────────────────────────────

def assign_origin_container_tiers(origins):
    TIERS = {
        "high":   {"rate_min": 0.75, "rate_max": 0.80},
        "medium": {"rate_min": 0.60, "rate_max": 0.74},
        "low":    {"rate_min": 0.40, "rate_max": 0.59},
    }

    shuffled = origins.copy()
    random.shuffle(shuffled)
    n        = len(shuffled)
    n_high   = round(n * 0.50)
    n_medium = round(n * 0.30)

    origin_tiers = {}
    for i, origin in enumerate(shuffled):
        if i < n_high:
            tier = "high"
        elif i < n_high + n_medium:
            tier = "medium"
        else:
            tier = "low"
        origin_tiers[origin["node_id"]] = {"tier": tier, **TIERS[tier]}

    return origin_tiers


# ── STEP 5: HUB CONTAINER TYPE TIERS ─────────────────────────────────────────

def assign_hub_container_tiers(hubs):
    hub_tiers = {}
    for hub in hubs:
        base_min = round(random.uniform(0.85, 0.90), 2)
        base_max = min(round(base_min + random.uniform(0.03, 0.07), 2), 0.97)
        hub_tiers[hub["node_id"]] = {
            "tier":     "high",
            "rate_min": base_min,
            "rate_max": base_max,
        }
    return hub_tiers


# ── STEP 6: PER-LANE CONTAINER CORRECTNESS RATE ───────────────────────────────

def assign_lane_container_rates(all_lanes, origin_tiers, hub_tiers):
    lane_rates = {}

    for lane in all_lanes:
        if not lane["is_active"]:
            continue

        lane_type = lane["lane_type"]
        origin_id = lane["origin_id"]
        dest_id   = lane["destination_id"]
        key       = (origin_id, dest_id)

        if lane_type == "direct":
            tier = origin_tiers.get(origin_id, {"rate_min": 0.60, "rate_max": 0.74})
            lane_rates[key] = round(random.uniform(tier["rate_min"], tier["rate_max"]), 4)

        elif lane_type == "indirect_leg2":
            tier = hub_tiers.get(origin_id, {"rate_min": 0.85, "rate_max": 0.95})
            lane_rates[key] = round(random.uniform(tier["rate_min"], tier["rate_max"]), 4)

    return lane_rates


# ── STEP 7: BUILD ACTIVE LANE SET ─────────────────────────────────────────────

def build_active_lanes(origin_lanes, cross_border_check):
    all_lanes    = []
    active_lanes = set()

    for origin_id, lanes in origin_lanes.items():

        for dn_id in lanes["direct"]:
            is_cb       = cross_border_check(origin_id, dn_id)
            deactivated = (not is_cb) and (random.random() < 0.10)
            lane        = {
                "origin_id":       origin_id,
                "destination_id":  dn_id,
                "lane_type":       "direct",
                "is_cross_border": False,
                "is_active":       not deactivated,
            }
            all_lanes.append(lane)
            if not deactivated:
                active_lanes.add((origin_id, dn_id))

        for hub_id, dn_list in lanes["indirect"].items():
            is_cb = hub_id in lanes["cross_border_hubs"]

            deact_leg1 = (not is_cb) and (random.random() < 0.10)
            leg1 = {
                "origin_id":       origin_id,
                "destination_id":  hub_id,
                "lane_type":       "indirect_leg1",
                "is_cross_border": is_cb,
                "is_active":       True if is_cb else not deact_leg1,
            }
            all_lanes.append(leg1)
            if leg1["is_active"]:
                active_lanes.add((origin_id, hub_id))

            for dn_id in dn_list:
                deact_leg2 = (not is_cb) and (random.random() < 0.10)
                leg2 = {
                    "origin_id":       hub_id,
                    "destination_id":  dn_id,
                    "lane_type":       "indirect_leg2",
                    "is_cross_border": is_cb,
                    "is_active":       True if is_cb else not deact_leg2,
                }
                all_lanes.append(leg2)
                if leg2["is_active"]:
                    active_lanes.add((hub_id, dn_id))

    return all_lanes, active_lanes


# ── MAIN BUILD FUNCTION ───────────────────────────────────────────────────────

def build_network(csv_path=None):
    """
    Entry point for other scripts.

    Returns dict with:
      origins, hubs, deliveries
      hub_coverage
      origin_lanes
      all_lanes
      active_lanes
      node_country
      capability_map
      lane_volume_profiles
      origin_tiers
      hub_tiers
      lane_container_rates
    """
    if csv_path is None:
        base     = os.path.dirname(__file__)
        csv_path = os.path.join(base, "..", "..", "data", "mdm_network.csv")

    origins, hubs, deliveries, capability_map = load_network(csv_path)

    node_country = {}
    for n in origins + hubs + deliveries:
        node_country[n["node_id"]] = n["country"]

    def cross_border_check(origin_id, dest_id):
        return node_country.get(origin_id) != node_country.get(dest_id)

    hub_coverage  = assign_hub_coverage(hubs, deliveries)
    origin_lanes  = assign_origin_lanes(origins, hubs, deliveries, hub_coverage)
    all_lanes, active_lanes = build_active_lanes(origin_lanes, cross_border_check)

    lane_volume_profiles = assign_lane_volume_profiles(all_lanes)
    origin_tiers         = assign_origin_container_tiers(origins)
    hub_tiers            = assign_hub_container_tiers(hubs)
    lane_container_rates = assign_lane_container_rates(all_lanes, origin_tiers, hub_tiers)

    total        = len(all_lanes)
    active       = len(active_lanes)
    cross_border = sum(1 for l in all_lanes if l["is_cross_border"] and l["is_active"])
    direct       = sum(1 for l in all_lanes if l["lane_type"] == "direct" and l["is_active"])
    leg1         = sum(1 for l in all_lanes if l["lane_type"] == "indirect_leg1" and l["is_active"])

    profiles = list(lane_volume_profiles.values())
    n_heavy  = sum(1 for p in profiles if p["profile"] == "heavy")
    n_medium = sum(1 for p in profiles if p["profile"] == "medium")
    n_thin   = sum(1 for p in profiles if p["profile"] == "thin")

    o_high   = sum(1 for t in origin_tiers.values() if t["tier"] == "high")
    o_medium = sum(1 for t in origin_tiers.values() if t["tier"] == "medium")
    o_low    = sum(1 for t in origin_tiers.values() if t["tier"] == "low")

    print(f"Network connectivity built:")
    print(f"  Origins:                {len(origins)}")
    print(f"  Consolidation hubs:     {len(hubs)}")
    print(f"  Delivery nodes:         {len(deliveries)}")
    print(f"  Total lanes defined:    {total}")
    print(f"  Active lanes:           {active}")
    print(f"  Cross-border active:    {cross_border}")
    print(f"  Direct active:          {direct}")
    print(f"  Indirect leg1 active:   {leg1}")
    print()
    print(f"  Lane volume profiles:")
    print(f"    Heavy:  {n_heavy}  ({n_heavy/len(profiles)*100:.0f}%)")
    print(f"    Medium: {n_medium}  ({n_medium/len(profiles)*100:.0f}%)")
    print(f"    Thin:   {n_thin}  ({n_thin/len(profiles)*100:.0f}%)")
    print()
    print(f"  Origin container tiers:")
    print(f"    High:   {o_high}  (75-80% correct)")
    print(f"    Medium: {o_medium}  (60-74% correct)")
    print(f"    Low:    {o_low}  (40-59% correct)")
    print()

    origins_with_cb = set(
        l["origin_id"] for l in all_lanes
        if l["is_cross_border"] and l["is_active"] and l["lane_type"] == "indirect_leg1"
    )
    print(f"  Origins with >=1 cross-border CH: {len(origins_with_cb)}/{len(origins)}")

    reachable   = set(
        l["destination_id"] for l in all_lanes
        if l["is_active"] and l["lane_type"] in ("direct", "indirect_leg2")
    )
    unreachable = {d["node_id"] for d in deliveries} - reachable
    print(f"  Unreachable delivery nodes:       {len(unreachable)}")
    if unreachable:
        print(f"  WARNING: {unreachable}")

    return {
        "origins":              origins,
        "hubs":                 hubs,
        "deliveries":           deliveries,
        "hub_coverage":         hub_coverage,
        "origin_lanes":         origin_lanes,
        "all_lanes":            all_lanes,
        "active_lanes":         active_lanes,
        "node_country":         node_country,
        "capability_map":       capability_map,
        "lane_volume_profiles": lane_volume_profiles,
        "origin_tiers":         origin_tiers,
        "hub_tiers":            hub_tiers,
        "lane_container_rates": lane_container_rates,
    }


if __name__ == "__main__":
    build_network()