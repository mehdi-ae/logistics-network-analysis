"""
lanes.py
--------
Internal module — not a source system table, no CSV output.

Builds the complete network connectivity map for the EU logistics network.
Imported by generate_transportation.py and generate_shipment_details.py.

Rules implemented:
  1. Same-country connections filled first (direct DN + indirect CH)
  2. Cross-border connections always go to a consolidation hub, never a DN directly
  3. Minimum 1 guaranteed cross-border CH connection per origin
  4. Geographic expansion to nearest neighboring country when capacity remains
  5. Hub coverage: 10-20 delivery nodes, same-country first
  6. 67/33 split: direct DN vs consolidation hub connections per origin
  7. 10% deactivation applied to same-country lanes only
  8. Path exclusivity: a given (origin, delivery_node) pair is always direct
     or always indirect — never both
  9. Cross-border lanes are always indirect and always active

Output: build_network() returns a dict with:
  - hub_coverage:   {hub_id: [dn_id, ...]}
  - origin_lanes:   {origin_id: {"direct": [dn_id, ...],
                                  "indirect": {hub_id: [dn_id, ...]},
                                  "cross_border_hubs": [hub_id, ...]}}
  - active_lanes:   set of (origin_id, destination_id) tuples that are active
  - all_lanes:      list of dicts describing every lane
"""

import csv
import math
import random
import os
from collections import defaultdict

random.seed(42)

# ── HELPERS ──────────────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    """Returns distance in km between two lat/lon points."""
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_network(csv_path):
    """
    Loads mdm_network.csv and returns clean node lists.
    Skips rows with missing coordinates or invalid node_type (quality issues).
    Deduplicates on node_id keeping first occurrence.
    """
    origins, hubs, deliveries = [], [], []
    seen_ids = set()
    valid_types = {"origin_node", "consolidation_hub", "delivery_node"}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node_id = row["node_id"].strip()

            # Skip duplicates
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)

            # Skip invalid node_type (injected quality issue)
            node_type = row["node_type"].strip()
            if node_type not in valid_types:
                continue

            # Skip missing coordinates (injected quality issue)
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (ValueError, TypeError):
                continue

            # Normalise country code to uppercase (injected quality issue)
            country = row["country"].strip().upper()

            node = {
                "node_id": node_id,
                "node_type": node_type,
                "city": row["city"].strip(),
                "country": country,
                "latitude": lat,
                "longitude": lon,
            }

            if node_type == "origin_node":
                origins.append(node)
            elif node_type == "consolidation_hub":
                hubs.append(node)
            elif node_type == "delivery_node":
                deliveries.append(node)

    return origins, hubs, deliveries


def nearest_nodes(source, candidates, n=None):
    """Returns candidates sorted by haversine distance from source."""
    scored = sorted(
        candidates,
        key=lambda c: haversine_km(
            source["latitude"], source["longitude"],
            c["latitude"], c["longitude"]
        )
    )
    return scored if n is None else scored[:n]


# ── STEP 1: HUB COVERAGE ─────────────────────────────────────────────────────

def assign_hub_coverage(hubs, deliveries):
    """
    Each hub covers 10-20 delivery nodes.
    Same-country delivery nodes are assigned first.
    Remaining slots filled by nearest cross-country delivery nodes.
    Every delivery node must be covered by at least one hub.
    """
    hub_coverage = {h["node_id"]: [] for h in hubs}

    for hub in hubs:
        capacity = random.randint(10, 20)
        same_country = [d for d in deliveries if d["country"] == hub["country"]]
        other_country = [d for d in deliveries if d["country"] != hub["country"]]

        # Fill same-country first, sorted by distance
        candidates = nearest_nodes(hub, same_country)
        assigned = [d["node_id"] for d in candidates[:capacity]]

        # Fill remaining slots with nearest cross-country nodes
        remaining = capacity - len(assigned)
        if remaining > 0:
            cross = nearest_nodes(hub, other_country)
            assigned += [d["node_id"] for d in cross[:remaining]]

        hub_coverage[hub["node_id"]] = assigned

    # Guarantee every delivery node is covered by at least one hub
    all_dn_ids = {d["node_id"] for d in deliveries}
    covered = {dn for dns in hub_coverage.values() for dn in dns}
    uncovered = all_dn_ids - covered

    for dn_id in uncovered:
        dn = next(d for d in deliveries if d["node_id"] == dn_id)
        # Assign to nearest hub that has room (capacity max 20)
        sorted_hubs = nearest_nodes(dn, hubs)
        for hub in sorted_hubs:
            if len(hub_coverage[hub["node_id"]]) < 20:
                hub_coverage[hub["node_id"]].append(dn_id)
                break

    return hub_coverage


# ── STEP 2: ORIGIN LANE ASSIGNMENT ───────────────────────────────────────────

def assign_origin_lanes(origins, hubs, deliveries, hub_coverage):
    """
    For each origin:
      - Draw total capacity from U[5, 15]
      - Split 67% direct DN / 33% indirect CH (rounded, direct gets remainder)
      - Fill same-country direct DNs first
      - Fill same-country indirect CHs
      - Guarantee minimum 1 cross-border CH connection
      - Use remaining capacity for nearest cross-border CH
      - Enforce path exclusivity per (origin, delivery_node) pair
    """
    dn_by_country = defaultdict(list)
    for d in deliveries:
        dn_by_country[d["country"]].append(d)

    hub_by_country = defaultdict(list)
    for h in hubs:
        hub_by_country[h["country"]].append(h)

    origin_lanes = {}

    for origin in origins:
        oc = origin["country"]
        oid = origin["node_id"]

        # Draw capacity and split
        capacity = random.randint(5, 15)
        n_hubs = max(1, round(capacity * 0.33))
        n_direct = capacity - n_hubs

        # ── Same-country direct DNs ──
        same_dns = nearest_nodes(origin, dn_by_country[oc])
        direct_dns = [d["node_id"] for d in same_dns[:n_direct]]

        # ── Same-country indirect CHs ──
        same_chs = nearest_nodes(origin, hub_by_country[oc])
        indirect_chs = [h["node_id"] for h in same_chs[:n_hubs]]

        # ── Cross-border CHs (always indirect, always active) ──
        foreign_hubs = [h for h in hubs if h["country"] != oc]
        foreign_hubs_sorted = nearest_nodes(origin, foreign_hubs)

        # Guaranteed minimum 1 cross-border CH
        cross_border_chs = []
        if foreign_hubs_sorted:
            cross_border_chs.append(foreign_hubs_sorted[0]["node_id"])

        # Fill remaining capacity with next nearest foreign CHs
        remaining_capacity = capacity - len(direct_dns) - len(indirect_chs)
        if remaining_capacity > 0 and len(foreign_hubs_sorted) > 1:
            for h in foreign_hubs_sorted[1:remaining_capacity + 1]:
                cross_border_chs.append(h["node_id"])

        # ── Build indirect DN map per hub ──
        # Indirect DNs = hub coverage minus any already assigned as direct
        direct_dn_set = set(direct_dns)
        indirect_map = {}
        for hub_id in indirect_chs + cross_border_chs:
            covered_dns = hub_coverage.get(hub_id, [])
            # Path exclusivity: exclude DNs already assigned direct
            available = [dn for dn in covered_dns if dn not in direct_dn_set]
            if available:
                indirect_map[hub_id] = available

        origin_lanes[oid] = {
            "direct": direct_dns,
            "indirect": indirect_map,
            "cross_border_hubs": cross_border_chs,
        }

    return origin_lanes


# ── STEP 3: BUILD ACTIVE LANE SET ─────────────────────────────────────────────

def build_active_lanes(origin_lanes, cross_border_check):
    """
    Builds the full set of (origin, destination) active lanes.
    Applies 10% deactivation to same-country lanes only.
    Cross-border lanes (origin → foreign CH) are always active.
    """
    all_lanes = []
    active_lanes = set()

    for origin_id, lanes in origin_lanes.items():

        # Direct same-country lanes
        for dn_id in lanes["direct"]:
            is_cross_border = cross_border_check(origin_id, dn_id)
            deactivated = (not is_cross_border) and (random.random() < 0.10)
            lane = {
                "origin_id": origin_id,
                "destination_id": dn_id,
                "lane_type": "direct",
                "is_cross_border": False,
                "is_active": not deactivated,
            }
            all_lanes.append(lane)
            if not deactivated:
                active_lanes.add((origin_id, dn_id))

        # Indirect lanes (same-country and cross-border CHs)
        for hub_id, dn_list in lanes["indirect"].items():
            is_cross_border = hub_id in lanes["cross_border_hubs"]

            # Origin → Hub leg
            deactivated_leg1 = (not is_cross_border) and (random.random() < 0.10)
            lane_leg1 = {
                "origin_id": origin_id,
                "destination_id": hub_id,
                "lane_type": "indirect_leg1",
                "is_cross_border": is_cross_border,
                "is_active": True if is_cross_border else not deactivated_leg1,
            }
            all_lanes.append(lane_leg1)
            if lane_leg1["is_active"]:
                active_lanes.add((origin_id, hub_id))

            # Hub → DN legs
            for dn_id in dn_list:
                deactivated_leg2 = (not is_cross_border) and (random.random() < 0.10)
                lane_leg2 = {
                    "origin_id": hub_id,
                    "destination_id": dn_id,
                    "lane_type": "indirect_leg2",
                    "is_cross_border": is_cross_border,
                    "is_active": True if is_cross_border else not deactivated_leg2,
                }
                all_lanes.append(lane_leg2)
                if lane_leg2["is_active"]:
                    active_lanes.add((hub_id, dn_id))

    return all_lanes, active_lanes


# ── MAIN BUILD FUNCTION ───────────────────────────────────────────────────────

def build_network(csv_path=None):
    """
    Entry point for other scripts.
    Returns the full network connectivity map.

    Usage:
        from lanes import build_network
        network = build_network()
        active_lanes = network["active_lanes"]
        hub_coverage = network["hub_coverage"]
        origin_lanes = network["origin_lanes"]
    """
    if csv_path is None:
        base = os.path.dirname(__file__)
        csv_path = os.path.join(base, "..", "..", "data", "mdm_network.csv")

    origins, hubs, deliveries = load_network(csv_path)

    # Build lookup for cross-border check
    node_country = {}
    for n in origins + hubs + deliveries:
        node_country[n["node_id"]] = n["country"]

    def cross_border_check(origin_id, dest_id):
        return node_country.get(origin_id) != node_country.get(dest_id)

    hub_coverage = assign_hub_coverage(hubs, deliveries)
    origin_lanes = assign_origin_lanes(origins, hubs, deliveries, hub_coverage)
    all_lanes, active_lanes = build_active_lanes(origin_lanes, cross_border_check)

    # ── Summary stats ──
    total = len(all_lanes)
    active = len(active_lanes)
    cross_border = sum(1 for l in all_lanes if l["is_cross_border"] and l["is_active"])
    direct = sum(1 for l in all_lanes if l["lane_type"] == "direct" and l["is_active"])
    indirect_leg1 = sum(1 for l in all_lanes if l["lane_type"] == "indirect_leg1" and l["is_active"])

    print(f"Network connectivity built:")
    print(f"  Origins:              {len(origins)}")
    print(f"  Consolidation hubs:   {len(hubs)}")
    print(f"  Delivery nodes:       {len(deliveries)}")
    print(f"  Total lanes defined:  {total}")
    print(f"  Active lanes:         {active}")
    print(f"  Cross-border active:  {cross_border}")
    print(f"  Direct active:        {direct}")
    print(f"  Indirect leg1 active: {indirect_leg1}")
    print()

    # Verify cross-border coverage
    origins_with_cb = set()
    for l in all_lanes:
        if l["is_cross_border"] and l["is_active"] and l["lane_type"] == "indirect_leg1":
            origins_with_cb.add(l["origin_id"])
    print(f"  Origins with >=1 cross-border CH: {len(origins_with_cb)}/{len(origins)}")

    # Verify all DNs reachable
    reachable_dns = set()
    for l in all_lanes:
        if l["is_active"] and l["lane_type"] in ("direct", "indirect_leg2"):
            reachable_dns.add(l["destination_id"])
    all_dn_ids = {d["node_id"] for d in deliveries}
    unreachable = all_dn_ids - reachable_dns
    print(f"  Unreachable delivery nodes:       {len(unreachable)}")
    if unreachable:
        print(f"  WARNING — unreachable: {unreachable}")

    return {
        "origins": origins,
        "hubs": hubs,
        "deliveries": deliveries,
        "hub_coverage": hub_coverage,
        "origin_lanes": origin_lanes,
        "all_lanes": all_lanes,
        "active_lanes": active_lanes,
        "node_country": node_country,
    }


if __name__ == "__main__":
    build_network()