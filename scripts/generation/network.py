"""
generate_network.py
-------------------
Generates mdm_network.csv — all 51 nodes.

Columns:
  node_id, node_type, city, country, latitude, longitude,
  delivery_node_capability

delivery_node_capability:
  delivery_node     — MTL (70%) or BOX (30%)
  origin_node       — ALL
  consolidation_hub — ALL

Data quality issues injected:
  - NULL latitude/longitude (~3% of rows)
  - Mixed case country codes (~5% of rows)
  - Extra whitespace in city names (~4% of rows)
  - Extra duplicate row with invalid node_type appended
    (original node row unchanged — Silver deduplication practice)
  - NULL delivery_node_capability in 1 delivery node row
  - Invalid delivery_node_capability value in 1 row
  - Duplicate row for 1 node

All 51 clean nodes are preserved intact so downstream generation
scripts can build the full network. Quality issues are additive
(extra rows) or non-structural (field value issues that don't
affect node identity or type).

Output: data/mdm_network.csv
"""

import csv
import os
import random

random.seed(42)

# ── NODE DEFINITIONS ─────────────────────────────────────────────────────────

NODES = [
    # Origin nodes
    {"node_id": "ON_CDG", "node_type": "origin_node",       "city": "Paris",      "country": "FR", "latitude": 48.8566,  "longitude":  2.3522},
    {"node_id": "ON_LYS", "node_type": "origin_node",       "city": "Lyon",       "country": "FR", "latitude": 45.7640,  "longitude":  4.8357},
    {"node_id": "ON_HAM", "node_type": "origin_node",       "city": "Hamburg",    "country": "DE", "latitude": 53.5511,  "longitude":  9.9937},
    {"node_id": "ON_FRA", "node_type": "origin_node",       "city": "Frankfurt",  "country": "DE", "latitude": 50.1109,  "longitude":  8.6821},
    {"node_id": "ON_MUC", "node_type": "origin_node",       "city": "Munich",     "country": "DE", "latitude": 48.1351,  "longitude": 11.5820},
    {"node_id": "ON_MAD", "node_type": "origin_node",       "city": "Madrid",     "country": "ES", "latitude": 40.4168,  "longitude": -3.7038},
    {"node_id": "ON_BCN", "node_type": "origin_node",       "city": "Barcelona",  "country": "ES", "latitude": 41.3851,  "longitude":  2.1734},
    {"node_id": "ON_MXP", "node_type": "origin_node",       "city": "Milan",      "country": "IT", "latitude": 45.4654,  "longitude":  9.1859},
    {"node_id": "ON_FCO", "node_type": "origin_node",       "city": "Rome",       "country": "IT", "latitude": 41.9028,  "longitude": 12.4964},
    {"node_id": "ON_AMS", "node_type": "origin_node",       "city": "Amsterdam",  "country": "NL", "latitude": 52.3676,  "longitude":  4.9041},
    {"node_id": "ON_BRU", "node_type": "origin_node",       "city": "Brussels",   "country": "BE", "latitude": 50.8503,  "longitude":  4.3517},
    {"node_id": "ON_WAW", "node_type": "origin_node",       "city": "Warsaw",     "country": "PL", "latitude": 52.2297,  "longitude": 21.0122},
    {"node_id": "ON_KRK", "node_type": "origin_node",       "city": "Krakow",     "country": "PL", "latitude": 50.0647,  "longitude": 19.9450},
    {"node_id": "ON_PRG", "node_type": "origin_node",       "city": "Prague",     "country": "CZ", "latitude": 50.0755,  "longitude": 14.4378},
    {"node_id": "ON_ANR", "node_type": "origin_node",       "city": "Antwerp",    "country": "BE", "latitude": 51.2194,  "longitude":  4.4025},

    # Consolidation hubs
    {"node_id": "CH_VNO", "node_type": "consolidation_hub", "city": "Venlo",      "country": "NL", "latitude": 51.3704,  "longitude":  6.1724},
    {"node_id": "CH_DUI", "node_type": "consolidation_hub", "city": "Duisburg",   "country": "DE", "latitude": 51.4344,  "longitude":  6.7623},
    {"node_id": "CH_MVV", "node_type": "consolidation_hub", "city": "Metz",       "country": "FR", "latitude": 49.1193,  "longitude":  6.1757},
    {"node_id": "CH_ZAZ", "node_type": "consolidation_hub", "city": "Zaragoza",   "country": "ES", "latitude": 41.6488,  "longitude": -0.8891},
    {"node_id": "CH_BLQ", "node_type": "consolidation_hub", "city": "Bologna",    "country": "IT", "latitude": 44.4949,  "longitude": 11.3426},
    {"node_id": "CH_WRO", "node_type": "consolidation_hub", "city": "Wroclaw",    "country": "PL", "latitude": 51.1079,  "longitude": 17.0385},

    # Delivery nodes
    {"node_id": "DN_LIL", "node_type": "delivery_node",     "city": "Lille",      "country": "FR", "latitude": 50.6292,  "longitude":  3.0573},
    {"node_id": "DN_BOD", "node_type": "delivery_node",     "city": "Bordeaux",   "country": "FR", "latitude": 44.8378,  "longitude": -0.5792},
    {"node_id": "DN_MRS", "node_type": "delivery_node",     "city": "Marseille",  "country": "FR", "latitude": 43.2965,  "longitude":  5.3698},
    {"node_id": "DN_TLS", "node_type": "delivery_node",     "city": "Toulouse",   "country": "FR", "latitude": 43.6047,  "longitude":  1.4442},
    {"node_id": "DN_NTE", "node_type": "delivery_node",     "city": "Nantes",     "country": "FR", "latitude": 47.2184,  "longitude": -1.5536},
    {"node_id": "DN_SXB", "node_type": "delivery_node",     "city": "Strasbourg", "country": "FR", "latitude": 48.5734,  "longitude":  7.7521},
    {"node_id": "DN_BER", "node_type": "delivery_node",     "city": "Berlin",     "country": "DE", "latitude": 52.5200,  "longitude": 13.4050},
    {"node_id": "DN_CGN", "node_type": "delivery_node",     "city": "Cologne",    "country": "DE", "latitude": 50.9333,  "longitude":  6.9500},
    {"node_id": "DN_STR", "node_type": "delivery_node",     "city": "Stuttgart",  "country": "DE", "latitude": 48.7758,  "longitude":  9.1829},
    {"node_id": "DN_DUS", "node_type": "delivery_node",     "city": "Dusseldorf", "country": "DE", "latitude": 51.2217,  "longitude":  6.7762},
    {"node_id": "DN_LEJ", "node_type": "delivery_node",     "city": "Leipzig",    "country": "DE", "latitude": 51.3397,  "longitude": 12.3731},
    {"node_id": "DN_NUE", "node_type": "delivery_node",     "city": "Nuremberg",  "country": "DE", "latitude": 49.4521,  "longitude": 11.0767},
    {"node_id": "DN_SVQ", "node_type": "delivery_node",     "city": "Seville",    "country": "ES", "latitude": 37.3891,  "longitude": -5.9845},
    {"node_id": "DN_VLC", "node_type": "delivery_node",     "city": "Valencia",   "country": "ES", "latitude": 39.4699,  "longitude": -0.3763},
    {"node_id": "DN_BIO", "node_type": "delivery_node",     "city": "Bilbao",     "country": "ES", "latitude": 43.2630,  "longitude": -2.9350},
    {"node_id": "DN_AGP", "node_type": "delivery_node",     "city": "Malaga",     "country": "ES", "latitude": 36.7213,  "longitude": -4.4213},
    {"node_id": "DN_ALC", "node_type": "delivery_node",     "city": "Alicante",   "country": "ES", "latitude": 38.3452,  "longitude": -0.4810},
    {"node_id": "DN_TRN", "node_type": "delivery_node",     "city": "Turin",      "country": "IT", "latitude": 45.0703,  "longitude":  7.6869},
    {"node_id": "DN_NAP", "node_type": "delivery_node",     "city": "Naples",     "country": "IT", "latitude": 40.8518,  "longitude": 14.2681},
    {"node_id": "DN_FLR", "node_type": "delivery_node",     "city": "Florence",   "country": "IT", "latitude": 43.7696,  "longitude": 11.2558},
    {"node_id": "DN_VCE", "node_type": "delivery_node",     "city": "Venice",     "country": "IT", "latitude": 45.4408,  "longitude": 12.3155},
    {"node_id": "DN_RTM", "node_type": "delivery_node",     "city": "Rotterdam",  "country": "NL", "latitude": 51.9244,  "longitude":  4.4777},
    {"node_id": "DN_HAG", "node_type": "delivery_node",     "city": "The Hague",  "country": "NL", "latitude": 52.0705,  "longitude":  4.3007},
    {"node_id": "DN_GNT", "node_type": "delivery_node",     "city": "Ghent",      "country": "BE", "latitude": 51.0543,  "longitude":  3.7174},
    {"node_id": "DN_LGG", "node_type": "delivery_node",     "city": "Liege",      "country": "BE", "latitude": 50.6326,  "longitude":  5.5797},
    {"node_id": "DN_GDN", "node_type": "delivery_node",     "city": "Gdansk",     "country": "PL", "latitude": 54.3520,  "longitude": 18.6466},
    {"node_id": "DN_POZ", "node_type": "delivery_node",     "city": "Poznan",     "country": "PL", "latitude": 52.4064,  "longitude": 16.9252},
    {"node_id": "DN_LCJ", "node_type": "delivery_node",     "city": "Lodz",       "country": "PL", "latitude": 51.7592,  "longitude": 19.4560},
    {"node_id": "DN_BRQ", "node_type": "delivery_node",     "city": "Brno",       "country": "CZ", "latitude": 49.1951,  "longitude": 16.6068},
    {"node_id": "DN_OSR", "node_type": "delivery_node",     "city": "Ostrava",    "country": "CZ", "latitude": 49.8209,  "longitude": 18.2625},
]


def assign_delivery_node_capabilities(nodes):
    delivery_nodes = [n for n in nodes if n["node_type"] == "delivery_node"]
    n_mtl          = round(len(delivery_nodes) * 0.70)
    shuffled       = delivery_nodes.copy()
    random.shuffle(shuffled)
    mtl_ids        = {n["node_id"] for n in shuffled[:n_mtl]}

    result = []
    for node in nodes:
        n = node.copy()
        if n["node_type"] == "delivery_node":
            n["delivery_node_capability"] = "MTL" if n["node_id"] in mtl_ids else "BOX"
        else:
            n["delivery_node_capability"] = "ALL"
        result.append(n)
    return result


def inject_quality_issues(nodes):
    """
    Injects realistic data quality issues.

    All 51 clean nodes are preserved intact — quality issues are
    either additive (extra rows appended) or affect non-structural
    fields only (country case, city whitespace, capability value).
    This ensures downstream generation scripts see all 51 nodes.

    Issues:
      1. NULL latitude/longitude (~3% of rows)
      2. Mixed case country codes (~5% of rows)
      3. Extra whitespace in city names (~4% of rows)
      4. Extra duplicate row with invalid node_type appended
         (original node row unchanged)
      5. NULL delivery_node_capability in 1 row
      6. Invalid delivery_node_capability value in 1 row
      7. Standard exact duplicate row for 1 node
    """
    total  = len(nodes)
    result = [row.copy() for row in nodes]
    used   = []

# Issue 1: Extra rows with NULL lat/lon (~3% — additive, originals intact)
    null_coord_count = max(1, round(total * 0.03))
    null_coord_indices = random.sample(range(total), null_coord_count)
    for i in null_coord_indices:
        bad_row = result[i].copy()
        bad_row["latitude"]  = ""
        bad_row["longitude"] = ""
        result.append(bad_row)
    used.extend(null_coord_indices)

    # Issue 2: Mixed case country (~5%)
    mixed_count   = max(1, round(total * 0.05))
    eligible      = [i for i in range(total) if i not in used]
    mixed_indices = random.sample(eligible, mixed_count)
    for i in mixed_indices:
        result[i]["country"] = result[i]["country"].lower()
    used.extend(mixed_indices)

    # Issue 3: Extra whitespace in city (~4%)
    ws_count   = max(1, round(total * 0.04))
    eligible   = [i for i in range(total) if i not in used]
    ws_indices = random.sample(eligible, ws_count)
    for i in ws_indices:
        city  = result[i]["city"]
        issue = random.choice(["leading", "trailing", "double"])
        if issue == "leading":
            result[i]["city"] = " " + city
        elif issue == "trailing":
            result[i]["city"] = city + " "
        else:
            mid = len(city) // 2
            result[i]["city"] = city[:mid] + "  " + city[mid:]
    used.extend(ws_indices)

    # Issue 4: Extra duplicate row with invalid node_type (additive)
    eligible = [i for i in range(total) if i not in used]
    idx4     = random.choice(eligible)
    bad_row  = result[idx4].copy()
    bad_row["node_type"] = "orign_node"
    result.append(bad_row)

    # Issue 5: NULL delivery_node_capability in 1 row
    dn_indices = [
        i for i in range(total)
        if result[i]["node_type"] == "delivery_node" and i not in used
    ]
    if dn_indices:
        idx5 = random.choice(dn_indices)
        result[idx5]["delivery_node_capability"] = ""
        used.append(idx5)

    # Issue 6: Invalid delivery_node_capability value in 1 row
    eligible = [i for i in range(total) if i not in used]
    idx6     = random.choice(eligible)
    result[idx6]["delivery_node_capability"] = "BOTH"
    used.append(idx6)

    # Issue 7: Standard exact duplicate row
    dup_idx = random.randint(0, total - 1)
    result.append(result[dup_idx].copy())

    return result


def main():
    output_dir  = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "mdm_network.csv")

    nodes_with_capability = assign_delivery_node_capabilities(NODES)
    nodes_with_issues     = inject_quality_issues(nodes_with_capability)

    fieldnames = [
        "node_id", "node_type", "city", "country",
        "latitude", "longitude", "delivery_node_capability",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(nodes_with_issues)

    clean     = len(NODES)
    total     = len(nodes_with_issues)
    dn_nodes  = [n for n in nodes_with_capability if n["node_type"] == "delivery_node"]
    mtl_count = sum(1 for n in dn_nodes if n["delivery_node_capability"] == "MTL")
    box_count = sum(1 for n in dn_nodes if n["delivery_node_capability"] == "BOX")

    print(f"Nodes defined (clean):      {clean}")
    print(f"Rows written:               {total} (clean + quality issue rows)")
    print(f"Output:                     {output_path}")
    print()
    print(f"Delivery node capability:")
    print(f"  MTL-capable:              {mtl_count} ({mtl_count/len(dn_nodes)*100:.0f}%)")
    print(f"  BOX-only:                 {box_count} ({box_count/len(dn_nodes)*100:.0f}%)")
    print()
    print("Quality issues injected:")
    print(f"  NULL coordinates:         ~{max(1, round(clean*0.03))} rows")
    print(f"  Mixed case country:       ~{max(1, round(clean*0.05))} rows")
    print(f"  Whitespace in city:       ~{max(1, round(clean*0.04))} rows")
    print(f"  Invalid node_type row:    1 extra row appended (original intact)")
    print(f"  NULL capability:          1 row")
    print(f"  Invalid capability value: 1 row")
    print(f"  Duplicate row:            1 row")
    print()
    print("All 51 clean nodes preserved — no structural nodes corrupted.")


if __name__ == "__main__":
    main()