"""
generate_network.py
-------------------
Generates dim_network: all 51 nodes (origin, consolidation hub, delivery)
with real EU coordinates, city, country, and node type.

Data quality issues injected (Silver layer practice):
  - NULL values in latitude/longitude (~3% of rows)
  - Mixed case country codes (e.g. "fr" instead of "FR") (~5% of rows)
  - Extra whitespace in city names (~4% of rows)
  - Invalid node_type value injected in 1 row
  - Duplicate row injected for 1 node

Output: data/mdm_network.csv
"""

import csv
import random
import os

random.seed(42)

# ── NODE DEFINITIONS ────────────────────────────────────────────────────────

NODES = [
    # Origin nodes
    {"node_id": "ON_CDG", "node_type": "origin_node",       "city": "Paris",      "country": "FR", "latitude": 48.8566,  "longitude": 2.3522},
    {"node_id": "ON_LYS", "node_type": "origin_node",       "city": "Lyon",       "country": "FR", "latitude": 45.7640,  "longitude": 4.8357},
    {"node_id": "ON_HAM", "node_type": "origin_node",       "city": "Hamburg",    "country": "DE", "latitude": 53.5511,  "longitude": 9.9937},
    {"node_id": "ON_FRA", "node_type": "origin_node",       "city": "Frankfurt",  "country": "DE", "latitude": 50.1109,  "longitude": 8.6821},
    {"node_id": "ON_MUC", "node_type": "origin_node",       "city": "Munich",     "country": "DE", "latitude": 48.1351,  "longitude": 11.5820},
    {"node_id": "ON_MAD", "node_type": "origin_node",       "city": "Madrid",     "country": "ES", "latitude": 40.4168,  "longitude": -3.7038},
    {"node_id": "ON_BCN", "node_type": "origin_node",       "city": "Barcelona",  "country": "ES", "latitude": 41.3851,  "longitude": 2.1734},
    {"node_id": "ON_MXP", "node_type": "origin_node",       "city": "Milan",      "country": "IT", "latitude": 45.4654,  "longitude": 9.1859},
    {"node_id": "ON_FCO", "node_type": "origin_node",       "city": "Rome",       "country": "IT", "latitude": 41.9028,  "longitude": 12.4964},
    {"node_id": "ON_AMS", "node_type": "origin_node",       "city": "Amsterdam",  "country": "NL", "latitude": 52.3676,  "longitude": 4.9041},
    {"node_id": "ON_BRU", "node_type": "origin_node",       "city": "Brussels",   "country": "BE", "latitude": 50.8503,  "longitude": 4.3517},
    {"node_id": "ON_WAW", "node_type": "origin_node",       "city": "Warsaw",     "country": "PL", "latitude": 52.2297,  "longitude": 21.0122},
    {"node_id": "ON_KRK", "node_type": "origin_node",       "city": "Krakow",     "country": "PL", "latitude": 50.0647,  "longitude": 19.9450},
    {"node_id": "ON_PRG", "node_type": "origin_node",       "city": "Prague",     "country": "CZ", "latitude": 50.0755,  "longitude": 14.4378},
    {"node_id": "ON_ANR", "node_type": "origin_node",       "city": "Antwerp",    "country": "BE", "latitude": 51.2194,  "longitude": 4.4025},

    # Consolidation hubs
    {"node_id": "CH_VNO", "node_type": "consolidation_hub", "city": "Venlo",      "country": "NL", "latitude": 51.3704,  "longitude": 6.1724},
    {"node_id": "CH_DUI", "node_type": "consolidation_hub", "city": "Duisburg",   "country": "DE", "latitude": 51.4344,  "longitude": 6.7623},
    {"node_id": "CH_MVV", "node_type": "consolidation_hub", "city": "Metz",       "country": "FR", "latitude": 49.1193,  "longitude": 6.1757},
    {"node_id": "CH_ZAZ", "node_type": "consolidation_hub", "city": "Zaragoza",   "country": "ES", "latitude": 41.6488,  "longitude": -0.8891},
    {"node_id": "CH_BLQ", "node_type": "consolidation_hub", "city": "Bologna",    "country": "IT", "latitude": 44.4949,  "longitude": 11.3426},
    {"node_id": "CH_WRO", "node_type": "consolidation_hub", "city": "Wroclaw",    "country": "PL", "latitude": 51.1079,  "longitude": 17.0385},

    # Delivery nodes
    {"node_id": "DN_LIL", "node_type": "delivery_node",     "city": "Lille",      "country": "FR", "latitude": 50.6292,  "longitude": 3.0573},
    {"node_id": "DN_BOD", "node_type": "delivery_node",     "city": "Bordeaux",   "country": "FR", "latitude": 44.8378,  "longitude": -0.5792},
    {"node_id": "DN_MRS", "node_type": "delivery_node",     "city": "Marseille",  "country": "FR", "latitude": 43.2965,  "longitude": 5.3698},
    {"node_id": "DN_TLS", "node_type": "delivery_node",     "city": "Toulouse",   "country": "FR", "latitude": 43.6047,  "longitude": 1.4442},
    {"node_id": "DN_NTE", "node_type": "delivery_node",     "city": "Nantes",     "country": "FR", "latitude": 47.2184,  "longitude": -1.5536},
    {"node_id": "DN_SXB", "node_type": "delivery_node",     "city": "Strasbourg", "country": "FR", "latitude": 48.5734,  "longitude": 7.7521},
    {"node_id": "DN_BER", "node_type": "delivery_node",     "city": "Berlin",     "country": "DE", "latitude": 52.5200,  "longitude": 13.4050},
    {"node_id": "DN_CGN", "node_type": "delivery_node",     "city": "Cologne",    "country": "DE", "latitude": 50.9333,  "longitude": 6.9500},
    {"node_id": "DN_STR", "node_type": "delivery_node",     "city": "Stuttgart",  "country": "DE", "latitude": 48.7758,  "longitude": 9.1829},
    {"node_id": "DN_DUS", "node_type": "delivery_node",     "city": "Dusseldorf", "country": "DE", "latitude": 51.2217,  "longitude": 6.7762},
    {"node_id": "DN_LEJ", "node_type": "delivery_node",     "city": "Leipzig",    "country": "DE", "latitude": 51.3397,  "longitude": 12.3731},
    {"node_id": "DN_NUE", "node_type": "delivery_node",     "city": "Nuremberg",  "country": "DE", "latitude": 49.4521,  "longitude": 11.0767},
    {"node_id": "DN_SVQ", "node_type": "delivery_node",     "city": "Seville",    "country": "ES", "latitude": 37.3891,  "longitude": -5.9845},
    {"node_id": "DN_VLC", "node_type": "delivery_node",     "city": "Valencia",   "country": "ES", "latitude": 39.4699,  "longitude": -0.3763},
    {"node_id": "DN_BIO", "node_type": "delivery_node",     "city": "Bilbao",     "country": "ES", "latitude": 43.2630,  "longitude": -2.9350},
    {"node_id": "DN_AGP", "node_type": "delivery_node",     "city": "Malaga",     "country": "ES", "latitude": 36.7213,  "longitude": -4.4213},
    {"node_id": "DN_ALC", "node_type": "delivery_node",     "city": "Alicante",   "country": "ES", "latitude": 38.3452,  "longitude": -0.4810},
    {"node_id": "DN_TRN", "node_type": "delivery_node",     "city": "Turin",      "country": "IT", "latitude": 45.0703,  "longitude": 7.6869},
    {"node_id": "DN_NAP", "node_type": "delivery_node",     "city": "Naples",     "country": "IT", "latitude": 40.8518,  "longitude": 14.2681},
    {"node_id": "DN_FLR", "node_type": "delivery_node",     "city": "Florence",   "country": "IT", "latitude": 43.7696,  "longitude": 11.2558},
    {"node_id": "DN_VCE", "node_type": "delivery_node",     "city": "Venice",     "country": "IT", "latitude": 45.4408,  "longitude": 12.3155},
    {"node_id": "DN_RTM", "node_type": "delivery_node",     "city": "Rotterdam",  "country": "NL", "latitude": 51.9244,  "longitude": 4.4777},
    {"node_id": "DN_HAG", "node_type": "delivery_node",     "city": "The Hague",  "country": "NL", "latitude": 52.0705,  "longitude": 4.3007},
    {"node_id": "DN_GNT", "node_type": "delivery_node",     "city": "Ghent",      "country": "BE", "latitude": 51.0543,  "longitude": 3.7174},
    {"node_id": "DN_LGG", "node_type": "delivery_node",     "city": "Liege",      "country": "BE", "latitude": 50.6326,  "longitude": 5.5797},
    {"node_id": "DN_GDN", "node_type": "delivery_node",     "city": "Gdansk",     "country": "PL", "latitude": 54.3520,  "longitude": 18.6466},
    {"node_id": "DN_POZ", "node_type": "delivery_node",     "city": "Poznan",     "country": "PL", "latitude": 52.4064,  "longitude": 16.9252},
    {"node_id": "DN_LCJ", "node_type": "delivery_node",     "city": "Lodz",       "country": "PL", "latitude": 51.7592,  "longitude": 19.4560},
    {"node_id": "DN_BRQ", "node_type": "delivery_node",     "city": "Brno",       "country": "CZ", "latitude": 49.1951,  "longitude": 16.6068},
    {"node_id": "DN_OSR", "node_type": "delivery_node",     "city": "Ostrava",    "country": "CZ", "latitude": 49.8209,  "longitude": 18.2625},
]

# ── DATA QUALITY INJECTION ───────────────────────────────────────────────────

def inject_quality_issues(nodes):
    """
    Injects realistic data quality issues into the node list.

    Issues injected:
      1. NULL latitude/longitude — simulates missing GPS data (~3% of rows)
      2. Mixed case country codes — simulates inconsistent source system formatting (~5%)
      3. Extra whitespace in city names — simulates manual data entry errors (~4%)
      4. Invalid node_type — 1 row with a typo to test enum validation
      5. Duplicate row — 1 exact duplicate to test deduplication logic
    """

    total = len(nodes)
    result = [row.copy() for row in nodes]

    # Issue 1: NULL latitude/longitude (~3% of rows — round up to at least 1)
    null_coord_count = max(1, round(total * 0.03))
    null_coord_indices = random.sample(range(total), null_coord_count)
    for i in null_coord_indices:
        result[i]["latitude"] = ""
        result[i]["longitude"] = ""

    # Issue 2: Mixed case country codes (~5% of rows)
    mixed_case_count = max(1, round(total * 0.05))
    eligible = [i for i in range(total) if i not in null_coord_indices]
    mixed_case_indices = random.sample(eligible, mixed_case_count)
    for i in mixed_case_indices:
        result[i]["country"] = result[i]["country"].lower()

    # Issue 3: Extra whitespace in city names (~4% of rows)
    whitespace_count = max(1, round(total * 0.04))
    remaining = [i for i in range(total) if i not in null_coord_indices + mixed_case_indices]
    whitespace_indices = random.sample(remaining, whitespace_count)
    for i in whitespace_indices:
        city = result[i]["city"]
        issue = random.choice(["leading", "trailing", "double"])
        if issue == "leading":
            result[i]["city"] = " " + city
        elif issue == "trailing":
            result[i]["city"] = city + " "
        else:
            result[i]["city"] = city[:len(city)//2] + "  " + city[len(city)//2:]

    # Issue 4: Invalid node_type — 1 row with a typo
    invalid_type_index = random.choice(
        [i for i in range(total) if i not in null_coord_indices + mixed_case_indices + whitespace_indices]
    )
    result[invalid_type_index]["node_type"] = "orign_node"

    # Issue 5: Exact duplicate — duplicate 1 random row and append at end
    duplicate_index = random.randint(0, total - 1)
    result.append(result[duplicate_index].copy())

    return result


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "mdm_network.csv")

    nodes_with_issues = inject_quality_issues(NODES)

    fieldnames = ["node_id", "node_type", "city", "country", "latitude", "longitude"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(nodes_with_issues)

    # ── Summary ──
    clean_count = len(NODES)
    total_written = len(nodes_with_issues)
    print(f"Nodes defined:       {clean_count}")
    print(f"Rows written:        {total_written} (includes 1 duplicate)")
    print(f"Output:              {output_path}")
    print()
    print("Quality issues injected:")
    print(f"  NULL coordinates:  ~{max(1, round(clean_count * 0.03))} rows")
    print(f"  Mixed case country:~{max(1, round(clean_count * 0.05))} rows")
    print(f"  Whitespace in city:~{max(1, round(clean_count * 0.04))} rows")
    print(f"  Invalid node_type: 1 row")
    print(f"  Duplicate row:     1 row")


if __name__ == "__main__":
    main()