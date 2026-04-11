"""
generate_financials.py
----------------------
Generates two financial reference tables:

  1. fin_transport_rates.csv
     Grain: one row per (country, carrier)
     Columns: country, carrier, fixed_cost_eur, cost_per_km_eur

  2. fin_handling_costs.csv
     Grain: one row per consolidation hub
     Columns: node_id, cost_per_package_eur

Carriers (5 total):
  - FastFreight EU  — Premium tier
  - EuroHaul        — Mid tier
  - TransCargo      — Mid tier
  - QuickMove       — Budget tier
  - DirectLog       — Budget tier

Data quality issues injected:
  - Negative cost value in 1 row (invalid value)
  - NULL cost_per_km in 1 row (missing value)
  - Duplicate row for 1 (country, carrier) pair
  - Carrier name with extra whitespace in 1 row
  - Handling cost out of expected range in 1 hub (tests range validation)

Output:
  data/fin_transport_rates.csv
  data/fin_handling_costs.csv
"""

import csv
import os
import random

random.seed(42)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

COUNTRIES = ["FR", "DE", "ES", "IT", "NL", "BE", "PL", "CZ"]

CONSOLIDATION_HUBS = ["CH_VNO", "CH_DUI", "CH_MVV", "CH_ZAZ", "CH_BLQ", "CH_WRO"]

# Carrier definitions with cost ranges
# Each carrier has a base fixed cost range and base cost per km range
# Country-level variation applied as a multiplier (±10%) to simulate
# local fuel costs, tolls, and labour differences
CARRIERS = [
    {
        "name":            "FastFreight EU",
        "tier":            "premium",
        "fixed_min":       380,
        "fixed_max":       420,
        "per_km_min":      1.10,
        "per_km_max":      1.30,
    },
    {
        "name":            "EuroHaul",
        "tier":            "mid",
        "fixed_min":       310,
        "fixed_max":       350,
        "per_km_min":      0.85,
        "per_km_max":      1.05,
    },
    {
        "name":            "TransCargo",
        "tier":            "mid",
        "fixed_min":       300,
        "fixed_max":       340,
        "per_km_min":      0.80,
        "per_km_max":      1.00,
    },
    {
        "name":            "QuickMove",
        "tier":            "budget",
        "fixed_min":       250,
        "fixed_max":       290,
        "per_km_min":      0.65,
        "per_km_max":      0.80,
    },
    {
        "name":            "DirectLog",
        "tier":            "budget",
        "fixed_min":       240,
        "fixed_max":       280,
        "per_km_min":      0.60,
        "per_km_max":      0.75,
    },
]

# Country cost multipliers — reflects local operating cost differences
# Values around 1.0 with realistic variation
COUNTRY_MULTIPLIERS = {
    "FR": 1.05,   # France — moderate tolls
    "DE": 1.08,   # Germany — autobahn fees, high labour
    "ES": 0.95,   # Spain — lower fuel costs
    "IT": 1.10,   # Italy — high tolls
    "NL": 1.03,   # Netherlands — moderate
    "BE": 1.02,   # Belgium — moderate
    "PL": 0.88,   # Poland — lower labour and fuel costs
    "CZ": 0.90,   # Czech Republic — lower costs
}


# ── GENERATE TRANSPORT RATES ─────────────────────────────────────────────────

def generate_transport_rates():
    """
    Generates one row per (country, carrier) pair.
    Applies country multiplier to base carrier cost ranges.
    Returns list of dicts.
    """
    rows = []

    for carrier in CARRIERS:
        for country in COUNTRIES:
            multiplier = COUNTRY_MULTIPLIERS[country]

            fixed_cost = round(
                random.uniform(carrier["fixed_min"], carrier["fixed_max"]) * multiplier, 2
            )
            cost_per_km = round(
                random.uniform(carrier["per_km_min"], carrier["per_km_max"]) * multiplier, 4
            )

            rows.append({
                "country":         country,
                "carrier":         carrier["name"],
                "fixed_cost_eur":  fixed_cost,
                "cost_per_km_eur": cost_per_km,
            })

    return rows


# ── GENERATE HANDLING COSTS ──────────────────────────────────────────────────

def generate_handling_costs():
    """
    Generates one row per consolidation hub.
    Cost per package between €1.00 and €2.50.
    Returns list of dicts.
    """
    rows = []

    for hub_id in CONSOLIDATION_HUBS:
        cost = round(random.uniform(1.00, 2.50), 2)
        rows.append({
            "node_id":              hub_id,
            "cost_per_package_eur": cost,
        })

    return rows


# ── DATA QUALITY INJECTION ───────────────────────────────────────────────────

def inject_quality_issues_transport(rows):
    """
    Injects data quality issues into transport rates.

    Issues:
      1. Negative fixed_cost_eur in 1 row — invalid value
      2. NULL cost_per_km_eur in 1 row — missing value
      3. Duplicate row for 1 (country, carrier) pair
      4. Extra whitespace in carrier name in 1 row
    """
    result = [row.copy() for row in rows]
    total = len(result)

    used_indices = []

    # Issue 1: Negative fixed cost — invalid value
    idx1 = random.randint(0, total - 1)
    result[idx1]["fixed_cost_eur"] = -abs(result[idx1]["fixed_cost_eur"])
    used_indices.append(idx1)

    # Issue 2: NULL cost_per_km
    available = [i for i in range(total) if i not in used_indices]
    idx2 = random.choice(available)
    result[idx2]["cost_per_km_eur"] = ""
    used_indices.append(idx2)

    # Issue 3: Extra whitespace in carrier name
    available = [i for i in range(total) if i not in used_indices]
    idx3 = random.choice(available)
    result[idx3]["carrier"] = " " + result[idx3]["carrier"]
    used_indices.append(idx3)

    # Issue 4: Exact duplicate row
    dup_idx = random.choice([i for i in range(total) if i not in used_indices])
    result.append(result[dup_idx].copy())

    return result


def inject_quality_issues_handling(rows):
    """
    Injects data quality issues into handling costs.

    Issues:
      1. Cost out of expected range (>2.50) in 1 hub — tests range validation
      2. NULL cost in 1 hub — missing value
    """
    result = [row.copy() for row in rows]

    # Issue 1: Out of range cost
    idx1 = random.randint(0, len(result) - 1)
    result[idx1]["cost_per_package_eur"] = round(random.uniform(3.50, 5.00), 2)

    # Issue 2: NULL cost
    available = [i for i in range(len(result)) if i != idx1]
    idx2 = random.choice(available)
    result[idx2]["cost_per_package_eur"] = ""

    return result


# ── WRITE CSV ────────────────────────────────────────────────────────────────

def write_csv(rows, fieldnames, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)

    # Transport rates
    transport_rates = generate_transport_rates()
    transport_with_issues = inject_quality_issues_transport(transport_rates)
    transport_path = os.path.join(output_dir, "fin_transport_rates.csv")
    write_csv(
        transport_with_issues,
        ["country", "carrier", "fixed_cost_eur", "cost_per_km_eur"],
        transport_path,
    )

    # Handling costs
    handling_costs = generate_handling_costs()
    handling_with_issues = inject_quality_issues_handling(handling_costs)
    handling_path = os.path.join(output_dir, "fin_handling_costs.csv")
    write_csv(
        handling_with_issues,
        ["node_id", "cost_per_package_eur"],
        handling_path,
    )

    # ── Summary ──
    print(f"Transport rates:")
    print(f"  Clean rows:           {len(transport_rates)}")
    print(f"  Rows written:         {len(transport_with_issues)} (includes 1 duplicate)")
    print(f"  Output:               {transport_path}")
    print()
    print(f"  Quality issues injected:")
    print(f"    Negative fixed cost: 1 row")
    print(f"    NULL cost_per_km:    1 row")
    print(f"    Whitespace carrier:  1 row")
    print(f"    Duplicate row:       1 row")
    print()
    print(f"Handling costs:")
    print(f"  Clean rows:           {len(handling_costs)}")
    print(f"  Rows written:         {len(handling_with_issues)}")
    print(f"  Output:               {handling_path}")
    print()
    print(f"  Quality issues injected:")
    print(f"    Out of range cost:   1 row")
    print(f"    NULL cost:           1 row")
    print()

    # Carrier overview
    print("Carrier cost profiles (clean data, FR rates):")
    fr_rows = [r for r in transport_rates if r["country"] == "FR"]
    for row in fr_rows:
        carrier_meta = next(c for c in CARRIERS if c["name"] == row["carrier"])
        print(f"  {row['carrier']:<20} [{carrier_meta['tier']:<7}]  "
              f"fixed=€{row['fixed_cost_eur']:.2f}  "
              f"per_km=€{row['cost_per_km_eur']:.4f}")


if __name__ == "__main__":
    main()