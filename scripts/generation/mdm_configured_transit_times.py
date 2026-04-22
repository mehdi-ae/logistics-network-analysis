"""
generate_mdm_configured_transit_times.py
-----------------------------------------
Generates mdm_configured_transit_times.csv — configured transit time per lane.

Reads observed p50/p90/p95/p98 per (origin, destination) from BigQuery silver_tms_transportation, then assigns a configured transit time based on the following distribution (seed 42):

    80% correct        — configured = p95 + uniform noise [0, 2] hours
    15% under_configured — configured = uniform random between p50 and p90 (promising faster than network can reliably deliver)
    5% over_configured  — configured = p98 + uniform noise [3, 6] hours (unnecessarily conservative — missed cutoff opportunity)

Columns:
  origin_node_id, destination_node_id, configured_transit_time_hours, configuration_type

Output: data/mdm_configured_transit_times.csv
"""

import csv
import os
import random
from google.cloud import bigquery

random.seed(42)

PROJECT_ID = "supply-chain-analytics-492110"

QUERY = """
SELECT
  origin_node_id,
  destination_node_id,
  ROUND(PERCENTILE_CONT(CAST(transit_time_hours AS FLOAT64), 0.50) OVER(PARTITION BY origin_node_id, destination_node_id), 2) AS p50,
  ROUND(PERCENTILE_CONT(CAST(transit_time_hours AS FLOAT64), 0.90) OVER(PARTITION BY origin_node_id, destination_node_id), 2) AS p90,
  ROUND(PERCENTILE_CONT(CAST(transit_time_hours AS FLOAT64), 0.95) OVER(PARTITION BY origin_node_id, destination_node_id), 2) AS p95,
  ROUND(PERCENTILE_CONT(CAST(transit_time_hours AS FLOAT64), 0.98) OVER(PARTITION BY origin_node_id, destination_node_id), 2) AS p98
FROM silver_logistics.silver_tms_transportation
WHERE quality_record != 'critical'
QUALIFY ROW_NUMBER() OVER(PARTITION BY origin_node_id, destination_node_id ORDER BY departure_datetime DESC) = 1
"""


def assign_configuration_type():
    r = random.random()
    if r < 0.80:
        return "correct"
    elif r < 0.95:
        return "under_configured"
    else:
        return "over_configured"


def compute_configured_time(row):
    config_type = assign_configuration_type()
    p50  = float(row["p50"])
    p90  = float(row["p90"])
    p95  = float(row["p95"])
    p98  = float(row["p98"])

    if config_type == "correct":
        configured = p95 + random.uniform(0, 2)
    elif config_type == "under_configured":
        configured = random.uniform(p50, p90)
    else:
        configured = p98 + random.uniform(3, 6)

    return round(configured, 2), config_type


def main():
    print("Connecting to BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)

    print("Querying observed transit time distribution...")
    df = client.query(QUERY).to_dataframe()
    print(f"  Lanes found: {len(df)}")

    output_dir  = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "mdm_configured_transit_times.csv")

    fieldnames = [
        "origin_node_id",
        "destination_node_id",
        "configured_transit_time_hours",
        "configuration_type",
    ]

    counts = {"correct": 0, "under_configured": 0, "over_configured": 0}

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _, row in df.iterrows():
            configured_time, config_type = compute_configured_time(row)
            counts[config_type] += 1
            writer.writerow({
                "origin_node_id":                row["origin_node_id"],
                "destination_node_id":           row["destination_node_id"],
                "configured_transit_time_hours": configured_time,
                "configuration_type":            config_type,
            })

    total = len(df)
    print(f"\nRows written: {total}")
    print(f"Output:       {output_path}")
    print()
    print("Configuration type distribution:")
    for config_type, count in counts.items():
        pct = count / total * 100
        print(f"  {config_type:<20} {count:>4} ({pct:.0f}%)")


if __name__ == "__main__":
    main()