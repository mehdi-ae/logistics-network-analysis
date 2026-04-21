"""
load_bronze.py
--------------
Loads all source CSV files into BigQuery as Bronze layer tables.

Rules:
  - All columns loaded as STRING (no casting — Bronze is raw ingestion)
  - Full reload: dataset and tables recreated on every run
  - Source system prefix preserved in table names
  - Location: europe-west9

Tables created in dataset bronze_logistics:
  mdm_network
  fin_transport_rates
  fin_handling_costs
  tms_transportation
  wms_shipment_details

Usage:
  python3 scripts/load/load_bronze.py

Requirements:
  google-cloud-bigquery
  gcloud auth application-default login
"""

import os
import sys
import time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ── CONFIG ───────────────────────────────────────────────────────────────────

PROJECT_ID  = "supply-chain-analytics-492110"
DATASET_ID  = "bronze_logistics"
LOCATION    = "europe-west9"

SCRIPT_DIR  = os.path.dirname(__file__)
DATA_DIR    = os.path.join(SCRIPT_DIR, "..", "..", "data")

TABLES = [
    (
        "mdm_network.csv",
        "mdm_network",
        "Network nodes — origins, consolidation hubs, delivery nodes",
    ),
    (
        "fin_transport_rates.csv",
        "fin_transport_rates",
        "Transport cost rates per country and carrier",
    ),
    (
        "fin_handling_costs.csv",
        "fin_handling_costs",
        "Package handling cost per consolidation hub",
    ),
    (
        "tms_transportation.csv",
        "tms_transportation",
        "Truck runs — one row per truck run across simulation period",
    ),
    (
        "wms_shipment_details.csv",
        "wms_shipment_details",
        "Shipment legs — one row per shipment per leg",
    ),
        (
        "fin_procurement.csv",
        "fin_procurement",
        "Procurement items — one row per item",
    ),
]


# ── HELPERS ──────────────────────────────────────────────────────────────────

def get_csv_headers(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        header = f.readline().strip()
    return [col.strip() for col in header.split(",")]


def build_schema(columns):
    return [
        bigquery.SchemaField(col, "STRING", mode="NULLABLE")
        for col in columns
    ]


def sizeof_fmt(num_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


# ── DATASET SETUP ─────────────────────────────────────────────────────────────

def create_dataset(client):
    dataset_ref             = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location    = LOCATION
    dataset_ref.description = (
        "Bronze layer — raw ingestion from EU logistics network source systems. "
        "All columns STRING. No transformations applied."
    )

    try:
        client.get_dataset(dataset_ref)
        print(f"  Dataset {DATASET_ID} already exists — using existing dataset")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"  Dataset {DATASET_ID} created in {LOCATION}")


# ── TABLE LOAD ────────────────────────────────────────────────────────────────

def load_table(client, csv_filename, table_id, description):
    csv_path  = os.path.join(DATA_DIR, csv_filename)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    file_size = os.path.getsize(csv_path)

    if not os.path.exists(csv_path):
        print(f"  SKIP — file not found: {csv_path}")
        return False

    print(f"\n  Loading {table_id}")
    print(f"    Source:  {csv_filename} ({sizeof_fmt(file_size)})")

    try:
        client.delete_table(table_ref)
        print(f"    Dropped existing table")
    except NotFound:
        pass

    columns = get_csv_headers(csv_path)
    schema  = build_schema(columns)
    print(f"    Columns: {len(columns)} ({', '.join(columns[:4])}{'...' if len(columns) > 4 else ''})")

    table             = bigquery.Table(table_ref, schema=schema)
    table.description = description
    client.create_table(table)

    job_config                       = bigquery.LoadJobConfig()
    job_config.schema                = schema
    job_config.skip_leading_rows     = 1
    job_config.source_format         = bigquery.SourceFormat.CSV
    job_config.write_disposition     = bigquery.WriteDisposition.WRITE_APPEND
    job_config.allow_quoted_newlines = True
    job_config.allow_jagged_rows     = True
    job_config.max_bad_records       = 0

    start = time.time()
    with open(csv_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_ref, job_config=job_config)

    print(f"    Uploading...", end="", flush=True)
    load_job.result()
    elapsed = time.time() - start

    loaded_table = client.get_table(table_ref)
    row_count    = loaded_table.num_rows

    print(f" done ({elapsed:.1f}s)")
    print(f"    Rows loaded: {row_count:,}")

    if load_job.errors:
        print(f"    WARNINGS: {load_job.errors}")

    return True


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Bronze layer load — EU logistics network")
    print(f"Project:  {PROJECT_ID}")
    print(f"Dataset:  {DATASET_ID}")
    print(f"Location: {LOCATION}")
    print("=" * 60)

    client = bigquery.Client(project=PROJECT_ID)

    print("\nSetting up dataset...")
    create_dataset(client)

    print("\nLoading tables...")
    results = {}
    for csv_filename, table_id, description in TABLES:
        success = load_table(client, csv_filename, table_id, description)
        results[table_id] = success

    print("\n" + "=" * 60)
    print("Load complete — summary:")
    for table_id, success in results.items():
        status = "OK" if success else "SKIPPED"
        print(f"  {status:<8} {DATASET_ID}.{table_id}")
    print("=" * 60)
    print(f"\nBigQuery console:")
    print(f"  https://console.cloud.google.com/bigquery?project={PROJECT_ID}")


if __name__ == "__main__":
    main()