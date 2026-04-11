# EU Logistics Network — Operations Analytics

A production-grade logistics analytics warehouse built on a synthetic EU network, designed around three operational decision loops — lane profitability, container compliance, and transit time governance — each with a direct cost or customer experience consequence.

## Projects

**Lane Health Dashboard** — monitors lane volume and utilization to identify unprofitable lanes and trigger network optimization decisions before costs compound.

**Container Utilization Compliance** — tracks container type compliance per node and lane to anticipate procurement shortages, reduce unnecessary packaging costs, and hold sites accountable for compliance gaps.

**Transit Time Governance** — compares configured transit times against measured p95 actuals to protect customer delivery promises, flag misconfigured lanes, and surface underperforming carriers before they impact experience.

## Stack

BigQuery · Power BI · Python · SQL · Git

## Architecture

The warehouse follows a medallion architecture with three layers:

- **Bronze** — raw data loaded as-is from source CSVs, all columns as STRING, no transformations applied
- **Silver** — cleaned, typed, and conformed data with a standardised quality flag vocabulary across all five tables, producing a record-level quality score (clean / warning / critical) consumed by the Gold layer
- **Gold** — business-ready dimensional model with aggregations and business logic powering the three operational dashboards

## Data Sources

Five source systems feed the warehouse. In production these would be loaded automatically via a connector tool such as Fivetran or Airbyte. The Python scripts in `scripts/generation/` simulate the source data and `scripts/load/` simulates the ingestion process.

**MDM — Network Configuration System**
Node registry for the EU logistics network: 15 origin nodes, 6 consolidation hubs, 30 delivery nodes across 8 countries, with geographic coordinates and container capability per node (MTL / BOX).

**TMS — Transport Management System**
One row per truck run: origin, destination, carrier, departure datetime, actual transit time, container count, CBM loaded, and package count. Covers 184 days (July–December 2025) across 205 active lanes with seasonal volume multipliers.

**WMS — Warehouse Management System**
One row per shipment leg: traces each package from origin to final delivery node via sort code, container ID, and truck ID. Direct shipments produce one row, indirect shipments (via consolidation hub) produce two rows sharing the same shipment ID.

**FIN — Transport Rates**
Linehaul cost parameters per carrier and country: fixed cost per truck run and variable cost per km, covering 5 carriers across 8 countries with realistic tier differentiation (premium to budget).

**FIN — Handling Costs**
Processing cost per package for each consolidation hub, reflecting operational cost differences across hub locations.

## Structure

```
README.md
docs/
  data_contract.md
  architecture.md
  decision_log.md
  bronze_layer.md
  silver_layer.md
  gold_layer.md
scripts/
  generation/       — synthetic data generation scripts
  load/             — BigQuery ingestion scripts
sql/
  silver/           — Silver layer transformation SQL
  gold/             — Gold layer transformation SQL
data/               — generated CSVs (gitignored)
```

## Reproducing the Data

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Generate source data (run in order)
python3 scripts/generation/network.py
python3 scripts/generation/financials.py
python3 scripts/generation/transportation.py
python3 scripts/generation/shipment_details.py  # ~37M rows, takes several minutes

# Load into BigQuery Bronze layer
python3 scripts/load/load_bronze.py
```
