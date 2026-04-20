# 🚛 Supply Chain Analytics Platform
### Cost Optimization, Operational Performance & Data Governance

---

## 📌 Executive Summary

This project builds a production-grade analytics platform on a synthetic EU logistics network, combining data from four operational systems — TMS, WMS, Finance, and MDM — into a structured medallion data warehouse (Bronze / Silver / Gold) that powers three operational dashboards.

The platform surfaces three categories of avoidable cost: unprofitable transport lanes, container compliance gaps, and transit time misconfigurations. Each dashboard is backed by a simulation model that quantifies the financial impact of each corrective action — giving decision makers a ranked list of interventions with an estimated monthly saving per action.

---

## 🎯 Business Problem

Large-scale logistics networks generate cost leakage that is invisible without structured data:

- Thin transport lanes run trucks at low fill rates, incurring full fixed costs for partial loads
- Origins shipping the wrong container type to compliant nodes create recurring procurement waste
- Transit times configured in the network drift from operational reality, eroding delivery promises to customers
- Fragmented source systems (TMS, WMS, Finance) make it impossible to link operational decisions to financial impact without a unified data model

These issues compound silently — each one is individually small, but across hundreds of lanes and millions of shipments they represent material cost.

---

## 💡 Solution Overview

This platform consolidates multi-source operational data into a layered architecture that:

- Ingests raw data from five source tables across four systems into a Bronze layer, preserved exactly as received
- Cleans, types, and quality-flags every column in a Silver layer using a standardised flag vocabulary — producing a record-level quality score (clean / warning / critical) consumed by all downstream models
- Builds a Gold dimensional model with pre-computed business logic, cost simulations, and lane classification — keeping analytical decisions in SQL and out of the dashboard tool
- Delivers three Power BI dashboards that surface cost reduction opportunities with quantified monthly savings

---

## 📊 Key Business Insights

The Lane Health simulation across 6 months of data (July–December 2025) produced the following findings:

- **32 thin direct lanes** identified with a combined monthly saving of **€152,714** if deprecated and rerouted through existing consolidation hubs
- **95 heavy indirect triples per month** identified with a combined monthly saving of **€1.9M** if converted to direct lanes
- **3 thin lanes** where deprecation would cost more than the current direct route — flagged as non-actionable by the simulation model
- Data quality analysis identified **792,771 critical rows** across the WMS shipment details table — equivalent to 2.1% of total shipment volume — driven by NULL sort codes, invalid container formats, duplicate shipment IDs, and temporal integrity failures

---

## 🏗️ Data Architecture

The warehouse follows a medallion architecture across three layers:

**Bronze** — raw ingestion layer. All source columns loaded as STRING with no transformations. Serves as an immutable audit trail and reprocessing recovery point.

**Silver** — cleaning and conforming layer. Five tables transformed using a four-CTE pattern: cast types → apply deterministic corrections → flag quality issues → produce record-level quality score. Every quality decision is documented in the data contract. Silent fixes are not applied.

**Gold** — business-ready analytical layer. Two fact tables power the Lane Health dashboard: `gold_fact_lane_daily` (one row per lane, carrier, date — with 30-day rolling CBM and cost averages) and `gold_fact_lane_simulation` (one row per lane or indirect triple per month — with lane classification, recommended action, and pre-computed cost reduction).

```
Source CSVs (Python-generated)
        ↓
   Bronze layer    — raw ingestion, STRING columns, no transformations
        ↓
   Silver layer    — typed, cleaned, quality-flagged (clean / warning / critical)
        ↓
    Gold layer     — dimensional model, business logic, simulation calculations
        ↓
  Power BI         — Lane Health · Container Compliance · Transit Time Governance
```

---

## 🔐 Data Governance

A formal data contract governs the Silver layer output. It defines the grain, primary keys, column types, quality flag vocabulary, and critical conditions for each of the five Silver tables. Any change to schema, grain, or quality standards that would break a downstream dependency requires a version increment and advance consumer notification.

Key governance decisions documented in the decision log:

- **Flag vocabulary standardised across all five tables** — ok / corrected / null_in_source / null_expected / invalid_value / referential_integrity_fail — with explicit distinction between correctable and non-correctable issues
- **Silent fixes prohibited** — if source data arrives outside the contract, it is flagged as invalid_value and the contract is updated before the next run
- **Quality injection is additive** — corrupted rows are appended rather than replacing clean rows, preserving network integrity for downstream generation while providing realistic quality issues for Silver to handle
- **Container count coherence check deferred to Gold** — the Silver layer cannot reliably validate container count against shipment count because the generation script does not guarantee every container carries at least one package (ADR-013)

---

## 📈 Dashboards

### 🚛 Lane Health
Identifies unprofitable lanes and quantifies the saving from two types of network intervention: deprecating thin direct lanes (rerouting low-volume flows through consolidation hubs) and converting heavy indirect triples to direct lanes. Each recommendation is backed by a monthly cost reduction estimate pre-computed in the Gold simulation model.

### 📦 Container Utilization & Compliance
Tracks container type compliance per origin, lane, and carrier. Surfaces origins shipping the wrong container type to MTL-capable delivery nodes — a recurring procurement cost that is invisible without shipment-level data. Informs site accountability and procurement planning.

### ⏱️ Transit Time Governance
Compares configured transit times against measured p95 actuals per lane and carrier. Surfaces lanes where operational reality has diverged from network configuration — protecting customer delivery promises and identifying underperforming carriers before they generate complaints.

---

## ⚙️ Tech Stack

| Layer | Tool |
|---|---|
| Data warehouse | BigQuery |
| Transformation | SQL |
| Visualisation | Power BI |
| Data generation | Python |
| Version control | Git / GitHub |
| Future | dbt, Airflow, Docker |

---

## 🧠 Key Design Decisions

Seventeen architectural decisions are documented in `docs/decision_log.md`. The most consequential:

- **Synthetic data with intentionally injected quality issues** — forces defensive SQL design that a clean public dataset would never surface. Seven categories of issues injected across all five tables.
- **Gold simulation pre-computed in SQL** — cost reduction calculations live in the warehouse, not in DAX. The dashboard reads pre-computed savings directly, keeping business logic traceable and testable.
- **CBM pro-rating for indirect lanes** — a lane from origin O through hub H to delivery node D shares the H→D truck with other origins. CBM is allocated as (O's packages / total H→D packages) × total H→D CBM to avoid inflating cost attribution.
- **UNKNOWN carrier imputation** — NULL carrier truck runs (~1% of TMS data) are preserved with imputed average-carrier costs rather than excluded, preventing volume undercount on affected lanes.
- **Hub selection for thin lane deprecation** — the recommended consolidation hub is the one already operationally connected to both the origin and the delivery node, minimising rerouting risk. Distance tiebreaker applied when multiple hubs qualify.

---

## 🗂️ Repository Structure

```
docs/
  architecture.md       — system design and layer responsibilities
  data_contract.md      — Silver layer quality guarantees and table definitions
  decision_log.md       — architectural decision records (ADR-001 to ADR-017)
  bronze_layer.md       — Bronze layer documentation
  silver_layer.md       — Silver layer documentation
  gold_layer.md         — Gold layer documentation
scripts/
  generation/           — synthetic data generation (Python)
  load/                 — BigQuery ingestion scripts
sql/
  silver/               — Silver layer transformation SQL (5 tables)
  gold/                 — Gold layer transformation SQL (2 tables)
data/                   — generated CSVs (gitignored)
```

---

## 🚀 Reproducing the Data

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Generate source data (run in order)
python3 scripts/generation/network.py
python3 scripts/generation/financials.py
python3 scripts/generation/transportation.py
python3 scripts/generation/shipment_details.py  # ~37M rows, takes several minutes

# Load into BigQuery Bronze layer
python3 scripts/load/load_bronze.py
```

---

## 📬 Open to Collaboration

Available for freelance analytics engineering missions focused on:

- Supply chain analytics and cost optimization
- End-to-end data pipeline design
- Operational performance dashboards

📍 Based in France · Working remotely across Europe
