# Architecture

## What This System Does

This system models a European logistics network operating across 8 countries, with 15 origin nodes, 6 consolidation hubs, and 30 delivery nodes. It stores and transforms operational data to surface three categories of money leaks:

**Thin lane inefficiency** — truck runs where the volume loaded does not justify the operating cost. Rather than deprecating the connection entirely, thin lanes can be rerouted through consolidation hubs, which absorb low-volume flows from multiple origins and dispatch them together. The decision to deprecate or reroute is what the Lane Health dashboard drives.

**Container compliance gaps** — delivery nodes capable of receiving metallic containers should receive them. Metallic containers are reusable, which eliminates the recurring cost of purchasing cardboard boxes and pallets. When an origin ships the wrong container type to a compliant node, it creates an avoidable procurement cost. The Container Compliance dashboard tracks this at lane and origin level to hold sites accountable and inform procurement planning.

**Transit time misconfiguration** — the transit time configured in the network is used to calculate promised delivery dates to customers. If the measured p95 consistently exceeds the configured value, either the configuration is wrong or a carrier is underperforming. Both have customer experience consequences. The Transit Time Governance pipeline surfaces which lanes and carriers are drifting from their configured benchmarks.

---

## Data Flow

```
Source systems (Python-generated CSVs)
        ↓
   Bronze layer        — raw ingestion, all columns as STRING
        ↓
   Silver layer        — cleaned, typed, quality-flagged
        ↓
    Gold layer         — dimensional model, business logic
        ↓
  Power BI dashboards  — Lane Health · Container Compliance · Transit Time Governance
```

---

## Layers

### Bronze

Raw data loaded exactly as received from source systems. Every column is stored as STRING regardless of its intended type. No transformations, no filtering, no casting.

This serves two purposes. First, it ensures nothing is lost at ingestion — a malformed numeric value that would fail a FLOAT cast is preserved as-is and handled downstream. Second, it creates an immutable audit trail. If a Silver transformation introduces a bug, Bronze is the recovery point. Every reprocessing decision starts here.

### Silver

The cleaning and conforming layer. Each table follows a four-CTE pattern: cast types, apply deterministic corrections, flag all quality issues using a standardised vocabulary, and produce a record-level quality score (clean / warning / critical).

The flag vocabulary is defined in the data contract. If data arrives that the contract does not account for, the correct response is to update the contract first, then update the Silver SQL. Silent fixes are not acceptable — every decision about data quality must be documented and traceable.

Silver serves analysts and data engineers who need clean, typed data with full visibility into what was corrected and what remains problematic.

### Gold

Business-ready tables built on top of Silver. Aggregations, dimensional modeling, and business logic live here — not in Silver, and not in the dashboard tool. Gold tables answer specific business questions and are the direct input to Power BI.

Gold consumers — dashboard developers and business analysts — should never need to reason about data quality. By the time data reaches Gold, quality decisions have already been made and documented in Silver.

---

## Source Systems

Five source systems feed the warehouse. In a production environment these would be loaded automatically via a connector such as Fivetran or Airbyte. Here they are simulated via Python generation scripts.

The naming convention follows modern supply chain system categories to reflect how data would realistically arrive in a production warehouse:

| Prefix | System | Tables |
|---|---|---|
| MDM | Network Configuration System | mdm_network |
| TMS | Transport Management System | tms_transportation |
| WMS | Warehouse Management System | wms_shipment_details |
| FIN | Financial Rate Management | fin_transport_rates, fin_handling_costs |

---

## Why Medallion

The medallion architecture was chosen because each layer serves a distinct consumer with different needs.

Bronze serves the audit and reprocessing use case — the consumer is the pipeline itself, not a person. Silver serves analysts who need clean, typed data and full quality transparency. Gold serves dashboard consumers who need pre-aggregated metrics without having to reason about quality flags or raw string types.

Separating these concerns means a bug in the Gold layer can be fixed without touching Silver. A schema change in Bronze can be absorbed in Silver without breaking Gold. Each layer is independently testable and independently reprocessable.

---

## Synthetic Data

The source data is generated via Python scripts rather than using a public dataset. This was a deliberate choice for two reasons.

First, it allowed the full system to be modeled end-to-end — five source systems, a realistic network topology, seasonal volume patterns, carrier performance variance, and container compliance rules — rather than working from a single table with a fixed schema.

Second, seven categories of data quality issues were intentionally injected across all five tables. This forces every Silver transformation to be defensive by design, handling edge cases that a clean public dataset would never surface. The quality issues injected include NULL values in critical fields, mixed-case formatting, duplicate rows, invalid enum values, referential integrity failures, out-of-range values, and temporal inconsistencies.

The generation scripts are parameterized and can be re-run to produce updated data, which supports extending the simulation period for real-time dashboard scenarios in future iterations.
