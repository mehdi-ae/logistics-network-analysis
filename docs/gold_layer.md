# Gold Layer

**Layer:** Gold
**Dataset:** `gold_logistics`
**Last updated:** April 2026
**Depends on:** Silver layer (`silver_logistics`)
**Consumed by:** Power BI dashboards

---

## Purpose

The Gold layer translates Silver quality data into business-ready analytical tables. Aggregations, dimensional modeling, simulation logic, and business rules live here — not in Silver, and not in the dashboard tool.

Gold consumers — dashboard developers and business analysts — should never need to reason about data quality flags or raw string types. By the time data reaches Gold, quality decisions have already been made and documented in Silver.

---

## Design Principles

**Business logic in SQL, not DAX.** Cost reduction calculations, lane classification, and hub recommendation logic are all pre-computed in Gold. Power BI reads pre-computed results — it does not implement business rules. This keeps logic traceable, testable, and independent of the visualization tool.

**Pre-aggregate before joining.** Large fact tables (610K truck runs, 37M shipment legs) are aggregated to the required grain before joining to dimension tables or rate tables. Joining first and aggregating after is significantly more expensive in BigQuery and produces harder-to-debug intermediate results.

**Clustering over ordering.** Gold tables are clustered on the columns most commonly used in dashboard filters (origin_node_id, destination_node_id, date). Clustering reduces bytes scanned on filtered queries without requiring explicit partitioning on a date column.

**Transparent imputation.** Where values are estimated rather than measured (UNKNOWN carrier costs, CBM pro-rating for indirect lanes), a flag column documents the imputation. Dashboard consumers can filter on these flags to understand the confidence of the underlying data.

---

## Tables

### gold_fact_lane_daily

**Grain:** one row per (origin_node_id, destination_node_id, carrier, date)
**Rows:** 107,444
**Clustering:** origin_node_id, destination_node_id, date
**Simulation period:** 2025-07-01 to 2025-12-31 (184 days)

**Purpose:** Operational daily fact table for the Lane Health dashboard. Powers trend charts, carrier comparison, and rolling average monitoring. Every lane-carrier combination has a row for every calendar day in the simulation period — including days with zero truck runs (filled via date spine with COALESCE to 0).

**Key columns:**

| Column | Description |
|---|---|
| date | Calendar date |
| origin_node_id | Origin node |
| origin_node_type | origin_node |
| origin_node_country | ISO country code of origin |
| destination_node_id | Destination node |
| destination_node_type | consolidation_hub or delivery_node |
| destination_node_country | ISO country code of destination |
| carrier | Carrier name or UNKNOWN for NULL carrier runs |
| distance_km | Haversine distance origin→destination in km |
| daily_trucks | Truck runs on this lane by this carrier on this date |
| daily_packages | Total packages shipped |
| daily_cbm | Total CBM loaded |
| daily_linehaul_cost_eur | (fixed_cost + distance × cost_per_km) × daily_trucks |
| cost_is_imputed | TRUE when carrier = UNKNOWN and cost uses country average rates |
| rolling_avg_cbm_30d | 30-day rolling average CBM (ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) |
| rolling_avg_cost_30d | 30-day rolling average linehaul cost |

**Build logic:**

1. `date_spine` — GENERATE_DATE_ARRAY from 2025-07-01 to 2025-12-31
2. `lane_carrier_combinations` — DISTINCT (origin, destination, carrier, node types, country, distance) for known carriers only
3. `daily_actuals` — aggregated truck run metrics per (origin, destination, carrier, date)
4. `date_spine_lanes` — CROSS JOIN date spine × lane combinations, LEFT JOIN actuals, COALESCE nulls to 0. UNION ALL with UNKNOWN carrier real-run rows (no gap filling for UNKNOWN)
5. `linehaul_costs` — CASE WHEN carrier = UNKNOWN then imputed average rate, else actual carrier rate
6. `final` — rolling averages computed last on top of zero-filled daily values

**NULL carrier handling:** NULL carrier truck runs (~1% of TMS data) are relabelled UNKNOWN and added via UNION ALL to the date spine output. They appear only on days when trucks actually ran — no zero-volume UNKNOWN rows are generated. Cost uses country-level average carrier rates. See ADR-015.

---

### gold_fact_lane_simulation

**Grain:** one row per (origin_node_id, destination_node_id, sort_code, departure_month)
**Rows:** 5,994 (999 unique triplets × 6 months)
**Simulation period:** 2025-07-01 to 2025-12-31 (6 months)

**Purpose:** Decision-support simulation table for the Lane Health dashboard. Classifies each lane or indirect triple as thin / thick / healthy, recommends a network action, and quantifies the monthly cost reduction from implementing that action.

**Key columns:**

| Column | Description |
|---|---|
| departure_month | Month truncated to first day (DATE_TRUNC) |
| origin_node_id | Origin node — always origin_node type |
| origin_node_country | ISO country code of origin |
| destination_node_id | First-leg destination (hub for indirect, delivery node for direct) |
| sort_code | Final delivery node |
| triplet_type | direct or indirect |
| distance1_km | Haversine distance origin→destination |
| distance2_km | Haversine distance hub→sort_code (NULL for direct lanes) |
| direct_distance_km | Haversine distance origin→sort_code (used for thick lane conversion cost) |
| adjusted_triplet_cbm | Pro-rated CBM: origin's proportional share of hub→DN lane volume for indirect lanes; raw monthly triplet CBM for direct lanes |
| monthly_triplet_packages | Total packages shipped on this triplet this month |
| current_monthly_cost_ratio | Origin's proportional share of the (origin→destination) lane monthly cost |
| processing_cost_ratio | Origin's proportional share of hub processing cost for indirect lanes |
| category | thin / thick / healthy |
| recommended_hub | Closest hub already connected to both origin and delivery node (thin lanes only) |
| deprecation_feasible | TRUE if a qualifying hub exists for thin lane rerouting |
| cost_of_recommended_design | Estimated monthly cost of implementing the recommendation |
| cost_reduction | current_monthly_cost_ratio − cost_of_recommended_design |

**Lane classification thresholds (see ADR-017):**

| Category | Condition | Daily equivalent |
|---|---|---|
| Thin | direct AND adjusted_triplet_cbm < 138 monthly CBM | < 4.6 CBM/day |
| Thick | indirect AND adjusted_triplet_cbm > 322 monthly CBM | > 10.7 CBM/day |
| Healthy | everything else | — |

**Cost formulas:**

Thin lane deprecation (reroute through recommended hub):
```
cost_of_recommended_design =
  (leg1_avg_fixed + leg1_avg_cost_per_km × distance1_km) × (adjusted_cbm / 28)
  + (leg2_avg_fixed + leg2_avg_cost_per_km × hub_to_sortcode_distance) × (adjusted_cbm / 28)
  + processing_cost_ratio
```

Thick triple conversion (launch direct lane):
```
cost_of_recommended_design =
  (leg1_avg_fixed + leg1_avg_cost_per_km × direct_distance_km) × (adjusted_cbm / 28)
```

All carrier rates use country-level averages from `silver_fin_transportation_rates` (see ADR-016 on CBM pro-rating).

**Hub selection for thin lane deprecation:** The recommended hub is the consolidation hub already operationally connected to both the origin and the delivery node — meaning truck runs exist from origin→hub AND from hub→sort_code in `silver_tms_transportation`. When multiple hubs qualify, the closest to the origin by haversine distance is selected using ROW_NUMBER ordered by distance1_km ASC. If no qualifying hub exists, `deprecation_feasible = FALSE`.

**CBM pro-rating for indirect lanes:** A hub→delivery node truck carries packages from multiple origins consolidated at the hub. The raw CBM from the WMS join at triplet level sums to the full hub→DN lane CBM, not the origin's share. The adjusted CBM is calculated as `(origin monthly packages / total hub→DN monthly packages) × total hub→DN monthly CBM`. See ADR-016.

**Simulation results (July–December 2025):**

| Category | Triplets | Total current cost | Total recommended cost | Monthly saving |
|---|---|---|---|---|
| Thin direct | 32 | €352,429 | €199,715 | €152,714 |
| Thick indirect | 570 | €10,803,101 | €8,895,443 | €1,907,658 |
| Healthy | 5,392 | €35M+ | — | — |

3 thin lanes show negative saving (deprecation would cost more than current direct route) — flagged as non-actionable.

---

## Running the Gold Layer

Gold tables depend on Silver. Run Silver first, then Gold in this order:

```
sql/gold/gold_fact_lane_daily.sql     — run first, simulation table depends on it
sql/gold/gold_fact_lane_simulation.sql
```

Before running, create the dataset if it does not exist:

```sql
CREATE SCHEMA IF NOT EXISTS gold_logistics
OPTIONS (location = 'europe-west9');
```
