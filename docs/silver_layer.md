# Silver Layer

**Layer:** Silver
**Dataset:** `silver_logistics`
**Last updated:** April 2026
**Depends on:** Bronze layer (`bronze_logistics`)
**Consumed by:** Gold layer (`gold_logistics`)

---

## Purpose

The Silver layer cleans, types, and quality-flags the raw Bronze data. It is the single source of truth for data quality decisions — every correction, every flag, and every record-level quality score is applied here and documented in the data contract.

Gold layer transformations read exclusively from Silver, filtering on `quality_record != 'critical'` by default.

---

## Transformation Pattern

Every Silver table follows the same four-CTE pattern:

**CTE 1 — cast:** Cast all STRING columns to their target types. Use SAFE_CAST to avoid silent failures — values that cannot be cast produce NULL rather than erroring. Apply TRIM to all string fields. Apply UPPER to enum fields (country codes, node types).

**CTE 2 — correct:** Apply deterministic corrections where the correct value can be derived with certainty from the raw value. Carry the raw value as a `_raw` alias alongside the corrected value for auditability. Corrections are limited to cases where the business rule is unambiguous — for example, trimming whitespace from a carrier name, or uppercasing a country code.

**CTE 3 — flag:** Apply one quality flag per column using the standardised vocabulary defined in the data contract. Each flag is evaluated independently. The source null check must precede the referential integrity check in CASE WHEN ordering — reversed order misattributes `null_in_source` as `referential_integrity_fail`.

**CTE 4 — summarise:** Evaluate all column-level flags to produce a record-level `quality_record` score (clean / warning / critical). Use EXCEPT to drop raw alias columns from the final output.

---

## Tables

### silver_mdm_network

**Source:** `bronze_logistics.mdm_network`
**Grain:** one row per node (deduplicated)
**Rows:** 51 (after deduplication of 55 Bronze rows)
**Quality distribution:** 49 clean, 2 warning

**Transformations applied:**
- Deduplicated on `node_id` using ROW_NUMBER, retaining first occurrence
- `country` corrected to UPPER(TRIM()) — mixed-case country codes normalised
- `node_type` corrected — typos mapped to valid enum values where the correction is unambiguous
- `latitude` and `longitude` validated against EU bounding box (lat 35–72, lon -10–40)
- `node_id` validated against pattern `^(ON|CH|DN)_[A-Z]{2,4}$`

**Critical conditions:** NULL or invalid node_type, NULL coordinates, coordinates outside EU bounding box, node_id not matching expected pattern.

---

### silver_fin_transport_rates

**Source:** `bronze_logistics.fin_transport_rates`
**Grain:** one row per (country, carrier) — deduplicated
**Rows:** 40 (after deduplication of 41 Bronze rows)
**Quality distribution:** 36 clean, 4 critical

**Transformations applied:**
- Deduplicated on (country, carrier) using ROW_NUMBER
- `carrier` trimmed — one carrier had leading whitespace in the source
- `country` validated against expected 8-country set (FR, DE, ES, IT, NL, BE, PL, CZ)
- `fixed_cost_eur` and `cost_per_km_eur` validated as positive values

**Critical conditions:** negative or zero cost values. These rows cannot be used for lane cost calculations and are excluded from Gold.

---

### silver_fin_handling_costs

**Source:** `bronze_logistics.fin_handling_costs`
**Grain:** one row per consolidation hub
**Rows:** 6
**Quality distribution:** 4 clean, 2 critical

**Transformations applied:**
- `node_id` validated via referential integrity check against `silver_mdm_network`
- `node_id` validated as consolidation_hub type — not other node types
- `cost_per_package_eur` validated against expected range €1.00–€2.50

**Critical conditions:** NULL cost, cost outside expected range, node_id not referencing a consolidation_hub.

---

### silver_tms_transportation

**Source:** `bronze_logistics.tms_transportation`
**Grain:** one row per truck run
**Rows:** 610,216
**Quality distribution:** 601,060 clean, 6,102 warning, 3,054 critical

**Transformations applied:**
- `departure_datetime` and `arrival_datetime` cast using SAFE_CAST with fallback PARSE_DATETIME for non-standard formats
- `transit_time_hours` validated as positive — negative values are physically impossible
- `arrival_datetime` validated as after `departure_datetime`
- `cbm` validated against container capacity ceiling (container_count × 1 CBM)
- `origin_node_id` and `destination_node_id` validated via referential integrity against `silver_mdm_network`
- `carrier` NULL classified as warning — truck runs with unknown carrier retain valid volume and timing data
- `truck_id` validated against pattern `^TRK_[0-9]{7}$`

**Warning conditions:** NULL carrier (~1% of rows). These rows are preserved in Gold with imputed average-carrier cost (see ADR-015).

**Critical conditions:** negative transit time, arrival before departure, CBM exceeding container capacity, referential integrity failure on node IDs.

---

### silver_wms_shipment_details

**Source:** `bronze_logistics.wms_shipment_details`
**Grain:** one row per shipment leg (deduplicated)
**Rows:** 37,751,033 (after deduplication of 37,864,286 Bronze rows)
**Quality distribution:** 36,958,262 clean, 792,771 critical

**Transformations applied:**
- Deduplicated on (shipment_id, truck_id) using ROW_NUMBER, retaining first occurrence
- `order_date` cast with dual-format fallback (ISO and DD/MM/YYYY)
- `order_date` validated as before `departure_datetime` of the associated truck run
- `truck_id` validated via referential integrity against `silver_tms_transportation`
- `origin_node_id` and `destination_node_id` cross-validated against the associated truck run
- `sort_code` validated via referential integrity against `silver_mdm_network` — must be a delivery_node
- `container_id` prefix validated (MTL or BOX)
- `shipment_id` validated against pattern `^SHP_[0-9]{9}$`
- Package count coherence validated: COUNT(shipment_id) per truck must equal `package_count` in silver_tms_transportation

**Critical conditions (by injected issue rate):**
- NULL sort_code: ~1% of rows (377,510)
- order_date after departure_datetime: ~0.5% of rows (188,755)
- Invalid container_id prefix: ~0.3% of rows (113,253)
- Duplicate shipment_id per truck: ~0.3% of rows (113,253)

**Known limitation:** Container count coherence (distinct container IDs per truck = container_count in transportation) is not validated. The generation script does not guarantee every container carries at least one package. See ADR-013.

---

## Quality Flag Vocabulary

| Flag | Meaning |
|---|---|
| ok | Value is present and valid |
| corrected | Value was invalid but a deterministic correction was applied |
| null_in_source | Value was NULL or empty in the source data |
| null_expected | NULL is a valid state for this field |
| invalid_value | Value is present but fails validation — kept as-is |
| invalid_format | Value does not conform to the expected format |
| referential_integrity_fail | Foreign key lookup found no matching record in the referenced Silver table |
| duplicate | Row is a duplicate — handled at deduplication stage |

---

## Record-Level Quality Score

| Score | Meaning |
|---|---|
| clean | All flags are ok or null_expected |
| warning | At least one corrected, null_in_source, or non-critical invalid flag |
| critical | At least one flag that makes the row unusable for its primary downstream purpose |

Gold layer transformations filter `quality_record != 'critical'` by default. Warning rows are included — their degraded fields are handled in Gold business logic where appropriate.

---

## Running the Silver Layer

Silver tables are created in BigQuery using the SQL files in `sql/silver/`. Each file creates or replaces the full table. Run in any order — the only dependency is that `silver_mdm_network` and `silver_tms_transportation` must exist before `silver_wms_shipment_details`, which joins both.

```
sql/silver/silver_mdm_network.sql
sql/silver/silver_fin_transport_rates.sql
sql/silver/silver_fin_handling_costs.sql
sql/silver/silver_tms_transportation.sql
sql/silver/silver_wms_shipment_details.sql
```
