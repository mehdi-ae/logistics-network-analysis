# Data Contract — EU Logistics Network Analytics Platform

**Version:** 1.1  
**Status:** Active  
**Effective date:** July 2025  
**Last reviewed:** April 2026  
**Data producer:** Analytics Engineering  
**Data consumers:** BI / Dashboard layer, Gold layer transformations  
**Owner:** El Mehdi Charaf

---

## Purpose

This contract defines the data made available by the analytics platform to downstream consumers. It specifies what tables exist, what each table contains, what quality guarantees apply, and what constitutes a breaking change requiring consumer notification.

Consumers of Silver and Gold layer tables can rely on this contract. Any change to schema, grain, or quality standards that would break a downstream dependency requires a version increment and advance notice.

---

## Scope

This contract covers the Silver layer output tables produced from five source systems. Gold layer tables are covered in a separate contract.

| Table | Source system | Layer |
|---|---|---|
| silver_mdm_network | MDM — Network Configuration | Silver |
| silver_fin_transport_rates | FIN — Transport Rate Management | Silver |
| silver_fin_handling_costs | FIN — Transport Rate Management | Silver |
| silver_tms_transportation | TMS — Transport Management | Silver |
| silver_wms_shipment_details | WMS — Warehouse Management | Silver |

---

## Quality Standards

All Silver layer tables conform to the following standards before being made available to consumers.

**Typing** — all columns are cast to their target data types. String columns are trimmed and normalised. Numeric columns are validated against expected ranges. Datetime columns are validated for logical consistency (arrival after departure, order date before shipment date).

**Deduplication** — duplicate rows are identified and removed. The first occurrence is retained. Duplicates are counted and reported in the quality metadata.

**Flags** — every column carries a quality flag using the following vocabulary:

| Flag | Meaning |
|---|---|
| ok | Value is present and valid |
| corrected | Value was invalid but a deterministic correction was applied |
| null_in_source | Value was NULL or empty in the source data |
| null_expected | NULL is a valid state for this field |
| invalid_value | Value is present but fails validation — kept as-is |
| invalid_format | Value does not conform to the expected format |
| invalid_country | Country code is valid format but not in the expected set for this network |
| not_iso_format | Country code does not conform to 2-character ISO standard |
| unexpected_node_format | node_id does not match the expected pattern (ON|CH|DN)_[A-Z]{2,4} |
| invalid_node_type | Node exists in the network but is not the expected type for this table |
| out_of_range | Value is present and valid type but outside the expected business range |
| invalid_cost | Cost value is zero or negative |
| referential_integrity_fail | Foreign key lookup found no matching record |
| duplicate | Row is a duplicate of another row in the same table |

**Record-level quality score** — every row carries a `record_quality` field:

| Score | Meaning |
|---|---|
| clean | All flags are ok or null_expected |
| warning | At least one corrected, null_in_source, or non-critical invalid_value |
| critical | At least one flag that makes the row unusable for its primary downstream purpose |

Gold layer transformations filter on `record_quality != 'critical'` by default. Consumers requiring full data including critical rows must explicitly opt in.

**Contract alignment** — if source data arrives that is not covered by this contract, the Silver transformation flags it as `invalid_value` and the contract is updated before the next production run. Silent fixes are not applied.

---

## Table Definitions

### silver_mdm_network

**Grain:** one row per node (deduplicated)  
**Refresh:** full refresh on source update  
**Primary key:** `node_id`

| Column | Type | Nullable | Description |
|---|---|---|---|
| node_id | STRING | No | Unique node identifier |
| node_type | STRING | No | origin_node \| consolidation_hub \| delivery_node |
| city | STRING | No | City name, trimmed |
| country | STRING | No | ISO 3166-1 alpha-2, uppercase |
| latitude | FLOAT | Conditional | Geographic latitude. NULL = critical |
| longitude | FLOAT | Conditional | Geographic longitude. NULL = critical |
| delivery_node_capability | STRING | Conditional | MTL \| BOX \| ALL. NULL or invalid = warning |
| record_quality | STRING | No | clean \| warning \| critical |

**Critical conditions:** NULL or invalid `node_type`, NULL `latitude` or `longitude`, 
`latitude` outside 35–72, `longitude` outside -10–40, `node_id` not matching 
pattern `^(ON|CH|DN)_[A-Z]{2,4}$`.

**Validation rules:**
- `node_id` must match `^(ON|CH|DN)_[A-Z]{2,4}$`
- `country` must be exactly 2 characters and uppercase
- `latitude` must be between 35 and 72
- `longitude` must be between -10 and 40
- `city` empty string after trim is treated as null_in_source
- `delivery_node_capability` must be one of MTL, BOX, ALL

---

### silver_fin_transport_rates

**Grain:** one row per (country, carrier)  
**Refresh:** full refresh on source update  
**Primary key:** `country`, `carrier`

| Column | Type | Nullable | Description |
|---|---|---|---|
| country | STRING | No | ISO 3166-1 alpha-2, uppercase |
| carrier | STRING | No | Carrier name, trimmed |
| fixed_cost_eur | FLOAT | Conditional | Fixed cost per truck run in EUR. Negative = critical |
| cost_per_km_eur | FLOAT | Conditional | Variable cost per km in EUR. NULL = critical |
| record_quality | STRING | No | clean \| warning \| critical |

**Critical conditions:** negative `fixed_cost_eur`, NULL `cost_per_km_eur`. 
These rows cannot be used for lane cost calculations.

**Validation rules:**
- `country` must be exactly 2 characters and one of: FR, DE, ES, IT, NL, BE, PL, CZ
- `carrier` leading and trailing whitespace is corrected in Silver
- `fixed_cost_eur` must be greater than 0
- `cost_per_km_eur` must be greater than 0

---

### silver_fin_handling_costs

**Grain:** one row per consolidation hub  
**Refresh:** full refresh on source update  
**Primary key:** `node_id`  
**Foreign key:** `node_id` → `silver_mdm_network.node_id`

| Column | Type | Nullable | Description |
|---|---|---|---|
| node_id | STRING | No | Consolidation hub identifier |
| cost_per_package_eur | FLOAT | Conditional | Handling cost per package. Expected range €1.00–€2.50. NULL = critical |
| record_quality | STRING | No | clean \| warning \| critical |

**Critical conditions:** NULL `cost_per_package_eur`, `node_id` not referencing 
a consolidation_hub in silver_mdm_network.

**Validation rules:**
- `node_id` must reference a node of type consolidation_hub in silver_mdm_network
- `cost_per_package_eur` expected range €1.00–€2.50
- Values outside the expected range are flagged as out_of_range and treated as critical
- Values zero or negative are flagged as invalid_cost and treated as critical

---

### silver_tms_transportation

**Grain:** one row per truck run  
**Refresh:** append-only  
**Primary key:** `truck_id`  
**Foreign keys:** `origin_node_id`, `destination_node_id` → `silver_mdm_network.node_id`

| Column | Type | Nullable | Description |
|---|---|---|---|
| truck_id | STRING | No | Unique truck run identifier |
| origin_node_id | STRING | No | Origin node |
| destination_node_id | STRING | No | Destination node |
| carrier | STRING | Yes | Carrier name. NULL = warning |
| departure_datetime | DATETIME | No | Actual departure timestamp |
| transit_time_hours | FLOAT | No | Actual transit time in hours. Negative = critical |
| arrival_datetime | DATETIME | No | Arrival timestamp. Must be after departure = critical if not |
| container_count | INTEGER | No | Number of containers. Range 1–32 |
| package_count | INTEGER | No | Total packages on this truck |
| cbm | FLOAT | No | Volume loaded. Must not exceed container_count × 1 CBM |
| record_quality | STRING | No | clean \| warning \| critical |

**Critical conditions:** negative `transit_time_hours`, `arrival_datetime` before `departure_datetime`, `cbm` exceeding container capacity, referential integrity failure on node IDs, `truck_id` format not matching expected pattern.

---

### silver_wms_shipment_details

**Grain:** one row per shipment leg  
**Refresh:** append-only  
**Primary key:** `shipment_id`, `truck_id` (composite — a shipment appears once per leg)  
**Foreign keys:** `truck_id` → `silver_tms_transportation.truck_id`, `origin_node_id`, `destination_node_id`, `sort_code` → `silver_mdm_network.node_id`

| Column | Type | Nullable | Description |
|---|---|---|---|
| shipment_id | STRING | No | Shipment identifier. Shared across both legs for indirect shipments |
| truck_id | STRING | No | FK to transportation table |
| container_id | STRING | No | Container identifier. Prefix MTL_ or BOX_ encodes container type |
| order_date | DATE | Yes | Original order date. Must be before first leg departure. NULL = warning |
| origin_node_id | STRING | No | Origin of this leg |
| destination_node_id | STRING | No | Destination of this leg |
| sort_code | STRING | No | Final delivery node. NULL = critical |
| record_quality | STRING | No | clean \| warning \| critical |

**Critical conditions:** NULL `sort_code`, referential integrity failure on any node ID or `truck_id`, `container_id` with invalid format.

**Known limitation:** container count coherence (C_count in shipment details = 
container_count in transportation) is not validated in Silver. The generation 
layer does not guarantee every container carries at least one package. This 
constraint will be validated in Gold after the generation script is corrected. 
See ADR-014.

---

## Coherence Guarantees

The following constraints are validated in Silver and must hold for `record_quality = clean` rows.

| # | Constraint |
|---|---|
| C1 | Package count on each truck in `silver_tms_transportation` equals the count of shipment legs in `silver_wms_shipment_details` for that truck |
| C2 | CBM per truck does not exceed container_count × 1 CBM |
| C3 | Each truck has exactly one origin and one destination |
| C4 | Every shipment departing an origin arrives at its sort_code destination — no shipments are created or lost in transit |
| C5 | Direct shipments have exactly one row where destination_node_id = sort_code |
| C6 | Indirect shipments have exactly two rows sharing the same shipment_id |
| C7 | For any (origin, sort_code) pair, all shipments follow the same path type — always direct or always indirect |
| C8 | Inbound container IDs at a consolidation hub never appear as outbound container IDs |
| C9 | All foreign keys resolve to a valid record in the referenced table |

---

## Breaking Changes

The following changes require a version increment and advance consumer notification:

- Removal of any column
- Change to a column's data type
- Change to the grain of a table
- Change to the definition of `record_quality` scores
- Change to the primary key of any table

The following changes do not require a version increment but must be documented in the decision log:

- Addition of a new column
- Addition of a new flag value
- Change to an expected value range that does not affect existing rows
- Addition of a new table

---

## Contact

For questions about this contract or to report data issues, contact the data producer directly. Any data behaviour not covered by this contract should be raised immediately — it triggers a contract review, not a silent fix.
