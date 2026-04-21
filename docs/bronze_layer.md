# Bronze Layer

## Purpose

The Bronze layer is the raw ingestion layer. It stores data exactly as received from source systems — no transformations, no type casting, no filtering. Every column is loaded as STRING regardless of its intended type.

This layer serves two purposes. First, it is an immutable audit trail — if a Silver transformation introduces a bug, Bronze is the recovery point. Second, it preserves data that would otherwise be lost at ingestion: a malformed numeric value that would fail a FLOAT cast is kept as-is and handled downstream where the decision is explicit and documented.

Bronze tables are never read directly by dashboards or analysts. They are the input to the Silver layer only.

---

## Dataset

**BigQuery project:** supply-chain-analytics-492110  
**Dataset:** bronze_logistics  
**Location:** europe-west9  
**Load script:** `scripts/load/load_bronze.py`  
**Load strategy:** full reload — tables are dropped and recreated on every run

---

## Source Systems

Five source systems feed the Bronze layer. In a production environment these would be loaded automatically via a connector tool such as Fivetran or Airbyte on a defined schedule. Here they are simulated via Python generation scripts that produce CSV files loaded into BigQuery.

| Source system | Prefix | Tables | Type |
|---|---|---|---|
| MDM — Network Configuration | mdm_ | mdm_network | Static reference |
| FIN — Financial Rate Management | fin_ | fin_transport_rates, fin_handling_costs | Static reference |
| TMS — Transport Management | tms_ | tms_transportation | Transactional |
| WMS — Warehouse Management | wms_ | wms_shipment_details | Transactional |

Static reference tables are full-refreshed on every load. Transactional tables are append-only in production — the load script uses full reload during development for simplicity.

---

## Tables

### bronze_logistics.mdm_network

**Source file:** `data/mdm_network.csv`  
**Grain:** one row per node (includes injected duplicate rows)  
**Row count:** 55

| Column | Source type | Description |
|---|---|---|
| node_id | STRING | Node identifier |
| node_type | STRING | Node classification |
| city | STRING | City name |
| country | STRING | Country code |
| latitude | STRING | Geographic latitude |
| longitude | STRING | Geographic longitude |
| node_capability | STRING | Container capability |

---

### bronze_logistics.fin_transport_rates

**Source file:** `data/fin_transport_rates.csv`  
**Grain:** one row per (country, carrier) combination (includes injected duplicate)  
**Row count:** 41

| Column | Source type | Description |
|---|---|---|
| country | STRING | Country code |
| carrier | STRING | Carrier name |
| fixed_cost_eur | STRING | Fixed cost per truck run |
| cost_per_km_eur | STRING | Variable cost per km |

---

### bronze_logistics.fin_handling_costs

**Source file:** `data/fin_handling_costs.csv`  
**Grain:** one row per consolidation hub  
**Row count:** 6

| Column | Source type | Description |
|---|---|---|
| node_id | STRING | Consolidation hub identifier |
| cost_per_package_eur | STRING | Handling cost per package |

---

### bronze_logistics.tms_transportation

**Source file:** `data/tms_transportation.csv`  
**Grain:** one row per truck run  
**Row count:** 610,216  
**Simulation period:** July 1 – December 31 2025

| Column | Source type | Description |
|---|---|---|
| truck_id | STRING | Truck run identifier |
| origin_node_id | STRING | Origin node |
| destination_node_id | STRING | Destination node |
| carrier | STRING | Carrier name |
| departure_datetime | STRING | Departure timestamp |
| transit_time_hours | STRING | Actual transit time in hours |
| arrival_datetime | STRING | Arrival timestamp |
| container_count | STRING | Number of containers |
| package_count | STRING | Total packages on this truck |
| cbm | STRING | Volume loaded in CBM |

---

### bronze_logistics.wms_shipment_details

**Source file:** `data/wms_shipment_details.csv`  
**Grain:** one row per shipment leg  
**Row count:** 37,864,286  
**Note:** indirect shipments produce two rows sharing the same shipment_id

| Column | Source type | Description |
|---|---|---|
| shipment_id | STRING | Shipment identifier |
| truck_id | STRING | Truck run identifier |
| container_id | STRING | Container identifier |
| order_date | STRING | Original order date |
| origin_node_id | STRING | Origin of this leg |
| destination_node_id | STRING | Destination of this leg |
| sort_code | STRING | Final delivery node |

---

## Injected Data Quality Issues

Quality issues were intentionally injected across all five source files to force defensive SQL at every Silver transformation. The issues reflect real-world data problems encountered in production logistics systems.

### mdm_network

| Issue | Rate | Description |
|---|---|---|
| Duplicate rows | 2 rows | One exact duplicate, one duplicate with invalid node_type value |
| NULL coordinates | 2 rows | Additive rows with empty latitude and longitude |
| Mixed case country | ~5% of rows | Country codes in lowercase instead of uppercase |
| Whitespace in city | ~4% of rows | Leading, trailing, or double internal spaces |
| Invalid node_type | 1 row | Typo: "orign_node" instead of "origin_node" |


### fin_transport_rates

| Issue | Rate | Description |
|---|---|---|
| Negative fixed_cost_eur | 1 row | Invalid negative value |
| NULL cost_per_km_eur | 1 row | Missing value |
| Whitespace in carrier | 1 row | Leading space in carrier name |
| Duplicate row | 1 row | Exact duplicate of an existing (country, carrier) row |

### fin_handling_costs

| Issue | Rate | Description |
|---|---|---|
| Out of range cost | 1 row | cost_per_package_eur above the expected €2.50 maximum |
| NULL cost | 1 row | Missing cost_per_package_eur value |

### tms_transportation

| Issue | Rate | Description |
|---|---|---|
| Negative transit_time_hours | 1 row | Invalid negative value |
| Arrival before departure | 1 row | arrival_datetime earlier than departure_datetime |
| NULL carrier | ~1% of rows | Missing carrier value |
| CBM over capacity | ~0.5% of rows | cbm exceeds container_count × 1 CBM |
| Wrong truck_id format | 1 row | "TRUCK-" prefix instead of "TRK_" |

### wms_shipment_details

| Issue | Rate | Description |
|---|---|---|
| NULL sort_code | ~1% of rows | Missing final delivery node |
| order_date after departure | ~0.5% of rows | Order date later than the truck departure date |
| Duplicate rows | ~0.3% of rows | Exact duplicate shipment leg rows |
| Wrong container_id format | ~0.3% of rows | Corrupted container ID prefix — "M-" or "B-" instead of "MTL_" or "BOX_" |

---

## Generation Parameters

The source data was generated using Python scripts in `scripts/generation/`. The following parameters governed the generation.

| Parameter | Value |
|---|---|
| Simulation period | July 1 – December 31 2025 (184 days) |
| Random seed | 42 — generation is fully reproducible |
| Origin nodes | 15 across 8 EU countries |
| Consolidation hubs | 6 across 8 EU countries |
| Delivery nodes | 30 across 8 EU countries |
| Active lanes | 205 |
| Average truck speed | 80 km/h (used for expected transit time) |
| Packages per CBM | 3–5 |
| Containers per truck | 1–32 (weighted: 15% light, 25% mid, 50% heavy, 10% peak) |
| Carriers | 5 — FastFreight EU, EuroHaul, TransCargo, QuickMove, DirectLog |
| Carriers per lane | 2–3 fixed at network build time |
| Lane volume profiles | 25% heavy, 50% medium, 25% thin |
| Direct lane SLA | 90% of trucks arrive before 10:00 |
| Hub departure window | 22:00–05:00 (hard constraint) |

---

## Reloading the Bronze Layer

To regenerate the source data and reload Bronze from scratch:

```bash
# Activate virtual environment
source venv/bin/activate

# Regenerate source data (run in order)
cd scripts/generation
python3 network.py
python3 financials.py
python3 transportation.py
python3 shipment_details.py    # ~37M rows, takes several minutes
cd ../..

# Reload Bronze layer
python3 scripts/load/load_bronze.py
```

The load script drops and recreates all five tables on every run. Existing data in `bronze_logistics` will be overwritten.
