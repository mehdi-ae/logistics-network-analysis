CREATE OR REPLACE TABLE silver_logistics.silver_wms_shipment_details AS 
WITH 
casted AS (
  SELECT 
    TRIM(shipment_id) AS shipment_id, 
    ROW_NUMBER() OVER(PARTITION BY shipment_id, truck_id) AS row_num,
    TRIM(truck_id) AS truck_id, 
    TRIM(container_id) AS container_id, 
    CASE
      WHEN SAFE_CAST(order_date AS DATE) IS NOT NULL 
        THEN SAFE_CAST(order_date AS DATE)
      WHEN SAFE.PARSE_DATE('%d/%m/%Y', order_date) IS NOT NULL 
        THEN SAFE.PARSE_DATE('%d/%m/%Y', order_date)
      ELSE NULL
    END AS order_date, 
    TRIM(origin_node_id) AS origin_node_id,
    TRIM(destination_node_id) AS destination_node_id, 
    TRIM(sort_code) AS sort_code
  FROM `bronze_logistics.wms_shipment_details`
), 
integrity AS (
  SELECT 
    c.*, 
    CASE 
      WHEN c.order_date IS NULL THEN 'null_in_source'
      WHEN tms.departure_datetime IS NULL THEN 'ok'
      WHEN c.order_date > CAST(tms.departure_datetime AS DATE) THEN 'date_integrity_fail'
      ELSE 'ok'
    END AS order_date_quality_flag,
    CASE 
      WHEN c.truck_id IS NULL THEN 'null_in_source'
      WHEN tms.truck_id IS NULL THEN 'truck_integrity_fail'
      ELSE 'ok'
    END AS truck_integrity_quality_flag,
    CASE 
      WHEN c.origin_node_id IS NULL THEN 'null_in_source'
      WHEN tms.origin_node_id IS NULL THEN 'ok'
      WHEN c.origin_node_id != tms.origin_node_id THEN 'origin_integrity_fail'
      ELSE 'ok'
    END AS origin_integrity_quality_flag, 
    CASE 
      WHEN c.destination_node_id IS NULL THEN 'null_in_source'
      WHEN tms.destination_node_id IS NULL THEN 'ok'
      WHEN c.destination_node_id != tms.destination_node_id THEN 'destination_integrity_fail'
      ELSE 'ok'
    END AS destination_integrity_quality_flag, 
    CASE
      WHEN c.container_id IS NULL THEN 'null_in_source'
      WHEN LEFT(c.container_id, 3) NOT IN ('MTL', 'BOX') THEN 'invalid_container'
      ELSE 'ok'
    END AS container_integrity_quality_flag,
    CASE 
      WHEN c.shipment_id IS NULL THEN 'null_in_source'
      WHEN COUNT(*) OVER(PARTITION BY c.shipment_id, c.truck_id) > 1 
        THEN 'duplicate_shipment_in_truck'
      WHEN NOT REGEXP_CONTAINS(c.shipment_id, r'^SHP_[0-9]{9}$') THEN 'invalid_format'
      ELSE 'ok'
    END AS shipment_integrity_quality_flag
  FROM casted c
  LEFT JOIN `silver_logistics.silver_tms_transportation` tms
  ON c.truck_id = tms.truck_id
),
sort_code_integrity AS (
  SELECT 
    i.*, 
    CASE 
      WHEN i.sort_code IS NULL THEN 'null_in_source'
      WHEN mdm.node_id IS NULL THEN 'sort_code_integrity_fail'
      WHEN mdm.node_type != 'delivery_node' THEN 'cant_be_a_sort_code'
      ELSE 'ok'
    END AS sort_code_quality_flag
  FROM integrity i 
  LEFT JOIN silver_logistics.silver_mdm_network mdm
  ON i.sort_code = mdm.node_id
), 
deduped AS (
  SELECT *
  FROM sort_code_integrity
  WHERE row_num = 1
), 
coherence AS (
  SELECT 
    d.*,
    -- Container count coherence removed: generation script does not guarantee
    -- every container on a truck has at least one shipment in wms_shipment_details.
    -- This check belongs in a Gold reconciliation model after the generation
    -- script is fixed to ensure package_count >= container_count.
    -- See decision log ADR-014.
    CASE
      WHEN container_integrity_quality_flag = 'invalid_container' THEN 'invalid_container'
      WHEN container_integrity_quality_flag = 'null_in_source' THEN 'null_in_source'
      ELSE 'ok'
    END AS container_coherence_flag,
    CASE
      WHEN shipment_integrity_quality_flag IN (
        'null_in_source', 
        'duplicate_shipment_in_truck', 
        'invalid_format'
      ) THEN shipment_integrity_quality_flag
      WHEN tms.package_count IS NULL THEN 'ok'
      WHEN COUNT(d.shipment_id) OVER(PARTITION BY d.truck_id) != tms.package_count 
        THEN 'incoherent_package_count'
      ELSE 'ok'
    END AS shipment_coherence_flag
  FROM deduped d
  LEFT JOIN `silver_logistics.silver_tms_transportation` tms
  ON d.truck_id = tms.truck_id
), 
final AS (
  SELECT 
    *, 
    CASE 
      WHEN order_date_quality_flag != 'ok'
        OR truck_integrity_quality_flag != 'ok'
        OR origin_integrity_quality_flag != 'ok'
        OR destination_integrity_quality_flag != 'ok'
        OR container_coherence_flag != 'ok'
        OR shipment_coherence_flag != 'ok'
        OR sort_code_quality_flag != 'ok'
      THEN 'critical'
      ELSE 'clean'
    END AS quality_record
  FROM coherence
)
SELECT * 
FROM final