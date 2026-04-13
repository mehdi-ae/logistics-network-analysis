CREATE OR REPLACE TABLE silver_logistics.silver_tms_transportation AS 
WITH 
casted AS (
  SELECT 
    truck_id, 
    ROW_NUMBER() OVER(PARTITION BY truck_id) AS row_num,
    UPPER(TRIM(origin_node_id)) AS origin_node_id, 
    UPPER(TRIM(destination_node_id)) AS destination_node_id, 
    TRIM(carrier) AS carrier,
    departure_datetime AS departure_datetime_raw,
    CASE
     WHEN SAFE_CAST(departure_datetime AS DATETIME) IS NOT NULL 
     THEN SAFE_CAST(departure_datetime AS DATETIME)
     WHEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', departure_datetime) IS NOT NULL 
     THEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', departure_datetime)
    ELSE NULL
    END AS departure_datetime, 
    SAFE_CAST(NULLIF(TRIM(transit_time_hours), '') AS FLOAT64) AS transit_time_hours,
    arrival_datetime as arrival_datetime_raw,
    CASE
     WHEN SAFE_CAST(arrival_datetime AS DATETIME) IS NOT NULL 
     THEN SAFE_CAST(arrival_datetime AS DATETIME)
     WHEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', arrival_datetime) IS NOT NULL 
     THEN SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', arrival_datetime)
    ELSE NULL
    END AS arrival_datetime, 
    SAFE_CAST(NULLIF(container_count, '') AS INT64) AS container_count,
    SAFE_CAST(NULLIF(package_count, '') AS INT64) AS package_count, 
    SAFE_CAST(NULLIF(cbm, '') AS FLOAT64) AS cbm
  FROM `bronze_logistics.tms_transportation`
), 
origin_integrity AS (
  SELECT 
    c.*, 
    CASE 
      WHEN c.origin_node_id IS NULL THEN 'null_in_source'
      WHEN mdm.node_id IS NULL THEN 'referential_integrity_fail'
      WHEN mdm.node_type = 'delivery_node' THEN 'cant_be_an_origin'
      ELSE 'ok'
    END AS origin_integrity_quality_flag
  FROM casted c 
  LEFT JOIN `silver_logistics.silver_mdm_network` mdm
  ON c.origin_node_id = mdm.node_id
), 
destination_integrity AS(
  SELECT 
    oi.*, 
    CASE 
      WHEN oi.destination_node_id IS NULL THEN 'null_in_source'
      WHEN mdm.node_id IS NULL THEN 'referential_integrity_fail'
      WHEN mdm.node_type = 'origin_node' THEN 'cant_be_a_destination'
      ELSE 'ok'
    END AS destination_integrity_quality_flag
  FROM origin_integrity oi
  LEFT JOIN `silver_logistics.silver_mdm_network` mdm 
  ON oi.destination_node_id = mdm.node_id
),
carrier_integrity AS (
  SELECT
    *, 
    CASE 
      WHEN carrier IS NULL THEN 'null_in_source'
      WHEN carrier NOT IN (
        SELECT DISTINCT carrier 
        FROM `silver_logistics.silver_fin_transportation_rates`
        WHERE quality_record != 'critical') 
      THEN 'unknown_node'
      ELSE 'ok'
    END AS carrier_integrity_quality_flag
  FROM destination_integrity 

),

flagged AS (
  SELECT
    *, 
    CASE
      WHEN transit_time_hours IS NULL THEN 'null_in_source'
      WHEN transit_time_hours <= 0 THEN 'invalid_value'
      ELSE 'ok'
    END AS transit_time_quaity_flag,
    CASE 
      WHEN truck_id IS NULL THEN 'null_in_source'
      WHEN NOT REGEXP_CONTAINS(truck_id, r'^TRK_[0-9]{7}$') THEN 'invalid_format'
      WHEN COUNT(*) OVER(PARTITION BY truck_id) > 1 THEN 'duplicate'
      ELSE 'ok'
    END AS truck_id_quality_flag, 
    CASE 
      WHEN departure_datetime_raw IS NULL THEN 'null_in_source'
      WHEN departure_datetime IS NULL and departure_datetime_raw IS NOT NULL THEN 'unexpected_format'
      ELSE 'ok'
    END AS departure_datetime_quality_flag,
    CASE 
      WHEN arrival_datetime_raw IS NULL THEN 'null_in_source'
      WHEN arrival_datetime IS NULL and arrival_datetime_raw IS NOT NULL THEN 'unexpected_format'
      WHEN departure_datetime IS NULL THEN 'ok'
      WHEN DATETIME_DIFF(arrival_datetime, departure_datetime, minute) <= 0 THEN 'arrival_before_departure'
      ELSE 'ok'
    END AS arrival_datetime_quality_flag, 
    CASE 
      WHEN container_count IS NULL THEN 'null_in_source'
      WHEN container_count <= 0 THEN 'invalid_value'
      WHEN container_count > 32 THEN 'out_of_range'
      ELSE 'ok'
    END AS container_count_quality_flag, 
    CASE 
      WHEN package_count IS NULL THEN 'null_in_source'
      WHEN package_count < 0 THEN 'invalid_value'
      ELSE 'ok'
    END AS package_count_quality_flag, 
    CASE 
      WHEN cbm IS NULL THEN 'null_in_source'
      WHEN cbm < 0 THEN 'invalid_value'
      WHEN container_count IS NULL THEN 'ok'
      WHEN cbm > container_count THEN 'out_of_range'
      ELSE 'ok'
    END AS cbm_quality_flag
  FROM carrier_integrity 
), 
final AS (
  SELECT 
    *, 
    CASE 
      WHEN origin_integrity_quality_flag != 'ok' OR destination_integrity_quality_flag != 'ok' OR truck_id_quality_flag != 'ok' OR transit_time_quaity_flag != 'ok' OR container_count_quality_flag != 'ok' OR arrival_datetime_quality_flag != 'ok' OR departure_datetime_quality_flag != 'ok' THEN 'critical'
      WHEN carrier_integrity_quality_flag != 'ok' THEN 'warning'
      ELSE 'clean'
    END AS quality_record
  FROM flagged
  WHERE row_num = 1
)

SELECT * 
FROM final