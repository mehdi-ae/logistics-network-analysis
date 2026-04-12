CREATE OR REPLACE TABLE silver_logistics.silver_mdm_network AS
WITH
casted AS (
  SELECT 
    node_id, 
    node_type, 
    CASE 
      WHEN LEFT(TRIM(UPPER(node_id)), 2) = 'ON' THEN 'origin_node'
      WHEN LEFT(TRIM(UPPER(node_id)), 2) = 'CH' THEN 'consolidation_hub'
      WHEN LEFT(TRIM(UPPER(node_id)), 2) = 'DN' THEN 'delivery_node'
      ELSE 'unknown_prefix'
    END AS expected_node_type,    
    city, 
    country, 
    SAFE_CAST(NULLIF(TRIM(latitude), '') AS NUMERIC) as latitude, 
    SAFE_CAST(NULLIF(TRIM(longitude), '') AS NUMERIC) as longitude,
    UPPER(TRIM(delivery_node_capability)) AS delivery_node_capability
  FROM `bronze_logistics.mdm_network`     
), 
corrected AS (
  SELECT 
    UPPER(TRIM(node_id)) AS node_id,
    CASE 
      WHEN REGEXP_CONTAINS(node_id, r'^(ON|CH|DN)_[A-Z]{2,4}$') THEN 'ok'
      ELSE 'unexpected_node_format'
    END AS node_check, 
    ROW_NUMBER() OVER(PARTITION BY node_id) AS row_num, 
    LOWER(TRIM(node_type)) AS node_type_raw,
    CASE
      WHEN LOWER(TRIM(node_type)) NOT IN ('origin_node', 'consolidation_hub', 'delivery_node') THEN expected_node_type
      ELSE LOWER(TRIM(node_type))
    END AS node_type, 
    NULLIF(TRIM(city), '') AS city,
    NULLIF(TRIM(country), '') AS country,
    latitude, 
    longitude, 
    delivery_node_capability
  FROM casted
),
flagged AS (
  SELECT 
    *, 
    CASE 
      WHEN node_id IS NULL THEN 'null_in_source'
      WHEN row_num > 1 THEN 'duplicate'
      WHEN node_check != 'ok' THEN 'unexpected_node_format' 
      ELSE 'ok'
    END AS node_quality_flag, 
    CASE 
      WHEN node_type = 'unknown_prefix' THEN 'invalid_value'
      WHEN node_type_raw IS NULL THEN 'null_in_source'
      WHEN node_type != node_type_raw THEN 'corrected'
      ELSE 'ok'
    END AS node_type_quality_flag,
    CASE 
      WHEN city IS NULL THEN 'null_in_source'
      ELSE 'ok'
    END AS city_quality_flag, 
    CASE
      WHEN country IS NULL THEN 'null_in_source'
      WHEN LENGTH(country) != 2 THEN 'not_iso_format'
      ELSE 'ok'
    END AS country_quality_flag, 
    CASE 
      WHEN latitude IS NULL OR longitude IS NULL THEN 'null_in_source'
      WHEN latitude < 35 OR latitude > 72 OR longitude < -10 OR longitude > 40 THEN 'invalid_value'
      ELSE 'ok'
    END AS coordinates_quality_flag, 
    CASE 
      WHEN delivery_node_capability IS NULL THEN 'null_in_source'
      WHEN delivery_node_capability NOT IN ('BOX', 'MTL', 'ALL') THEN 'unexpected_capability'
      ELSE 'ok'
    END AS capabilities_quality_flag
  FROM corrected 
), 
final AS (
  SELECT 
    * EXCEPT(node_check, row_num, node_type_raw), 
    CASE 
      WHEN node_quality_flag != 'ok' OR node_type_quality_flag NOT IN ('ok', 'corrected') OR coordinates_quality_flag != 'ok' THEN 'critical'
      WHEN city_quality_flag != 'ok' OR capabilities_quality_flag != 'ok' OR country_quality_flag != 'ok' THEN 'warning'
      ELSE 'clean'
    END AS quality_record
  FROM flagged 
  WHERE row_num = 1
)
SELECT *
FROM final 

