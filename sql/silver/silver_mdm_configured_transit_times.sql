CREATE OR REPLACE TABLE `silver_logistics.silver_mdm_configured_transit_times` AS
WITH 
casted AS (
  SELECT 
    origin_node_id, 
    destination_node_id, 
    SAFE_CAST(configured_transit_time_hours AS FLOAT64) AS configured_transit_time_hours, 
    configuration_type
  FROM `bronze_logistics.mdm_configured_transit_times`
), 
origin_integrity_check AS (
  SELECT 
    c.origin_node_id,
    c.destination_node_id,
    c.configured_transit_time_hours,
    c.configuration_type,
    CASE 
      WHEN c.origin_node_id IS NULL THEN 'null_in_origin'
      WHEN mdmn.node_id IS NULL THEN 'unknown_node'
      WHEN  mdmn.node_type NOT IN ('origin_node', 'consolidation_hub') THEN 'cant_be_an_origin'
      ELSE 'ok'
    END AS origin_node_integrity_quality_flag,  
    CASE 
      WHEN c.configured_transit_time_hours IS NULL THEN 'null_in_source'
      WHEN c.configured_transit_time_hours <= 0 THEN 'invalid_value'
      ELSE 'ok'
    END AS transit_time_quality_flag, 
  FROM casted c
  LEFT JOIN `silver_logistics.silver_mdm_network` mdmn
  ON c.origin_node_id = mdmn.node_id
), 
destination_integrity_check AS (
  SELECT 
    oic.*, 
    CASE
      WHEN oic.destination_node_id IS NULL THEN 'null_in_source'
      WHEN mdmn.node_id IS NULL THEN 'unknown_node'
      WHEN mdmn.node_type NOT IN ('consolidation_hub', 'delivery_node') THEN 'cant_be_a_destination'
      ELSE 'ok'
    END AS destination_node_integrity_quality_flag
  FROM origin_integrity_check oic 
  LEFT JOIN `silver_logistics.silver_mdm_network` mdmn
  ON oic.destination_node_id = mdmn.node_id 
), 
final AS (
  SELECT 
    *, 
    CASE 
      WHEN origin_node_integrity_quality_flag != 'ok'
      OR destination_node_integrity_quality_flag != 'ok'
      OR transit_time_quality_flag != 'ok'
      THEN 'critical'
      ELSE 'clean'
    END AS quality_record
  FROM destination_integrity_check
)
SELECT * 
FROM final
