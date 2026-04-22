CREATE OR REPLACE TABLE `gold_logistics.gold_fact_transit_time`AS 
SELECT 
  DATE(tms.departure_datetime) AS departure_date, 
  EXTRACT(HOUR FROM tms.departure_datetime) AS departure_hour,
  tms.origin_node_id, 
  tms.destination_node_id, 
  tms.carrier, 
  tms.transit_time_hours, 
  ctt.configured_transit_time_hours
FROM `silver_logistics.silver_tms_transportation` tms 
JOIN `silver_logistics.silver_mdm_configured_transit_times` ctt
ON tms.origin_node_id = ctt.origin_node_id
AND tms.destination_node_id = ctt.destination_node_id
WHERE 
  tms.quality_record != 'critical'
  AND ctt.quality_record != 'critical'
