CREATE OR REPLACE TABLE `gold_logistics.gold_fact_container_compliance` AS
WITH 
containers AS (
  SELECT 
    DATE(tms.departure_datetime) AS departure_date, 
    wms.origin_node_id, 
    wms.destination_node_id, 
    mdm.node_capability AS node_capability, 
    COUNT(DISTINCT CASE WHEN LEFT(wms.container_id, 3)= 'MTL' THEN wms.container_id END) AS mtl_container_count,    
    COUNT(DISTINCT CASE WHEN LEFT(wms.container_id, 3)= 'BOX' THEN wms.container_id END) AS box_container_count
  FROM `silver_logistics.silver_wms_shipment_details` wms 
  LEFT JOIN `silver_logistics.silver_tms_transportation` tms
  ON wms.truck_id = tms.truck_id
  AND wms.origin_node_id = tms.origin_node_id
  AND wms.destination_node_id = tms.destination_node_id
  LEFT JOIN `silver_logistics.silver_mdm_network` mdm 
  ON wms.destination_node_id = mdm.node_id
  AND mdm.quality_record != 'critical'
  WHERE 
      wms.quality_record != 'critical'
      AND tms.quality_record != 'critical'
  GROUP BY 1,2,3,4
), 
compliance AS (
  SELECT 
    c.*, 
    CASE 
      WHEN c.node_capability = 'MTL' AND c.box_container_count > 0 THEN FALSE
      WHEN c.node_capability = 'BOX' AND c.mtl_container_count > 0  THEN FALSE 
      ELSE TRUE
    END AS is_compliant, 
    CASE 
      WHEN c.node_capability = 'MTL' THEN ROUND(c.mtl_container_count/NULLIF(c.mtl_container_count + c.box_container_count, 0) * 100, 2)
      WHEN c.node_capability = 'BOX' THEN ROUND(c.box_container_count/NULLIF(c.mtl_container_count + c.box_container_count, 0) * 100, 2)
      ELSE NULL 
    END AS compliance_percentage, 
    CASE 
      WHEN c.node_capability = 'MTL' THEN ROUND(c.box_container_count * proc.cost_per_unit_eur,2)
      WHEN c.node_capability = 'BOX' THEN 0
      ELSE NULL
    END AS avoidable_cost
  FROM containers c
  LEFT JOIN `silver_logistics.silver_fin_procurement` proc
  ON proc.item = 'BOX_CONTAINER'
  AND proc.quality_record != 'critical'
)
SELECT * 
FROM compliance