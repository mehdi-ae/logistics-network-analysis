CREATE OR REPLACE TABLE silver_logistics.silver_fin_handling_costs AS
WITH 
casted AS (
  SELECT 
    UPPER(TRIM(node_id)) AS node_id, 
    ROW_NUMBER() OVER(PARTITION BY UPPER(TRIM(node_id))) AS row_num,
    SAFE_CAST(NULLIF(cost_per_package_eur, '') AS NUMERIC) AS cost_per_package_eur
  FROM `bronze_logistics.fin_handling_costs`
), 
flagged AS (
  SELECT 
    c.*,
    CASE 
      WHEN c.node_id IS NULL THEN 'null_in_source'
      WHEN smn.node_id IS NULL THEN 'referential_integrity_fail'
      WHEN smn.node_type != 'consolidation_hub' THEN 'invalid_node_type'
      WHEN COUNT(*) OVER(PARTITION BY c.node_id) > 1 THEN 'duplicate'
      ELSE 'ok'
    END AS node_integrity_quality_flag, 
    CASE 
      WHEN c.cost_per_package_eur IS NULL THEN 'null_in_source'
      WHEN c.cost_per_package_eur <= 0 THEN 'invalid_cost'
      WHEN c.cost_per_package_eur > 2.5 THEN 'out_of_range'
      WHEN c.cost_per_package_eur < 1 THEN 'out_of_range'
      ELSE 'ok'
    END AS cost_quality_flag
  FROM casted c 
  LEFT JOIN `silver_logistics.silver_mdm_network` smn
  ON c.node_id = smn.node_id
), 
final AS (
  SELECT 
    * EXCEPT(row_num), 
    CASE 
      WHEN node_integrity_quality_flag != 'ok' OR cost_quality_flag != 'ok' THEN 'critical'
      ELSE 'clean'
    END AS quality_record 
  FROM flagged 
  WHERE row_num = 1 
)

SELECT * 
FROM final 