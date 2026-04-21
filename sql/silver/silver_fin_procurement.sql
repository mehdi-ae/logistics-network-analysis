CREATE OR REPLACE TABLE `silver_logistics.silver_fin_procurement` AS
WITH
casted AS (
  SELECT 
    TRIM(UPPER(item)) AS item, 
    SAFE_CAST(cost_per_unit_eur AS FLOAT64) AS cost_per_unit_eur, 
    CASE 
      WHEN TRIM(LOWER(reusable)) IN ('yes', 'true', '1') THEN TRUE
      WHEN TRIM(LOWER(reusable)) IN ('no', 'false', '0') THEN FALSE
      ELSE NULL 
    END AS reusable, 
    description
  FROM `bronze_logistics.fin_procurement`
), 
flagged AS (
  SELECT  
    *, 
    CASE 
      WHEN item IS NULL THEN 'null_in_source'
      ELSE 'ok'
    END AS item_quality_flag, 
    CASE 
      WHEN cost_per_unit_eur IS NULL THEN 'null_in_source'
      WHEN cost_per_unit_eur <= 0 THEN 'invalid_cost'
      ELSE 'ok'
    END AS cost_per_unit_quality_flag, 
    CASE 
      WHEN reusable IS NULL THEN 'invalid_format'
      ELSE 'ok'
    END AS reusable_quality_flag 
  FROM casted 
), 
final AS (
  SELECT 
    *, 
    CASE 
      WHEN item_quality_flag != 'ok' OR cost_per_unit_quality_flag != 'ok' THEN 'critical'
      WHEN reusable_quality_flag != 'ok' THEN 'warning'
      ELSE 'clean'
    END AS quality_record 
  FROM flagged
)

SELECT * 
FROM final