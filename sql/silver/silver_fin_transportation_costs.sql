CREATE OR REPLACE TABLE silver_logistics.silver_fin_transportation_rates AS
WITH 
casted AS (
  SELECT
    UPPER(TRIM(country)) AS country,
    carrier AS carrier_raw, 
    TRIM(carrier) AS carrier,
    ROW_NUMBER() OVER(PARTITION BY UPPER(TRIM(country)), TRIM(carrier)) as row_num,
    SAFE_CAST(NULLIF(fixed_cost_eur, '') AS FLOAT64) AS  fixed_cost_eur,
    SAFE_CAST(NULLIF(cost_per_km_eur, '') AS FLOAT64) AS  cost_per_km_eur
  FROM `bronze_logistics.fin_transport_rates`
), 
flagged AS (
  SELECT 
    *,
    CASE 
      WHEN COUNT(*) OVER(PARTITION BY country, carrier) > 1 THEN 'duplicate'
      ELSE 'ok'
    END AS duplicates_flag,
    CASE 
      WHEN country IS NULL THEN 'null_in_source'
      WHEN LENGTH(country) != 2 THEN 'invalid_format'
      WHEN country NOT IN ('FR','DE','ES','IT','NL','BE','PL','CZ') THEN 'invalid_country'
      ELSE 'ok'
    END AS country_quality_flag, 
    CASE 
      WHEN carrier_raw IS NULL THEN 'null_in_source'
      WHEN carrier!= carrier_raw THEN 'corrected'
      ELSE 'ok'
    END AS carrier_quality_flag, 
    CASE 
      WHEN fixed_cost_eur IS NULL THEN 'null_in_source'
      WHEN fixed_cost_eur <= 0 THEN 'invalid_value'
      WHEN fixed_cost_eur > 500 THEN 'out_of_range'
      WHEN fixed_cost_eur < 200 THEN 'out_of_range'
      ELSE 'ok'
    END AS fixed_cost_quality_flag, 
    CASE 
      WHEN cost_per_km_eur IS NULL THEN 'null_in_source'
      WHEN cost_per_km_eur <= 0 THEN 'invalid_value'
      WHEN cost_per_km_eur > 5 THEN 'out_of_range'
      WHEN cost_per_km_eur < 0.5 THEN 'out_of_range'
      ELSE 'ok'
    END AS cost_per_km_quality_flag
  FROM casted 
), 
final AS (
  SELECT 
    * EXCEPT(carrier_raw, row_num), 
    CASE 
      WHEN country_quality_flag != 'ok' OR carrier_quality_flag != 'ok'OR fixed_cost_quality_flag != 'ok' or cost_per_km_quality_flag != 'ok'  OR duplicates_flag != 'ok' THEN 'critical'
      ELSE 'clean'
    END AS quality_record
  FROM flagged
  WHERE row_num = 1
)

SELECT * 
FROM final