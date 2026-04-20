CREATE OR REPLACE TABLE gold_logistics.gold_fact_lane_simulation AS
WITH 
triplets_packages AS (
  SELECT 
    DATE_TRUNC(CAST(tms.departure_datetime AS DATE), MONTH) AS departure_month,
    wms.truck_id,
    wms.origin_node_id, 
    wms.destination_node_id,
    wms.sort_code, 
    CASE 
      WHEN wms.destination_node_id = wms.sort_code AND wms.origin_node_id LIKE 'ON_%' THEN 'direct'
      ELSE 'indirect'
    END AS triplet_type,
    tms.cbm AS truck_cbm,
    tms.package_count AS truck_packages,
    COUNT(wms.shipment_id) AS triplet_shipment_count,
    CAST(COUNT(wms.shipment_id) AS FLOAT64) / tms.package_count * tms.cbm AS triplet_cbm
  FROM `silver_logistics.silver_wms_shipment_details` wms 
  JOIN `silver_logistics.silver_tms_transportation` tms
  ON wms.truck_id = tms.truck_id
  WHERE wms.quality_record != 'critical'
  AND tms.quality_record != 'critical'
  GROUP BY 1,2,3,4,5,6,7,8
), 
triplets_volume AS (
  SELECT 
    departure_month, 
    origin_node_id, 
    destination_node_id, 
    sort_code, 
    triplet_type,
    SUM(triplet_shipment_count) AS monthly_triplet_packages, 
    SUM(triplet_cbm) AS monthly_triplet_cbm
  FROM triplets_packages tp
  GROUP BY 1,2,3,4,5
), 
hub_dn_totals AS (
  SELECT
    destination_node_id AS hub_node_id,
    sort_code,
    departure_month,
    SUM(monthly_triplet_packages) AS total_hub_dn_packages,
    SUM(monthly_triplet_cbm) AS total_hub_dn_cbm
  FROM triplets_volume
  WHERE triplet_type = 'indirect'
  GROUP BY 1, 2, 3
),
triplets_volume_adjusted AS (
  SELECT 
    tv.*,
    CASE 
      WHEN triplet_type = 'indirect' THEN (tv.monthly_triplet_packages / hdt.total_hub_dn_packages) * hdt.total_hub_dn_cbm
      ELSE monthly_triplet_cbm
    END AS adjusted_triplet_cbm
  FROM triplets_volume tv
  LEFT JOIN hub_dn_totals hdt
  ON tv.destination_node_id = hdt.hub_node_id
  AND tv.sort_code = hdt.sort_code
  AND tv.departure_month = hdt.departure_month
),
distances AS(
  SELECT 
    orig.node_id AS origin_node, 
    orig.country AS origin_node_country,
    orig.node_type AS origin_node_type,
    desti.node_id AS destination_node, 
    desti.country AS destination_node_country,
    desti.node_type AS destination_node_type, 
    ROUND(
      6371 * 2 * ASIN(SQRT(
        POW(SIN((desti.latitude - orig.latitude) * ACOS(-1) / 180 /2), 2)
        + COS(orig.latitude * ACOS(-1) / 180)
        * COS(desti.latitude * ACOS(-1) / 180)
        * POW(SIN((desti.longitude - orig.longitude) * ACOS(-1) /180 / 2), 2)
      )),
    2) AS distance_km
  FROM `silver_logistics.silver_mdm_network`  orig
  CROSS JOIN `silver_logistics.silver_mdm_network` desti
  WHERE (
    (orig.node_type = 'origin_node' AND desti.node_type = 'consolidation_hub')
    OR (orig.node_type = 'origin_node' AND desti.node_type = 'delivery_node')
    OR (orig.node_type = 'consolidation_hub' AND desti.node_type = 'delivery_node')
  )
  AND orig.quality_record != 'critical'
  AND desti.quality_record != 'critical'
), 
avg_carrier_cost AS (
  SELECT 
      country, 
      AVG(fixed_cost_eur) AS avg_fixed_cost_eur, 
      AVG(cost_per_km_eur) AS avg_cost_per_km_eur
  FROM  `silver_logistics.silver_fin_transportation_rates`
  WHERE quality_record != 'critical'
  GROUP BY country
), 
triplets_distance_one AS (
  SELECT 
    tv.departure_month,
    tv.origin_node_id, 
    d.origin_node_type,
    d.origin_node_country,
    tv.destination_node_id, 
    d.destination_node_type,
    d.destination_node_country,
    d.distance_km AS distance1_km,
    d2.distance_km AS direct_distance_km,
    tv.sort_code, 
    tv.triplet_type,
    tv.adjusted_triplet_cbm,
    tv.monthly_triplet_packages, 
    tv.monthly_triplet_cbm
  FROM triplets_volume_adjusted tv
  JOIN distances d
  ON tv.origin_node_id = d.origin_node
  AND tv.destination_node_id = d.destination_node
  LEFT JOIN  distances d2
  ON tv.origin_node_id = d2.origin_node
  AND tv.sort_code = d2.destination_node
  WHERE d.origin_node_type = 'origin_node'
), 
triplets_distance_two AS (
  SELECT 
    tdo.departure_month, 
    tdo.origin_node_id, 
    tdo.origin_node_type,
    tdo.origin_node_country,
    tdo.destination_node_id, 
    tdo.destination_node_type,
    tdo.destination_node_country,
    tdo.distance1_km, 
    tdo.sort_code, 
    d.destination_node_country AS sort_code_country,
    tdo.triplet_type,
    d.distance_km AS distance2_km, 
    tdo.adjusted_triplet_cbm,
    tdo.direct_distance_km,
    tdo.monthly_triplet_packages, 
    tdo.monthly_triplet_cbm
  FROM triplets_distance_one tdo
  LEFT JOIN distances d 
  ON tdo.destination_node_id = d.origin_node
  AND tdo.sort_code = d.destination_node
), 
triplets_distance1_costs AS (
  SELECT 
    tdt.*, 
    acc.avg_fixed_cost_eur AS leg1_avg_fixed_cost_eur, 
    acc.avg_cost_per_km_eur AS leg1_avg_cost_per_km_eur
  FROM triplets_distance_two tdt
  LEFT JOIN avg_carrier_cost acc
  ON tdt.origin_node_country = acc.country
), 
triplets_distances_carrier_costs AS (
  SELECT 
    tdc.*, 
    acc.avg_fixed_cost_eur AS leg2_avg_fixed_cost_eur, 
    acc.avg_cost_per_km_eur AS leg2_avg_cost_per_km_eur
  FROM triplets_distance1_costs tdc
  LEFT JOIN avg_carrier_cost acc
  ON tdc.destination_node_country = acc.country
), 
monthly_lane_costs AS (
  SELECT 
    origin_node_id,
    destination_node_id, 
    DATE_TRUNC(date, MONTH) AS departure_month,
    SUM(daily_linehaul_cost_eur) AS monthly_linehaul_cost_eur, 
    SUM(daily_cbm) AS monthly_cbm
  FROM `gold_logistics.gold_fact_lane_daily`
  GROUP BY 1, 2, 3 
),
costs AS (
  SELECT 
    dcc.*, 
    mlc.monthly_linehaul_cost_eur AS current_monthly_costs
  FROM triplets_distances_carrier_costs dcc
  JOIN monthly_lane_costs mlc
  ON dcc.origin_node_id = mlc.origin_node_id
  AND dcc.destination_node_id = mlc.destination_node_id
  AND dcc.departure_month = mlc.departure_month
), 
total_costs AS (
  SELECT 
    c.*, 
    CASE 
      WHEN c.triplet_type = 'indirect' 
      THEN fhc.cost_per_package_eur * c.monthly_triplet_packages 
      ELSE 0 
    END AS monthly_processing_costs
  FROM costs c
  LEFT JOIN `silver_logistics.silver_fin_handling_costs` fhc
  ON c.destination_node_id = fhc.node_id 
  AND fhc.quality_record != 'critical'
), 
categorization AS (
  SELECT 
    departure_month, 
    origin_node_id, 
    origin_node_country,
    destination_node_id, 
    destination_node_country, 
    distance1_km, 
    distance2_km,
    direct_distance_km,
    sort_code, 
    triplet_type, 
    adjusted_triplet_cbm,
    monthly_triplet_packages, 
    (monthly_triplet_packages / SUM(monthly_triplet_packages) OVER (PARTITION BY origin_node_id, destination_node_id, departure_month)) * current_monthly_costs AS current_monthly_cost_ratio,
    monthly_triplet_cbm, 
    (monthly_triplet_packages / SUM(monthly_triplet_packages) OVER (PARTITION BY origin_node_id, destination_node_id, departure_month)) * monthly_processing_costs AS processing_cost_ratio,
    leg1_avg_fixed_cost_eur, 
    leg1_avg_cost_per_km_eur, 
    leg2_avg_fixed_cost_eur, 
    leg2_avg_cost_per_km_eur,
    current_monthly_costs, 
    monthly_processing_costs,
    CASE 
      WHEN triplet_type = 'indirect' AND adjusted_triplet_cbm > 322 THEN 'thick'
      WHEN triplet_type = 'direct' AND adjusted_triplet_cbm < 138 THEN 'thin'
      ELSE 'healthy'
    END AS category
  FROM total_costs 
), 
candidates_ranking AS (
  SELECT 
    origin_node_id, 
    distance1_km, 
    destination_node_id, 
    distance2_km, 
    sort_code, 
    ROW_NUMBER() OVER(PARTITION BY origin_node_id, sort_code ORDER BY distance1_km ASC) AS row_num
  FROM triplets_distance_two
  WHERE origin_node_type = 'origin_node'
  AND destination_node_type = 'consolidation_hub'
), 
recommendation AS (
  SELECT 
    c.*, 
    CASE 
      WHEN c.category = 'thin' THEN cr.destination_node_id
      ELSE 'not applicable'
    END AS recommended_hub
  FROM categorization c
  LEFT JOIN candidates_ranking cr
  ON c.origin_node_id = cr.origin_node_id
  AND c.sort_code = cr.sort_code
  AND cr.row_num = 1
), 
new_costs  AS (
  SELECT  
    r.*, 
    CASE 
      WHEN r.category  = 'thin' AND COALESCE(r.recommended_hub, 'not applicable') != 'not applicable' THEN TRUE
      WHEN r.category = 'thin' THEN FALSE
      ELSE NULL 
    END AS deprecation_feasible, 
    CASE 
      WHEN r.category = 'thin' THEN (r.leg1_avg_fixed_cost_eur + r.leg1_avg_cost_per_km_eur * distance1_km) * (r.adjusted_triplet_cbm / 28)
        + (r.leg2_avg_fixed_cost_eur + r.leg2_avg_cost_per_km_eur * d.distance_km) * (r.adjusted_triplet_cbm / 28)
        + r.processing_cost_ratio
      WHEN r.category = 'thick' THEN (r.leg1_avg_fixed_cost_eur + r.leg1_avg_cost_per_km_eur * r.direct_distance_km) * (r.adjusted_triplet_cbm / 28)
      ELSE NULL
    END AS cost_of_recommended_design
  FROM recommendation r
  LEFT JOIN distances d
  ON r.recommended_hub = d.origin_node
  AND r.sort_code = d.destination_node
), 
final AS (
  SELECT 
    *, 
    current_monthly_cost_ratio - cost_of_recommended_design AS cost_reduction 
  FROM new_costs
)

SELECT *
FROM final  