CREATE OR REPLACE TABLE gold_logistics.gold_fact_lane_daily
AS
WITH 
date_spine AS (
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-07-01', '2025-12-31', INTERVAL 1 DAY)) AS date
),
 
origins_details AS (
  SELECT
    tms.truck_id, 
    tms.origin_node_id,
    tms.destination_node_id, 
    COALESCE(tms.carrier, 'UNKNOWN') AS carrier, 
    mdm.country                      AS origin_node_country,
    mdm.node_type                    AS origin_node_type,
    mdm.latitude                     AS origin_node_latitude, 
    mdm.longitude                    AS origin_node_longitude, 
    tms.departure_datetime,
    tms.package_count, 
    tms.cbm
  FROM `silver_logistics.silver_tms_transportation` tms
  JOIN `silver_logistics.silver_mdm_network` mdm
    ON tms.origin_node_id = mdm.node_id  
   AND mdm.quality_record != 'critical'
  WHERE tms.quality_record != 'critical'
), 
 
destination_details AS (
  SELECT 
    od.*,
    mdm.country   AS destination_node_country, 
    mdm.node_type AS destination_node_type, 
    mdm.latitude  AS destination_node_latitude,
    mdm.longitude AS destination_node_longitude
  FROM origins_details od
  JOIN `silver_logistics.silver_mdm_network` mdm
    ON od.destination_node_id = mdm.node_id
   AND mdm.quality_record != 'critical'
), 
 
-- Unique lane-carrier combinations for known carriers only
-- Used for the CROSS JOIN with the date spine
lane_carrier_combinations AS (
  SELECT DISTINCT
    origin_node_id,
    destination_node_id,
    carrier,
    origin_node_type,
    destination_node_type,
    origin_node_country,
    destination_node_country,
    ROUND(
      6371 * 2 * ASIN(SQRT(
        POW(SIN((destination_node_latitude - origin_node_latitude) * ACOS(-1) / 180 / 2), 2)
        + COS(origin_node_latitude  * ACOS(-1) / 180)
        * COS(destination_node_latitude * ACOS(-1) / 180)
        * POW(SIN((destination_node_longitude - origin_node_longitude) * ACOS(-1) / 180 / 2), 2)
      )),
    2) AS distance_km
  FROM destination_details
  WHERE carrier != 'UNKNOWN'
),
 
-- Unique lane attributes (no carrier) for joining to UNKNOWN rows
lane_attributes AS (
  SELECT DISTINCT
    origin_node_id,
    destination_node_id,
    origin_node_type,
    destination_node_type,
    origin_node_country,
    destination_node_country,
    ROUND(
      6371 * 2 * ASIN(SQRT(
        POW(SIN((destination_node_latitude - origin_node_latitude) * ACOS(-1) / 180 / 2), 2)
        + COS(origin_node_latitude  * ACOS(-1) / 180)
        * COS(destination_node_latitude * ACOS(-1) / 180)
        * POW(SIN((destination_node_longitude - origin_node_longitude) * ACOS(-1) / 180 / 2), 2)
      )),
    2) AS distance_km
  FROM destination_details
),
 
-- Daily aggregated actuals per lane-carrier-date
daily_actuals AS (
  SELECT
    DATE(departure_datetime) AS departure_date,
    origin_node_id,
    destination_node_id,
    carrier,
    COUNT(truck_id)    AS daily_trucks,
    SUM(package_count) AS daily_packages,
    SUM(cbm)           AS daily_cbm
  FROM destination_details
  GROUP BY 1, 2, 3, 4
),
 
-- Country-level average rates for UNKNOWN carrier cost imputation
avg_rates AS (
  SELECT 
    country,
    AVG(fixed_cost_eur)    AS avg_fixed_cost,
    AVG(cost_per_km_eur)   AS avg_cost_per_km
  FROM `silver_logistics.silver_fin_transportation_rates`
  WHERE quality_record != 'critical'
  GROUP BY country
),
 
-- Date spine CROSS JOIN with known carriers + UNION UNKNOWN real runs
date_spine_lanes AS (
 
  -- Known carriers: full date spine with gap filling
  SELECT
    ds.date,
    lcc.origin_node_id,
    lcc.destination_node_id,
    lcc.carrier,
    lcc.origin_node_type,
    lcc.destination_node_type,
    lcc.origin_node_country,
    lcc.destination_node_country,
    lcc.distance_km,
    COALESCE(da.daily_trucks,   0) AS daily_trucks,
    COALESCE(da.daily_packages, 0) AS daily_packages,
    COALESCE(da.daily_cbm,      0) AS daily_cbm
  FROM date_spine ds
  CROSS JOIN lane_carrier_combinations lcc
  LEFT JOIN daily_actuals da
    ON  ds.date                   = da.departure_date
    AND lcc.origin_node_id        = da.origin_node_id
    AND lcc.destination_node_id   = da.destination_node_id
    AND lcc.carrier               = da.carrier
 
  UNION ALL
 
  -- UNKNOWN carrier: real run days only, no gap filling
  SELECT
    da.departure_date            AS date,
    da.origin_node_id,
    da.destination_node_id,
    da.carrier,
    la.origin_node_type,
    la.destination_node_type,
    la.origin_node_country,
    la.destination_node_country,
    la.distance_km,
    da.daily_trucks,
    da.daily_packages,
    da.daily_cbm
  FROM daily_actuals da
  LEFT JOIN lane_attributes la
    ON da.origin_node_id        = la.origin_node_id
   AND da.destination_node_id   = la.destination_node_id
  WHERE da.carrier = 'UNKNOWN'
    AND da.departure_date BETWEEN '2025-07-01' AND '2025-12-31'
),
 
-- Linehaul cost calculation
linehaul_costs AS (
  SELECT
    dsl.*,
    CASE
      WHEN dsl.carrier = 'UNKNOWN' THEN
        COALESCE(
          (ar.avg_fixed_cost + dsl.distance_km * ar.avg_cost_per_km) * dsl.daily_trucks,
          0)
      ELSE
        COALESCE(
          (tr.fixed_cost_eur + dsl.distance_km * tr.cost_per_km_eur) * dsl.daily_trucks,
          0)
    END AS daily_linehaul_cost_eur,
    CASE WHEN dsl.carrier = 'UNKNOWN' THEN TRUE ELSE FALSE END AS cost_is_imputed
  FROM date_spine_lanes dsl
  LEFT JOIN `silver_logistics.silver_fin_transportation_rates` tr
    ON  dsl.origin_node_country = tr.country
    AND dsl.carrier             = tr.carrier
    AND tr.quality_record       != 'critical'
  LEFT JOIN avg_rates ar
    ON dsl.origin_node_country  = ar.country
),
 
-- Rolling averages
final AS (
  SELECT 
    date, 
    origin_node_country,
    origin_node_id, 
    origin_node_type, 
    destination_node_country, 
    destination_node_id,
    destination_node_type, 
    carrier, 
    distance_km,
    daily_trucks, 
    daily_packages, 
    daily_cbm, 
    ROUND(daily_linehaul_cost_eur, 2)                                                AS daily_linehaul_cost_eur,
    cost_is_imputed,
    ROUND(AVG(daily_cbm) OVER(
      PARTITION BY origin_node_id, destination_node_id, carrier 
      ORDER BY date 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2)                                AS rolling_avg_cbm_30d, 
    ROUND(AVG(daily_linehaul_cost_eur) OVER(
      PARTITION BY origin_node_id, destination_node_id, carrier 
      ORDER BY date 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2)                                AS rolling_avg_cost_30d
  FROM linehaul_costs
  WHERE carrier IS NOT NULL
)
 
SELECT *
FROM final