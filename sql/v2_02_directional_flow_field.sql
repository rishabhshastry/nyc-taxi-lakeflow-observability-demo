CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_directional_flow_field AS
WITH zone_totals AS (
  SELECT
    p.location_id,
    p.zone AS pickup_zone,
    p.borough AS pickup_borough,
    p.longitude,
    p.latitude,
    COUNT(*) AS annual_trip_count
  FROM main.nyc_taxi_operations.yellow_trips_2025 t
  JOIN main.nyc_taxi_operations.taxi_zones p
    ON t.PULocationID = p.location_id
  WHERE t.PULocationID <> t.DOLocationID
    AND p.borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'EWR')
  GROUP BY p.location_id, p.zone, p.borough, p.longitude, p.latitude
), ranked_zones AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY pickup_borough ORDER BY annual_trip_count DESC) AS borough_rank
  FROM zone_totals
), selected_zones AS (
  SELECT * FROM ranked_zones WHERE borough_rank <= 20
)
SELECT
  t.trip_date,
  t.pickup_hour,
  z.pickup_zone,
  z.pickup_borough,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END AS airport_flag,
  z.longitude,
  z.latitude,
  COUNT(*) AS trip_count,
  SUM(t.trip_distance) AS distance_total,
  SUM(d.longitude - z.longitude) AS dx_total,
  SUM(d.latitude - z.latitude) AS dy_total
FROM main.nyc_taxi_operations.yellow_trips_2025 t
JOIN selected_zones z ON t.PULocationID = z.location_id
JOIN main.nyc_taxi_operations.taxi_zones d
  ON t.DOLocationID = d.location_id
WHERE t.PULocationID <> t.DOLocationID
GROUP BY
  t.trip_date,
  t.pickup_hour,
  z.pickup_zone,
  z.pickup_borough,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END,
  z.longitude,
  z.latitude;
