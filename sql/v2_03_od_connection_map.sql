CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_od_connection_map AS
WITH origin_totals AS (
  SELECT
    p.location_id,
    p.zone AS origin_zone,
    p.borough AS pickup_borough,
    p.longitude AS origin_longitude,
    p.latitude AS origin_latitude,
    COUNT(*) AS origin_trip_count
  FROM main.nyc_taxi_operations.yellow_trips_2025 t
  JOIN main.nyc_taxi_operations.taxi_zones p
    ON t.PULocationID = p.location_id
  WHERE t.PULocationID <> t.DOLocationID
    AND p.borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island')
    AND p.location_id NOT IN (1, 132, 138)
  GROUP BY p.location_id, p.zone, p.borough, p.longitude, p.latitude
), ranked_origins AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY pickup_borough ORDER BY origin_trip_count DESC) AS borough_rank
  FROM origin_totals
), selected_origins AS (
  SELECT * FROM ranked_origins WHERE borough_rank <= 3 OR location_id = 161
), annual_pairs AS (
  SELECT
    o.location_id AS origin_id,
    o.origin_zone,
    o.pickup_borough,
    o.origin_longitude,
    o.origin_latitude,
    d.location_id AS destination_id,
    d.zone AS destination_zone,
    d.longitude AS destination_longitude,
    d.latitude AS destination_latitude,
    COUNT(*) AS annual_trip_count
  FROM main.nyc_taxi_operations.yellow_trips_2025 t
  JOIN selected_origins o ON t.PULocationID = o.location_id
  JOIN main.nyc_taxi_operations.taxi_zones d
    ON t.DOLocationID = d.location_id
  WHERE t.PULocationID <> t.DOLocationID
  GROUP BY
    o.location_id,
    o.origin_zone,
    o.pickup_borough,
    o.origin_longitude,
    o.origin_latitude,
    d.location_id,
    d.zone,
    d.longitude,
    d.latitude
), ranked_pairs AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY origin_id ORDER BY annual_trip_count DESC) AS destination_rank
  FROM annual_pairs
), candidate_pairs AS (
  SELECT * FROM ranked_pairs WHERE destination_rank <= 12
)
SELECT
  t.trip_date,
  t.pickup_hour,
  c.pickup_borough,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END AS airport_flag,
  c.origin_zone,
  c.destination_zone,
  c.origin_longitude,
  c.origin_latitude,
  c.destination_longitude,
  c.destination_latitude,
  COUNT(*) AS trip_count,
  SUM(CAST(t.duration_min AS DOUBLE)) AS duration_total
FROM main.nyc_taxi_operations.yellow_trips_2025 t
JOIN candidate_pairs c
  ON t.PULocationID = c.origin_id AND t.DOLocationID = c.destination_id
GROUP BY
  t.trip_date,
  t.pickup_hour,
  c.pickup_borough,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END,
  c.origin_zone,
  c.destination_zone,
  c.origin_longitude,
  c.origin_latitude,
  c.destination_longitude,
  c.destination_latitude;
