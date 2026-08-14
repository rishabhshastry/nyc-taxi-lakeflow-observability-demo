CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_airport_quantile_fan AS
SELECT
  t.trip_date,
  t.pickup_hour,
  p.borough AS pickup_borough,
  CASE t.DOLocationID
    WHEN 132 THEN 'JFK'
    WHEN 138 THEN 'LGA'
    WHEN 1 THEN 'EWR'
  END AS airport,
  'Airport' AS airport_flag,
  CAST(floor(CAST(t.duration_min AS DOUBLE) / 2.0) * 2 AS INT) AS duration_bin_minutes,
  COUNT(*) AS bin_trip_count
FROM main.nyc_taxi_operations.yellow_trips_2025 t
JOIN main.nyc_taxi_operations.taxi_zones p
  ON t.PULocationID = p.location_id
WHERE t.pickup_day_of_week BETWEEN 2 AND 6
  AND t.DOLocationID IN (1, 132, 138)
  AND t.duration_min BETWEEN 5 AND 180
  AND p.borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'EWR')
GROUP BY
  t.trip_date,
  t.pickup_hour,
  p.borough,
  CASE t.DOLocationID
    WHEN 132 THEN 'JFK'
    WHEN 138 THEN 'LGA'
    WHEN 1 THEN 'EWR'
  END,
  CAST(floor(CAST(t.duration_min AS DOUBLE) / 2.0) * 2 AS INT);
