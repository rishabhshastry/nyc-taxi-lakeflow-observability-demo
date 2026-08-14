CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_parallel_coordinates AS
WITH zone_totals AS (
  SELECT
    p.location_id,
    p.zone AS pickup_zone,
    p.borough AS pickup_borough,
    COUNT(*) AS annual_trip_count
  FROM main.nyc_taxi_operations.yellow_trips_2025 t
  JOIN main.nyc_taxi_operations.taxi_zones p
    ON t.PULocationID = p.location_id
  WHERE p.borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island')
    AND p.location_id NOT IN (1, 132, 138)
  GROUP BY p.location_id, p.zone, p.borough
), ranked_zones AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY pickup_borough ORDER BY annual_trip_count DESC) AS borough_rank
  FROM zone_totals
), selected_zones AS (
  SELECT * FROM ranked_zones WHERE borough_rank <= 5
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
  COUNT(*) AS trip_count,
  SUM(CAST(t.duration_min AS DOUBLE)) AS duration_total,
  SUM(CASE WHEN t.trip_distance >= 0.5 THEN t.fare_amount / t.trip_distance ELSE 0.0 END) AS fare_per_mile_total,
  SUM(CASE WHEN t.trip_distance >= 0.5 THEN 1 ELSE 0 END) AS fare_per_mile_count,
  SUM(CASE WHEN t.payment_type = 1 THEN 1 ELSE 0 END) AS card_trip_count,
  SUM(CASE WHEN t.payment_type = 1 AND t.tip_amount > 0 THEN 1 ELSE 0 END) AS tipped_card_count,
  SUM(CASE WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138) THEN 1 ELSE 0 END) AS airport_trip_count
FROM main.nyc_taxi_operations.yellow_trips_2025 t
JOIN selected_zones z ON t.PULocationID = z.location_id
GROUP BY
  t.trip_date,
  t.pickup_hour,
  z.pickup_zone,
  z.pickup_borough,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END;
