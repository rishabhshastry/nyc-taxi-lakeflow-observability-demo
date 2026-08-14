CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_fare_ternary AS
SELECT
  t.trip_date,
  t.pickup_hour,
  p.borough AS pickup_borough,
  CASE t.payment_type
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    ELSE 'Other'
  END AS payment_category,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END AS airport_flag,
  COUNT(*) AS trip_count,
  SUM(greatest(t.fare_amount, 0.0)) AS fare_total,
  SUM(greatest(t.tip_amount, 0.0)) AS tip_total,
  SUM(greatest(
    t.total_amount - greatest(t.fare_amount, 0.0) - greatest(t.tip_amount, 0.0),
    0.0
  )) AS extras_total
FROM main.nyc_taxi_operations.yellow_trips_2025 t
JOIN main.nyc_taxi_operations.taxi_zones p
  ON t.PULocationID = p.location_id
WHERE t.total_amount > 0
  AND p.borough IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'EWR')
GROUP BY
  t.trip_date,
  t.pickup_hour,
  p.borough,
  CASE t.payment_type
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    ELSE 'Other'
  END,
  CASE
    WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
      THEN 'Airport'
    ELSE 'Non-airport'
  END;
