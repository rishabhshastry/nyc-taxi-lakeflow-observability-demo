CREATE OR REPLACE TABLE main.nyc_taxi_operations.mart_zone_demand_horizon AS
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
  SELECT * FROM ranked_zones WHERE borough_rank <= 2
), daily AS (
  SELECT
    t.trip_date,
    t.pickup_day_of_week,
    t.pickup_hour,
    z.pickup_zone,
    z.pickup_borough,
    CASE
      WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
        THEN 'Airport'
      ELSE 'Non-airport'
    END AS airport_flag,
    COUNT(*) AS trip_count
  FROM main.nyc_taxi_operations.yellow_trips_2025 t
  JOIN selected_zones z ON t.PULocationID = z.location_id
  GROUP BY
    t.trip_date,
    t.pickup_day_of_week,
    t.pickup_hour,
    z.pickup_zone,
    z.pickup_borough,
    CASE
      WHEN t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)
        THEN 'Airport'
      ELSE 'Non-airport'
    END
)
SELECT
  trip_date,
  pickup_hour,
  pickup_zone,
  pickup_borough,
  airport_flag,
  trip_count,
  AVG(trip_count) OVER (
    PARTITION BY pickup_zone, pickup_day_of_week, pickup_hour, airport_flag
  ) AS expected_trip_count
FROM daily;
