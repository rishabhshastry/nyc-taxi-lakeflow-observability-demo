"""Daily additive histogram bins used by the airport quantile fan."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import SUPPORTED_BOROUGHS


@dp.materialized_view(
    name="mart_airport_quantile_fan",
    comment="Weekday airport duration histogram at dashboard filter grain.",
    cluster_by=["trip_date", "pickup_borough", "airport"],
    table_properties={"quality": "gold"},
)
def mart_airport_quantile_fan():
    trips = spark.read.table("yellow_trips_2025").alias("t")
    pickup_zones = spark.read.table("taxi_zones").alias("p")
    airport = (
        F.when(F.col("t.DOLocationID") == 132, F.lit("JFK"))
        .when(F.col("t.DOLocationID") == 138, F.lit("LGA"))
        .when(F.col("t.DOLocationID") == 1, F.lit("EWR"))
    )
    duration_bin = (
        F.floor(F.col("t.duration_min").cast("double") / F.lit(2.0))
        * F.lit(2)
    ).cast("int")

    prepared = (
        trips.join(
            pickup_zones, F.col("t.PULocationID") == F.col("p.location_id")
        )
        .where(F.col("t.pickup_day_of_week").between(2, 6))
        .where(F.col("t.DOLocationID").isin(1, 132, 138))
        .where(F.col("t.duration_min").between(5.0, 180.0))
        .where(F.col("p.borough").isin(*SUPPORTED_BOROUGHS))
        .select(
            F.col("t.trip_date").alias("trip_date"),
            F.col("t.pickup_hour").alias("pickup_hour"),
            F.col("p.borough").alias("pickup_borough"),
            airport.alias("airport"),
            F.lit("Airport").alias("airport_flag"),
            duration_bin.alias("duration_bin_minutes"),
        )
    )
    return prepared.groupBy(
        "trip_date",
        "pickup_hour",
        "pickup_borough",
        "airport",
        "airport_flag",
        "duration_bin_minutes",
    ).agg(F.count(F.lit(1)).alias("bin_trip_count"))
