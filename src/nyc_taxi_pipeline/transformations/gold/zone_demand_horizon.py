"""Daily demand and full-year weekday/hour baseline for the horizon view."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import AIRPORT_LOCATION_IDS, NON_AIRPORT_BOROUGHS, airport_flag


@dp.materialized_view(
    name="mart_zone_demand_horizon",
    comment="Observed daily demand and full-year expected demand by selected zone.",
    cluster_by=["trip_date", "pickup_borough", "airport_flag"],
    table_properties={"quality": "gold"},
)
def mart_zone_demand_horizon():
    trips = spark.read.table("yellow_trips_2025").alias("t")
    zones = spark.read.table("taxi_zones")

    zone_totals = (
        trips.join(
            zones.alias("p"), F.col("t.PULocationID") == F.col("p.location_id")
        )
        .where(F.col("p.borough").isin(*NON_AIRPORT_BOROUGHS))
        .where(~F.col("p.location_id").isin(*AIRPORT_LOCATION_IDS))
        .groupBy(
            F.col("p.location_id").alias("location_id"),
            F.col("p.zone").alias("pickup_zone"),
            F.col("p.borough").alias("pickup_borough"),
        )
        .agg(F.count(F.lit(1)).alias("annual_trip_count"))
    )
    rank_window = Window.partitionBy("pickup_borough").orderBy(
        F.col("annual_trip_count").desc()
    )
    selected = zone_totals.withColumn(
        "borough_rank", F.row_number().over(rank_window)
    ).where(F.col("borough_rank") <= 2)

    daily = (
        trips.join(
            selected.alias("z"), F.col("t.PULocationID") == F.col("z.location_id")
        )
        .select(
            F.col("t.trip_date").alias("trip_date"),
            F.col("t.pickup_day_of_week").alias("pickup_day_of_week"),
            F.col("t.pickup_hour").alias("pickup_hour"),
            F.col("z.pickup_zone").alias("pickup_zone"),
            F.col("z.pickup_borough").alias("pickup_borough"),
            airport_flag(
                F.col("t.PULocationID"), F.col("t.DOLocationID")
            ).alias("airport_flag"),
        )
        .groupBy(
            "trip_date",
            "pickup_day_of_week",
            "pickup_hour",
            "pickup_zone",
            "pickup_borough",
            "airport_flag",
        )
        .agg(F.count(F.lit(1)).alias("trip_count"))
    )
    baseline = Window.partitionBy(
        "pickup_zone", "pickup_day_of_week", "pickup_hour", "airport_flag"
    )
    return daily.withColumn(
        "expected_trip_count", F.avg("trip_count").over(baseline)
    ).select(
        "trip_date",
        "pickup_hour",
        "pickup_zone",
        "pickup_borough",
        "airport_flag",
        "trip_count",
        "expected_trip_count",
    )
