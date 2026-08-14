"""Daily additive operating measures for the parallel-coordinates view."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import AIRPORT_LOCATION_IDS, NON_AIRPORT_BOROUGHS, airport_flag


@dp.materialized_view(
    name="mart_parallel_coordinates",
    comment="Top pickup-zone operating measures at dashboard filter grain.",
    cluster_by=["trip_date", "pickup_borough", "airport_flag"],
    table_properties={"quality": "gold"},
)
def mart_parallel_coordinates():
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
    ).where(F.col("borough_rank") <= 5)

    joined = trips.join(
        selected.alias("z"), F.col("t.PULocationID") == F.col("z.location_id")
    ).select(
        F.col("t.trip_date").alias("trip_date"),
        F.col("t.pickup_hour").alias("pickup_hour"),
        F.col("z.pickup_zone").alias("pickup_zone"),
        F.col("z.pickup_borough").alias("pickup_borough"),
        airport_flag(F.col("t.PULocationID"), F.col("t.DOLocationID")).alias(
            "airport_flag"
        ),
        F.col("t.duration_min").cast("double").alias("duration_min"),
        F.col("t.trip_distance").alias("trip_distance"),
        F.col("t.fare_amount").alias("fare_amount"),
        F.col("t.payment_type").alias("payment_type"),
        F.col("t.tip_amount").alias("tip_amount"),
        F.col("t.PULocationID").alias("PULocationID"),
        F.col("t.DOLocationID").alias("DOLocationID"),
    )
    return joined.groupBy(
        "trip_date", "pickup_hour", "pickup_zone", "pickup_borough", "airport_flag"
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("duration_min").alias("duration_total"),
        F.sum(
            F.when(
                F.col("trip_distance") >= 0.5,
                F.col("fare_amount") / F.col("trip_distance"),
            ).otherwise(F.lit(0.0))
        ).alias("fare_per_mile_total"),
        F.sum(
            F.when(F.col("trip_distance") >= 0.5, F.lit(1)).otherwise(F.lit(0))
        ).alias("fare_per_mile_count"),
        F.sum(
            F.when(F.col("payment_type") == 1, F.lit(1)).otherwise(F.lit(0))
        ).alias("card_trip_count"),
        F.sum(
            F.when(
                (F.col("payment_type") == 1) & (F.col("tip_amount") > 0), F.lit(1)
            ).otherwise(F.lit(0))
        ).alias("tipped_card_count"),
        F.sum(
            F.when(
                F.col("PULocationID").isin(*AIRPORT_LOCATION_IDS)
                | F.col("DOLocationID").isin(*AIRPORT_LOCATION_IDS),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("airport_trip_count"),
    )
