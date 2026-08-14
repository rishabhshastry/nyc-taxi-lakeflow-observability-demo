"""Daily additive candidate pairs used by the origin-destination map."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import AIRPORT_LOCATION_IDS, NON_AIRPORT_BOROUGHS, airport_flag


@dp.materialized_view(
    name="mart_od_connection_map",
    comment="Selected origin/destination candidates at dashboard filter grain.",
    cluster_by=["trip_date", "pickup_borough", "airport_flag"],
    table_properties={"quality": "gold"},
)
def mart_od_connection_map():
    trips = spark.read.table("yellow_trips_2025").alias("t")
    zones = spark.read.table("taxi_zones")

    origin_totals = (
        trips.join(
            zones.alias("p"), F.col("t.PULocationID") == F.col("p.location_id")
        )
        .where(F.col("t.PULocationID") != F.col("t.DOLocationID"))
        .where(F.col("p.borough").isin(*NON_AIRPORT_BOROUGHS))
        .where(~F.col("p.location_id").isin(*AIRPORT_LOCATION_IDS))
        .groupBy(
            F.col("p.location_id").alias("location_id"),
            F.col("p.zone").alias("origin_zone"),
            F.col("p.borough").alias("pickup_borough"),
            F.col("p.longitude").alias("origin_longitude"),
            F.col("p.latitude").alias("origin_latitude"),
        )
        .agg(F.count(F.lit(1)).alias("origin_trip_count"))
    )
    origin_rank = Window.partitionBy("pickup_borough").orderBy(
        F.col("origin_trip_count").desc()
    )
    selected_origins = origin_totals.withColumn(
        "borough_rank", F.row_number().over(origin_rank)
    ).where((F.col("borough_rank") <= 3) | (F.col("location_id") == 161))

    annual_pairs = (
        trips.join(
            selected_origins.alias("o"),
            F.col("t.PULocationID") == F.col("o.location_id"),
        )
        .join(
            zones.alias("d"), F.col("t.DOLocationID") == F.col("d.location_id")
        )
        .where(F.col("t.PULocationID") != F.col("t.DOLocationID"))
        .groupBy(
            F.col("o.location_id").alias("origin_id"),
            F.col("o.origin_zone").alias("origin_zone"),
            F.col("o.pickup_borough").alias("pickup_borough"),
            F.col("o.origin_longitude").alias("origin_longitude"),
            F.col("o.origin_latitude").alias("origin_latitude"),
            F.col("d.location_id").alias("destination_id"),
            F.col("d.zone").alias("destination_zone"),
            F.col("d.longitude").alias("destination_longitude"),
            F.col("d.latitude").alias("destination_latitude"),
        )
        .agg(F.count(F.lit(1)).alias("annual_trip_count"))
    )
    destination_rank = Window.partitionBy("origin_id").orderBy(
        F.col("annual_trip_count").desc()
    )
    candidate_pairs = annual_pairs.withColumn(
        "destination_rank", F.row_number().over(destination_rank)
    ).where(F.col("destination_rank") <= 12)

    joined = trips.join(
        candidate_pairs.alias("c"),
        (F.col("t.PULocationID") == F.col("c.origin_id"))
        & (F.col("t.DOLocationID") == F.col("c.destination_id")),
    ).select(
        F.col("t.trip_date").alias("trip_date"),
        F.col("t.pickup_hour").alias("pickup_hour"),
        F.col("c.pickup_borough").alias("pickup_borough"),
        airport_flag(F.col("t.PULocationID"), F.col("t.DOLocationID")).alias(
            "airport_flag"
        ),
        F.col("c.origin_zone").alias("origin_zone"),
        F.col("c.destination_zone").alias("destination_zone"),
        F.col("c.origin_longitude").alias("origin_longitude"),
        F.col("c.origin_latitude").alias("origin_latitude"),
        F.col("c.destination_longitude").alias("destination_longitude"),
        F.col("c.destination_latitude").alias("destination_latitude"),
        F.col("t.duration_min").cast("double").alias("duration_min"),
    )
    return joined.groupBy(
        "trip_date",
        "pickup_hour",
        "pickup_borough",
        "airport_flag",
        "origin_zone",
        "destination_zone",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("duration_min").alias("duration_total"),
    )
