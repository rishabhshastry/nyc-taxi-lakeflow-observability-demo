"""Daily additive vector components for the directional flow map."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import SUPPORTED_BOROUGHS, airport_flag


@dp.materialized_view(
    name="mart_directional_flow_field",
    comment="Top pickup-zone directional vectors at dashboard filter grain.",
    cluster_by=["trip_date", "pickup_borough", "airport_flag"],
    table_properties={"quality": "gold"},
)
def mart_directional_flow_field():
    trips = spark.read.table("yellow_trips_2025").alias("t")
    zones = spark.read.table("taxi_zones")

    zone_totals = (
        trips.join(
            zones.alias("p"), F.col("t.PULocationID") == F.col("p.location_id")
        )
        .where(F.col("t.PULocationID") != F.col("t.DOLocationID"))
        .where(F.col("p.borough").isin(*SUPPORTED_BOROUGHS))
        .groupBy(
            F.col("p.location_id").alias("location_id"),
            F.col("p.zone").alias("pickup_zone"),
            F.col("p.borough").alias("pickup_borough"),
            F.col("p.longitude").alias("longitude"),
            F.col("p.latitude").alias("latitude"),
        )
        .agg(F.count(F.lit(1)).alias("annual_trip_count"))
    )
    rank_window = Window.partitionBy("pickup_borough").orderBy(
        F.col("annual_trip_count").desc()
    )
    selected = zone_totals.withColumn(
        "borough_rank", F.row_number().over(rank_window)
    ).where(F.col("borough_rank") <= 20)

    joined = (
        trips.join(
            selected.alias("z"), F.col("t.PULocationID") == F.col("z.location_id")
        )
        .join(
            zones.alias("d"), F.col("t.DOLocationID") == F.col("d.location_id")
        )
        .where(F.col("t.PULocationID") != F.col("t.DOLocationID"))
        .select(
            F.col("t.trip_date").alias("trip_date"),
            F.col("t.pickup_hour").alias("pickup_hour"),
            F.col("z.pickup_zone").alias("pickup_zone"),
            F.col("z.pickup_borough").alias("pickup_borough"),
            airport_flag(
                F.col("t.PULocationID"), F.col("t.DOLocationID")
            ).alias("airport_flag"),
            F.col("z.longitude").alias("longitude"),
            F.col("z.latitude").alias("latitude"),
            F.col("t.trip_distance").alias("trip_distance"),
            (F.col("d.longitude") - F.col("z.longitude")).alias("dx"),
            (F.col("d.latitude") - F.col("z.latitude")).alias("dy"),
        )
    )
    return joined.groupBy(
        "trip_date",
        "pickup_hour",
        "pickup_zone",
        "pickup_borough",
        "airport_flag",
        "longitude",
        "latitude",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("trip_distance").alias("distance_total"),
        F.sum("dx").alias("dx_total"),
        F.sum("dy").alias("dy_total"),
    )
