"""Daily additive fare components for the fare-composition simplex."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import SUPPORTED_BOROUGHS, airport_flag


@dp.materialized_view(
    name="mart_fare_ternary",
    comment="Nonnegative receipt components at dashboard filter grain.",
    cluster_by=["trip_date", "pickup_borough", "airport_flag"],
    table_properties={"quality": "gold"},
)
def mart_fare_ternary():
    trips = spark.read.table("yellow_trips_2025").alias("t")
    zones = spark.read.table("taxi_zones").alias("p")
    fare = F.greatest(F.col("t.fare_amount"), F.lit(0.0))
    tip = F.greatest(F.col("t.tip_amount"), F.lit(0.0))
    extras = F.greatest(F.col("t.total_amount") - fare - tip, F.lit(0.0))
    payment_category = (
        F.when(F.col("t.payment_type") == 1, F.lit("Credit card"))
        .when(F.col("t.payment_type") == 2, F.lit("Cash"))
        .otherwise(F.lit("Other"))
    )

    prepared = (
        trips.join(zones, F.col("t.PULocationID") == F.col("p.location_id"))
        .where(F.col("t.total_amount") > 0)
        .where(F.col("p.borough").isin(*SUPPORTED_BOROUGHS))
        .select(
            F.col("t.trip_date").alias("trip_date"),
            F.col("t.pickup_hour").alias("pickup_hour"),
            F.col("p.borough").alias("pickup_borough"),
            payment_category.alias("payment_category"),
            airport_flag(
                F.col("t.PULocationID"), F.col("t.DOLocationID")
            ).alias("airport_flag"),
            fare.alias("fare_component"),
            tip.alias("tip_component"),
            extras.alias("extras_component"),
        )
    )
    return prepared.groupBy(
        "trip_date",
        "pickup_hour",
        "pickup_borough",
        "payment_category",
        "airport_flag",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("fare_component").alias("fare_total"),
        F.sum("tip_component").alias("tip_total"),
        F.sum("extras_component").alias("extras_total"),
    )
