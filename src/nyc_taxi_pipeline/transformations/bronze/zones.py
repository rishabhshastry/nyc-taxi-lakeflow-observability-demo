"""Publish and validate the official TLC taxi-zone centroid dimension."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

LANDING_PATH = spark.conf.get("source.landing_path")

ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
    ]
)


@dp.materialized_view(
    name="taxi_zones",
    comment="Official TLC taxi zones with EPSG:4326 polygon centroids.",
    table_properties={"quality": "silver"},
)
@dp.expect("valid_location_id", "location_id BETWEEN 1 AND 263")
@dp.expect("non_null_labels", "borough IS NOT NULL AND zone IS NOT NULL")
@dp.expect(
    "coordinates_in_nyc_bounds",
    "longitude BETWEEN -75.0 AND -72.0 AND latitude BETWEEN 39.0 AND 42.0",
)
def taxi_zones():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(ZONE_SCHEMA)
        .load(f"{LANDING_PATH}/taxi_zone_centroids.csv")
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            "service_zone",
            F.col("longitude").cast("double").alias("longitude"),
            F.col("latitude").cast("double").alias("latitude"),
        )
    )


@dp.materialized_view(name="taxi_zones_contract", private=True)
@dp.expect_or_fail(
    "exact_taxi_zone_contract",
    "row_count = 263 AND distinct_location_ids = 263 "
    "AND minimum_location_id = 1 AND maximum_location_id = 263 "
    "AND null_required_fields = 0",
)
def taxi_zones_contract():
    zones = spark.read.table("taxi_zones")
    return zones.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("location_id").alias("distinct_location_ids"),
        F.min("location_id").alias("minimum_location_id"),
        F.max("location_id").alias("maximum_location_id"),
        F.sum(
            F.when(
                F.col("borough").isNull()
                | F.col("zone").isNull()
                | F.col("longitude").isNull()
                | F.col("latitude").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("null_required_fields"),
    )
