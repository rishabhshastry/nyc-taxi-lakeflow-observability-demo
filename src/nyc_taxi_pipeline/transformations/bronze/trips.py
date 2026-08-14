"""Incrementally ingest the official 2025 Yellow Taxi Parquet files."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("source.landing_path")
SCHEMA_LOCATION_BASE = spark.conf.get("schema_location_base")
EXPECTED_YEAR = int(spark.conf.get("source.expected_year", "2025"))


@dp.table(
    name="yellow_trips_2025_raw",
    comment="Official NYC TLC Yellow Taxi records with ingestion metadata.",
    cluster_by=["PULocationID", "DOLocationID"],
    table_properties={
        "quality": "bronze",
        "delta.feature.timestampNtz": "supported",
    },
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
@dp.expect(
    "expected_source_year",
    f"regexp_extract(_source_file, 'yellow_tripdata_([0-9]{{4}})-', 1) = '{EXPECTED_YEAR}'",
)
def yellow_trips_2025_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaLocation",
            f"{SCHEMA_LOCATION_BASE}/yellow_trips_2025_raw",
        )
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/yellow_2025")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
