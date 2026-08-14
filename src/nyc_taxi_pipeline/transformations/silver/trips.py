"""Apply named DQ rules and publish the dashboard-compatible Silver trip table."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import silver_trip_projection, with_trip_quality_columns

EXPECTED_YEAR = int(spark.conf.get("source.expected_year", "2025"))

# CUSTOMER CHANGE POINT: adapt these SQL expectation expressions to your data
# contract. Keep the equivalent rejection logic in common.py synchronized so
# accepted, quarantined, and reported DQ counts use the same rules.
TRIP_QUALITY_EXPECTATIONS = {
    "pickup_year_is_expected": f"year(tpep_pickup_datetime) = {EXPECTED_YEAR}",
    "dropoff_after_pickup": "tpep_dropoff_datetime > tpep_pickup_datetime",
    "duration_between_1_and_180_minutes": (
        "unix_timestamp(tpep_dropoff_datetime) - "
        "unix_timestamp(tpep_pickup_datetime) BETWEEN 60 AND 10800"
    ),
    "distance_between_point_1_and_100_miles": "trip_distance BETWEEN 0.1 AND 100",
    "total_amount_between_0_and_1000": "total_amount BETWEEN 0 AND 1000",
    "pickup_and_dropoff_locations_are_valid": (
        "PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263"
    ),
}


@dp.temporary_view(name="trips_enriched_with_rejection_reasons")
@dp.expect_all(TRIP_QUALITY_EXPECTATIONS)
def trips_enriched_with_rejection_reasons():
    return with_trip_quality_columns(
        spark.readStream.table("yellow_trips_2025_raw"), EXPECTED_YEAR
    )


@dp.table(
    name="yellow_trips_2025",
    comment="Validated 2025 Yellow Taxi trips with dashboard-ready measures.",
    cluster_by=["trip_date", "PULocationID", "DOLocationID"],
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)
def yellow_trips_2025():
    accepted = spark.readStream.table(
        "trips_enriched_with_rejection_reasons"
    ).where(F.size("rejection_reasons") == 0)
    return silver_trip_projection(accepted)
