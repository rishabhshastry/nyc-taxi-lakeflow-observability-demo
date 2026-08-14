"""Shared, action-free DataFrame expressions for the NYC Taxi SDP."""

from __future__ import annotations

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

AIRPORT_LOCATION_IDS = (1, 132, 138)
SUPPORTED_BOROUGHS = (
    "Manhattan",
    "Brooklyn",
    "Queens",
    "Bronx",
    "Staten Island",
    "EWR",
)
NON_AIRPORT_BOROUGHS = SUPPORTED_BOROUGHS[:-1]

# CUSTOMER CHANGE POINT: if you alter the trip-quality contract, keep this
# rejection-reason list/expressions synchronized with TRIP_QUALITY_EXPECTATIONS
# in transformations/silver/trips.py so quarantine and expectation metrics agree.
QUALITY_RULES = (
    "pickup_year_not_expected",
    "dropoff_not_after_pickup",
    "duration_out_of_range",
    "distance_out_of_range",
    "total_amount_out_of_range",
    "location_id_out_of_range",
)


def airport_flag(pickup: Column, dropoff: Column) -> Column:
    """Return the dashboard's stable Airport/Non-airport classification."""

    return F.when(
        pickup.isin(*AIRPORT_LOCATION_IDS) | dropoff.isin(*AIRPORT_LOCATION_IDS),
        F.lit("Airport"),
    ).otherwise(F.lit("Non-airport"))


def duration_seconds() -> Column:
    """Match the recovered SQL's whole-second duration calculation."""

    return F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp(
        "tpep_pickup_datetime"
    )


def _reason_when_invalid(valid: Column, reason: str) -> Column:
    # `otherwise` intentionally classifies NULL predicates as invalid, matching
    # how the original WHERE clause excluded NULL values.
    return F.when(valid, F.lit(None).cast("string")).otherwise(F.lit(reason))


def with_trip_quality_columns(df: DataFrame, expected_year: int) -> DataFrame:
    """Add dashboard-derived fields and a complete multi-rule rejection array."""

    seconds = duration_seconds()
    reasons = F.array_compact(
        F.array(
            _reason_when_invalid(
                F.year("tpep_pickup_datetime") == F.lit(expected_year),
                QUALITY_RULES[0],
            ),
            _reason_when_invalid(
                F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"),
                QUALITY_RULES[1],
            ),
            _reason_when_invalid(seconds.between(60, 10800), QUALITY_RULES[2]),
            _reason_when_invalid(
                F.col("trip_distance").between(0.1, 100.0), QUALITY_RULES[3]
            ),
            _reason_when_invalid(
                F.col("total_amount").between(0.0, 1000.0), QUALITY_RULES[4]
            ),
            _reason_when_invalid(
                F.col("PULocationID").between(1, 263)
                & F.col("DOLocationID").between(1, 263),
                QUALITY_RULES[5],
            ),
        )
    )

    return (
        df.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn(
            "day_type",
            F.when(
                F.dayofweek("tpep_pickup_datetime").isin(1, 7), F.lit("Weekend")
            ).otherwise(F.lit("Weekday")),
        )
        .withColumn("duration_min", seconds.cast("double") / F.lit(60.0))
        .withColumn(
            "speed_mph",
            F.col("trip_distance") / (seconds.cast("double") / F.lit(3600.0)),
        )
        .withColumn("rejection_reasons", reasons)
    )


def silver_trip_projection(df: DataFrame) -> DataFrame:
    """Project the exact column contract consumed by the six Gold marts."""

    return df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_date",
        "pickup_month",
        "pickup_hour",
        "pickup_day_of_week",
        "day_type",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "duration_min",
        "speed_mph",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "payment_type",
        "RatecodeID",
        "VendorID",
        "Airport_fee",
        "cbd_congestion_fee",
    )
