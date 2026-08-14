"""Publish current, per-rule rejection counts for dashboard pipeline monitoring."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import QUALITY_RULES, with_trip_quality_columns

EXPECTED_YEAR = int(spark.conf.get("source.expected_year", "2025"))


@dp.materialized_view(
    name="yellow_trips_2025_dq_metrics",
    comment="Current total, accepted, rejected, and per-rule DQ metrics.",
    cluster_by=["rule_name"],
    table_properties={"quality": "monitoring"},
)
def yellow_trips_2025_dq_metrics():
    quality = with_trip_quality_columns(
        spark.read.table("yellow_trips_2025_raw"), EXPECTED_YEAR
    )
    totals = quality.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(
            F.when(F.size("rejection_reasons") == 0, F.lit(1)).otherwise(F.lit(0))
        ).alias("accepted_rows"),
        F.sum(
            F.when(F.size("rejection_reasons") > 0, F.lit(1)).otherwise(F.lit(0))
        ).alias("quarantined_rows"),
    )
    failed = (
        quality.select(F.explode("rejection_reasons").alias("rule_name"))
        .groupBy("rule_name")
        .agg(F.count(F.lit(1)).alias("failed_rows"))
    )
    rules = spark.createDataFrame([(name,) for name in QUALITY_RULES], ["rule_name"])
    return (
        rules.join(failed, "rule_name", "left")
        .fillna(0, subset=["failed_rows"])
        .crossJoin(totals)
        .withColumn(
            "failure_rate",
            F.when(
                F.col("total_rows") > 0,
                F.col("failed_rows").cast("double") / F.col("total_rows"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "rule_name",
            "failed_rows",
            "failure_rate",
            "total_rows",
            "accepted_rows",
            "quarantined_rows",
            "computed_at",
        )
    )
