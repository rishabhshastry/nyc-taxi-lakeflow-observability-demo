"""A reversible expectation failure used for monitoring delivery drills."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

FAILURE_MODE = spark.conf.get("demo.failure_mode", "none")
TEST_RUN_ID = spark.conf.get("demo.test_run_id", "normal")


@dp.materialized_view(name="monitoring_failure_drill", private=True)
@dp.expect_or_fail(
    "demo_expectation_must_pass", "failure_mode <> 'expectation'"
)
def monitoring_failure_drill():
    return spark.range(1).select(
        F.lit(TEST_RUN_ID).alias("test_run_id"),
        F.lit(FAILURE_MODE).alias("failure_mode"),
        F.current_timestamp().alias("evaluated_at"),
    )
