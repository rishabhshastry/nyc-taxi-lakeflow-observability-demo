"""Retain every rejected row together with all violated rule names."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="yellow_trips_2025_quarantine",
    comment="Rejected Yellow Taxi rows with complete DQ rejection reasons.",
    cluster_by=["_source_file", "PULocationID", "DOLocationID"],
    table_properties={
        "quality": "quarantine",
        "delta.feature.timestampNtz": "supported",
    },
)
def yellow_trips_2025_quarantine():
    return spark.readStream.table("trips_enriched_with_rejection_reasons").where(
        F.size("rejection_reasons") > 0
    )
