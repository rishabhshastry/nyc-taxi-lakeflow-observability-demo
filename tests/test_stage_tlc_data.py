from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.bootstrap.stage_tlc_data import (
    copy_atomic,
    download_atomic,
    stage_tlc_assets,
    valid_centroids,
    valid_parquet,
)


class StageTlcDataTest(unittest.TestCase):
    def test_configured_year_controls_landing_directory(self) -> None:
        asset = {"name": "asset", "status": "retained", "bytes": 1}
        with patch(
            "src.bootstrap.stage_tlc_data.download_atomic", return_value=asset
        ) as download, patch(
            "src.bootstrap.stage_tlc_data.copy_atomic", return_value=asset
        ):
            stage_tlc_assets(
                landing_path=Path("/Volumes/catalog/schema/landing"),
                centroids_source=Path("centroids.csv"),
                source_base_url="https://example.test/trips",
                year=2024,
                month_count=1,
                overwrite=False,
                timeout_seconds=120,
            )

        self.assertEqual(
            Path(
                "/Volumes/catalog/schema/landing/yellow_2024/"
                "yellow_tripdata_2024-01.parquet"
            ),
            download.call_args.args[1],
        )

    def test_download_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            source.write_bytes(b"PAR1" + (b"x" * 32) + b"PAR1")
            target = root / "landing" / "trip.parquet"
            validator = lambda path: valid_parquet(path, minimum_bytes=40)

            first = download_atomic(source.as_uri(), target, validator)
            second = download_atomic(source.as_uri(), target, validator)

            self.assertEqual(first["status"], "downloaded")
            self.assertEqual(second["status"], "retained")
            self.assertTrue(validator(target))

    def test_centroid_copy_requires_exact_zone_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "centroids.csv"
            rows = [
                "LocationID,Borough,Zone,service_zone,longitude,latitude",
                *[
                    f"{location_id},Queens,Zone {location_id},Boro Zone,-73.9,40.7"
                    for location_id in range(1, 264)
                ],
            ]
            source.write_text("\n".join(rows) + "\n", encoding="utf-8")
            target = root / "landing" / "taxi_zone_centroids.csv"

            result = copy_atomic(source, target, valid_centroids)

            self.assertEqual(result["status"], "copied")
            self.assertTrue(valid_centroids(target))


if __name__ == "__main__":
    unittest.main()
