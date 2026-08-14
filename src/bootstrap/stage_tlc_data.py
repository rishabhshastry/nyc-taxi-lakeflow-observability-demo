#!/usr/bin/env python3
"""Stage official NYC TLC source assets in a Unity Catalog volume.

The task is intentionally idempotent: valid assets are retained, while missing
or incomplete files are downloaded to a temporary path and atomically moved
into place. It uses only the Python standard library so it can run on
serverless Jobs compute without extra dependencies.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

# CUSTOMER CHANGE POINT: normally override `tlc_source_base_url` in the bundle
# instead of editing this fallback. Use an approved internal mirror when public
# HTTPS egress is unavailable from serverless compute.
DEFAULT_SOURCE_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PARQUET_MINIMUM_BYTES = 10_000_000
CENTROID_COLUMNS = {
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
    "longitude",
    "latitude",
}
USER_AGENT = "nyc-taxi-lakeflow-observability-demo/1.0"


def valid_parquet(path: Path, minimum_bytes: int = PARQUET_MINIMUM_BYTES) -> bool:
    """Return whether a file has a plausible size and Parquet magic bytes."""

    try:
        if path.stat().st_size < minimum_bytes:
            return False
        with path.open("rb") as handle:
            if handle.read(4) != b"PAR1":
                return False
            handle.seek(-4, os.SEEK_END)
            return handle.read(4) == b"PAR1"
    except (FileNotFoundError, OSError):
        return False


def valid_centroids(path: Path) -> bool:
    """Validate the bundled 263-zone lookup and centroid contract."""

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or not CENTROID_COLUMNS.issubset(reader.fieldnames):
                return False
            location_ids = {int(row["LocationID"]) for row in reader}
        return location_ids == set(range(1, 264))
    except (FileNotFoundError, OSError, TypeError, ValueError):
        return False


def _temporary_path(target: Path) -> Path:
    return target.with_name(f".{target.name}.{os.getpid()}.partial")


def download_atomic(
    url: str,
    target: Path,
    validator: Callable[[Path], bool],
    *,
    overwrite: bool = False,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Download one asset and publish it only after validation succeeds."""

    if not overwrite and validator(target):
        return {"name": target.name, "status": "retained", "bytes": target.stat().st_size}

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(target)
    temporary.unlink(missing_ok=True)
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=timeout_seconds) as response, temporary.open(
            "wb"
        ) as output:
            shutil.copyfileobj(response, output, length=8 * 1024 * 1024)
        if not validator(temporary):
            raise RuntimeError(f"Downloaded asset failed validation: {url}")
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {"name": target.name, "status": "downloaded", "bytes": target.stat().st_size}


def copy_atomic(
    source: Path,
    target: Path,
    validator: Callable[[Path], bool],
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Copy a bundle-provided asset into the landing volume atomically."""

    if not validator(source):
        raise RuntimeError(f"Bundled source asset failed validation: {source}")
    if not overwrite and validator(target):
        return {"name": target.name, "status": "retained", "bytes": target.stat().st_size}

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(target)
    temporary.unlink(missing_ok=True)
    try:
        shutil.copyfile(source, temporary)
        if not validator(temporary):
            raise RuntimeError(f"Copied asset failed validation: {source}")
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {"name": target.name, "status": "copied", "bytes": target.stat().st_size}


def stage_tlc_assets(
    *,
    landing_path: Path,
    centroids_source: Path,
    source_base_url: str,
    year: int,
    month_count: int,
    overwrite: bool,
    timeout_seconds: int,
) -> dict[str, Any]:
    """Stage all monthly trip files plus the dashboard's zone dimension."""

    if not 1 <= month_count <= 12:
        raise ValueError("month_count must be between 1 and 12")

    trip_directory = landing_path / f"yellow_{year}"
    trip_results = []
    for month in range(1, month_count + 1):
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        trip_results.append(
            download_atomic(
                f"{source_base_url.rstrip('/')}/{filename}",
                trip_directory / filename,
                valid_parquet,
                overwrite=overwrite,
                timeout_seconds=timeout_seconds,
            )
        )

    centroid_result = copy_atomic(
        centroids_source,
        landing_path / "taxi_zone_centroids.csv",
        valid_centroids,
        overwrite=overwrite,
    )
    all_results = [*trip_results, centroid_result]
    return {
        "landing_path": str(landing_path),
        "year": year,
        "month_count": month_count,
        "downloaded": sum(item["status"] == "downloaded" for item in all_results),
        "copied": sum(item["status"] == "copied" for item in all_results),
        "retained": sum(item["status"] == "retained" for item in all_results),
        "total_bytes": sum(item["bytes"] for item in all_results),
        "assets": all_results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing-path", type=Path, required=True)
    parser.add_argument("--centroids-source", type=Path, required=True)
    parser.add_argument("--source-base-url", default=DEFAULT_SOURCE_BASE_URL)
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month-count", type=int, default=12)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = stage_tlc_assets(
        landing_path=args.landing_path,
        centroids_source=args.centroids_source,
        source_base_url=args.source_base_url,
        year=args.year,
        month_count=args.month_count,
        overwrite=args.overwrite,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
