#!/usr/bin/env python3
"""Build the single-canvas NYC Taxi Vega-Lite showcase dashboard."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
DISPLAY_NAME = "NYC Taxi Operations Intelligence"
PARENT_PATH = os.getenv("DATABRICKS_DASHBOARD_PARENT_PATH", "/Users/<user-name>")
BASEMAP_FEATURES = json.loads(
    (ROOT / "specs" / "nyc_taxi_zone_basemap.geojson").read_text(encoding="utf-8")
)["features"]


def dataset(name: str, display_name: str, query: str) -> dict[str, Any]:
    return {
        "name": name,
        "displayName": display_name,
        "queryLines": [query],
    }


def text_widget(
    name: str,
    text: str,
    *,
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict[str, Any]:
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": [text]}},
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def filter_widget(
    *,
    name: str,
    title: str,
    widget_type: str,
    field_name: str,
    display_name: str,
    dataset_names: list[str],
    x: int,
    y: int,
    width: int,
    height: int,
) -> dict[str, Any]:
    queries = []
    fields = []
    for dataset_name in dataset_names:
        query_name = f"{name}_{dataset_name}"
        queries.append(
            {
                "name": query_name,
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {"name": field_name, "expression": f"`{field_name}`"}
                    ],
                    "disaggregated": False,
                },
            }
        )
        fields.append(
            {
                "fieldName": field_name,
                "displayName": display_name,
                "queryName": query_name,
            }
        )
    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": widget_type,
                "encodings": {"fields": fields},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def embed_named_data(value: Any, name: str, rows: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        if value.get("data") == {"name": name}:
            value["data"] = {"values": rows}
        for child in value.values():
            embed_named_data(child, name, rows)
    elif isinstance(value, list):
        for child in value:
            embed_named_data(child, name, rows)


def custom_widget(
    *,
    name: str,
    title: str,
    dataset_name: str,
    fields: list[str],
    aggregations: dict[str, str] | None = None,
    spec_file: str,
    x: int,
    y: int,
    width: int = 6,
    height: int = 13,
) -> dict[str, Any]:
    spec = json.loads((ROOT / "specs" / spec_file).read_text(encoding="utf-8"))
    embed_named_data(spec, "nyc_basemap", BASEMAP_FEATURES)
    aggregations = aggregations or {}
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": field,
                                "expression": aggregations.get(field, f"`{field}`"),
                            }
                            for field in fields
                        ],
                        "disaggregated": not bool(aggregations),
                    },
                }
            ],
            "spec": {
                "version": 1,
                "frame": {"showTitle": True, "title": title},
                "jsonSpec": {
                    "type": "vega-lite",
                    "spec": json.dumps(spec, separators=(",", ":")),
                },
                "widgetType": "custom-vega-viz",
                "encodings": {
                    "fields": [
                        {"fieldName": field, "displayName": field.replace("_", " ").title()}
                        for field in fields
                    ]
                },
                "data": {"queryName": "main_query"},
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def build() -> dict[str, Any]:
    datasets = [
        dataset(
            "airport_quantile",
            "Daily weekday borough-to-airport travel-time histogram",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, airport, "
            "duration_bin_minutes, bin_trip_count "
            "FROM mart_airport_quantile_fan",
        ),
        dataset(
            "directional_flow",
            "Daily taxi-zone directional vector components",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, pickup_zone, "
            "longitude, latitude, trip_count, distance_total, dx_total, dy_total "
            "FROM mart_directional_flow_field",
        ),
        dataset(
            "od_connection",
            "Daily selected-origin geographic OD connections",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, origin_zone, destination_zone, "
            "origin_longitude, origin_latitude, destination_longitude, destination_latitude, "
            "trip_count, duration_total "
            "FROM mart_od_connection_map",
        ),
        dataset(
            "parallel_coordinates",
            "Daily additive taxi operating-regime measures",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, pickup_zone, trip_count, "
            "duration_total, fare_per_mile_total, fare_per_mile_count, card_trip_count, "
            "tipped_card_count, airport_trip_count "
            "FROM mart_parallel_coordinates",
        ),
        dataset(
            "horizon",
            "Daily zone demand and same-weekday baseline",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, pickup_zone, "
            "trip_count, expected_trip_count "
            "FROM mart_zone_demand_horizon",
        ),
        dataset(
            "fare_ternary",
            "Daily fare-component composition totals",
            "SELECT trip_date, pickup_hour, pickup_borough, airport_flag, payment_category, "
            "trip_count, fare_total, tip_total, extras_total "
            "FROM mart_fare_ternary",
        ),
    ]

    dataset_names = [item["name"] for item in datasets]

    layout = [
        text_widget(
            "dashboard_title",
            "# NYC Taxi Operations Intelligence · Network, demand & revenue",
            x=0,
            y=0,
            width=12,
            height=2,
        ),
        text_widget(
            "dashboard_subtitle",
            "Production operations view over 45.17M validated 2025 trips. All four filters update every compatible view; Airport reliability is airport-bound by definition.",
            x=0,
            y=2,
            width=12,
            height=2,
        ),
        filter_widget(
            name="pickup_hour_filter",
            title="Pickup hour (0–23)",
            widget_type="filter-multi-select",
            field_name="pickup_hour",
            display_name="Pickup hour",
            dataset_names=dataset_names,
            x=0,
            y=4,
            width=3,
            height=2,
        ),
        filter_widget(
            name="trip_date_filter",
            title="Pickup date",
            widget_type="filter-date-range-picker",
            field_name="trip_date",
            display_name="Pickup date",
            dataset_names=dataset_names,
            x=3,
            y=4,
            width=3,
            height=2,
        ),
        filter_widget(
            name="pickup_borough_filter",
            title="Pickup borough",
            widget_type="filter-multi-select",
            field_name="pickup_borough",
            display_name="Pickup borough",
            dataset_names=dataset_names,
            x=6,
            y=4,
            width=3,
            height=2,
        ),
        filter_widget(
            name="airport_flag_filter",
            title="Airport trip",
            widget_type="filter-multi-select",
            field_name="airport_flag",
            display_name="Airport trip",
            dataset_names=dataset_names,
            x=9,
            y=4,
            width=3,
            height=2,
        ),
        text_widget(
            "quantile_guide",
            "### 1 · Airport reliability\nWeekday trips from the selected pickup boroughs. Pale = p10–p90, dark = IQR, yellow = median; two-minute duration bins keep quantiles composable across date ranges.",
            x=0,
            y=6,
            width=6,
            height=2,
        ),
        text_widget(
            "vector_guide",
            "### 2 · Directional flow field\nEach wedge marks a pickup zone, points toward the weighted mean destination for the selected slice, and grows with outbound trip volume.",
            x=6,
            y=6,
            width=6,
            height=2,
        ),
        custom_widget(
            name="airport_quantile_fan",
            title="Selected boroughs → airports · weekday travel-time quantiles",
            dataset_name="airport_quantile",
            fields=[
                "airport",
                "pickup_hour",
                "duration_bin_minutes",
                "bin_trip_count",
            ],
            aggregations={"bin_trip_count": "SUM(`bin_trip_count`)"},
            spec_file="nyc_taxi_airport_quantile_fan.vl.json",
            x=0,
            y=8,
        ),
        custom_widget(
            name="directional_flow_field",
            title="Where each zone sends riders · selected slice",
            dataset_name="directional_flow",
            fields=[
                "pickup_zone",
                "pickup_borough",
                "longitude",
                "latitude",
                "trip_count",
                "distance_total",
                "dx_total",
                "dy_total",
            ],
            aggregations={
                "trip_count": "SUM(`trip_count`)",
                "distance_total": "SUM(`distance_total`)",
                "dx_total": "SUM(`dx_total`)",
                "dy_total": "SUM(`dy_total`)",
            },
            spec_file="nyc_taxi_directional_flow.vl.json",
            x=6,
            y=8,
        ),
        text_widget(
            "od_guide",
            "### 3 · Geographic origin–destination connections\nClick an origin to reveal its six most common destinations in the selected slice. Thicker links mean more trips; warmer links mean longer mean travel time. Lines are relationships, not road routes.",
            x=0,
            y=21,
            width=6,
            height=3,
        ),
        text_widget(
            "parallel_guide",
            "### 4 · Operating regimes\nEach line is one pickup zone in the selected slice. Click a line to isolate how demand, mean duration, fare efficiency, card tipping, and airport exposure co-vary.",
            x=6,
            y=21,
            width=6,
            height=3,
        ),
        custom_widget(
            name="od_connection_map",
            title="Selected-slice OD map · click an origin for its top six destinations",
            dataset_name="od_connection",
            fields=[
                "pickup_borough",
                "origin_zone",
                "destination_zone",
                "origin_longitude",
                "origin_latitude",
                "destination_longitude",
                "destination_latitude",
                "trip_count",
                "duration_total",
            ],
            aggregations={
                "trip_count": "SUM(`trip_count`)",
                "duration_total": "SUM(`duration_total`)",
            },
            spec_file="nyc_taxi_od_connection_map.vl.json",
            x=0,
            y=24,
        ),
        custom_widget(
            name="parallel_coordinates",
            title="Taxi operating regimes · normalized multivariate profiles",
            dataset_name="parallel_coordinates",
            fields=[
                "pickup_zone",
                "pickup_borough",
                "trip_count",
                "duration_total",
                "fare_per_mile_total",
                "fare_per_mile_count",
                "card_trip_count",
                "tipped_card_count",
                "airport_trip_count",
            ],
            aggregations={
                "trip_count": "SUM(`trip_count`)",
                "duration_total": "SUM(`duration_total`)",
                "fare_per_mile_total": "SUM(`fare_per_mile_total`)",
                "fare_per_mile_count": "SUM(`fare_per_mile_count`)",
                "card_trip_count": "SUM(`card_trip_count`)",
                "tipped_card_count": "SUM(`tipped_card_count`)",
                "airport_trip_count": "SUM(`airport_trip_count`)",
            },
            spec_file="nyc_taxi_parallel_coordinates.vl.json",
            x=6,
            y=24,
        ),
        text_widget(
            "horizon_guide",
            "### 5 · Demand horizon\nTen compact time series compare each date with that zone's same-weekday baseline across the selected hours and trip types. Teal = above normal; red = below; darker bands = larger departures.",
            x=0,
            y=37,
            width=6,
            height=3,
        ),
        text_widget(
            "ternary_guide",
            "### 6 · Fare-composition simplex\nEach point aggregates a pickup borough × payment type × airport/non-airport segment in the selected slice. Corners mean 100% meter fare, recorded tip, or other charges; size = trips. Square-root display coordinates spread overlaps; tooltips retain true shares.",
            x=6,
            y=37,
            width=6,
            height=3,
        ),
        custom_widget(
            name="zone_demand_horizon",
            title="Daily demand deviations · top two zones per borough · selected slice",
            dataset_name="horizon",
            fields=[
                "trip_date",
                "pickup_zone",
                "pickup_borough",
                "trip_count",
                "expected_trip_count",
            ],
            aggregations={
                "trip_count": "SUM(`trip_count`)",
                "expected_trip_count": "SUM(`expected_trip_count`)",
            },
            spec_file="nyc_taxi_horizon.vl.json",
            x=0,
            y=40,
            height=14,
        ),
        custom_widget(
            name="fare_composition_ternary",
            title="Fare composition simplex · selected slice · size = trips",
            dataset_name="fare_ternary",
            fields=[
                "pickup_borough",
                "payment_category",
                "airport_flag",
                "trip_count",
                "fare_total",
                "tip_total",
                "extras_total",
            ],
            aggregations={
                "trip_count": "SUM(`trip_count`)",
                "fare_total": "SUM(`fare_total`)",
                "tip_total": "SUM(`tip_total`)",
                "extras_total": "SUM(`extras_total`)",
            },
            spec_file="nyc_taxi_fare_ternary.vl.json",
            x=6,
            y=40,
            height=14,
        ),
        text_widget(
            "method_note",
            "**Filter and method notes.** All controls default to All. The date picker spans the available Jan–Dec 2025 data and supports native absolute/relative presets; relative periods are evaluated against today, so a current 'Last 3 months' can be empty. Figure 1 is airport-only and uses two-minute histogram quantiles. Basemaps use embedded TLC polygons. Double-click clears chart-local selections.",
            x=0,
            y=54,
            width=12,
            height=3,
        ),
    ]

    return {
        "datasets": datasets,
        "pages": [
            {
                "name": "six_custom_views",
                "displayName": "Operations intelligence",
                "pageType": "PAGE_TYPE_CANVAS",
                "layoutVersion": "GRID_V1",
                "layout": layout,
            }
        ],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
            "applyModeEnabled": False,
        },
    }


def main() -> None:
    dashboard = build()
    dashboard_path = ROOT / "dashboards" / "nyc_taxi_custom_viz.lvdash.json"
    payload_path = ROOT / "deployment" / "create_dashboard.json"
    update_payload_path = ROOT / "deployment" / "update_dashboard.json"
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(dashboard, indent=2) + "\n"
    dashboard_path.write_text(serialized, encoding="utf-8")
    payload = {
        "display_name": DISPLAY_NAME,
        "parent_path": PARENT_PATH,
        "serialized_dashboard": serialized,
        "warehouse_id": WAREHOUSE_ID,
    }
    payload_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    update_payload = {
        "display_name": DISPLAY_NAME,
        "serialized_dashboard": serialized,
        "warehouse_id": WAREHOUSE_ID,
    }
    update_payload_path.write_text(
        json.dumps(update_payload, indent=2) + "\n", encoding="utf-8"
    )
    print(dashboard_path)
    print(payload_path)
    print(update_payload_path)


if __name__ == "__main__":
    main()
