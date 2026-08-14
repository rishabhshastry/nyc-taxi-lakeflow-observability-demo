#!/usr/bin/env python3
"""Validate the static dashboard, layout, and custom-widget contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import vl_convert as vlc

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "dashboards" / "nyc_taxi_custom_viz.lvdash.json"


def assert_no_layout_overlap(page: dict[str, Any]) -> None:
    occupied: set[tuple[int, int]] = set()
    for item in page["layout"]:
        position = item["position"]
        if position["x"] < 0 or position["x"] + position["width"] > 12:
            raise AssertionError(f"{item['widget']['name']} falls outside the 12-column grid")
        for x in range(position["x"], position["x"] + position["width"]):
            for y in range(position["y"], position["y"] + position["height"]):
                cell = (x, y)
                if cell in occupied:
                    raise AssertionError(f"Layout overlap on {page['name']} at {cell}")
                occupied.add(cell)


def main() -> None:
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    datasets = {item["name"]: item for item in dashboard["datasets"]}
    expected_pages = ["six_custom_views"]
    if [item["name"] for item in dashboard["pages"]] != expected_pages:
        raise AssertionError("Dashboard must contain exactly one canvas with all six views")

    for item in datasets.values():
        sql = "".join(item["queryLines"])
        if ";" in sql or "${" in sql:
            raise AssertionError(f"Unsafe or unresolved SQL in {item['name']}")
        has_catalog = item.get("catalog") is not None
        has_schema = item.get("schema") is not None
        if has_catalog != has_schema:
            raise AssertionError(
                f"Dataset {item['name']} must set both catalog and schema or rely on bundle defaults"
            )

    custom_count = 0
    expected_filters = {
        "pickup_hour_filter": ("filter-multi-select", "pickup_hour"),
        "trip_date_filter": ("filter-date-range-picker", "trip_date"),
        "pickup_borough_filter": ("filter-multi-select", "pickup_borough"),
        "airport_flag_filter": ("filter-multi-select", "airport_flag"),
    }
    found_filters: set[str] = set()
    map_widgets = {"directional_flow_field", "od_connection_map"}
    for page in dashboard["pages"]:
        if page.get("layoutVersion") != "GRID_V1":
            raise AssertionError(f"{page['name']} does not use GRID_V1")
        assert_no_layout_overlap(page)
        for item in page["layout"]:
            widget = item["widget"]
            spec = widget.get("spec", {})
            if str(spec.get("widgetType", "")).startswith("filter-"):
                name = widget["name"]
                if name not in expected_filters:
                    raise AssertionError(f"Unexpected filter {name}")
                expected_type, expected_field = expected_filters[name]
                if spec["widgetType"] != expected_type:
                    raise AssertionError(f"{name} must use {expected_type}")
                if spec.get("selection") or spec.get("disallowAll"):
                    raise AssertionError(f"{name} must default to All")
                if spec.get("frame", {}).get("showTitle") is not True:
                    raise AssertionError(f"{name} must show its title")
                filtered_datasets = {
                    query["query"]["datasetName"] for query in widget["queries"]
                }
                if filtered_datasets != set(datasets):
                    raise AssertionError(f"{name} must bind all six datasets")
                for query in widget["queries"]:
                    query_fields = query["query"]["fields"]
                    if query_fields != [
                        {"name": expected_field, "expression": f"`{expected_field}`"}
                    ]:
                        raise AssertionError(f"{name} has an invalid field binding")
                    if query["query"].get("disaggregated") is not False:
                        raise AssertionError(f"{name} filter queries must aggregate values")
                found_filters.add(name)
            if spec.get("widgetType") != "custom-vega-viz":
                continue
            custom_count += 1
            if item["position"]["x"] not in {0, 6} or item["position"]["width"] != 6:
                raise AssertionError(f"{widget['name']} must occupy one half of the 12-column grid")
            if item["position"]["height"] < 13:
                raise AssertionError(f"{widget['name']} is too short for a hero visual")
            if spec.get("version") != 1 or spec.get("jsonSpec", {}).get("type") != "vega-lite":
                raise AssertionError(f"{widget['name']} has an invalid custom-viz contract")
            if spec.get("data", {}).get("queryName") != "main_query":
                raise AssertionError(f"{widget['name']} is not bound to main_query")
            if len(widget.get("queries", [])) != 1 or widget["queries"][0]["name"] != "main_query":
                raise AssertionError(f"{widget['name']} must define exactly one main_query")
            query = widget["queries"][0]["query"]
            if query["datasetName"] not in datasets:
                raise AssertionError(f"{widget['name']} references an unknown dataset")
            query_fields = {field["name"] for field in query["fields"]}
            encoded_fields = {field["fieldName"] for field in spec["encodings"]["fields"]}
            if query_fields != encoded_fields:
                raise AssertionError(f"Field contract mismatch in {widget['name']}")
            if query.get("disaggregated") is not False:
                raise AssertionError(f"{widget['name']} must aggregate after page filters")
            vega_lite = json.loads(spec["jsonSpec"]["spec"])
            if vega_lite.get("data", {}).get("name") != "databricks_query":
                raise AssertionError(f"{widget['name']} must bind databricks_query")
            if widget["name"] in map_widgets:
                basemap = vega_lite["layer"][0]
                if basemap.get("mark", {}).get("type") != "geoshape":
                    raise AssertionError(f"{widget['name']} lacks a geoshape basemap")
                if len(basemap.get("data", {}).get("values", [])) < 300:
                    raise AssertionError(f"{widget['name']} lacks embedded TLC polygons")
            try:
                vlc.vegalite_to_vega(vega_lite)
                svg = vlc.vegalite_to_svg(vega_lite)
            except Exception as error:
                raise AssertionError(f"{widget['name']} failed Vega-Lite validation: {error}") from error
            if "<svg" not in svg:
                raise AssertionError(f"{widget['name']} did not render SVG")

    if custom_count != 6:
        raise AssertionError(f"Expected six custom visualizations, found {custom_count}")
    if found_filters != set(expected_filters):
        raise AssertionError(f"Expected four page filters, found {sorted(found_filters)}")
    if len(datasets) != 6:
        raise AssertionError(f"Expected six dashboard datasets, found {len(datasets)}")
    print("Validated 1 page, 4 All-default filters, 6 datasets, and 6 custom Vega-Lite widgets")


if __name__ == "__main__":
    main()
