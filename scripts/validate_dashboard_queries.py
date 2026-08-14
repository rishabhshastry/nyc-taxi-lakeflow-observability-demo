#!/usr/bin/env python3
"""Validate dashboard datasets, post-filter aggregations, and live Vega renders."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import vl_convert as vlc
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DASHBOARD = ROOT / "dashboards" / "nyc_taxi_custom_viz.lvdash.json"
AGGREGATE_EXPRESSION = re.compile(r"^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dashboard", type=Path, default=DEFAULT_DASHBOARD)
    return parser.parse_args()


def type_name(column: Any) -> str:
    value = getattr(column.type_name, "value", column.type_name)
    return str(value).upper()


def coerce(value: Any, column: Any) -> Any:
    if value is None:
        return None
    kind = type_name(column)
    if kind in {"BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT"}:
        return int(value)
    if kind in {"FLOAT", "DOUBLE", "DECIMAL"}:
        return float(value)
    if kind == "BOOLEAN":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def result_records(response: Any) -> tuple[list[Any], list[dict[str, Any]]]:
    if not response.manifest or not response.manifest.schema:
        raise RuntimeError("SQL response did not include a result schema")
    if not response.result or response.result.data_array is None:
        raise RuntimeError("SQL response did not include inline rows")
    columns = response.manifest.schema.columns
    records = [
        {
            column.name: coerce(value, column)
            for value, column in zip(row, columns, strict=True)
        }
        for row in response.result.data_array
    ]
    return columns, records


def execute(client: WorkspaceClient, warehouse_id: str, dataset: dict[str, Any], statement: str) -> Any:
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=dataset["catalog"],
        schema=dataset["schema"],
        statement=statement,
        wait_timeout="50s",
    )
    state = response.status.state if response.status else None
    if state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed for {dataset['name']}: {response.status}")
    return response


def widgets(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return [item["widget"] for page in dashboard["pages"] for item in page["layout"]]


def expected_dataset_fields(dashboard: dict[str, Any]) -> dict[str, set[str]]:
    expected = {dataset["name"]: set() for dataset in dashboard["datasets"]}
    for widget in widgets(dashboard):
        for query in widget.get("queries", []):
            expected[query["query"]["datasetName"]].update(
                field["name"] for field in query["query"]["fields"]
            )
    return expected


def custom_widgets_by_dataset(dashboard: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for widget in widgets(dashboard):
        if widget.get("spec", {}).get("widgetType") != "custom-vega-viz":
            continue
        dataset_name = widget["queries"][0]["query"]["datasetName"]
        result[dataset_name] = widget
    return result


def runtime_widget_sql(
    dataset_sql: str, widget: dict[str, Any], where_clause: str | None = None
) -> str:
    query = widget["queries"][0]["query"]
    selections: list[str] = []
    group_by: list[str] = []
    for field in query["fields"]:
        name = field["name"]
        expression = field["expression"]
        selections.append(f"{expression} AS `{name}`")
        if not AGGREGATE_EXPRESSION.match(expression):
            group_by.append(expression)
    statement = f"SELECT {', '.join(selections)} FROM ({dataset_sql}) AS dashboard_dataset"
    if where_clause:
        statement += f" WHERE {where_clause}"
    if group_by and query.get("disaggregated") is False:
        statement += f" GROUP BY {', '.join(group_by)}"
    return statement


def render_custom_widgets(
    dashboard: dict[str, Any], rows_by_dataset: dict[str, list[dict[str, Any]]]
) -> None:
    rendered = 0
    preview_dir = ROOT / "deployment" / "previews"
    preview_dir.mkdir(parents=True, exist_ok=True)
    for widget in widgets(dashboard):
        spec = widget.get("spec", {})
        if spec.get("widgetType") != "custom-vega-viz":
            continue
        dataset_name = widget["queries"][0]["query"]["datasetName"]
        rows = rows_by_dataset[dataset_name]
        if not rows:
            raise RuntimeError(f"{widget['name']} cannot render an empty dataset")
        vega_lite = json.loads(spec["jsonSpec"]["spec"])
        vega_lite["data"] = {"values": rows}
        try:
            svg = vlc.vegalite_to_svg(vega_lite)
        except Exception as error:
            raise RuntimeError(
                f"{widget['name']} failed live-data Vega-Lite rendering: {error}"
            ) from error
        if "<svg" not in svg:
            raise RuntimeError(f"{widget['name']} did not produce SVG")
        (preview_dir / f"{widget['name']}.svg").write_text(svg, encoding="utf-8")
        (preview_dir / f"{widget['name']}.png").write_bytes(
            vlc.vegalite_to_png(vega_lite)
        )
        rendered += 1
        print(f"RENDER PASS {widget['name']}: {len(rows):,} post-filter rows")
    if rendered != 6:
        raise RuntimeError(f"Expected six rendered custom widgets, got {rendered}")
    print(preview_dir)


def main() -> None:
    args = parse_args()
    dashboard = json.loads(args.dashboard.read_text(encoding="utf-8"))
    expected = expected_dataset_fields(dashboard)
    custom_widgets = custom_widgets_by_dataset(dashboard)
    client = WorkspaceClient(profile=args.profile)
    rows_by_dataset: dict[str, list[dict[str, Any]]] = {}
    evidence: dict[str, Any] = {}
    filter_scenario = (
        "pickup_hour IN (8, 9) "
        "AND trip_date BETWEEN DATE '2025-10-01' AND DATE '2025-12-31' "
        "AND pickup_borough = 'Queens' "
        "AND airport_flag = 'Airport'"
    )

    for dataset in dashboard["datasets"]:
        dataset.setdefault("catalog", args.catalog)
        dataset.setdefault("schema", args.schema)
        name = dataset["name"]
        dataset_sql = "".join(dataset["queryLines"])

        schema_response = execute(
            client,
            args.warehouse_id,
            dataset,
            f"SELECT * FROM ({dataset_sql}) AS dashboard_dataset LIMIT 1",
        )
        schema_columns, _ = result_records(schema_response)
        column_names = [column.name for column in schema_columns]
        if set(column_names) != expected[name]:
            raise RuntimeError(
                f"Dataset {name} returned {column_names}, but widget/filter contracts require "
                f"{sorted(expected[name])}"
            )
        column_types = {column.name: type_name(column) for column in schema_columns}
        if column_types["trip_date"] != "DATE":
            raise RuntimeError(f"Dataset {name} trip_date must be DATE")
        if column_types["pickup_hour"] not in {"BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT"}:
            raise RuntimeError(f"Dataset {name} pickup_hour must be integral")

        runtime_sql = runtime_widget_sql(dataset_sql, custom_widgets[name])
        runtime_response = execute(client, args.warehouse_id, dataset, runtime_sql)
        runtime_columns, rows = result_records(runtime_response)
        required_output = [
            field["name"]
            for field in custom_widgets[name]["queries"][0]["query"]["fields"]
        ]
        if [column.name for column in runtime_columns] != required_output:
            raise RuntimeError(f"Runtime field contract failed for {name}")
        rows_by_dataset[name] = rows

        filtered_sql = runtime_widget_sql(
            dataset_sql, custom_widgets[name], where_clause=filter_scenario
        )
        filtered_response = execute(client, args.warehouse_id, dataset, filtered_sql)
        _, filtered_rows = result_records(filtered_response)
        if not filtered_rows:
            raise RuntimeError(f"Four-filter smoke scenario returned no rows for {name}")
        evidence[name] = {
            "source_columns": column_names,
            "source_schema_statement_id": schema_response.statement_id,
            "runtime_rows": len(rows),
            "runtime_statement_id": runtime_response.statement_id,
            "filtered_smoke_rows": len(filtered_rows),
            "filtered_smoke_statement_id": filtered_response.statement_id,
        }
        print(
            f"QUERY PASS {name}: {len(rows):,} runtime rows "
            f"({runtime_response.statement_id})"
        )
        print(
            f"FILTER PASS {name}: {len(filtered_rows):,} rows for Q4 Queens airport "
            f"hours 8–9 ({filtered_response.statement_id})"
        )

    render_custom_widgets(dashboard, rows_by_dataset)
    evidence_path = ROOT / "deployment" / "query_validation.json"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        json.dumps({"warehouse_id": args.warehouse_id, "datasets": evidence}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(f"Validated all {len(dashboard['datasets'])} dataset and runtime queries")
    print(evidence_path)


if __name__ == "__main__":
    main()
