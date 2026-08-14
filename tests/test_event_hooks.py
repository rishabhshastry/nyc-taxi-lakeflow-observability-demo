from __future__ import annotations

import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class _FakeConf:
    def __init__(self) -> None:
        self.values = {
            "monitoring.hook.enabled": "true",
            "monitoring.environment": "integration",
            "monitoring.workspace_url": "",
            "spark.databricks.workspaceUrl": "example.cloud.databricks.com",
            "monitoring.hook.emit_flow_metrics": "true",
            "monitoring.hook.expected_writer_flows": (
                "yellow_trips_2025_raw,yellow_trips_2025,taxi_zones,"
                "mart_od_connection_map"
            ),
            "monitoring.hook.streaming_flows": (
                "yellow_trips_2025_raw,yellow_trips_2025"
            ),
        }

    def get(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)


class _FakeSpark:
    conf = _FakeConf()


def _identity_hook(**_kwargs):
    return lambda function: function


class EventHookTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fake_pyspark = types.ModuleType("pyspark")
        fake_pyspark.pipelines = types.SimpleNamespace(on_event_hook=_identity_hook)
        path = (
            Path(__file__).parents[1]
            / "src/nyc_taxi_pipeline/transformations/monitoring/event_hooks.py"
        )
        with patch.dict(sys.modules, {"pyspark": fake_pyspark}):
            cls.module = runpy.run_path(
                str(path),
                init_globals={
                    "spark": _FakeSpark(),
                    "dbutils": types.SimpleNamespace(),
                },
            )

    def event(
        self,
        flow: str,
        metrics: dict | None = None,
        *,
        status: str = "COMPLETED",
        timestamp: str = "2026-08-14T22:20:14.431Z",
    ) -> dict:
        progress = {"status": status}
        if metrics is not None:
            progress["metrics"] = metrics
        return {
            "id": "event-123",
            "event_type": "flow_progress",
            "timestamp": timestamp,
            "message": f"Flow '{flow}' is {status}.",
            "origin": {
                "pipeline_id": "pipeline-123",
                "pipeline_name": "Taxi pipeline",
                "update_id": "update-123",
                "flow_name": f"catalog.schema.{flow}",
            },
            "details": {"flow_progress": progress},
        }

    def test_block_kit_summary_uses_short_flow_and_available_metrics(self) -> None:
        payload = self.module["_normalized_event"](
            self.event("mart_od_connection_map", {"num_output_rows": 388_844})
        )
        self.assertEqual("Gold", payload["layer"])
        self.assertEqual("mart_od_connection_map", payload["flow_key"])
        self.assertEqual(
            "https://example.cloud.databricks.com/pipelines/pipeline-123/updates/update-123",
            payload["diagnostic_url"],
        )

        body = self.module["_slack_body"](payload)
        rendered = str(body)
        self.assertIn("388,844", rendered)
        self.assertIn("bounded refresh", rendered)
        self.assertIn("Open pipeline update", rendered)
        self.assertNotIn("schema_version", rendered)

    def test_monitored_flow_emits_even_when_metrics_are_unavailable(self) -> None:
        payload = self.module["_normalized_event"](self.event("taxi_zones"))
        self.assertIsNotNone(payload)
        body = self.module["_slack_body"](payload)
        self.assertIn("Not reported", str(body))

    def test_derived_throughput_and_runtime_are_reported(self) -> None:
        self.assertIsNone(
            self.module["_normalized_event"](
                self.event(
                    "mart_od_connection_map",
                    status="STARTING",
                    timestamp="2026-08-14T22:20:00.000Z",
                )
            )
        )
        payload = self.module["_normalized_event"](
            self.event(
                "mart_od_connection_map",
                {"num_output_rows": 388_844},
                timestamp="2026-08-14T22:20:10.000Z",
            )
        )
        self.assertEqual(10.0, payload["metrics"]["observed_duration_seconds"])
        self.assertEqual(
            38_884.4, payload["metrics"]["avg_output_rows_per_second"]
        )
        rendered = str(self.module["_slack_body"](payload))
        self.assertIn("38,884.40 rows/s", rendered)
        self.assertIn("average output (derived)", rendered)
        self.assertIn("Observed runtime", rendered)

    def test_streaming_completion_reports_drained_trigger(self) -> None:
        payload = self.module["_normalized_event"](
            self.event("yellow_trips_2025", {"num_output_rows": 1_000})
        )
        rendered = str(self.module["_slack_body"](payload))
        self.assertIn("Drained", rendered)
        self.assertIn("native pending count not emitted", rendered)

    def test_fully_qualified_zero_output_flow_is_warning(self) -> None:
        payload = self.module["_normalized_event"](
            self.event("yellow_trips_2025", {"num_output_rows": 0})
        )
        self.assertEqual("warning", payload["severity"])
        self.assertEqual("unexpected_zero_output", payload["condition"])

    def test_unmonitored_info_flow_without_metrics_is_suppressed(self) -> None:
        payload = self.module["_normalized_event"](self.event("internal_helper"))
        self.assertIsNone(payload)


if __name__ == "__main__":
    unittest.main()
