"""Selective, best-effort SDP event delivery to Slack or an HTTPS relay."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark import pipelines as dp

HOOK_ENABLED = spark.conf.get("monitoring.hook.enabled", "false").lower() == "true"
DESTINATION_TYPE = spark.conf.get("monitoring.hook.destination_type", "slack").lower()
SECRET_SCOPE = spark.conf.get("monitoring.hook.secret_scope", "")
SECRET_KEY = spark.conf.get("monitoring.hook.secret_key", "")
ENVIRONMENT = spark.conf.get("monitoring.environment", "unknown")
WORKSPACE_URL = spark.conf.get("monitoring.workspace_url", "").rstrip("/")
EMIT_FLOW_METRICS = (
    spark.conf.get("monitoring.hook.emit_flow_metrics", "false").lower() == "true"
)
DQ_FAILURE_RATIO_WARN = float(
    spark.conf.get("monitoring.hook.dq_failure_ratio_warn", "0.01")
)
DQ_MIN_RECORDS = int(spark.conf.get("monitoring.hook.dq_min_records", "1000"))
EXPECTED_WRITER_FLOWS = {
    item.strip()
    for item in spark.conf.get("monitoring.hook.expected_writer_flows", "").split(",")
    if item.strip()
}

FAILURE_STATES = {"FAILED", "ERROR", "CANCELED", "CANCELLED", "STOPPED"}
WARNING_STATES = {"SKIPPED", "EXCLUDED"}
TERMINAL_STATES = FAILURE_STATES | WARNING_STATES | {"COMPLETED", "SUCCEEDED"}
METRIC_KEYS = {
    "num_input_rows",
    "num_output_rows",
    "num_output_bytes",
    "num_upserted_rows",
    "num_deleted_rows",
    "backlog_records",
    "backlog_bytes",
    "backlog_files",
}


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _sanitize(value: Any, limit: int = 500) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def _collect_metrics(value: Any, output: dict[str, float | int]) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key in METRIC_KEYS and isinstance(nested, (int, float)):
                output.setdefault(key, nested)
            else:
                _collect_metrics(nested, output)
    elif isinstance(value, list):
        for nested in value:
            _collect_metrics(nested, output)


def _expectations(details: dict[str, Any]) -> list[dict[str, Any]]:
    progress = _mapping(details.get("flow_progress"))
    data_quality = _mapping(progress.get("data_quality"))
    raw = data_quality.get("expectations", [])
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        expectation = _mapping(item)
        passed = expectation.get("passed_records")
        failed = expectation.get("failed_records")
        if not isinstance(passed, (int, float)) or not isinstance(
            failed, (int, float)
        ):
            continue
        total = passed + failed
        normalized.append(
            {
                "name": _sanitize(expectation.get("name"), 120),
                "dataset": _sanitize(expectation.get("dataset"), 120),
                "passed_records": passed,
                "failed_records": failed,
                "failure_ratio": (failed / total) if total else None,
            }
        )
    return normalized


def _state(event_type: str, details: dict[str, Any]) -> str:
    progress = _mapping(details.get(event_type))
    return _sanitize(progress.get("state") or progress.get("status"), 50).upper()


def _normalized_event(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = _sanitize(event.get("event_type"), 80)
    if event_type not in {"update_progress", "flow_progress"}:
        return None

    origin = _mapping(event.get("origin"))
    details = _mapping(event.get("details"))
    state = _state(event_type, details)
    flow_name = _sanitize(
        origin.get("flow_name")
        or origin.get("dataset_name")
        or _mapping(details.get("flow_progress")).get("name"),
        160,
    )
    pipeline_id = _sanitize(origin.get("pipeline_id"), 100)
    pipeline_name = _sanitize(origin.get("pipeline_name"), 160)
    update_id = _sanitize(origin.get("update_id"), 100)
    event_id = _sanitize(event.get("id"), 120)
    metrics: dict[str, float | int] = {}
    _collect_metrics(details, metrics)
    expectations = _expectations(details)

    severity = "info"
    condition = "flow_metrics" if event_type == "flow_progress" else "update_state"
    reasons: list[str] = []

    if state in FAILURE_STATES:
        severity = "critical"
        condition = f"{event_type}_failed"
        reasons.append(f"state={state}")
    elif state in WARNING_STATES:
        severity = "warning"
        condition = f"{event_type}_unexpected_terminal_state"
        reasons.append(f"state={state}")

    high_failure_expectations = [
        item
        for item in expectations
        if item["failure_ratio"] is not None
        and item["passed_records"] + item["failed_records"] >= DQ_MIN_RECORDS
        and item["failure_ratio"] >= DQ_FAILURE_RATIO_WARN
    ]
    if high_failure_expectations and severity != "critical":
        severity = "warning"
        condition = "expectation_failure_ratio"
        reasons.append(
            f"{len(high_failure_expectations)} expectation(s) exceeded "
            f"{DQ_FAILURE_RATIO_WARN:.2%}"
        )

    output_rows = metrics.get("num_output_rows")
    if (
        event_type == "flow_progress"
        and flow_name in EXPECTED_WRITER_FLOWS
        and state in TERMINAL_STATES
        and output_rows == 0
        and severity == "info"
    ):
        severity = "warning"
        condition = "unexpected_zero_output"
        reasons.append("num_output_rows=0")

    should_emit = severity != "info" or (
        EMIT_FLOW_METRICS
        and event_type == "flow_progress"
        and state in TERMINAL_STATES
        and bool(metrics or expectations)
    )
    if not should_emit:
        return None

    diagnostic_url = f"{WORKSPACE_URL}/pipelines/{pipeline_id}" if pipeline_id else ""
    if diagnostic_url and update_id:
        diagnostic_url = f"{diagnostic_url}/updates/{update_id}"
    message = _sanitize(event.get("message"), 500)
    if reasons:
        message = _sanitize(f"{'; '.join(reasons)}. {message}", 500)

    return {
        "schema_version": "1.0",
        "source": "sdp_event_hook",
        "event_id": event_id,
        "idempotency_key": f"{event_id}:{DESTINATION_TYPE}",
        "destination_key": DESTINATION_TYPE,
        "environment": ENVIRONMENT,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_name,
        "update_id": update_id,
        "flow_name": flow_name,
        "event_type": event_type,
        "state": state,
        "severity": severity,
        "condition": condition,
        "event_time": _sanitize(event.get("timestamp"), 80),
        "observed_time": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
        "expectations": expectations,
        "message": message,
        "monitor_key": ":".join(
            item
            for item in (pipeline_id or pipeline_name, flow_name, condition)
            if item
        ),
        "diagnostic_url": diagnostic_url,
    }


def _slack_body(payload: dict[str, Any]) -> dict[str, Any]:
    identity = payload["flow_name"] or payload["pipeline_name"] or "SDP pipeline"
    summary = (
        f"[{payload['severity'].upper()}] {identity}: {payload['condition']}"
        f" ({payload['state'] or 'METRICS'})"
    )
    details = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {"text": f"{summary}\n```{details[:3200]}```"}


@dp.on_event_hook(max_allowable_consecutive_failures=3)
def send_granular_pipeline_alerts(event: dict[str, Any]) -> None:
    if not HOOK_ENABLED:
        return

    payload = _normalized_event(event)
    if payload is None:
        return

    if not SECRET_SCOPE or not SECRET_KEY:
        raise ValueError("Event-hook Slack secret scope/key must be configured")
    webhook_url = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    body = _slack_body(payload) if DESTINATION_TYPE == "slack" else payload
    request = Request(
        webhook_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "nyc-taxi-sdp-event-hook/1.0",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            response.read(1024)
    except HTTPError as error:
        # Slack returns concise, non-secret reason strings such as
        # ``action_prohibited`` or ``no_active_hooks``. Preserve that signal in
        # hook_progress while never logging the secret-backed webhook URL.
        response_body = _sanitize(error.read(1024).decode("utf-8", "replace"), 200)
        reason = response_body or _sanitize(error.reason, 200) or "unknown"
        raise RuntimeError(
            f"Webhook destination returned HTTP {error.code}: {reason}"
        ) from error
