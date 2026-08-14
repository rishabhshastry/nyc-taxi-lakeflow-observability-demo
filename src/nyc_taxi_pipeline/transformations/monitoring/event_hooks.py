"""Selective, best-effort SDP event delivery to Slack or an HTTPS relay."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from threading import Lock
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark import pipelines as dp

# CUSTOMER CHANGE POINT: these values are injected by resources/nyc_taxi.pipeline.yml.
# Job lifecycle notifications use a Databricks destination ID, while this hook
# requires a separate incoming-webhook URL stored in the configured secret.
# Never place the webhook value in source code or bundle YAML.
HOOK_ENABLED = spark.conf.get("monitoring.hook.enabled", "false").lower() == "true"
DESTINATION_TYPE = spark.conf.get("monitoring.hook.destination_type", "slack").lower()
SECRET_SCOPE = spark.conf.get("monitoring.hook.secret_scope", "")
SECRET_KEY = spark.conf.get("monitoring.hook.secret_key", "")
ENVIRONMENT = spark.conf.get("monitoring.environment", "unknown")
WORKSPACE_URL = (
    spark.conf.get("monitoring.workspace_url", "").strip()
    or spark.conf.get("spark.databricks.workspaceUrl", "").strip()
)
if WORKSPACE_URL and not WORKSPACE_URL.startswith(("http://", "https://")):
    WORKSPACE_URL = f"https://{WORKSPACE_URL}"
WORKSPACE_URL = WORKSPACE_URL.rstrip("/")
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
STREAMING_FLOWS = {
    item.strip()
    for item in spark.conf.get("monitoring.hook.streaming_flows", "").split(",")
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
    "input_rows_per_second",
    "processed_rows_per_second",
}

_FLOW_TIMINGS: dict[tuple[str, str, str], dict[str, datetime]] = {}
_FLOW_TIMINGS_LOCK = Lock()


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


def _flow_key(flow_name: str) -> str:
    return flow_name.rsplit(".", 1)[-1]


def _flow_layer(flow_key: str) -> str:
    if flow_key.endswith("_raw"):
        return "Bronze"
    if flow_key.startswith("mart_"):
        return "Gold"
    if flow_key in {
        "trips_enriched_with_rejection_reasons",
        "yellow_trips_2025_dq_metrics",
        "taxi_zones_contract",
    }:
        return "Silver / DQ"
    if flow_key in {
        "yellow_trips_2025",
        "yellow_trips_2025_quarantine",
        "taxi_zones",
    }:
        return "Silver"
    return "Pipeline"


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _add_observed_flow_metrics(
    *,
    timing_key: tuple[str, str, str],
    state: str,
    event_time: datetime | None,
    metrics: dict[str, float | int],
) -> None:
    """Add rates derived from SDP lifecycle timestamps to terminal flow events."""
    if not all(timing_key) or event_time is None:
        return

    with _FLOW_TIMINGS_LOCK:
        timing = _FLOW_TIMINGS.setdefault(timing_key, {})
        if state == "QUEUED":
            timing.setdefault("queued_at", event_time)
            return
        if state == "STARTING":
            timing.setdefault("started_at", event_time)
            return
        if state == "RUNNING":
            # STARTING is normally present; RUNNING is a safe fallback when it is not.
            timing.setdefault("started_at", event_time)
            return
        if state not in TERMINAL_STATES:
            return
        timing = _FLOW_TIMINGS.pop(timing_key, timing)

    started_at = timing.get("started_at") or timing.get("queued_at")
    queued_at = timing.get("queued_at")
    if started_at is not None:
        duration_seconds = max((event_time - started_at).total_seconds(), 0.001)
        metrics.setdefault("observed_duration_seconds", duration_seconds)
        output_rows = metrics.get("num_output_rows")
        if isinstance(output_rows, (int, float)):
            metrics.setdefault(
                "avg_output_rows_per_second", output_rows / duration_seconds
            )
    if queued_at is not None and started_at is not None:
        metrics.setdefault(
            "observed_queue_seconds",
            max((started_at - queued_at).total_seconds(), 0.0),
        )


def _format_number(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "Not reported"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.2f}"
    return f"{int(value):,}"


def _format_bytes(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "Not reported"
    amount = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit = units[0]
    for unit in units:
        if abs(amount) < 1024 or unit == units[-1]:
            break
        amount /= 1024
    return f"{amount:,.1f} {unit}"


def _format_duration(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "Not reported"
    if value < 60:
        return f"{value:,.1f} s"
    minutes, seconds = divmod(value, 60)
    return f"{int(minutes):,}m {seconds:,.1f}s"


def _throughput_text(metrics: dict[str, Any]) -> str:
    processed_rate = metrics.get("processed_rows_per_second")
    input_rate = metrics.get("input_rows_per_second")
    derived_rate = metrics.get("avg_output_rows_per_second")
    duration = metrics.get("observed_duration_seconds")

    if isinstance(processed_rate, (int, float)):
        rate_text = f"`{_format_number(processed_rate)} rows/s` processed (native)"
    elif isinstance(input_rate, (int, float)):
        rate_text = f"`{_format_number(input_rate)} rows/s` input (native)"
    elif isinstance(derived_rate, (int, float)):
        rate_text = f"`{_format_number(derived_rate)} rows/s` average output (derived)"
    else:
        rate_text = "Row rate unavailable; SDP emitted no row count"

    if isinstance(duration, (int, float)):
        rate_text += f"\nObserved runtime: `{_format_duration(duration)}`"
    return rate_text


def _backlog_text(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    backlog_values = [
        metrics.get("backlog_records"),
        metrics.get("backlog_files"),
        metrics.get("backlog_bytes"),
    ]
    if any(value is not None for value in backlog_values):
        return (
            f"Records: `{_format_number(backlog_values[0])}`\n"
            f"Files: `{_format_number(backlog_values[1])}`\n"
            f"Bytes: `{_format_bytes(backlog_values[2])}` (native)"
        )
    if payload["flow_key"] in STREAMING_FLOWS:
        if payload["state"] in {"COMPLETED", "SUCCEEDED"}:
            return "`Drained` for this triggered update; native pending count not emitted"
        return "Streaming flow; native pending count not emitted"
    return "`N/A` — bounded refresh, not a streaming source"


def _slack_escape(value: Any) -> str:
    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
    flow_key = _flow_key(flow_name)
    pipeline_id = _sanitize(origin.get("pipeline_id"), 100)
    pipeline_name = _sanitize(origin.get("pipeline_name"), 160)
    update_id = _sanitize(origin.get("update_id"), 100)
    event_id = _sanitize(event.get("id"), 120)
    metrics: dict[str, float | int] = {}
    _collect_metrics(details, metrics)
    parsed_event_time = _parse_timestamp(event.get("timestamp"))
    _add_observed_flow_metrics(
        timing_key=(pipeline_id, update_id, flow_name),
        state=state,
        event_time=parsed_event_time,
        metrics=metrics,
    )
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
        and flow_key in EXPECTED_WRITER_FLOWS
        and state in TERMINAL_STATES
        and output_rows == 0
        and severity == "info"
    ):
        severity = "warning"
        condition = "unexpected_zero_output"
        reasons.append("num_output_rows=0")

    monitored_flow = flow_key in EXPECTED_WRITER_FLOWS
    should_emit = severity != "info" or (
        EMIT_FLOW_METRICS
        and event_type == "flow_progress"
        and state in TERMINAL_STATES
        and monitored_flow
    )
    if not should_emit:
        return None

    diagnostic_url = (
        f"{WORKSPACE_URL}/pipelines/{pipeline_id}"
        if WORKSPACE_URL and pipeline_id
        else ""
    )
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
        "flow_key": flow_key,
        "layer": _flow_layer(flow_key),
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
    severity = payload["severity"]
    emoji = {"critical": "🚨", "warning": "⚠️", "info": "✅"}.get(severity, "ℹ️")
    flow_key = payload["flow_key"] or payload["pipeline_name"] or "SDP pipeline"
    state = payload["state"] or "METRICS"
    metrics = payload["metrics"]
    expectations = payload["expectations"]
    output_rows = metrics.get("num_output_rows")
    summary = (
        f"{emoji} [{payload['environment']}] {payload['layer']} / {flow_key}: "
        f"{state}"
    )
    if output_rows is not None:
        summary += f" — {_format_number(output_rows)} output rows"

    throughput_text = _throughput_text(metrics)
    backlog_text = _backlog_text(payload)

    if expectations:
        passed = sum(item["passed_records"] for item in expectations)
        failed = sum(item["failed_records"] for item in expectations)
        failed_rules = [item for item in expectations if item["failed_records"] > 0]
        dq_text = f"Passed: `{_format_number(passed)}` • Failed: `{_format_number(failed)}`"
        if failed_rules:
            rule_names = ", ".join(item["name"] for item in failed_rules[:5])
            dq_text += f"\nFailing rules: `{_slack_escape(rule_names)}`"
    else:
        dq_text = "No expectations attached or reported for this flow event"

    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} {payload['layer']} flow {state.lower()}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Flow*\n`{_slack_escape(flow_key)}`"},
                {"type": "mrkdwn", "text": f"*Environment*\n`{_slack_escape(payload['environment'])}`"},
                {"type": "mrkdwn", "text": f"*State*\n`{_slack_escape(state)}`"},
                {"type": "mrkdwn", "text": f"*Severity*\n`{_slack_escape(severity.upper())}`"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Input rows*\n`{_format_number(metrics.get('num_input_rows'))}`"},
                {"type": "mrkdwn", "text": f"*Output rows*\n`{_format_number(output_rows)}`"},
                {"type": "mrkdwn", "text": f"*Output bytes*\n`{_format_bytes(metrics.get('num_output_bytes'))}`"},
                {"type": "mrkdwn", "text": f"*Throughput*\n{throughput_text}"},
                {"type": "mrkdwn", "text": f"*Backlog status*\n{backlog_text}"},
                {"type": "mrkdwn", "text": f"*Data quality*\n{dq_text}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Update `{_slack_escape(payload['update_id'])}` • "
                        f"Event `{_slack_escape(payload['event_time'])}`"
                    ),
                }
            ],
        },
    ]
    if payload["diagnostic_url"].startswith("https://"):
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Open pipeline update"},
                        "url": payload["diagnostic_url"],
                    }
                ],
            }
        )
    return {
        "text": summary,
        "blocks": blocks,
        "unfurl_links": False,
        "unfurl_media": False,
    }


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
