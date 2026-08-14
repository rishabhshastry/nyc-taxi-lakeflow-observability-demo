#!/usr/bin/env python3
"""Execute one SQL file through a Databricks SQL warehouse."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_file", type=Path)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    client = WorkspaceClient(profile=args.profile)
    response = client.statement_execution.execute_statement(
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        statement=args.sql_file.read_text(encoding="utf-8"),
        wait_timeout="50s",
    )
    running = {StatementState.PENDING, StatementState.RUNNING}
    while response.status and response.status.state in running:
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)
    state = response.status.state if response.status else None
    print(f"{args.sql_file}: {state} ({response.statement_id})")
    if state != StatementState.SUCCEEDED:
        raise RuntimeError(response.status)


if __name__ == "__main__":
    main()
