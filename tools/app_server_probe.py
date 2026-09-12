#!/usr/bin/env python3
"""Bounded initialize/feature probe over the reusable durable session."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.app_server_session import AppServerSession, Message


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True, help="live raw journal database")
    parser.add_argument("--raw", type=Path, required=True, help="ignored raw JSONL capture")
    parser.add_argument("--summary", type=Path, required=True, help="sanitized JSON report")
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--connection-id", default=None)
    parser.add_argument("--connection-epoch", type=int, default=1)
    parser.add_argument(
        "--server-request-delay",
        type=float,
        default=0.0,
        help="fixture aid: delay unsupported request disposition without blocking routing",
    )
    parser.add_argument(
        "--command",
        nargs="+",
        default=["codex", "app-server", "--stdio"],
        help="server command (default: installed Codex app-server)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection_id = args.connection_id or f"probe-{uuid.uuid4()}"

    def unsupported(message: Message) -> Message:
        if args.server_request_delay > 0:
            time.sleep(args.server_request_delay)
        return {
            "error": {
                "code": -32601,
                "message": "unsupported by Nema capability probe",
                "data": {"method": message.get("method")},
            }
        }

    session = AppServerSession(
        args.db,
        args.raw,
        command=args.command,
        connection_id=connection_id,
        connection_epoch=args.connection_epoch,
        server_request_handler=unsupported,
    )
    initialize_result: dict[str, Any] | None = None
    features_result: dict[str, Any] | None = None
    failure: str | None = None
    try:
        initialize_result = session.initialize(timeout=args.timeout)
        result = session.request(
            "experimentalFeature/list",
            {},
            timeout=args.timeout,
            identifier="nema-feature-list",
        )
        if not isinstance(result, dict):
            raise ValueError("experimentalFeature/list result is not an object")
        features_result = result
        session.drain_workers(timeout=args.timeout)
    except Exception as error:
        failure = f"{type(error).__name__}: {error}"
    process_exit_code = session.close()

    sanitized_initialize = None
    if initialize_result is not None:
        sanitized_initialize = {
            key: value for key, value in initialize_result.items() if key != "codexHome"
        }
        if "codexHome" in initialize_result:
            sanitized_initialize["codexHome"] = "<redacted-machine-path>"
            sanitized_initialize["codexHomeRedacted"] = True

    feature_rows: list[dict[str, Any]] = []
    if features_result is not None and isinstance(features_result.get("data"), list):
        for feature in features_result["data"]:
            if isinstance(feature, dict):
                feature_rows.append(
                    {
                        key: feature.get(key)
                        for key in ("name", "stage", "enabled", "defaultEnabled")
                    }
                )

    summary = {
        "schemaVersion": 1,
        "probe": "codex-app-server-initialize-and-feature-list",
        "command": args.command,
        "experimentalApiRequested": True,
        "connectionId": connection_id,
        "connectionEpoch": args.connection_epoch,
        "initialize": sanitized_initialize,
        "features": feature_rows,
        "rawCapture": str(args.raw),
        "rawRecordCount": session.capture.local_sequence,
        "decodedMessageCount": len(session.decoded_messages),
        "serverRequestCount": session.server_request_count,
        "pendingServerRequestCount": session.pending_workers,
        "processExitCode": process_exit_code,
        "failure": failure,
    }
    write_json(args.summary, summary)
    print(
        json.dumps(
            {
                "probe": summary["probe"],
                "connectionId": summary["connectionId"],
                "rawRecordCount": summary["rawRecordCount"],
                "decodedMessageCount": summary["decodedMessageCount"],
                "serverRequestCount": summary["serverRequestCount"],
                "featureCount": len(feature_rows),
                "processExitCode": summary["processExitCode"],
                "failure": summary["failure"],
                "summary": str(args.summary),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if failure is None else 1


if __name__ == "__main__":
    sys.exit(main())
