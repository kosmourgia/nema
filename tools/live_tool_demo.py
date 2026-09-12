#!/usr/bin/env python3
"""Bounded live Codex dynamic-tool -> interaction -> responder demonstration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.app_server_session import AppServerSession, Message, SessionError
from tools.state import create_interaction, snapshot


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=ROOT / ".nema/nema.db")
    parser.add_argument("--raw", type=Path, default=ROOT / ".nema/probes/live-tool.jsonl")
    parser.add_argument("--report", type=Path, default=ROOT / ".nema/reports/live-tool.json")
    parser.add_argument("--timeout", type=float, default=180.0)
    return parser.parse_args()


def text_from_thread(thread: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for turn in thread.get("turns", []):
        if not isinstance(turn, dict):
            continue
        for item in turn.get("items", []):
            if not isinstance(item, dict):
                continue
            for key in ("text", "content"):
                value = item.get(key)
                if isinstance(value, str):
                    texts.append(value)
    return texts


def main() -> int:
    args = parse_args()
    run_id = uuid.uuid4().hex[:12]
    interaction_id = f"live-tool-{run_id}"
    selected_answer = "candidate-a"
    observed_calls: list[dict[str, Any]] = []

    def handle_server_request(message: Message) -> Message:
        if message.get("method") != "item/tool/call":
            return {
                "error": {
                    "code": -32601,
                    "message": "unsupported live-demo server request",
                    "data": {"method": message.get("method")},
                }
            }
        params = message.get("params")
        if not isinstance(params, dict) or params.get("tool") != "nema_invoke":
            return {
                "result": {
                    "success": False,
                    "contentItems": [
                        {"type": "inputText", "text": "Unexpected dynamic tool call."}
                    ],
                }
            }
        arguments = params.get("arguments")
        if not isinstance(arguments, dict):
            raise ValueError("nema_invoke arguments are not an object")
        if arguments.get("interactionId") != interaction_id:
            raise ValueError("model supplied the wrong interaction id")
        prompt = arguments.get("prompt")
        candidates = arguments.get("candidates")
        if not isinstance(prompt, str) or not isinstance(candidates, list):
            raise ValueError("nema_invoke prompt/candidates do not match the advertised schema")
        normalized_candidates = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                raise ValueError("candidate is not an object")
            normalized_candidates.append(
                {"id": str(candidate.get("id", "")), "label": str(candidate.get("label", ""))}
            )
        observed_calls.append(
            {
                "method": message["method"],
                "tool": params["tool"],
                "callId": params.get("callId"),
                "threadId": params.get("threadId"),
                "turnId": params.get("turnId"),
                "arguments": arguments,
            }
        )
        create_interaction(
            args.db,
            interaction_id,
            prompt,
            normalized_candidates,
            requester=f"codex-dynamic-tool:{params.get('callId')}",
            operation="choose-live-tool-candidate",
        )

        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            current = snapshot(args.db)
            interaction = next(
                item for item in current["interactions"] if item["id"] == interaction_id
            )
            if interaction["status"] == "resolved":
                tool_result = {
                    "interactionId": interaction_id,
                    "answer": interaction["answer"],
                    "resolvedRevision": interaction["resolvedRevision"],
                }
                return {
                    "result": {
                        "success": True,
                        "contentItems": [
                            {
                                "type": "inputText",
                                "text": json.dumps(tool_result, sort_keys=True),
                            }
                        ],
                    }
                }
            time.sleep(0.05)
        return {
            "result": {
                "success": False,
                "contentItems": [
                    {
                        "type": "inputText",
                        "text": json.dumps(
                            {"interactionId": interaction_id, "error": "answer timed out"},
                            sort_keys=True,
                        ),
                    }
                ],
            }
        }

    session = AppServerSession(
        args.db,
        args.raw,
        connection_id=f"live-tool-{run_id}",
        server_request_handler=handle_server_request,
    )
    responder: subprocess.Popen[str] | None = None
    report: dict[str, Any] = {
        "experiment": "live-dynamic-tool-interaction-v1",
        "runId": run_id,
        "interactionId": interaction_id,
        "status": "started",
    }
    exit_code = 1
    try:
        initialize = session.initialize()
        thread_start = session.request(
            "thread/start",
            {
                "cwd": str(ROOT),
                "ephemeral": True,
                "sandbox": "read-only",
                "approvalPolicy": "never",
                "dynamicTools": [
                    {
                        "type": "function",
                        "name": "nema_invoke",
                        "description": (
                            "Publish one identified choice interaction and wait for its "
                            "answer. Use only when explicitly instructed by this experiment."
                        ),
                        "inputSchema": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "interactionId": {"type": "string"},
                                "prompt": {"type": "string"},
                                "candidates": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "properties": {
                                            "id": {"type": "string"},
                                            "label": {"type": "string"},
                                        },
                                        "required": ["id", "label"],
                                    },
                                    "minItems": 1,
                                },
                            },
                            "required": ["interactionId", "prompt", "candidates"],
                        },
                    }
                ],
            },
            timeout=30.0,
        )
        if not isinstance(thread_start, dict) or not isinstance(thread_start.get("thread"), dict):
            raise SessionError("thread/start returned no thread object")
        thread = thread_start["thread"]
        thread_id = thread["id"]
        report.update(
            {
                "threadId": thread_id,
                "effectiveModel": thread_start.get("model"),
                "effectiveModelProvider": thread_start.get("modelProvider"),
                "initializePlatform": initialize.get("platformOs"),
            }
        )

        responder = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "tools/nema_client.py"),
                "--db",
                str(args.db),
                "--interaction",
                interaction_id,
                "--answer",
                selected_answer,
                "--responder",
                "separate-human-client-process",
                "--timeout",
                str(args.timeout),
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        instruction = (
            "This is a bounded protocol integration test. Call the provided "
            "nema_invoke tool exactly once and do not use any other tool. Supply "
            f"interactionId {interaction_id!r}, prompt 'Choose the recorded candidate', "
            "and exactly these candidates in this order: "
            "candidate-a labeled 'Candidate A', candidate-b labeled 'Candidate B'. "
            "After the tool returns, reply with one short sentence naming its answer."
        )
        turn_start = session.request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": instruction}],
            },
            timeout=30.0,
        )
        if not isinstance(turn_start, dict) or not isinstance(turn_start.get("turn"), dict):
            raise SessionError("turn/start returned no turn object")
        turn_id = turn_start["turn"]["id"]
        completed = session.wait_for_notification(
            "turn/completed",
            predicate=lambda message: (
                isinstance(message.get("params"), dict)
                and message["params"].get("threadId") == thread_id
                and isinstance(message["params"].get("turn"), dict)
                and message["params"]["turn"].get("id") == turn_id
            ),
            timeout=args.timeout,
        )
        session.drain_workers(timeout=10.0)
        thread_read = session.request(
            "thread/read", {"threadId": thread_id, "includeTurns": False}, timeout=30.0
        )
        if not isinstance(thread_read, dict) or not isinstance(thread_read.get("thread"), dict):
            raise SessionError("thread/read returned no thread object")

        assert responder.stdout is not None and responder.stderr is not None
        responder_stdout, responder_stderr = responder.communicate(timeout=10.0)
        if responder.returncode != 0:
            raise SessionError(
                f"separate responder exited {responder.returncode}: {responder_stderr.strip()}"
            )
        current = snapshot(args.db)
        interaction = next(
            item for item in current["interactions"] if item["id"] == interaction_id
        )
        report.update(
            {
                "status": "completed",
                "turnId": turn_id,
                "dynamicToolCallCount": len(observed_calls),
                "dynamicToolCalls": observed_calls,
                "interaction": interaction,
                "responderReceipt": json.loads(responder_stdout),
                "threadText": text_from_thread(
                    {"turns": [completed["params"]["turn"]]}
                ),
                "historyReadMode": "metadata-only-ephemeral",
                "rawRecordCount": session.capture.local_sequence,
            }
        )
        exit_code = 0
    except Exception as error:
        report.update(
            {
                "status": "failed",
                "failureType": type(error).__name__,
                "failure": str(error),
                "dynamicToolCallCount": len(observed_calls),
                "dynamicToolCalls": observed_calls,
                "rawRecordCount": session.capture.local_sequence,
            }
        )
    finally:
        if responder is not None and responder.poll() is None:
            responder.terminate()
            try:
                responder.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                responder.kill()
                responder.wait(timeout=2.0)
        report["appServerExitCode"] = session.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    print(
        json.dumps(
            {
                "experiment": report["experiment"],
                "status": report["status"],
                "dynamicToolCallCount": report.get("dynamicToolCallCount", 0),
                "interactionDisposition": report.get("interaction", {}).get("status"),
                "answer": report.get("interaction", {}).get("answer"),
                "rawRecordCount": report.get("rawRecordCount"),
                "failure": report.get("failure"),
                "report": str(args.report),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
