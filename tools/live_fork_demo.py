#!/usr/bin/env python3
"""Bounded native fork/select/reintegrate/replay experiment."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.app_server_session import AppServerSession, Message, SessionError
from tools.state import (
    advance_live_experiment,
    complete_live_delivery,
    mark_live_delivery_started,
    mark_live_delivery_unknown,
    prepare_live_delivery,
    record_live_fork_experiment,
    render_prompt,
    respond,
    snapshot,
)


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=ROOT / ".nema/nema.db")
    parser.add_argument("--raw", type=Path, default=ROOT / ".nema/probes/live-fork.jsonl")
    parser.add_argument("--report", type=Path, default=ROOT / ".nema/reports/live-fork.json")
    parser.add_argument("--timeout", type=float, default=240.0)
    return parser.parse_args()


def turn_text(completed: Message) -> str:
    params = completed.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("turn"), dict):
        return ""
    fragments: list[str] = []
    for item in params["turn"].get("items", []):
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str):
            fragments.append(text)
        content = item.get("content")
        if isinstance(content, str):
            fragments.append(content)
    return "\n".join(fragments)


def expect_thread(response: Any, operation: str) -> dict[str, Any]:
    if not isinstance(response, dict) or not isinstance(response.get("thread"), dict):
        raise SessionError(f"{operation} returned no thread object")
    return response["thread"]


def start_turn(session: AppServerSession, thread_id: str, text: str) -> str:
    response = session.request(
        "turn/start",
        {"threadId": thread_id, "input": [{"type": "text", "text": text}]},
        timeout=30.0,
    )
    if not isinstance(response, dict) or not isinstance(response.get("turn"), dict):
        raise SessionError("turn/start returned no turn object")
    return str(response["turn"]["id"])


def wait_turn(
    session: AppServerSession, thread_id: str, turn_id: str, timeout: float
) -> Message:
    return session.wait_for_notification(
        "turn/completed",
        predicate=lambda message: (
            isinstance(message.get("params"), dict)
            and message["params"].get("threadId") == thread_id
            and isinstance(message["params"].get("turn"), dict)
            and message["params"]["turn"].get("id") == turn_id
        ),
        timeout=timeout,
    )


def main() -> int:
    args = parse_args()
    run_id = uuid.uuid4().hex[:12]
    experiment_id = f"live-fork-{run_id}"
    chooser_receipts: list[dict[str, Any]] = []

    def chooser_handler(message: Message) -> Message:
        if message.get("method") != "item/tool/call":
            return {
                "error": {
                    "code": -32601,
                    "message": "unsupported live-fork server request",
                    "data": {"method": message.get("method")},
                }
            }
        params = message.get("params")
        if not isinstance(params, dict) or params.get("tool") != "nema_invoke":
            raise ValueError("chooser called an unexpected dynamic tool")
        arguments = params.get("arguments")
        if not isinstance(arguments, dict):
            raise ValueError("nema_invoke arguments are not an object")
        interaction_id = str(arguments.get("interactionId", ""))
        answer = str(arguments.get("answer", ""))
        relevant_revision = arguments.get("relevantInputRevision")
        if not isinstance(relevant_revision, int):
            raise ValueError("relevantInputRevision is not an integer")
        receipt = respond(
            args.db,
            interaction_id,
            relevant_revision,
            answer,
            responder=f"chooser-agent:{params.get('threadId')}",
        )
        chooser_receipts.append(receipt)
        return {
            "result": {
                "success": receipt["disposition"] in ("accepted", "identical-retry"),
                "contentItems": [
                    {"type": "inputText", "text": json.dumps(receipt, sort_keys=True)}
                ],
            }
        }

    session = AppServerSession(
        args.db,
        args.raw,
        connection_id=experiment_id,
        server_request_handler=chooser_handler,
    )
    report: dict[str, Any] = {
        "experiment": "native-fork-select-reintegrate-v1",
        "experimentId": experiment_id,
        "status": "started",
    }
    delivery_started = False
    exit_code = 1
    try:
        session.initialize()
        common_thread = {
            "cwd": str(ROOT),
            "sandbox": "read-only",
            "approvalPolicy": "never",
        }
        parent_start = session.request(
            "thread/start", {**common_thread, "ephemeral": False}, timeout=30.0
        )
        parent = expect_thread(parent_start, "thread/start")
        parent_thread_id = str(parent["id"])
        checkpoint_turn_id = start_turn(
            session,
            parent_thread_id,
            (
                "Establish a completed checkpoint for a bounded Nema fork experiment. "
                "In one sentence, say that the checkpoint is ready for comparing two "
                "read-only JSONL parser designs. Do not use tools."
            ),
        )
        checkpoint_completed = wait_turn(
            session, parent_thread_id, checkpoint_turn_id, args.timeout
        )
        checkpoint_text = turn_text(checkpoint_completed)
        source_history = session.request(
            "thread/read",
            {"threadId": parent_thread_id, "includeTurns": True},
            timeout=30.0,
        )
        if not isinstance(source_history, dict):
            raise SessionError("thread/read source history is not an object")

        persisted_fork_response = session.request(
            "thread/fork",
            {
                **common_thread,
                "threadId": parent_thread_id,
                "lastTurnId": checkpoint_turn_id,
                "ephemeral": False,
            },
            timeout=30.0,
        )
        ephemeral_fork_response = session.request(
            "thread/fork",
            {
                **common_thread,
                "threadId": parent_thread_id,
                "lastTurnId": checkpoint_turn_id,
                "ephemeral": True,
                "excludeTurns": True,
            },
            timeout=30.0,
        )
        persisted_fork = expect_thread(persisted_fork_response, "persisted thread/fork")
        ephemeral_fork = expect_thread(ephemeral_fork_response, "ephemeral thread/fork")
        if persisted_fork.get("ephemeral") is not False:
            raise SessionError("persisted fork did not report ephemeral=false")
        if ephemeral_fork.get("ephemeral") is not True:
            raise SessionError("ephemeral fork did not report ephemeral=true")

        persisted_prompt = (
            "From the inherited checkpoint, propose a streaming JSONL parser design for "
            "Nema. In at most 100 words cover exact-byte retention, partial frames, and "
            "malformed input. Do not use tools or edit files."
        )
        ephemeral_prompt = (
            "From the inherited checkpoint, propose an explicit state-machine JSONL parser "
            "design for Nema. In at most 100 words cover exact-byte retention, partial "
            "frames, and malformed input. Do not use tools or edit files."
        )
        persisted_turn_id = start_turn(
            session, str(persisted_fork["id"]), persisted_prompt
        )
        ephemeral_turn_id = start_turn(
            session, str(ephemeral_fork["id"]), ephemeral_prompt
        )
        persisted_completed = wait_turn(
            session, str(persisted_fork["id"]), persisted_turn_id, args.timeout
        )
        ephemeral_completed = wait_turn(
            session, str(ephemeral_fork["id"]), ephemeral_turn_id, args.timeout
        )
        persisted_output = turn_text(persisted_completed)
        ephemeral_output = turn_text(ephemeral_completed)
        if not persisted_output or not ephemeral_output:
            raise SessionError("one or both fork outputs were empty")

        recorded = record_live_fork_experiment(
            args.db,
            experiment_id,
            source_native_thread_id=parent_thread_id,
            source_checkpoint=checkpoint_turn_id,
            source_history=source_history,
            branches=[
                {
                    "id": f"{experiment_id}-persisted",
                    "label": "persisted streaming-parser fork",
                    "nativeThreadId": persisted_fork["id"],
                    "persistenceMode": "persisted",
                    "workspaceMode": "read-only-shared",
                    "prompt": persisted_prompt,
                    "output": persisted_output,
                },
                {
                    "id": f"{experiment_id}-ephemeral",
                    "label": "ephemeral state-machine-parser fork",
                    "nativeThreadId": ephemeral_fork["id"],
                    "persistenceMode": "ephemeral",
                    "workspaceMode": "read-only-shared",
                    "prompt": ephemeral_prompt,
                    "output": ephemeral_output,
                },
            ],
        )
        selection_id = recorded["experiment"]["selectionId"]
        current = snapshot(args.db)
        selection = next(
            item for item in current["interactions"] if item["id"] == selection_id
        )
        chooser_start = session.request(
            "thread/start",
            {
                **common_thread,
                "ephemeral": True,
                "dynamicTools": [
                    {
                        "type": "function",
                        "name": "nema_invoke",
                        "description": "Resolve one currently offered Nema interaction action.",
                        "inputSchema": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "interactionId": {"type": "string"},
                                "relevantInputRevision": {"type": "integer"},
                                "answer": {"type": "string"},
                            },
                            "required": [
                                "interactionId",
                                "relevantInputRevision",
                                "answer",
                            ],
                        },
                    }
                ],
            },
            timeout=30.0,
        )
        chooser = expect_thread(chooser_start, "chooser thread/start")
        chooser_prompt = (
            "You are the separate chooser for a bounded Nema fork experiment. Compare "
            "the two candidate outputs below for observability and replayability. You must "
            "call nema_invoke exactly once with the existing interaction id, its exact "
            "relevant input revision, and one offered artifact id as answer. Do not call "
            "other tools.\n\n"
            + render_prompt(current)
            + "\n\nPersisted streaming candidate:\n"
            + persisted_output
            + "\n\nEphemeral state-machine candidate:\n"
            + ephemeral_output
        )
        chooser_turn_id = start_turn(session, str(chooser["id"]), chooser_prompt)
        chooser_completed = wait_turn(
            session, str(chooser["id"]), chooser_turn_id, args.timeout
        )
        session.drain_workers(timeout=10.0)
        if not chooser_receipts or chooser_receipts[-1]["disposition"] != "accepted":
            raise SessionError("chooser did not resolve the shared selection")
        if selection["relevantInputRevision"] != chooser_receipts[-1]["interaction"][
            "relevantInputRevision"
        ]:
            raise SessionError("chooser resolved a different interaction revision")

        bundle = advance_live_experiment(args.db, experiment_id)
        prepared = prepare_live_delivery(args.db, experiment_id)
        delivery_turn_id = start_turn(
            session, parent_thread_id, prepared["deliveryInput"]
        )
        mark_live_delivery_started(args.db, experiment_id, delivery_turn_id)
        delivery_started = True
        delivery_completed = wait_turn(
            session, parent_thread_id, delivery_turn_id, args.timeout
        )
        parent_output = turn_text(delivery_completed)
        if not parent_output:
            raise SessionError("parent produced no response to the attributed bundle")
        completed_controller = complete_live_delivery(
            args.db,
            experiment_id,
            native_turn_id=delivery_turn_id,
            parent_output=parent_output,
        )
        delivery_started = False

        report.update(
            {
                "status": "completed",
                "effectiveModel": parent_start.get("model"),
                "effectiveModelProvider": parent_start.get("modelProvider"),
                "parentThreadId": parent_thread_id,
                "checkpointTurnId": checkpoint_turn_id,
                "checkpointText": checkpoint_text,
                "persistedForkThreadId": persisted_fork["id"],
                "ephemeralForkThreadId": ephemeral_fork["id"],
                "persistedForkAncestry": persisted_fork.get("forkedFromId"),
                "ephemeralForkAncestry": ephemeral_fork.get("forkedFromId"),
                "persistedOutput": persisted_output,
                "ephemeralOutput": ephemeral_output,
                "chooserThreadId": chooser["id"],
                "chooserOutput": turn_text(chooser_completed),
                "chooserReceipt": chooser_receipts[-1],
                "resultBundle": bundle["resultBundle"],
                "deliveryTurnId": delivery_turn_id,
                "parentOutput": parent_output,
                "controller": completed_controller["experiment"],
                "rawRecordCount": session.capture.local_sequence,
            }
        )
        exit_code = 0
    except Exception as error:
        if delivery_started:
            try:
                mark_live_delivery_unknown(args.db, experiment_id, str(error))
            except Exception:
                pass
        report.update(
            {
                "status": "failed",
                "failureType": type(error).__name__,
                "failure": str(error),
                "rawRecordCount": session.capture.local_sequence,
            }
        )
    finally:
        report["appServerExitCode"] = session.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    print(
        json.dumps(
            {
                "experiment": report["experiment"],
                "experimentId": experiment_id,
                "status": report["status"],
                "effectiveModel": report.get("effectiveModel"),
                "selectedArtifactId": report.get("resultBundle", {}).get(
                    "chosenArtifactId"
                ),
                "deliveryStatus": report.get("controller", {}).get("deliveryStatus"),
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
