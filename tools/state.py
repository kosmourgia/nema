#!/usr/bin/env python3
"""Durable interaction commands and versioned observer surfaces.

This module is the SQLite boundary for the Flix interaction reducer. Accepted
commands and rejected attempts are retained as immutable domain events; the
``interactions`` table is a rebuildable current-state projection. Raw Codex
frames remain in the separate raw journal tables.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any
import uuid


STATE_SCHEMA_VERSION = "interaction-v1"


SCHEMA = """
CREATE TABLE IF NOT EXISTS domain_events (
    revision INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS interactions (
    interaction_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    operation_schema_version TEXT NOT NULL,
    requester TEXT NOT NULL,
    prompt TEXT NOT NULL,
    candidates_json TEXT NOT NULL,
    status TEXT NOT NULL,
    relevant_input_revision INTEGER NOT NULL,
    answer_json TEXT,
    resolved_revision INTEGER,
    updated_revision INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS domain_events_entity_revision
    ON domain_events(entity_id, revision);
CREATE INDEX IF NOT EXISTS interactions_status_id
    ON interactions(status, interaction_id);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    available_bytes BLOB NOT NULL,
    sha256 TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    created_revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fork_experiments (
    experiment_id TEXT PRIMARY KEY,
    controller_version TEXT NOT NULL,
    control_point TEXT NOT NULL,
    source_native_thread_id TEXT,
    source_checkpoint TEXT NOT NULL,
    source_frontier INTEGER NOT NULL,
    source_history_artifact_id TEXT,
    selection_id TEXT NOT NULL UNIQUE,
    result_bundle_artifact_id TEXT,
    delivery_input_artifact_id TEXT,
    delivery_native_turn_id TEXT,
    delivery_result_artifact_id TEXT,
    delivery_status TEXT NOT NULL,
    updated_revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fork_branches (
    branch_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    fork_ancestry_parent TEXT NOT NULL,
    delegation_parent TEXT,
    native_thread_id TEXT,
    persistence_mode TEXT NOT NULL,
    workspace_mode TEXT NOT NULL,
    prompt TEXT NOT NULL,
    status TEXT NOT NULL,
    output_artifact_id TEXT NOT NULL,
    completed_revision INTEGER NOT NULL,
    FOREIGN KEY (experiment_id) REFERENCES fork_experiments(experiment_id),
    FOREIGN KEY (output_artifact_id) REFERENCES artifacts(artifact_id)
);

CREATE INDEX IF NOT EXISTS fork_branches_experiment
    ON fork_branches(experiment_id, branch_id);

CREATE TABLE IF NOT EXISTS rendezvous_groups (
    rendezvous_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    parties INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_revision INTEGER NOT NULL,
    released_revision INTEGER,
    cancellation_reason TEXT,
    updated_revision INTEGER NOT NULL,
    PRIMARY KEY (rendezvous_key, generation)
);

CREATE TABLE IF NOT EXISTS rendezvous_arrivals (
    rendezvous_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    participant_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    arrival_revision INTEGER NOT NULL,
    PRIMARY KEY (rendezvous_key, generation, participant_id),
    FOREIGN KEY (rendezvous_key, generation)
        REFERENCES rendezvous_groups(rendezvous_key, generation)
);
"""


class StateError(ValueError):
    """A command could not be applied to the durable state."""


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path, factory=ClosingConnection)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA busy_timeout = 5000")
    database.execute("PRAGMA foreign_keys = ON")
    database.execute("PRAGMA journal_mode = WAL")
    database.execute("PRAGMA synchronous = FULL")
    database.executescript(SCHEMA)
    # This seed predates a migration runner. Keep additive upgrades explicit so
    # ignored local journals remain usable as slices grow.
    columns = {
        row[1] for row in database.execute("PRAGMA table_info(fork_experiments)")
    }
    for name in (
        "source_history_artifact_id",
        "delivery_input_artifact_id",
        "delivery_native_turn_id",
        "delivery_result_artifact_id",
    ):
        if name not in columns:
            database.execute(f"ALTER TABLE fork_experiments ADD COLUMN {name} TEXT")
    database.commit()
    return database


def append_event(
    database: sqlite3.Connection,
    event_type: str,
    entity_id: str,
    payload: dict[str, Any],
) -> int:
    cursor = database.execute(
        """
        INSERT INTO domain_events(event_id, event_type, entity_id, payload_json, observed_utc)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            event_type,
            entity_id,
            canonical_json(payload),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    return int(cursor.lastrowid)


def validate_candidates(candidates: list[dict[str, str]]) -> None:
    if not candidates:
        raise StateError("an interaction requires at least one candidate")
    identifiers = [candidate.get("id") for candidate in candidates]
    if any(not isinstance(identifier, str) or not identifier for identifier in identifiers):
        raise StateError("every candidate requires a non-empty string id")
    if len(set(identifiers)) != len(identifiers):
        raise StateError("candidate ids must be unique within an interaction")
    if any(not isinstance(candidate.get("label"), str) for candidate in candidates):
        raise StateError("every candidate requires a string label")


def interaction_from_row(row: sqlite3.Row) -> dict[str, Any]:
    answer = json.loads(row["answer_json"]) if row["answer_json"] is not None else None
    candidates = json.loads(row["candidates_json"])
    interaction = {
        "id": row["interaction_id"],
        "operation": row["operation"],
        "operationSchemaVersion": row["operation_schema_version"],
        "requester": row["requester"],
        "prompt": row["prompt"],
        "candidates": candidates,
        "status": row["status"],
        "relevantInputRevision": row["relevant_input_revision"],
        "answer": answer,
        "resolvedRevision": row["resolved_revision"],
        "updatedRevision": row["updated_revision"],
    }
    if row["status"] == "pending":
        interaction["actions"] = [
            {
                "id": f"respond:{row['interaction_id']}",
                "operation": "interaction.respond",
                "operationSchemaVersion": "1",
                "interactionId": row["interaction_id"],
                "relevantInputRevision": row["relevant_input_revision"],
                "choices": candidates,
            }
        ]
    else:
        interaction["actions"] = []
    return interaction


def create_interaction(
    database_path: Path,
    interaction_id: str,
    prompt: str,
    candidates: list[dict[str, str]],
    *,
    requester: str,
    operation: str = "choose-candidate",
    operation_schema_version: str = "1",
) -> dict[str, Any]:
    validate_candidates(candidates)
    candidate_json = canonical_json(candidates)
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        existing = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?", (interaction_id,)
        ).fetchone()
        if existing is not None:
            same = (
                existing["prompt"] == prompt
                and existing["candidates_json"] == candidate_json
                and existing["requester"] == requester
                and existing["operation"] == operation
                and existing["operation_schema_version"] == operation_schema_version
            )
            if not same:
                raise StateError(f"interaction {interaction_id!r} already exists with different inputs")
            return {
                "disposition": "existing",
                "surfaceRevision": current_revision(database),
                "interaction": interaction_from_row(existing),
            }

        payload = {
            "stateSchemaVersion": STATE_SCHEMA_VERSION,
            "operation": operation,
            "operationSchemaVersion": operation_schema_version,
            "requester": requester,
            "prompt": prompt,
            "candidates": candidates,
        }
        revision = append_event(database, "interaction.created", interaction_id, payload)
        database.execute(
            """
            INSERT INTO interactions(
                interaction_id, operation, operation_schema_version, requester,
                prompt, candidates_json, status, relevant_input_revision,
                answer_json, resolved_revision, updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, ?)
            """,
            (
                interaction_id,
                operation,
                operation_schema_version,
                requester,
                prompt,
                candidate_json,
                revision,
                revision,
            ),
        )
        row = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?", (interaction_id,)
        ).fetchone()
        assert row is not None
        return {
            "disposition": "created",
            "surfaceRevision": revision,
            "interaction": interaction_from_row(row),
        }


def respond(
    database_path: Path,
    interaction_id: str,
    relevant_input_revision: int,
    answer: str,
    *,
    responder: str,
) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        row = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?", (interaction_id,)
        ).fetchone()
        if row is None:
            raise StateError(f"unknown interaction {interaction_id!r}")

        status = row["status"]
        existing_answer = (
            json.loads(row["answer_json"]) if row["answer_json"] is not None else None
        )
        candidates = json.loads(row["candidates_json"])
        candidate_ids = {candidate["id"] for candidate in candidates}
        if status == "pending" and relevant_input_revision != row["relevant_input_revision"]:
            disposition = "stale"
        elif status == "pending" and answer not in candidate_ids:
            disposition = "invalid-answer"
        elif status == "pending":
            disposition = "accepted"
        elif status == "resolved" and existing_answer == answer:
            disposition = "identical-retry"
        elif status == "resolved":
            disposition = "already-resolved"
        else:
            disposition = "unavailable"

        revision = append_event(
            database,
            f"interaction.response-{disposition}",
            interaction_id,
            {
                "stateSchemaVersion": STATE_SCHEMA_VERSION,
                "providedRelevantInputRevision": relevant_input_revision,
                "answer": answer,
                "responder": responder,
                "disposition": disposition,
            },
        )
        if disposition == "accepted":
            database.execute(
                """
                UPDATE interactions
                SET status = 'resolved', answer_json = ?, resolved_revision = ?, updated_revision = ?
                WHERE interaction_id = ?
                """,
                (canonical_json(answer), revision, revision, interaction_id),
            )

        current = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?", (interaction_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": disposition,
            "surfaceRevision": revision,
            "interaction": interaction_from_row(current),
        }


def current_revision(database: sqlite3.Connection) -> int:
    row = database.execute("SELECT COALESCE(MAX(revision), 0) FROM domain_events").fetchone()
    assert row is not None
    return int(row[0])


def artifact_from_row(row: sqlite3.Row, *, include_content: bool = False) -> dict[str, Any]:
    artifact = {
        "id": row["artifact_id"],
        "mediaType": row["media_type"],
        "sha256": row["sha256"],
        "sourceEntityId": row["source_entity_id"],
        "createdRevision": row["created_revision"],
        "byteCount": len(row["available_bytes"]),
    }
    if include_content:
        artifact["utf8"] = row["available_bytes"].decode("utf-8", errors="replace")
    return artifact


def branch_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["branch_id"],
        "experimentId": row["experiment_id"],
        "forkAncestryParent": row["fork_ancestry_parent"],
        "delegationParent": row["delegation_parent"],
        "nativeThreadId": row["native_thread_id"],
        "persistenceMode": row["persistence_mode"],
        "workspaceMode": row["workspace_mode"],
        "prompt": row["prompt"],
        "status": row["status"],
        "outputArtifactId": row["output_artifact_id"],
        "completedRevision": row["completed_revision"],
    }


def rendezvous_from_row(
    database: sqlite3.Connection, row: sqlite3.Row
) -> dict[str, Any]:
    arrivals = database.execute(
        """
        SELECT participant_id, payload_json, arrival_revision
        FROM rendezvous_arrivals
        WHERE rendezvous_key = ? AND generation = ?
        ORDER BY arrival_revision, participant_id
        """,
        (row["rendezvous_key"], row["generation"]),
    ).fetchall()
    return {
        "key": row["rendezvous_key"],
        "generation": row["generation"],
        "parties": row["parties"],
        "status": row["status"],
        "createdRevision": row["created_revision"],
        "releasedRevision": row["released_revision"],
        "cancellationReason": row["cancellation_reason"],
        "updatedRevision": row["updated_revision"],
        "arrivals": [
            {
                "participantId": arrival["participant_id"],
                "payload": json.loads(arrival["payload_json"]),
                "arrivalRevision": arrival["arrival_revision"],
            }
            for arrival in arrivals
        ],
    }


def list_rendezvous(database: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = database.execute(
        "SELECT * FROM rendezvous_groups ORDER BY rendezvous_key, generation"
    ).fetchall()
    return [rendezvous_from_row(database, row) for row in rows]


def experiment_from_row(database: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    branches = database.execute(
        "SELECT * FROM fork_branches WHERE experiment_id = ? ORDER BY branch_id",
        (row["experiment_id"],),
    ).fetchall()
    return {
        "id": row["experiment_id"],
        "controllerVersion": row["controller_version"],
        "controlPoint": row["control_point"],
        "sourceNativeThreadId": row["source_native_thread_id"],
        "sourceCheckpoint": row["source_checkpoint"],
        "sourceFrontier": row["source_frontier"],
        "sourceHistoryArtifactId": row["source_history_artifact_id"],
        "selectionId": row["selection_id"],
        "resultBundleArtifactId": row["result_bundle_artifact_id"],
        "deliveryInputArtifactId": row["delivery_input_artifact_id"],
        "deliveryNativeTurnId": row["delivery_native_turn_id"],
        "deliveryResultArtifactId": row["delivery_result_artifact_id"],
        "deliveryStatus": row["delivery_status"],
        "updatedRevision": row["updated_revision"],
        "branches": [branch_from_row(branch) for branch in branches],
    }


def insert_artifact(
    database: sqlite3.Connection,
    artifact_id: str,
    media_type: str,
    content: bytes,
    source_entity_id: str,
    revision: int,
) -> None:
    database.execute(
        """
        INSERT INTO artifacts(
            artifact_id, media_type, available_bytes, sha256,
            source_entity_id, created_revision
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            artifact_id,
            media_type,
            content,
            hashlib.sha256(content).hexdigest(),
            source_entity_id,
            revision,
        ),
    )


FIXTURE_BRANCHES = (
    {
        "suffix": "streaming",
        "strategy": "streaming-parser",
        "context": "compact protocol context",
        "persistenceMode": "fixture",
        "workspaceMode": "read-only",
        "output": (
            "# Streaming parser candidate\n\n"
            "Process each complete JSONL frame immediately and retain the exact bytes. "
            "Bound memory for partial frames and keep decoder failure as an observation.\n"
        ),
    },
    {
        "suffix": "state-machine",
        "strategy": "state-machine-parser",
        "context": "full raw-frame context",
        "persistenceMode": "fixture-ephemeral-recipe-only",
        "workspaceMode": "read-only",
        "output": (
            "# State-machine parser candidate\n\n"
            "Model framing states explicitly, retain transitions beside exact bytes, and "
            "make partial-frame recovery inspectable at the cost of more machinery.\n"
        ),
    },
)


def flix_fixture_recipes(checkpoint: str) -> tuple[dict[str, str], ...]:
    environment = os.environ.copy()
    environment["NEMA_PARENT_CHECKPOINT"] = checkpoint
    completed = subprocess.run(
        ["flix", "run", "--entrypoint", "Nema.Lab.Continuation.emitRecipes"],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        text=True,
        capture_output=True,
        timeout=60.0,
    )
    if completed.returncode != 0:
        raise StateError(f"Flix recipe interpreter failed: {completed.stderr.strip()}")
    recipes: list[dict[str, str]] = []
    templates = {recipe["strategy"]: recipe for recipe in FIXTURE_BRANCHES}
    for line in completed.stdout.splitlines():
        parts = line.split("|")
        if len(parts) != 4 or parts[0] != checkpoint:
            continue
        _, strategy, context, workspace = parts
        template = templates.get(strategy)
        if template is None:
            raise StateError(f"Flix produced unknown fixture strategy {strategy!r}")
        recipes.append(
            {
                "suffix": template["suffix"],
                "strategy": strategy,
                "context": context,
                "persistenceMode": template["persistenceMode"],
                "workspaceMode": workspace,
                "output": template["output"],
            }
        )
    if len(recipes) != 2:
        raise StateError(
            f"expected two Flix multi-resumption recipes, observed {len(recipes)}"
        )
    return tuple(recipes)


def start_fixture_experiment(
    database_path: Path,
    experiment_id: str,
    *,
    recipes_from_flix: bool = False,
) -> dict[str, Any]:
    selection_id = f"{experiment_id}-selection"
    checkpoint = f"{experiment_id}-completed-checkpoint"
    recipes = flix_fixture_recipes(checkpoint) if recipes_from_flix else FIXTURE_BRANCHES
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        existing = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if existing is not None:
            return {
                "disposition": "existing",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, existing),
            }

        source_frontier = current_revision(database)
        started_revision = append_event(
            database,
            "fork-experiment.started",
            experiment_id,
            {
                "controllerVersion": "fixture-fork-controller-v1",
                "mode": "fixture",
                "sourceNativeThreadId": None,
                "sourceCheckpoint": checkpoint,
                "sourceFrontier": source_frontier,
                "nativeHistoryAvailability": "not-applicable-fixture",
            },
        )
        database.execute(
            """
            INSERT INTO fork_experiments(
                experiment_id, controller_version, control_point,
                source_native_thread_id, source_checkpoint, source_frontier,
                selection_id, result_bundle_artifact_id, delivery_status, updated_revision
            ) VALUES (?, 'fixture-fork-controller-v1', 'building-branches', NULL, ?, ?, ?, NULL, 'not-attempted', ?)
            """,
            (experiment_id, checkpoint, source_frontier, selection_id, started_revision),
        )

        candidates: list[dict[str, str]] = []
        for recipe in recipes:
            branch_id = f"{experiment_id}-{recipe['suffix']}"
            artifact_id = f"{branch_id}-output"
            prompt = (
                f"Evaluate {recipe['strategy']} from {checkpoint} using "
                f"{recipe['context']}."
            )
            branch_revision = append_event(
                database,
                "fork-branch.completed",
                branch_id,
                {
                    "experimentId": experiment_id,
                    "forkAncestryParent": checkpoint,
                    "delegationParent": None,
                    "nativeThreadId": None,
                    "persistenceMode": recipe["persistenceMode"],
                    "workspaceMode": recipe["workspaceMode"],
                    "prompt": prompt,
                    "outputArtifactId": artifact_id,
                },
            )
            insert_artifact(
                database,
                artifact_id,
                "text/markdown; charset=utf-8",
                recipe["output"].encode("utf-8"),
                branch_id,
                branch_revision,
            )
            database.execute(
                """
                INSERT INTO fork_branches(
                    branch_id, experiment_id, fork_ancestry_parent,
                    delegation_parent, native_thread_id, persistence_mode,
                    workspace_mode, prompt, status, output_artifact_id,
                    completed_revision
                ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, 'completed', ?, ?)
                """,
                (
                    branch_id,
                    experiment_id,
                    checkpoint,
                    recipe["persistenceMode"],
                    recipe["workspaceMode"],
                    prompt,
                    artifact_id,
                    branch_revision,
                ),
            )
            candidates.append({"id": artifact_id, "label": recipe["strategy"]})

        selection_revision = append_event(
            database,
            "interaction.created",
            selection_id,
            {
                "stateSchemaVersion": STATE_SCHEMA_VERSION,
                "operation": "choose-fork-output",
                "operationSchemaVersion": "1",
                "requester": f"fork-controller:{experiment_id}",
                "prompt": "Choose the branch output to carry into the parent result bundle",
                "candidates": candidates,
            },
        )
        database.execute(
            """
            INSERT INTO interactions(
                interaction_id, operation, operation_schema_version, requester,
                prompt, candidates_json, status, relevant_input_revision,
                answer_json, resolved_revision, updated_revision
            ) VALUES (?, 'choose-fork-output', '1', ?, ?, ?, 'pending', ?, NULL, NULL, ?)
            """,
            (
                selection_id,
                f"fork-controller:{experiment_id}",
                "Choose the branch output to carry into the parent result bundle",
                canonical_json(candidates),
                selection_revision,
                selection_revision,
            ),
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'awaiting-selection', updated_revision = ?
            WHERE experiment_id = ?
            """,
            (selection_revision, experiment_id),
        )
        row = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert row is not None
        return {
            "disposition": "started",
            "surfaceRevision": selection_revision,
            "experiment": experiment_from_row(database, row),
        }


def record_live_fork_experiment(
    database_path: Path,
    experiment_id: str,
    *,
    source_native_thread_id: str,
    source_checkpoint: str,
    source_history: dict[str, Any],
    branches: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist completed native forks and publish their shared selection."""
    if len(branches) < 2:
        raise StateError("a fork comparison requires at least two branches")
    selection_id = f"{experiment_id}-selection"
    source_history_id = f"{experiment_id}-source-history"
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        existing = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if existing is not None:
            return {
                "disposition": "existing",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, existing),
            }

        source_frontier = current_revision(database)
        started_revision = append_event(
            database,
            "fork-experiment.started",
            experiment_id,
            {
                "controllerVersion": "live-fork-controller-v1",
                "mode": "live",
                "sourceNativeThreadId": source_native_thread_id,
                "sourceCheckpoint": source_checkpoint,
                "sourceFrontier": source_frontier,
                "sourceHistoryArtifactId": source_history_id,
            },
        )
        source_bytes = (json.dumps(source_history, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        insert_artifact(
            database,
            source_history_id,
            "application/json; charset=utf-8",
            source_bytes,
            source_native_thread_id,
            started_revision,
        )
        database.execute(
            """
            INSERT INTO fork_experiments(
                experiment_id, controller_version, control_point,
                source_native_thread_id, source_checkpoint, source_frontier,
                source_history_artifact_id, selection_id,
                result_bundle_artifact_id, delivery_input_artifact_id,
                delivery_native_turn_id, delivery_result_artifact_id,
                delivery_status, updated_revision
            ) VALUES (?, 'live-fork-controller-v1', 'building-branches', ?, ?, ?, ?, ?,
                      NULL, NULL, NULL, NULL, 'not-attempted', ?)
            """,
            (
                experiment_id,
                source_native_thread_id,
                source_checkpoint,
                source_frontier,
                source_history_id,
                selection_id,
                started_revision,
            ),
        )

        candidates: list[dict[str, str]] = []
        for branch in branches:
            branch_id = str(branch["id"])
            artifact_id = f"{branch_id}-output"
            output = str(branch["output"])
            branch_revision = append_event(
                database,
                "fork-branch.completed",
                branch_id,
                {
                    "experimentId": experiment_id,
                    "forkAncestryParent": source_native_thread_id,
                    "sourceCheckpoint": source_checkpoint,
                    "delegationParent": branch.get("delegationParent"),
                    "nativeThreadId": branch.get("nativeThreadId"),
                    "persistenceMode": branch["persistenceMode"],
                    "workspaceMode": branch.get("workspaceMode", "read-only-shared"),
                    "prompt": branch["prompt"],
                    "outputArtifactId": artifact_id,
                },
            )
            insert_artifact(
                database,
                artifact_id,
                "text/markdown; charset=utf-8",
                output.encode("utf-8"),
                branch_id,
                branch_revision,
            )
            database.execute(
                """
                INSERT INTO fork_branches(
                    branch_id, experiment_id, fork_ancestry_parent,
                    delegation_parent, native_thread_id, persistence_mode,
                    workspace_mode, prompt, status, output_artifact_id,
                    completed_revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)
                """,
                (
                    branch_id,
                    experiment_id,
                    source_native_thread_id,
                    branch.get("delegationParent"),
                    branch.get("nativeThreadId"),
                    branch["persistenceMode"],
                    branch.get("workspaceMode", "read-only-shared"),
                    branch["prompt"],
                    artifact_id,
                    branch_revision,
                ),
            )
            candidates.append({"id": artifact_id, "label": str(branch["label"])})

        selection_revision = append_event(
            database,
            "interaction.created",
            selection_id,
            {
                "stateSchemaVersion": STATE_SCHEMA_VERSION,
                "operation": "choose-fork-output",
                "operationSchemaVersion": "1",
                "requester": f"fork-controller:{experiment_id}",
                "prompt": "Choose the native branch output to carry into the parent",
                "candidates": candidates,
            },
        )
        database.execute(
            """
            INSERT INTO interactions(
                interaction_id, operation, operation_schema_version, requester,
                prompt, candidates_json, status, relevant_input_revision,
                answer_json, resolved_revision, updated_revision
            ) VALUES (?, 'choose-fork-output', '1', ?, ?, ?, 'pending', ?, NULL, NULL, ?)
            """,
            (
                selection_id,
                f"fork-controller:{experiment_id}",
                "Choose the native branch output to carry into the parent",
                canonical_json(candidates),
                selection_revision,
                selection_revision,
            ),
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'awaiting-selection', updated_revision = ?
            WHERE experiment_id = ?
            """,
            (selection_revision, experiment_id),
        )
        row = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert row is not None
        return {
            "disposition": "recorded",
            "surfaceRevision": selection_revision,
            "experiment": experiment_from_row(database, row),
        }


def advance_live_experiment(database_path: Path, experiment_id: str) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["controller_version"] != "live-fork-controller-v1":
            raise StateError("fixture experiment cannot use the live controller")
        if experiment["control_point"] in (
            "bundle-ready",
            "delivery-prepared",
            "live-completed",
        ):
            return {
                "disposition": "already-advanced",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
            }
        if experiment["control_point"] != "awaiting-selection":
            raise StateError(
                f"experiment cannot advance from {experiment['control_point']!r}"
            )
        selection = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?",
            (experiment["selection_id"],),
        ).fetchone()
        assert selection is not None
        if selection["status"] != "resolved":
            return {
                "disposition": "awaiting-selection",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
            }

        chosen_artifact_id = json.loads(selection["answer_json"])
        artifacts = database.execute(
            """
            SELECT a.* FROM artifacts a
            JOIN fork_branches b ON b.output_artifact_id = a.artifact_id
            WHERE b.experiment_id = ? ORDER BY b.branch_id
            """,
            (experiment_id,),
        ).fetchall()
        bundle_id = f"{experiment_id}-result-bundle"
        bundle = {
            "bundleSchemaVersion": "1",
            "experimentId": experiment_id,
            "sourceNativeThreadId": experiment["source_native_thread_id"],
            "sourceCheckpoint": experiment["source_checkpoint"],
            "sourceHistoryArtifactId": experiment["source_history_artifact_id"],
            "selectionId": experiment["selection_id"],
            "chosenArtifactId": chosen_artifact_id,
            "sources": [
                {
                    "artifactId": artifact["artifact_id"],
                    "sourceEntityId": artifact["source_entity_id"],
                    "sha256": artifact["sha256"],
                    "selected": artifact["artifact_id"] == chosen_artifact_id,
                }
                for artifact in artifacts
            ],
            "attribution": (
                "Nema-selected branch material. This does not rewrite the parent prefix "
                "or attribute a branch assistant message to the parent."
            ),
        }
        content = (json.dumps(bundle, indent=2, sort_keys=True) + "\n").encode("utf-8")
        revision = append_event(
            database,
            "fork-experiment.result-bundle-created",
            experiment_id,
            {"resultBundleArtifactId": bundle_id, "chosenArtifactId": chosen_artifact_id},
        )
        insert_artifact(
            database,
            bundle_id,
            "application/json; charset=utf-8",
            content,
            experiment_id,
            revision,
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'bundle-ready', result_bundle_artifact_id = ?,
                updated_revision = ? WHERE experiment_id = ?
            """,
            (bundle_id, revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "advanced",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
            "resultBundle": bundle,
        }


def prepare_live_delivery(database_path: Path, experiment_id: str) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["control_point"] == "delivery-prepared":
            artifact = database.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?",
                (experiment["delivery_input_artifact_id"],),
            ).fetchone()
            assert artifact is not None
            return {
                "disposition": "existing",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
                "deliveryInput": artifact["available_bytes"].decode("utf-8"),
            }
        if experiment["control_point"] != "bundle-ready":
            raise StateError(
                f"delivery cannot be prepared from {experiment['control_point']!r}"
            )
        bundle = database.execute(
            "SELECT * FROM artifacts WHERE artifact_id = ?",
            (experiment["result_bundle_artifact_id"],),
        ).fetchone()
        assert bundle is not None
        delivery_id = f"{experiment_id}-delivery-input"
        delivery_input = (
            "Nema is explicitly delivering the following attributed result bundle from "
            "two forks of your completed checkpoint. Treat it as new evidence now; do not "
            "rewrite or impersonate your earlier history. Briefly acknowledge the selected "
            "artifact and its provenance.\n\n"
            + bundle["available_bytes"].decode("utf-8")
        )
        revision = append_event(
            database,
            "fork-experiment.delivery-prepared",
            experiment_id,
            {
                "deliveryInputArtifactId": delivery_id,
                "targetNativeThreadId": experiment["source_native_thread_id"],
                "status": "prepared-not-sent",
            },
        )
        insert_artifact(
            database,
            delivery_id,
            "text/plain; charset=utf-8",
            delivery_input.encode("utf-8"),
            experiment_id,
            revision,
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'delivery-prepared', delivery_input_artifact_id = ?,
                delivery_status = 'prepared-not-sent', updated_revision = ?
            WHERE experiment_id = ?
            """,
            (delivery_id, revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "prepared",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
            "deliveryInput": delivery_input,
        }


def complete_live_delivery(
    database_path: Path,
    experiment_id: str,
    *,
    native_turn_id: str,
    parent_output: str,
) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["control_point"] == "live-completed":
            return {
                "disposition": "already-completed",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
            }
        if experiment["control_point"] not in (
            "delivery-prepared",
            "delivery-awaiting-completion",
        ):
            raise StateError(
                f"delivery cannot complete from {experiment['control_point']!r}"
            )
        output_id = f"{experiment_id}-parent-response"
        revision = append_event(
            database,
            "fork-experiment.delivery-observed-completed",
            experiment_id,
            {
                "nativeTurnId": native_turn_id,
                "resultArtifactId": output_id,
                "status": "observed-completed",
            },
        )
        insert_artifact(
            database,
            output_id,
            "text/markdown; charset=utf-8",
            parent_output.encode("utf-8"),
            experiment_id,
            revision,
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'live-completed', delivery_native_turn_id = ?,
                delivery_result_artifact_id = ?, delivery_status = 'observed-completed',
                updated_revision = ? WHERE experiment_id = ?
            """,
            (native_turn_id, output_id, revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "completed",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
        }


def mark_live_delivery_started(
    database_path: Path, experiment_id: str, native_turn_id: str
) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["control_point"] != "delivery-prepared":
            raise StateError(
                f"delivery cannot start from {experiment['control_point']!r}"
            )
        revision = append_event(
            database,
            "fork-experiment.delivery-native-turn-started",
            experiment_id,
            {"nativeTurnId": native_turn_id, "status": "native-turn-started"},
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'delivery-awaiting-completion',
                delivery_native_turn_id = ?, delivery_status = 'native-turn-started',
                updated_revision = ? WHERE experiment_id = ?
            """,
            (native_turn_id, revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "started",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
        }


def mark_live_delivery_unknown(
    database_path: Path, experiment_id: str, reason: str
) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["control_point"] != "delivery-awaiting-completion":
            raise StateError(
                f"delivery outcome cannot become unknown from {experiment['control_point']!r}"
            )
        revision = append_event(
            database,
            "fork-experiment.delivery-outcome-unknown",
            experiment_id,
            {"nativeTurnId": experiment["delivery_native_turn_id"], "reason": reason},
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'delivery-unknown', delivery_status = 'outcome-unknown',
                updated_revision = ? WHERE experiment_id = ?
            """,
            (revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "unknown",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
        }


def choose_fixture_experiment(
    database_path: Path, experiment_id: str, answer: str, responder: str
) -> dict[str, Any]:
    with open_database(database_path) as database:
        row = database.execute(
            """
            SELECT i.interaction_id, i.relevant_input_revision
            FROM fork_experiments e
            JOIN interactions i ON i.interaction_id = e.selection_id
            WHERE e.experiment_id = ?
            """,
            (experiment_id,),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        selection_id = row["interaction_id"]
        relevant_revision = int(row["relevant_input_revision"])
    return respond(
        database_path,
        selection_id,
        relevant_revision,
        answer,
        responder=responder,
    )


def advance_fixture_experiment(database_path: Path, experiment_id: str) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        experiment = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        if experiment is None:
            raise StateError(f"unknown fork experiment {experiment_id!r}")
        if experiment["control_point"] == "fixture-completed":
            return {
                "disposition": "already-completed",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
            }
        if experiment["control_point"] != "awaiting-selection":
            raise StateError(
                f"experiment cannot advance from {experiment['control_point']!r}"
            )
        selection = database.execute(
            "SELECT * FROM interactions WHERE interaction_id = ?",
            (experiment["selection_id"],),
        ).fetchone()
        assert selection is not None
        if selection["status"] != "resolved":
            return {
                "disposition": "awaiting-selection",
                "surfaceRevision": current_revision(database),
                "experiment": experiment_from_row(database, experiment),
            }

        chosen_artifact_id = json.loads(selection["answer_json"])
        artifacts = database.execute(
            """
            SELECT a.* FROM artifacts a
            JOIN fork_branches b ON b.output_artifact_id = a.artifact_id
            WHERE b.experiment_id = ? ORDER BY b.branch_id
            """,
            (experiment_id,),
        ).fetchall()
        bundle_id = f"{experiment_id}-result-bundle"
        bundle = {
            "bundleSchemaVersion": "1",
            "experimentId": experiment_id,
            "sourceCheckpoint": experiment["source_checkpoint"],
            "selectionId": experiment["selection_id"],
            "chosenArtifactId": chosen_artifact_id,
            "sources": [
                {
                    "artifactId": artifact["artifact_id"],
                    "sourceEntityId": artifact["source_entity_id"],
                    "sha256": artifact["sha256"],
                    "selected": artifact["artifact_id"] == chosen_artifact_id,
                }
                for artifact in artifacts
            ],
            "attribution": (
                "Fixture controller selection only; no branch transcript was "
                "impersonated as parent history and no semantic synthesis was claimed."
            ),
        }
        content = (json.dumps(bundle, indent=2, sort_keys=True) + "\n").encode("utf-8")
        revision = append_event(
            database,
            "fork-experiment.result-bundle-created",
            experiment_id,
            {
                "resultBundleArtifactId": bundle_id,
                "chosenArtifactId": chosen_artifact_id,
                "deliveryStatus": "not-applicable-fixture",
            },
        )
        insert_artifact(
            database,
            bundle_id,
            "application/json; charset=utf-8",
            content,
            experiment_id,
            revision,
        )
        database.execute(
            """
            UPDATE fork_experiments
            SET control_point = 'fixture-completed', result_bundle_artifact_id = ?,
                delivery_status = 'not-applicable-fixture', updated_revision = ?
            WHERE experiment_id = ?
            """,
            (bundle_id, revision, experiment_id),
        )
        current = database.execute(
            "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
        ).fetchone()
        assert current is not None
        return {
            "disposition": "advanced",
            "surfaceRevision": revision,
            "experiment": experiment_from_row(database, current),
            "resultBundle": bundle,
        }


def snapshot(database_path: Path) -> dict[str, Any]:
    with open_database(database_path) as database:
        # The deferred transaction establishes one read snapshot. Any command
        # racing this query lands strictly after the returned watermark and is
        # available through changes_after.
        database.execute("BEGIN")
        revision = current_revision(database)
        rows = database.execute(
            "SELECT * FROM interactions ORDER BY interaction_id"
        ).fetchall()
        interactions = [interaction_from_row(row) for row in rows]
        experiment_rows = database.execute(
            "SELECT * FROM fork_experiments ORDER BY experiment_id"
        ).fetchall()
        experiments = [experiment_from_row(database, row) for row in experiment_rows]
        artifact_rows = database.execute(
            "SELECT * FROM artifacts ORDER BY artifact_id"
        ).fetchall()
        artifacts = [artifact_from_row(row) for row in artifact_rows]
        rendezvous = list_rendezvous(database)
        actions = [action for item in interactions for action in item["actions"]]
        return {
            "surfaceSchemaVersion": "1",
            "stateSchemaVersion": STATE_SCHEMA_VERSION,
            "revision": revision,
            "interactions": interactions,
            "forkExperiments": experiments,
            "artifacts": artifacts,
            "rendezvous": rendezvous,
            "availableActions": actions,
        }


def changes_after(database_path: Path, revision: int) -> dict[str, Any]:
    with open_database(database_path) as database:
        rows = database.execute(
            """
            SELECT revision, event_id, event_type, entity_id, payload_json, observed_utc
            FROM domain_events WHERE revision > ? ORDER BY revision
            """,
            (revision,),
        ).fetchall()
        frontier = current_revision(database)
    return {
        "stateSchemaVersion": STATE_SCHEMA_VERSION,
        "afterRevision": revision,
        "frontier": frontier,
        "events": [
            {
                "revision": row["revision"],
                "eventId": row["event_id"],
                "type": row["event_type"],
                "entityId": row["entity_id"],
                "payload": json.loads(row["payload_json"]),
                "observedUtc": row["observed_utc"],
            }
            for row in rows
        ],
    }


def replay_interactions(database_path: Path) -> dict[str, Any]:
    """Rebuild semantic projections and verify immutable artifact hashes."""
    with open_database(database_path) as database:
        event_rows = database.execute(
            "SELECT revision, event_type, entity_id, payload_json FROM domain_events ORDER BY revision"
        ).fetchall()
        stored_interaction_rows = database.execute(
            "SELECT * FROM interactions ORDER BY interaction_id"
        ).fetchall()
        stored_experiment_rows = database.execute(
            "SELECT * FROM fork_experiments ORDER BY experiment_id"
        ).fetchall()
        stored_branch_rows = database.execute(
            "SELECT * FROM fork_branches ORDER BY branch_id"
        ).fetchall()
        artifact_rows = database.execute(
            "SELECT * FROM artifacts ORDER BY artifact_id"
        ).fetchall()
        rendezvous_rows = database.execute(
            "SELECT * FROM rendezvous_groups ORDER BY rendezvous_key, generation"
        ).fetchall()
        stored_rendezvous = {
            (row["rendezvous_key"], row["generation"]): rendezvous_from_row(database, row)
            for row in rendezvous_rows
        }

    rebuilt_interactions: dict[str, dict[str, Any]] = {}
    rebuilt_experiments: dict[str, dict[str, Any]] = {}
    rebuilt_branches: dict[str, dict[str, Any]] = {}
    rebuilt_rendezvous: dict[tuple[str, int], dict[str, Any]] = {}
    for event in event_rows:
        event_type = event["event_type"]
        entity_id = event["entity_id"]
        payload = json.loads(event["payload_json"])
        if event_type == "interaction.created":
            rebuilt_interactions[entity_id] = {
                "id": entity_id,
                "operation": payload["operation"],
                "operationSchemaVersion": payload["operationSchemaVersion"],
                "requester": payload["requester"],
                "prompt": payload["prompt"],
                "candidates": payload["candidates"],
                "status": "pending",
                "relevantInputRevision": event["revision"],
                "answer": None,
                "resolvedRevision": None,
                "updatedRevision": event["revision"],
            }
            for experiment in rebuilt_experiments.values():
                if experiment["selectionId"] == entity_id:
                    experiment["controlPoint"] = "awaiting-selection"
                    experiment["updatedRevision"] = event["revision"]
        elif event_type == "interaction.response-accepted":
            state = rebuilt_interactions.get(entity_id)
            if state is None:
                raise StateError(
                    f"accepted response at revision {event['revision']} has no creation event"
                )
            state["status"] = "resolved"
            state["answer"] = payload["answer"]
            state["resolvedRevision"] = event["revision"]
            state["updatedRevision"] = event["revision"]
        elif event_type == "fork-experiment.started":
            rebuilt_experiments[entity_id] = {
                "id": entity_id,
                "controllerVersion": payload["controllerVersion"],
                "controlPoint": "building-branches",
                "sourceNativeThreadId": payload.get("sourceNativeThreadId"),
                "sourceCheckpoint": payload["sourceCheckpoint"],
                "sourceFrontier": payload["sourceFrontier"],
                "sourceHistoryArtifactId": payload.get("sourceHistoryArtifactId"),
                "selectionId": f"{entity_id}-selection",
                "resultBundleArtifactId": None,
                "deliveryInputArtifactId": None,
                "deliveryNativeTurnId": None,
                "deliveryResultArtifactId": None,
                "deliveryStatus": "not-attempted",
                "updatedRevision": event["revision"],
            }
        elif event_type == "fork-branch.completed":
            rebuilt_branches[entity_id] = {
                "id": entity_id,
                "experimentId": payload["experimentId"],
                "forkAncestryParent": payload["forkAncestryParent"],
                "delegationParent": payload.get("delegationParent"),
                "nativeThreadId": payload.get("nativeThreadId"),
                "persistenceMode": payload["persistenceMode"],
                "workspaceMode": payload["workspaceMode"],
                "prompt": payload["prompt"],
                "status": "completed",
                "outputArtifactId": payload["outputArtifactId"],
                "completedRevision": event["revision"],
            }
        elif event_type == "fork-experiment.result-bundle-created":
            experiment = rebuilt_experiments[entity_id]
            experiment["resultBundleArtifactId"] = payload["resultBundleArtifactId"]
            if experiment["controllerVersion"] == "fixture-fork-controller-v1":
                experiment["controlPoint"] = "fixture-completed"
                experiment["deliveryStatus"] = payload.get(
                    "deliveryStatus", "not-applicable-fixture"
                )
            else:
                experiment["controlPoint"] = "bundle-ready"
            experiment["updatedRevision"] = event["revision"]
        elif event_type == "fork-experiment.delivery-prepared":
            experiment = rebuilt_experiments[entity_id]
            experiment["controlPoint"] = "delivery-prepared"
            experiment["deliveryInputArtifactId"] = payload["deliveryInputArtifactId"]
            experiment["deliveryStatus"] = payload["status"]
            experiment["updatedRevision"] = event["revision"]
        elif event_type == "fork-experiment.delivery-native-turn-started":
            experiment = rebuilt_experiments[entity_id]
            experiment["controlPoint"] = "delivery-awaiting-completion"
            experiment["deliveryNativeTurnId"] = payload["nativeTurnId"]
            experiment["deliveryStatus"] = payload["status"]
            experiment["updatedRevision"] = event["revision"]
        elif event_type == "fork-experiment.delivery-observed-completed":
            experiment = rebuilt_experiments[entity_id]
            experiment["controlPoint"] = "live-completed"
            experiment["deliveryNativeTurnId"] = payload["nativeTurnId"]
            experiment["deliveryResultArtifactId"] = payload["resultArtifactId"]
            experiment["deliveryStatus"] = payload["status"]
            experiment["updatedRevision"] = event["revision"]
        elif event_type == "fork-experiment.delivery-outcome-unknown":
            experiment = rebuilt_experiments[entity_id]
            experiment["controlPoint"] = "delivery-unknown"
            experiment["deliveryNativeTurnId"] = payload["nativeTurnId"]
            experiment["deliveryStatus"] = "outcome-unknown"
            experiment["updatedRevision"] = event["revision"]
        elif event_type == "rendezvous.created":
            key = (payload["key"], payload["generation"])
            rebuilt_rendezvous[key] = {
                "key": payload["key"],
                "generation": payload["generation"],
                "parties": payload["parties"],
                "status": "waiting",
                "createdRevision": event["revision"],
                "releasedRevision": None,
                "cancellationReason": None,
                "updatedRevision": event["revision"],
                "arrivals": [],
            }
        elif event_type == "rendezvous.arrived":
            state = rebuilt_rendezvous[
                next(
                    key
                    for key in rebuilt_rendezvous
                    if entity_id == f"rendezvous:{key[1]}:{key[0]}"
                )
            ]
            state["arrivals"].append(
                {
                    "participantId": payload["participantId"],
                    "payload": payload["payload"],
                    "arrivalRevision": event["revision"],
                }
            )
            state["updatedRevision"] = event["revision"]
        elif event_type == "rendezvous.released":
            state = next(
                state
                for key, state in rebuilt_rendezvous.items()
                if entity_id == f"rendezvous:{key[1]}:{key[0]}"
            )
            state["status"] = "released"
            state["releasedRevision"] = event["revision"]
            state["updatedRevision"] = event["revision"]
        elif event_type == "rendezvous.cancelled":
            state = next(
                state
                for key, state in rebuilt_rendezvous.items()
                if entity_id == f"rendezvous:{key[1]}:{key[0]}"
            )
            state["status"] = "cancelled"
            state["cancellationReason"] = payload["reason"]
            state["updatedRevision"] = event["revision"]

    stored_interactions = {
        row["interaction_id"]: {
            key: value
            for key, value in interaction_from_row(row).items()
            if key != "actions"
        }
        for row in stored_interaction_rows
    }
    interaction_mismatches = sorted(
        identifier
        for identifier in set(rebuilt_interactions) | set(stored_interactions)
        if rebuilt_interactions.get(identifier) != stored_interactions.get(identifier)
    )
    experiment_keys = (
        "id",
        "controllerVersion",
        "controlPoint",
        "sourceNativeThreadId",
        "sourceCheckpoint",
        "sourceFrontier",
        "sourceHistoryArtifactId",
        "selectionId",
        "resultBundleArtifactId",
        "deliveryInputArtifactId",
        "deliveryNativeTurnId",
        "deliveryResultArtifactId",
        "deliveryStatus",
        "updatedRevision",
    )
    stored_experiments = {
        row["experiment_id"]: {
            "id": row["experiment_id"],
            "controllerVersion": row["controller_version"],
            "controlPoint": row["control_point"],
            "sourceNativeThreadId": row["source_native_thread_id"],
            "sourceCheckpoint": row["source_checkpoint"],
            "sourceFrontier": row["source_frontier"],
            "sourceHistoryArtifactId": row["source_history_artifact_id"],
            "selectionId": row["selection_id"],
            "resultBundleArtifactId": row["result_bundle_artifact_id"],
            "deliveryInputArtifactId": row["delivery_input_artifact_id"],
            "deliveryNativeTurnId": row["delivery_native_turn_id"],
            "deliveryResultArtifactId": row["delivery_result_artifact_id"],
            "deliveryStatus": row["delivery_status"],
            "updatedRevision": row["updated_revision"],
        }
        for row in stored_experiment_rows
    }
    experiment_mismatches = sorted(
        identifier
        for identifier in set(rebuilt_experiments) | set(stored_experiments)
        if {key: rebuilt_experiments.get(identifier, {}).get(key) for key in experiment_keys}
        != {key: stored_experiments.get(identifier, {}).get(key) for key in experiment_keys}
    )
    stored_branches = {
        row["branch_id"]: branch_from_row(row) for row in stored_branch_rows
    }
    branch_mismatches = sorted(
        identifier
        for identifier in set(rebuilt_branches) | set(stored_branches)
        if rebuilt_branches.get(identifier) != stored_branches.get(identifier)
    )
    rendezvous_mismatches = sorted(
        f"{key[0]}@{key[1]}"
        for key in set(rebuilt_rendezvous) | set(stored_rendezvous)
        if rebuilt_rendezvous.get(key) != stored_rendezvous.get(key)
    )
    artifact_hash_mismatches = sorted(
        row["artifact_id"]
        for row in artifact_rows
        if hashlib.sha256(row["available_bytes"]).hexdigest() != row["sha256"]
    )
    artifact_ids = {row["artifact_id"] for row in artifact_rows}
    referenced_artifact_ids = {
        branch["outputArtifactId"] for branch in rebuilt_branches.values()
    }
    for experiment in rebuilt_experiments.values():
        referenced_artifact_ids.update(
            value
            for value in (
                experiment["sourceHistoryArtifactId"],
                experiment["resultBundleArtifactId"],
                experiment["deliveryInputArtifactId"],
                experiment["deliveryResultArtifactId"],
            )
            if value is not None
        )
    missing_artifact_ids = sorted(referenced_artifact_ids - artifact_ids)
    deterministic = not (
        interaction_mismatches
        or experiment_mismatches
        or branch_mismatches
        or rendezvous_mismatches
        or artifact_hash_mismatches
        or missing_artifact_ids
    )
    return {
        "database": str(database_path),
        "stateSchemaVersion": STATE_SCHEMA_VERSION,
        "eventCount": len(event_rows),
        "interactionCount": len(rebuilt_interactions),
        "forkExperimentCount": len(rebuilt_experiments),
        "forkBranchCount": len(rebuilt_branches),
        "rendezvousCount": len(rebuilt_rendezvous),
        "artifactCount": len(artifact_rows),
        "mismatchedInteractionIds": interaction_mismatches,
        "mismatchedForkExperimentIds": experiment_mismatches,
        "mismatchedForkBranchIds": branch_mismatches,
        "mismatchedRendezvousIds": rendezvous_mismatches,
        "artifactHashMismatchIds": artifact_hash_mismatches,
        "missingReferencedArtifactIds": missing_artifact_ids,
        "deterministic": deterministic,
    }


def render_terminal(surface: dict[str, Any]) -> str:
    lines = [f"Nema interactions @ revision {surface['revision']}"]
    if not surface["interactions"]:
        return "\n".join(lines + ["  (none)"])
    for interaction in surface["interactions"]:
        lines.append(f"  {interaction['id']}  [{interaction['status']}]  {interaction['prompt']}")
        for candidate in interaction["candidates"]:
            marker = "*" if interaction["answer"] == candidate["id"] else "-"
            lines.append(f"    {marker} {candidate['id']}: {candidate['label']}")
        for action in interaction["actions"]:
            lines.append(
                "    respond: "
                f"./bin/nema interaction respond {interaction['id']} "
                f"--revision {action['relevantInputRevision']} --answer <candidate-id>"
            )
    for experiment in surface["forkExperiments"]:
        lines.append(
            f"  experiment {experiment['id']}  [{experiment['controlPoint']}]  "
            f"selection={experiment['selectionId']}"
        )
        for branch in experiment["branches"]:
            lines.append(
                f"    branch {branch['id']} [{branch['status']}] -> {branch['outputArtifactId']}"
            )
        if experiment["resultBundleArtifactId"] is not None:
            lines.append(f"    result bundle: {experiment['resultBundleArtifactId']}")
    return "\n".join(lines)


def render_prompt(surface: dict[str, Any]) -> str:
    pending = [item for item in surface["interactions"] if item["status"] == "pending"]
    lines = [f"Nema surface revision: {surface['revision']}"]
    if not pending:
        return "\n".join(lines + ["There are no pending interactions."])
    lines.append("Pending interactions:")
    for item in pending:
        choices = ", ".join(
            f"{choice['id']} ({choice['label']})" for choice in item["candidates"]
        )
        lines.append(
            f"- {item['id']}: {item['prompt']} Choices: {choices}. "
            f"Respond against input revision {item['relevantInputRevision']}."
        )
    return "\n".join(lines)


def parse_candidate(value: str) -> dict[str, str]:
    identifier, separator, label = value.partition("=")
    if not separator or not identifier:
        raise argparse.ArgumentTypeError("candidate must be ID=LABEL")
    return {"id": identifier, "label": label}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    interaction = subparsers.add_parser("interaction")
    interaction_subparsers = interaction.add_subparsers(dest="interaction_command", required=True)
    create = interaction_subparsers.add_parser("create")
    create.add_argument("interaction_id")
    create.add_argument("--prompt", required=True)
    create.add_argument("--candidate", action="append", type=parse_candidate, required=True)
    create.add_argument("--requester", default="human-cli")
    create.add_argument("--operation", default="choose-candidate")
    create.add_argument("--operation-schema-version", default="1")
    interaction_subparsers.add_parser("list")
    show = interaction_subparsers.add_parser("show")
    show.add_argument("interaction_id")
    response = interaction_subparsers.add_parser("respond")
    response.add_argument("interaction_id")
    response.add_argument("--revision", type=int, required=True)
    response.add_argument("--answer", required=True)
    response.add_argument("--responder", default="human-cli")

    surface = subparsers.add_parser("surface")
    surface.add_argument("--format", choices=("terminal", "json", "prompt"), default="terminal")
    changes = subparsers.add_parser("changes")
    changes.add_argument("--after", type=int, required=True)

    fork = subparsers.add_parser("fork")
    fork_subparsers = fork.add_subparsers(dest="fork_command", required=True)
    fixture_start = fork_subparsers.add_parser("fixture-start")
    fixture_start.add_argument("experiment_id")
    fixture_start.add_argument("--from-flix", action="store_true")
    fixture_choose = fork_subparsers.add_parser("choose")
    fixture_choose.add_argument("experiment_id")
    fixture_choose.add_argument("--answer", required=True)
    fixture_choose.add_argument("--responder", default="human-cli")
    fixture_advance = fork_subparsers.add_parser("advance")
    fixture_advance.add_argument("experiment_id")
    fixture_inspect = fork_subparsers.add_parser("inspect")
    fixture_inspect.add_argument("experiment_id")

    artifact = subparsers.add_parser("artifact")
    artifact_subparsers = artifact.add_subparsers(dest="artifact_command", required=True)
    artifact_show = artifact_subparsers.add_parser("show")
    artifact_show.add_argument("artifact_id")
    subparsers.add_parser("replay")
    return parser


def print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True))


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "interaction":
            if args.interaction_command == "create":
                result = create_interaction(
                    args.db,
                    args.interaction_id,
                    args.prompt,
                    args.candidate,
                    requester=args.requester,
                    operation=args.operation,
                    operation_schema_version=args.operation_schema_version,
                )
            elif args.interaction_command == "respond":
                result = respond(
                    args.db,
                    args.interaction_id,
                    args.revision,
                    args.answer,
                    responder=args.responder,
                )
            else:
                current = snapshot(args.db)
                if args.interaction_command == "list":
                    result = current
                else:
                    result = next(
                        (
                            item
                            for item in current["interactions"]
                            if item["id"] == args.interaction_id
                        ),
                        None,
                    )
                    if result is None:
                        raise StateError(f"unknown interaction {args.interaction_id!r}")
            print_json(result)
        elif args.command == "changes":
            print_json(changes_after(args.db, args.after))
        elif args.command == "fork":
            if args.fork_command == "fixture-start":
                result = start_fixture_experiment(
                    args.db,
                    args.experiment_id,
                    recipes_from_flix=args.from_flix,
                )
            elif args.fork_command == "choose":
                result = choose_fixture_experiment(
                    args.db, args.experiment_id, args.answer, args.responder
                )
            elif args.fork_command == "advance":
                result = advance_fixture_experiment(args.db, args.experiment_id)
            else:
                with open_database(args.db) as database:
                    row = database.execute(
                        "SELECT * FROM fork_experiments WHERE experiment_id = ?",
                        (args.experiment_id,),
                    ).fetchone()
                    if row is None:
                        raise StateError(f"unknown fork experiment {args.experiment_id!r}")
                    result = experiment_from_row(database, row)
            print_json(result)
        elif args.command == "artifact":
            with open_database(args.db) as database:
                row = database.execute(
                    "SELECT * FROM artifacts WHERE artifact_id = ?", (args.artifact_id,)
                ).fetchone()
                if row is None:
                    raise StateError(f"unknown artifact {args.artifact_id!r}")
                result = artifact_from_row(row, include_content=True)
            print_json(result)
        elif args.command == "replay":
            result = replay_interactions(args.db)
            print_json(result)
            return 0 if result["deterministic"] else 1
        else:
            current = snapshot(args.db)
            if args.format == "json":
                print_json(current)
            elif args.format == "prompt":
                print(render_prompt(current))
            else:
                print(render_terminal(current))
        return 0
    except StateError as error:
        print_json({"error": str(error)})
        return 2


if __name__ == "__main__":
    sys.exit(main())
