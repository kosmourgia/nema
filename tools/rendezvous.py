#!/usr/bin/env python3
"""Durable bounded rendezvous state over Nema's event journal."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.state import (
    StateError,
    append_event,
    canonical_json,
    open_database,
    rendezvous_from_row,
)


def entity_id(key: str, generation: int) -> str:
    return f"rendezvous:{generation}:{key}"


def inspect(database_path: Path, key: str, generation: int) -> dict[str, Any]:
    with open_database(database_path) as database:
        row = database.execute(
            "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
            (key, generation),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown rendezvous {key!r} generation {generation}")
        return rendezvous_from_row(database, row)


def arrive(
    database_path: Path,
    key: str,
    generation: int,
    participant_id: str,
    payload: Any,
    parties: int,
) -> dict[str, Any]:
    if not key or not participant_id:
        raise StateError("rendezvous key and participant id must be non-empty")
    if generation < 0:
        raise StateError("rendezvous generation must be non-negative")
    if not 2 <= parties <= 32:
        raise StateError("rendezvous parties must be between 2 and 32")
    encoded_payload = canonical_json(payload)
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        row = database.execute(
            "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
            (key, generation),
        ).fetchone()
        if row is None:
            created = append_event(
                database,
                "rendezvous.created",
                entity_id(key, generation),
                {"key": key, "generation": generation, "parties": parties},
            )
            database.execute(
                """
                INSERT INTO rendezvous_groups(
                    rendezvous_key, generation, parties, status,
                    created_revision, released_revision, cancellation_reason,
                    updated_revision
                ) VALUES (?, ?, ?, 'waiting', ?, NULL, NULL, ?)
                """,
                (key, generation, parties, created, created),
            )
            row = database.execute(
                "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
                (key, generation),
            ).fetchone()
            assert row is not None
        elif row["parties"] != parties:
            raise StateError(
                f"rendezvous {key!r} generation {generation} already expects "
                f"{row['parties']} parties"
            )

        previous = database.execute(
            """
            SELECT payload_json FROM rendezvous_arrivals
            WHERE rendezvous_key = ? AND generation = ? AND participant_id = ?
            """,
            (key, generation, participant_id),
        ).fetchone()
        if previous is not None:
            if previous["payload_json"] != encoded_payload:
                raise StateError(
                    f"participant {participant_id!r} retried with a different payload"
                )
            return rendezvous_from_row(database, row)
        if row["status"] != "waiting":
            raise StateError(
                f"rendezvous {key!r} generation {generation} is {row['status']}"
            )

        revision = append_event(
            database,
            "rendezvous.arrived",
            entity_id(key, generation),
            {"participantId": participant_id, "payload": payload},
        )
        database.execute(
            """
            INSERT INTO rendezvous_arrivals(
                rendezvous_key, generation, participant_id,
                payload_json, arrival_revision
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (key, generation, participant_id, encoded_payload, revision),
        )
        count = database.execute(
            """
            SELECT COUNT(*) FROM rendezvous_arrivals
            WHERE rendezvous_key = ? AND generation = ?
            """,
            (key, generation),
        ).fetchone()[0]
        if count == parties:
            released = append_event(
                database,
                "rendezvous.released",
                entity_id(key, generation),
                {"participantCount": count},
            )
            database.execute(
                """
                UPDATE rendezvous_groups
                SET status = 'released', released_revision = ?, updated_revision = ?
                WHERE rendezvous_key = ? AND generation = ?
                """,
                (released, released, key, generation),
            )
        else:
            database.execute(
                """
                UPDATE rendezvous_groups SET updated_revision = ?
                WHERE rendezvous_key = ? AND generation = ?
                """,
                (revision, key, generation),
            )
        current = database.execute(
            "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
            (key, generation),
        ).fetchone()
        assert current is not None
        return rendezvous_from_row(database, current)


def cancel(
    database_path: Path,
    key: str,
    generation: int,
    *,
    participant_id: str,
    reason: str,
) -> dict[str, Any]:
    with open_database(database_path) as database:
        database.execute("BEGIN IMMEDIATE")
        row = database.execute(
            "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
            (key, generation),
        ).fetchone()
        if row is None:
            raise StateError(f"unknown rendezvous {key!r} generation {generation}")
        if row["status"] != "waiting":
            return rendezvous_from_row(database, row)
        revision = append_event(
            database,
            "rendezvous.cancelled",
            entity_id(key, generation),
            {"participantId": participant_id, "reason": reason},
        )
        database.execute(
            """
            UPDATE rendezvous_groups
            SET status = 'cancelled', cancellation_reason = ?, updated_revision = ?
            WHERE rendezvous_key = ? AND generation = ?
            """,
            (reason, revision, key, generation),
        )
        current = database.execute(
            "SELECT * FROM rendezvous_groups WHERE rendezvous_key = ? AND generation = ?",
            (key, generation),
        ).fetchone()
        assert current is not None
        return rendezvous_from_row(database, current)
