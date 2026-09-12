#!/usr/bin/env python3
"""Serialized SQLite ingestion and deterministic replay for raw Nema captures."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
import json
import hashlib
from pathlib import Path
import sqlite3
import sys
from typing import Any


REDUCER_VERSION = "capture-v1"


SCHEMA = """
CREATE TABLE IF NOT EXISTS connections (
    connection_id TEXT NOT NULL,
    connection_epoch INTEGER NOT NULL,
    transport TEXT NOT NULL,
    PRIMARY KEY (connection_id, connection_epoch)
);

CREATE TABLE IF NOT EXISTS raw_records (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    connection_epoch INTEGER NOT NULL,
    local_sequence INTEGER NOT NULL,
    direction TEXT NOT NULL,
    stream TEXT NOT NULL,
    observed_monotonic_ns INTEGER NOT NULL,
    available_bytes BLOB NOT NULL,
    source_capture TEXT NOT NULL,
    UNIQUE (connection_id, connection_epoch, local_sequence),
    FOREIGN KEY (connection_id, connection_epoch)
        REFERENCES connections(connection_id, connection_epoch)
);

CREATE TABLE IF NOT EXISTS record_interpretations (
    record_seq INTEGER NOT NULL,
    reducer_version TEXT NOT NULL,
    parse_status TEXT NOT NULL,
    message_kind TEXT NOT NULL,
    method TEXT,
    json_id_type TEXT,
    json_id_text TEXT,
    initiating_peer TEXT,
    PRIMARY KEY (record_seq, reducer_version),
    FOREIGN KEY (record_seq) REFERENCES raw_records(seq)
);

CREATE TABLE IF NOT EXISTS external_ingress (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL UNIQUE,
    observed_monotonic_ns INTEGER NOT NULL,
    available_bytes BLOB,
    capture_status TEXT NOT NULL,
    original_byte_count INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    delayed_ingestion INTEGER NOT NULL,
    spool_path TEXT
);
"""


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, factory=ClosingConnection)
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = FULL")
    connection.executescript(SCHEMA)
    return connection


def id_parts(value: Any) -> tuple[str, str]:
    if value is None:
        return ("null", "null")
    if isinstance(value, bool):
        return ("boolean", "true" if value else "false")
    if isinstance(value, int):
        return ("integer", str(value))
    if isinstance(value, str):
        return ("string", value)
    return ("other", json.dumps(value, ensure_ascii=False, sort_keys=True))


def classify(payload: bytes, direction: str, stream: str) -> dict[str, Any]:
    if stream != "stdout" and stream != "stdin":
        return {
            "parse_status": "not-json-stream",
            "message_kind": "diagnostic",
            "method": None,
            "json_id_type": None,
            "json_id_text": None,
            "initiating_peer": None,
        }
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {
            "parse_status": "malformed",
            "message_kind": "malformed",
            "method": None,
            "json_id_type": None,
            "json_id_text": None,
            "initiating_peer": None,
        }
    if not isinstance(value, dict):
        return {
            "parse_status": "valid-json",
            "message_kind": "unknown",
            "method": None,
            "json_id_type": None,
            "json_id_text": None,
            "initiating_peer": None,
        }

    source_peer = "nema" if direction == "nema-to-server" else "server"
    method = value.get("method") if isinstance(value.get("method"), str) else None
    has_id = "id" in value
    identifier_type = None
    identifier_text = None
    if has_id:
        identifier_type, identifier_text = id_parts(value.get("id"))

    if method is not None and has_id:
        kind = "request"
        initiator = source_peer
    elif method is not None:
        kind = "notification"
        initiator = source_peer
    elif has_id and ("result" in value or "error" in value):
        kind = "response"
        initiator = "server" if source_peer == "nema" else "nema"
    else:
        kind = "unknown"
        initiator = None

    return {
        "parse_status": "decoded",
        "message_kind": kind,
        "method": method,
        "json_id_type": identifier_type,
        "json_id_text": identifier_text,
        "initiating_peer": initiator,
    }


def read_capture(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid capture envelope at line {line_number}: {error}") from error
        if not isinstance(record, dict):
            raise ValueError(f"capture envelope at line {line_number} is not an object")
        records.append(record)
    return records


def ingest_envelope(
    database: sqlite3.Connection,
    envelope: dict[str, Any],
    source_capture: str,
) -> tuple[bool, int]:
    """Insert one capture envelope and its versioned interpretation.

    The caller owns transaction serialization. The boolean reports whether a
    new raw observation was inserted; an exact repeat reuses its stable row.
    """
    payload = base64.b64decode(envelope["bytesBase64"], validate=True)
    connection_id = str(envelope["connectionId"])
    connection_epoch = int(envelope["connectionEpoch"])
    local_sequence = int(envelope["localSequence"])
    direction = str(envelope["direction"])
    stream = str(envelope["stream"])
    monotonic_ns = int(envelope["observedMonotonicNs"])

    database.execute(
        "INSERT OR IGNORE INTO connections(connection_id, connection_epoch, transport) VALUES (?, ?, ?)",
        (connection_id, connection_epoch, "stdio-jsonl"),
    )
    existing = database.execute(
        """
        SELECT seq, direction, stream, available_bytes
        FROM raw_records
        WHERE connection_id = ? AND connection_epoch = ? AND local_sequence = ?
        """,
        (connection_id, connection_epoch, local_sequence),
    ).fetchone()
    if existing is None:
        cursor = database.execute(
            """
            INSERT INTO raw_records(
                connection_id, connection_epoch, local_sequence,
                direction, stream, observed_monotonic_ns,
                available_bytes, source_capture
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                connection_id,
                connection_epoch,
                local_sequence,
                direction,
                stream,
                monotonic_ns,
                payload,
                source_capture,
            ),
        )
        record_seq = int(cursor.lastrowid)
        inserted = True
    else:
        record_seq = int(existing[0])
        if existing[1] != direction or existing[2] != stream or existing[3] != payload:
            raise ValueError(
                "conflicting observation for "
                f"{connection_id}/{connection_epoch}/{local_sequence}"
            )
        inserted = False

    interpretation = classify(payload, direction, stream)
    database.execute(
        """
        INSERT OR REPLACE INTO record_interpretations(
            record_seq, reducer_version, parse_status, message_kind,
            method, json_id_type, json_id_text, initiating_peer
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record_seq,
            REDUCER_VERSION,
            interpretation["parse_status"],
            interpretation["message_kind"],
            interpretation["method"],
            interpretation["json_id_type"],
            interpretation["json_id_text"],
            interpretation["initiating_peer"],
        ),
    )
    return inserted, record_seq


def ingest_external(
    database_path: Path,
    *,
    source: str,
    source_id: str,
    observed_monotonic_ns: int,
    available_bytes: bytes | None,
    original_byte_count: int,
    digest: str,
    capture_status: str,
    delayed_ingestion: bool,
    spool_path: str | None,
) -> dict[str, Any]:
    if capture_status not in ("exact", "gap"):
        raise ValueError(f"unsupported capture status {capture_status!r}")
    if available_bytes is not None:
        actual_digest = hashlib.sha256(available_bytes).hexdigest()
        if actual_digest != digest or len(available_bytes) != original_byte_count:
            raise ValueError("external ingress byte count or digest does not match payload")
    elif capture_status != "gap":
        raise ValueError("exact external ingress requires available bytes")
    with open_database(database_path) as database:
        existing = database.execute(
            """
            SELECT source, observed_monotonic_ns, available_bytes, capture_status,
                   original_byte_count, sha256
            FROM external_ingress WHERE source_id = ?
            """,
            (source_id,),
        ).fetchone()
        if existing is not None:
            expected = (
                source,
                observed_monotonic_ns,
                available_bytes,
                capture_status,
                original_byte_count,
                digest,
            )
            if tuple(existing) != expected:
                raise ValueError(f"conflicting external ingress source id {source_id!r}")
            return {"disposition": "existing", "sourceId": source_id}
        cursor = database.execute(
            """
            INSERT INTO external_ingress(
                source, source_id, observed_monotonic_ns, available_bytes,
                capture_status, original_byte_count, sha256,
                delayed_ingestion, spool_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                source_id,
                observed_monotonic_ns,
                available_bytes,
                capture_status,
                original_byte_count,
                digest,
                1 if delayed_ingestion else 0,
                spool_path,
            ),
        )
        return {
            "disposition": "inserted",
            "sourceId": source_id,
            "recordSeq": int(cursor.lastrowid),
            "captureStatus": capture_status,
            "delayedIngestion": delayed_ingestion,
        }


def ingest(database_path: Path, capture_path: Path) -> dict[str, Any]:
    capture_records = read_capture(capture_path)
    inserted = 0
    reused = 0
    with open_database(database_path) as database:
        for envelope in capture_records:
            was_inserted, _ = ingest_envelope(database, envelope, str(capture_path))
            if was_inserted:
                inserted += 1
            else:
                reused += 1

    return {
        "database": str(database_path),
        "capture": str(capture_path),
        "inserted": inserted,
        "reused": reused,
        "reducerVersion": REDUCER_VERSION,
    }


def replay(database_path: Path) -> dict[str, Any]:
    with open_database(database_path) as database:
        rows = database.execute(
            """
            SELECT r.seq, r.connection_id, r.connection_epoch, r.local_sequence,
                   r.direction, r.stream, r.available_bytes,
                   i.parse_status, i.message_kind, i.method, i.json_id_type,
                   i.json_id_text, i.initiating_peer
            FROM raw_records r
            JOIN record_interpretations i
              ON i.record_seq = r.seq AND i.reducer_version = ?
            ORDER BY r.seq
            """,
            (REDUCER_VERSION,),
        ).fetchall()
        external_rows = database.execute(
            """
            SELECT source_id, available_bytes, capture_status,
                   original_byte_count, sha256
            FROM external_ingress ORDER BY seq
            """
        ).fetchall()

    mismatches: list[int] = []
    kinds: Counter[str] = Counter()
    correlations: list[dict[str, Any]] = []
    for row in rows:
        (
            seq,
            connection_id,
            connection_epoch,
            local_sequence,
            direction,
            stream,
            payload,
            parse_status,
            message_kind,
            method,
            json_id_type,
            json_id_text,
            initiating_peer,
        ) = row
        derived = classify(payload, direction, stream)
        stored = {
            "parse_status": parse_status,
            "message_kind": message_kind,
            "method": method,
            "json_id_type": json_id_type,
            "json_id_text": json_id_text,
            "initiating_peer": initiating_peer,
        }
        if derived != stored:
            mismatches.append(seq)
        kinds[message_kind] += 1
        if json_id_type is not None:
            correlations.append(
                {
                    "connectionId": connection_id,
                    "connectionEpoch": connection_epoch,
                    "localSequence": local_sequence,
                    "messageKind": message_kind,
                    "initiatingPeer": initiating_peer,
                    "jsonIdType": json_id_type,
                    "jsonIdText": json_id_text,
                }
            )

    external_mismatches: list[str] = []
    external_statuses: Counter[str] = Counter()
    for source_id, available_bytes, capture_status, byte_count, digest in external_rows:
        external_statuses[capture_status] += 1
        if capture_status == "exact":
            valid = (
                available_bytes is not None
                and len(available_bytes) == byte_count
                and hashlib.sha256(available_bytes).hexdigest() == digest
            )
        else:
            valid = capture_status == "gap" and available_bytes is None
        if not valid:
            external_mismatches.append(source_id)

    return {
        "database": str(database_path),
        "reducerVersion": REDUCER_VERSION,
        "recordCount": len(rows),
        "kindCounts": dict(sorted(kinds.items())),
        "correlations": correlations,
        "mismatchedRecordSeqs": mismatches,
        "externalIngressCount": len(external_rows),
        "externalIngressStatusCounts": dict(sorted(external_statuses.items())),
        "mismatchedExternalIngressSourceIds": external_mismatches,
        "deterministic": not mismatches and not external_mismatches,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("--db", type=Path, required=True)
    ingest_parser.add_argument("--capture", type=Path, required=True)
    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--db", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "ingest":
        report = ingest(args.db, args.capture)
    else:
        report = replay(args.db)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
