#!/usr/bin/env python3
"""Idempotently merge and inspect Nema's project-local Codex hooks."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid
from typing import Any


EVENTS = (
    "SessionStart",
    "SessionEnd",
    "UserPromptSubmit",
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
    "Interrupt",
)
COMMAND = '/usr/bin/python3 "$(git rev-parse --show-toplevel)/tools/hook_capture.py"'
HANDLER = {"type": "command", "command": COMMAND, "timeout": 3}


def contains_handler(groups: list[Any]) -> bool:
    return any(
        isinstance(group, dict)
        and isinstance(group.get("hooks"), list)
        and HANDLER in group["hooks"]
        for group in groups
    )


def install(config_path: Path) -> dict[str, Any]:
    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(config, dict):
            raise ValueError("hooks config must contain a JSON object")
    else:
        config = {}
    hooks = config.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        raise ValueError("hooks config 'hooks' member must be an object")
    config.setdefault(
        "description", "Capture Codex lifecycle observations in Nema's raw journal."
    )
    added = []
    for event in EVENTS:
        groups = hooks.setdefault(event, [])
        if not isinstance(groups, list):
            raise ValueError(f"hooks config event {event!r} must contain an array")
        if not contains_handler(groups):
            groups.append({"hooks": [dict(HANDLER)]})
            added.append(event)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = config_path.with_name(f".{config_path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, config_path)
    return {"config": str(config_path), "addedEvents": added, "changed": bool(added)}


def inspect(project_root: Path) -> dict[str, Any]:
    config_path = project_root / ".codex/hooks.json"
    config = (
        json.loads(config_path.read_text(encoding="utf-8"))
        if config_path.exists()
        else {}
    )
    hooks = config.get("hooks", {}) if isinstance(config, dict) else {}
    configured = {
        event: contains_handler(groups)
        for event, groups in hooks.items()
        if isinstance(groups, list)
    }
    database_path = project_root / ".nema/nema.db"
    ingress = None
    if database_path.exists():
        with sqlite3.connect(database_path) as database:
            table = database.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='external_ingress'"
            ).fetchone()
            if table is not None:
                ingress = database.execute(
                    """
                    SELECT COUNT(*),
                           SUM(CASE WHEN capture_status = 'exact' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN capture_status = 'gap' THEN 1 ELSE 0 END)
                    FROM external_ingress
                    """
                ).fetchone()
    return {
        "config": str(config_path),
        "nemaEvents": sorted(event for event, present in configured.items() if present),
        "missingNemaEvents": sorted(set(EVENTS) - {e for e, present in configured.items() if present}),
        "spooledFiles": len(list((project_root / ".nema/hook-spool").glob("*.json"))),
        "ingress": (
            {"total": ingress[0], "exact": ingress[1] or 0, "gaps": ingress[2] or 0}
            if ingress is not None
            else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("install", "inspect"))
    parser.add_argument(
        "--project-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args()
    if args.command == "install":
        result = install(args.project_root / ".codex/hooks.json")
    else:
        result = inspect(args.project_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
