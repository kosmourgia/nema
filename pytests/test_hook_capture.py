from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from tools.hook_ingest import drain
from tools.journal import replay


ROOT = Path(__file__).resolve().parents[1]


class HookCaptureTest(unittest.TestCase):
    def test_concurrent_fallback_spool_preserves_exact_bytes_and_drains(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spool = root / "spool"
            database = root / "nema.db"
            environment = os.environ.copy()
            environment["NEMA_SOCKET"] = str(root / "absent.sock")
            environment["NEMA_HOOK_SPOOL"] = str(spool)
            payloads = [
                json.dumps(
                    {
                        "hook_event_name": "SessionStart",
                        "session_id": f"session-{index}",
                        "unknownFutureField": {"index": index},
                    },
                    separators=(",", ":"),
                ).encode()
                for index in range(6)
            ]
            processes = [
                subprocess.Popen(
                    [sys.executable, str(ROOT / "tools/hook_capture.py")],
                    cwd=ROOT,
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                for _ in payloads
            ]
            for process, payload in zip(processes, payloads, strict=True):
                stdout, stderr = process.communicate(payload, timeout=5.0)
                self.assertEqual(process.returncode, 0, stderr.decode())
                self.assertEqual(stdout, b"")

            self.assertEqual(len(list(spool.glob("*.json"))), len(payloads))
            report = drain(database, spool)
            self.assertEqual(report["inserted"], len(payloads))
            self.assertEqual(list(spool.glob("*.json")), [])
            with sqlite3.connect(database) as connection:
                rows = connection.execute(
                    "SELECT source, available_bytes, capture_status, delayed_ingestion "
                    "FROM external_ingress ORDER BY available_bytes"
                ).fetchall()
            self.assertEqual({row[1] for row in rows}, set(payloads))
            self.assertTrue(all(row[0] == "codex-hook:SessionStart" for row in rows))
            self.assertTrue(all(row[2:] == ("exact", 1) for row in rows))
            replayed = replay(database)
            self.assertTrue(replayed["deterministic"])
            self.assertEqual(replayed["externalIngressStatusCounts"], {"exact": 6})


if __name__ == "__main__":
    unittest.main()
