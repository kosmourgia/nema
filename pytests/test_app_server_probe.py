from __future__ import annotations

import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class AppServerProbeTest(unittest.TestCase):
    def test_fake_peer_retains_unknown_and_malformed_frames(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_path = root / "raw.jsonl"
            summary_path = root / "summary.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools/app_server_probe.py"),
                    "--db",
                    str(root / "online.db"),
                    "--raw",
                    str(raw_path),
                    "--summary",
                    str(summary_path),
                    "--server-request-delay",
                    "0.2",
                    "--command",
                    sys.executable,
                    str(ROOT / "tools/fake_app_server.py"),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertIsNone(summary["failure"])
            self.assertEqual(summary["serverRequestCount"], 1)
            self.assertEqual(summary["pendingServerRequestCount"], 0)
            self.assertEqual(summary["initialize"]["codexHome"], "<redacted-machine-path>")
            self.assertEqual(
                summary["initialize"]["futureInitializeField"],
                "retained-in-raw-and-summary",
            )

            records = [
                json.loads(line)
                for line in raw_path.read_text(encoding="utf-8").splitlines()
            ]
            exact_payloads = [
                base64.b64decode(record["bytesBase64"]) for record in records
            ]
            self.assertIn(b"{malformed-frame\n", exact_payloads)
            self.assertTrue(
                any(b'"futureField":{"nested":[1,2,3]}' in value for value in exact_payloads)
            )
            self.assertTrue(
                any(b'"method":"future/serverRequest"' in value for value in exact_payloads)
            )

            # The feature response is observed while request interpretation is
            # deliberately sleeping; only the routing thread later writes the
            # explicit disposition. A blocked operation worker did not stop
            # unrelated response processing.
            feature_response_index = next(
                index
                for index, value in enumerate(exact_payloads)
                if b'"id":"nema-feature-list"' in value and b'"result"' in value
            )
            disposition_index = next(
                index
                for index, value in enumerate(exact_payloads)
                if b'"id":"1"' in value and b'"error"' in value
            )
            self.assertLess(feature_response_index, disposition_index)

            database_path = root / "nema.db"
            ingest = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools/journal.py"),
                    "ingest",
                    "--db",
                    str(database_path),
                    "--capture",
                    str(raw_path),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(ingest.returncode, 0, ingest.stderr)

            first_replay = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools/journal.py"),
                    "replay",
                    "--db",
                    str(database_path),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
                check=True,
            ).stdout
            second_replay = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools/journal.py"),
                    "replay",
                    "--db",
                    str(database_path),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
                check=True,
            ).stdout
            self.assertEqual(first_replay, second_replay)
            replay = json.loads(first_replay)
            self.assertTrue(replay["deterministic"])
            self.assertEqual(replay["mismatchedRecordSeqs"], [])

            correlations = replay["correlations"]
            self.assertTrue(
                any(
                    row["initiatingPeer"] == "nema"
                    and row["jsonIdType"] == "integer"
                    and row["jsonIdText"] == "1"
                    for row in correlations
                )
            )
            self.assertTrue(
                any(
                    row["initiatingPeer"] == "server"
                    and row["jsonIdType"] == "string"
                    and row["jsonIdText"] == "1"
                    for row in correlations
                )
            )


if __name__ == "__main__":
    unittest.main()
