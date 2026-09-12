from __future__ import annotations

from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from tools.ipc_client import call
from tools.state import create_interaction


ROOT = Path(__file__).resolve().parents[1]


class DaemonProtocolTest(unittest.TestCase):
    def test_versioned_ipc_resumes_from_cursor_and_slow_client_does_not_block(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "nema.db"
            socket_path = root / "nema.sock"
            daemon = subprocess.Popen(
                [
                    sys.executable,
                    str(ROOT / "tools/daemon.py"),
                    "--db",
                    str(database),
                    "--socket",
                    str(socket_path),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            slow = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                deadline = time.monotonic() + 5.0
                while not socket_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(socket_path.exists())
                self.assertEqual(call(socket_path, "health", {})["status"], "ok")

                created = create_interaction(
                    database,
                    "ipc-choice",
                    "Choose over IPC",
                    [{"id": "yes", "label": "Yes"}],
                    requester="ipc-test",
                )
                watermark = created["surfaceRevision"]
                slow.connect(str(socket_path))
                slow.sendall(b'{"version":1,"id":"slow"')

                # One client is stalled mid-frame. A distinct handler still
                # serves a snapshot and the control command.
                surface = call(socket_path, "surface.get", {})
                interaction = surface["interactions"][0]
                receipt = call(
                    socket_path,
                    "interaction.respond",
                    {
                        "interactionId": "ipc-choice",
                        "relevantInputRevision": interaction["relevantInputRevision"],
                        "answer": "yes",
                        "responder": "separate-ipc-client",
                    },
                )
                self.assertEqual(receipt["disposition"], "accepted")
                tail = call(
                    socket_path, "events.list", {"afterRevision": watermark}
                )
                self.assertEqual(tail["events"][0]["type"], "interaction.response-accepted")
            finally:
                slow.close()
                daemon.terminate()
                daemon.wait(timeout=5.0)
                if daemon.stdout is not None:
                    daemon.stdout.close()
                if daemon.stderr is not None:
                    daemon.stderr.close()
            self.assertFalse(socket_path.exists())


if __name__ == "__main__":
    unittest.main()
