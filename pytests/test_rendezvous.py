from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from tools.ipc_client import call
from tools.state import replay_interactions


ROOT = Path(__file__).resolve().parents[1]


class RendezvousTest(unittest.TestCase):
    def test_independent_processes_timeout_cancel_generation_and_disconnect(self) -> None:
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
            )
            try:
                deadline = time.monotonic() + 5
                while not socket_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(socket_path.exists())

                base = [
                    sys.executable,
                    str(ROOT / "tools/rendezvous_client.py"),
                    "--socket",
                    str(socket_path),
                    "pair",
                    "--generation",
                    "7",
                    "--parties",
                    "2",
                    "--timeout",
                    "3s",
                ]
                first = subprocess.Popen(
                    base + ["--participant", "alpha", "--payload", '{"from":"alpha"}'],
                    cwd=ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                second = subprocess.Popen(
                    base + ["--participant", "beta", "--payload", '{"from":"beta"}'],
                    cwd=ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                first_stdout, first_stderr = first.communicate(timeout=5)
                second_stdout, second_stderr = second.communicate(timeout=5)
                self.assertEqual(first.returncode, 0, first_stderr)
                self.assertEqual(second.returncode, 0, second_stderr)
                first_result = json.loads(first_stdout)
                second_result = json.loads(second_stdout)
                self.assertEqual(first_result["peerMessages"][0]["participantId"], "beta")
                self.assertEqual(second_result["peerMessages"][0]["participantId"], "alpha")

                timeout = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "tools/rendezvous_client.py"),
                        "--socket",
                        str(socket_path),
                        "pair",
                        "--generation",
                        "8",
                        "--participant",
                        "lonely",
                        "--payload",
                        "null",
                        "--parties",
                        "2",
                        "--timeout",
                        "100ms",
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                self.assertEqual(timeout.returncode, 3, timeout.stderr)
                self.assertEqual(json.loads(timeout.stdout)["status"], "cancelled")
                self.assertEqual(
                    call(
                        socket_path,
                        "rendezvous.inspect",
                        {"key": "pair", "generation": 7},
                    )["status"],
                    "released",
                )

                waiting = subprocess.Popen(
                    base[:-7]
                    + [
                        "cancelled-pair",
                        "--generation",
                        "9",
                        "--participant",
                        "waiting",
                        "--payload",
                        "true",
                        "--parties",
                        "2",
                        "--timeout",
                        "3s",
                    ],
                    cwd=ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    try:
                        call(
                            socket_path,
                            "rendezvous.inspect",
                            {"key": "cancelled-pair", "generation": 9},
                        )
                        break
                    except RuntimeError:
                        time.sleep(0.02)
                call(
                    socket_path,
                    "rendezvous.cancel",
                    {
                        "key": "cancelled-pair",
                        "generation": 9,
                        "participantId": "test-controller",
                        "reason": "explicit-test-cancel",
                    },
                )
                waiting_stdout, waiting_stderr = waiting.communicate(timeout=3)
                self.assertEqual(waiting.returncode, 3, waiting_stderr)
                self.assertEqual(json.loads(waiting_stdout)["status"], "cancelled")

                disconnected = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                disconnected.connect(str(socket_path))
                disconnected.sendall(
                    json.dumps(
                        {
                            "version": 1,
                            "id": "disconnect",
                            "method": "rendezvous.join",
                            "params": {
                                "key": "gone",
                                "generation": 10,
                                "participantId": "vanished",
                                "payload": {"available": True},
                                "parties": 2,
                                "timeoutMs": 3000,
                            },
                        },
                        separators=(",", ":"),
                    ).encode()
                    + b"\n"
                )
                disconnected.close()
                deadline = time.monotonic() + 2
                disconnected_state = None
                while time.monotonic() < deadline:
                    try:
                        disconnected_state = call(
                            socket_path,
                            "rendezvous.inspect",
                            {"key": "gone", "generation": 10},
                        )
                    except RuntimeError:
                        time.sleep(0.02)
                        continue
                    if disconnected_state["status"] == "cancelled":
                        break
                    time.sleep(0.02)
                self.assertIsNotNone(disconnected_state)
                self.assertEqual(disconnected_state["status"], "cancelled")
                self.assertEqual(
                    disconnected_state["cancellationReason"],
                    "client-disconnected:vanished",
                )

                replay = replay_interactions(database)
                self.assertTrue(replay["deterministic"], replay)
                self.assertEqual(replay["rendezvousCount"], 4)
            finally:
                daemon.terminate()
                daemon.wait(timeout=5)
                if daemon.stdout is not None:
                    daemon.stdout.close()
                if daemon.stderr is not None:
                    daemon.stderr.close()
            self.assertFalse(socket_path.exists())


if __name__ == "__main__":
    unittest.main()
