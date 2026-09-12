from __future__ import annotations

from pathlib import Path
import tempfile
import time
import unittest

from tools.app_server_session import AppServerSession
from tools.journal import open_database, replay


ROOT = Path(__file__).resolve().parents[1]


class AppServerSessionTest(unittest.TestCase):
    def test_journals_online_while_request_worker_waits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "nema.db"
            capture = root / "session.jsonl"

            def delayed_disposition(message):
                time.sleep(0.2)
                return {
                    "error": {
                        "code": -32601,
                        "message": f"fixture rejects {message['method']}",
                    }
                }

            session = AppServerSession(
                database,
                capture,
                command=["python3", str(ROOT / "tools/fake_app_server.py")],
                connection_id="session-fixture",
                server_request_handler=delayed_disposition,
            )
            try:
                initialized = session.initialize()
                self.assertEqual(initialized["userAgent"], "nema-fake/1")
                features = session.request("experimentalFeature/list", {})
                self.assertEqual(features["data"][0]["name"], "fake_feature")

                # The response is already durable although the worker is still
                # waiting. This queries through a second live connection.
                self.assertEqual(session.pending_workers, 1)
                with open_database(database) as observer:
                    kinds = {
                        row[0]: row[1]
                        for row in observer.execute(
                            """
                            SELECT message_kind, COUNT(*) FROM record_interpretations
                            GROUP BY message_kind
                            """
                        )
                    }
                self.assertGreaterEqual(kinds["response"], 2)
                session.drain_workers()
                self.assertEqual(session.pending_workers, 0)
            finally:
                session.close()

            self.assertTrue(replay(database)["deterministic"])


if __name__ == "__main__":
    unittest.main()
