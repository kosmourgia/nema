from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from tools.hooks_config import EVENTS, HANDLER, install


class HooksConfigTest(unittest.TestCase):
    def test_install_is_additive_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / ".codex/hooks.json"
            config_path.parent.mkdir()
            custom = {
                "description": "keep me",
                "futureTopLevel": {"kept": True},
                "hooks": {
                    "SessionStart": [
                        {"hooks": [{"type": "command", "command": "custom-command"}]}
                    ],
                    "FutureEvent": [{"future": "unchanged"}],
                },
            }
            config_path.write_text(json.dumps(custom), encoding="utf-8")
            first = install(config_path)
            second = install(config_path)
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            result = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(result["description"], "keep me")
            self.assertEqual(result["futureTopLevel"], {"kept": True})
            self.assertEqual(result["hooks"]["FutureEvent"], [{"future": "unchanged"}])
            self.assertEqual(
                result["hooks"]["SessionStart"][0], custom["hooks"]["SessionStart"][0]
            )
            for event in EVENTS:
                count = sum(
                    HANDLER in group.get("hooks", [])
                    for group in result["hooks"][event]
                    if isinstance(group, dict)
                )
                self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
