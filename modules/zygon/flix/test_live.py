"""Real Flix JVM and retained Unix protocol; provider is a synthetic endpoint."""
import asyncio
import contextlib
import os
from pathlib import Path
import tempfile
import unittest

from zygon.client import Client
from zygon.server import Server

MODULE = Path(__file__).resolve().parents[1]
REPO = MODULE.parents[1]


class LiveFlixTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        base = REPO / ".nema" / "ft"
        base.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="r-", dir=base)
        self.server = await Server(Path(self.temp.name)).start()
        self.provider = await Client.connect(self.server.socket_path)
        self.host = dict(id="flix-test-host", incarnation="host-incarnation-1", kind="host",
            medium="cooperative-test-endpoint", mode="attached", parent=None,
            operations=["fixture.delay"], binding={"endpoint": "synthetic-protocol-fixture"})
        await self.provider.call("register", {"registration": self.host})
        for number in range(2):
            child = self.host | dict(id=f"flix-test-surface-{number}", kind="surface",
                incarnation=f"surface-incarnation-{number}", parent=self.ref(), operations=[])
            await self.provider.call("register", {"registration": child})

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.provider.close()
        await self.server.close()
        self.temp.cleanup()

    def ref(self):
        return {key: self.host[key] for key in ("id", "incarnation")}

    async def run_flix(self, **changes):
        environment = os.environ | dict(ZYGON_SOCKET=str(self.server.socket_path), ZYGON_TARGET_ID=self.host["id"],
            ZYGON_TARGET_INCARNATION=self.host["incarnation"], ZYGON_OPERATION="fixture.delay", ZYGON_ARGUMENTS="{}") | changes
        process = await asyncio.create_subprocess_exec(str(MODULE / "flix/run"), cwd=REPO, env=environment,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 90)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        return process.returncode, output.decode()

    async def test_actual_flix_observes_and_resolves_same_retained_record(self):
        terminal = asyncio.Queue()
        async def handler(invocation):
            params = dict(invocationId=invocation["id"], target=self.ref())
            await self.provider.call("result", params | dict(status="started"))
            await asyncio.sleep(0.05)
            await self.provider.call("observe", dict(source=self.host["id"], incarnation=self.host["incarnation"],
                kind="fixture.during-request", body={"correlation": invocation["correlation"]}, parents=[invocation["recordId"]]))
            await asyncio.sleep(0.1)
            result = await self.provider.call("result", params | dict(status="completed", value={"echo": "original correlation"}))
            await terminal.put(result)
        self.provider.on_invoke = handler
        code, output = await self.run_flix(ZYGON_REQUIRE_OBSERVATION="1")
        self.assertEqual(code, 0, output)
        winner = await asyncio.wait_for(terminal.get(), 2)
        self.assertIn("FLIX observation while request waits", output)
        self.assertIn("fixture.during-request", output)
        self.assertIn("status=Completed", output)
        self.assertIn("recordId=" + winner["recordId"], output)
        self.assertIn("correlation=" + winner["correlation"], output)
        self.assertIn("flix-test-surface-0", output)
        self.assertIn("flix-test-surface-1", output)
        self.assertLess(output.index("FLIX observation while request waits"), output.index("FLIX resolved"))

    async def test_rejected_stale_target_does_not_strand_csp_worker(self):
        code, output = await self.run_flix(ZYGON_TARGET_INCARNATION="retired-incarnation")
        self.assertNotEqual(code, 0, output)
        self.assertIn("stale-incarnation", output)
        self.assertEqual((await self.provider.call("inspect", {}))["invocations"], [])

    async def test_provider_disconnect_resumes_with_retained_uncertainty(self):
        async def handler(_invocation):
            await self.provider.close()
        self.provider.on_invoke = handler
        code, output = await self.run_flix()
        self.assertEqual(code, 0, output)
        self.assertIn("status=OutcomeUnknown", output)
        self.assertRegex(output, r"recordId=[0-9a-f-]{36}")
        self.assertIn("provider-disconnected", output)


if __name__ == "__main__":
    unittest.main()
