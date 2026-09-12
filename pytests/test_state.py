from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from tools.state import (
    advance_fixture_experiment,
    advance_live_experiment,
    changes_after,
    choose_fixture_experiment,
    complete_live_delivery,
    create_interaction,
    open_database,
    mark_live_delivery_started,
    prepare_live_delivery,
    record_live_fork_experiment,
    replay_interactions,
    respond,
    snapshot,
    start_fixture_experiment,
)


class DurableInteractionTest(unittest.TestCase):
    def test_surfaces_and_single_answer_semantics_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nema.db"
            created = create_interaction(
                database,
                "selection-1",
                "Choose a parser",
                [
                    {"id": "streaming", "label": "Streaming parser"},
                    {"id": "state-machine", "label": "State-machine parser"},
                ],
                requester="fixture-controller",
            )
            input_revision = created["interaction"]["relevantInputRevision"]
            watermark = created["surfaceRevision"]

            # An unrelated event advances the global frontier but cannot make
            # selection-1's still-current input revision stale.
            create_interaction(
                database,
                "unrelated-1",
                "Unrelated choice",
                [{"id": "ok", "label": "OK"}],
                requester="fixture-controller",
            )
            accepted = respond(
                database,
                "selection-1",
                input_revision,
                "streaming",
                responder="human-test",
            )
            self.assertEqual(accepted["disposition"], "accepted")

            # Each call opens a new SQLite connection, exercising durable
            # reconstruction rather than relying on a live waiter or closure.
            current = snapshot(database)
            selection = next(
                item for item in current["interactions"] if item["id"] == "selection-1"
            )
            self.assertEqual(selection["status"], "resolved")
            self.assertEqual(selection["answer"], "streaming")
            self.assertEqual(selection["actions"], [])

            identical = respond(
                database,
                "selection-1",
                input_revision,
                "streaming",
                responder="retry-test",
            )
            conflict = respond(
                database,
                "selection-1",
                input_revision,
                "state-machine",
                responder="conflict-test",
            )
            self.assertEqual(identical["disposition"], "identical-retry")
            self.assertEqual(conflict["disposition"], "already-resolved")

            tail = changes_after(database, watermark)
            self.assertGreaterEqual(tail["frontier"], accepted["surfaceRevision"])
            self.assertTrue(
                any(event["type"] == "interaction.response-accepted" for event in tail["events"])
            )

    def test_stale_and_invalid_attempts_are_retained_without_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nema.db"
            created = create_interaction(
                database,
                "selection-2",
                "Choose",
                [{"id": "a", "label": "A"}],
                requester="fixture-controller",
            )
            revision = created["interaction"]["relevantInputRevision"]
            stale = respond(database, "selection-2", revision + 1, "a", responder="test")
            invalid = respond(database, "selection-2", revision, "b", responder="test")
            self.assertEqual(stale["disposition"], "stale")
            self.assertEqual(invalid["disposition"], "invalid-answer")
            self.assertEqual(snapshot(database)["interactions"][0]["status"], "pending")

    def test_fixture_fork_controller_resumes_at_durable_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nema.db"
            started = start_fixture_experiment(database, "fork-1")
            self.assertEqual(started["experiment"]["controlPoint"], "awaiting-selection")
            self.assertEqual(len(started["experiment"]["branches"]), 2)
            self.assertEqual(
                start_fixture_experiment(database, "fork-1")["disposition"], "existing"
            )

            chosen_artifact = "fork-1-streaming-output"
            chosen = choose_fixture_experiment(
                database, "fork-1", chosen_artifact, "separate-fixture-chooser"
            )
            self.assertEqual(chosen["disposition"], "accepted")

            # Advancing through a fresh call reconstructs the named controller
            # step from records; it does not claim to revive a Flix stack.
            advanced = advance_fixture_experiment(database, "fork-1")
            self.assertEqual(advanced["disposition"], "advanced")
            self.assertEqual(
                advanced["experiment"]["controlPoint"], "fixture-completed"
            )
            bundle = advanced["resultBundle"]
            self.assertEqual(bundle["chosenArtifactId"], chosen_artifact)
            self.assertEqual(len(bundle["sources"]), 2)
            self.assertEqual(sum(source["selected"] for source in bundle["sources"]), 1)

            repeated = advance_fixture_experiment(database, "fork-1")
            self.assertEqual(repeated["disposition"], "already-completed")
            with open_database(database) as connection:
                bundle_count = connection.execute(
                    "SELECT COUNT(*) FROM artifacts WHERE artifact_id = 'fork-1-result-bundle'"
                ).fetchone()[0]
            self.assertEqual(bundle_count, 1)
            self.assertTrue(replay_interactions(database)["deterministic"])

    def test_replay_detects_projection_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nema.db"
            create_interaction(
                database,
                "selection-drift",
                "Choose",
                [{"id": "a", "label": "A"}],
                requester="test",
            )
            self.assertTrue(replay_interactions(database)["deterministic"])
            with open_database(database) as connection:
                connection.execute(
                    "UPDATE interactions SET prompt = 'corrupted projection' "
                    "WHERE interaction_id = 'selection-drift'"
                )
            report = replay_interactions(database)
            self.assertFalse(report["deterministic"])
            self.assertEqual(report["mismatchedInteractionIds"], ["selection-drift"])

    def test_live_controller_records_delivery_before_external_send(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nema.db"
            recorded = record_live_fork_experiment(
                database,
                "live-fork-1",
                source_native_thread_id="native-parent",
                source_checkpoint="native-turn-1",
                source_history={"thread": {"id": "native-parent", "turns": []}},
                branches=[
                    {
                        "id": "live-fork-1-a",
                        "label": "A",
                        "nativeThreadId": "native-a",
                        "persistenceMode": "persisted",
                        "workspaceMode": "read-only-shared",
                        "prompt": "Try A",
                        "output": "A output",
                    },
                    {
                        "id": "live-fork-1-b",
                        "label": "B",
                        "nativeThreadId": "native-b",
                        "persistenceMode": "ephemeral",
                        "workspaceMode": "read-only-shared",
                        "prompt": "Try B",
                        "output": "B output",
                    },
                ],
            )
            self.assertEqual(recorded["experiment"]["controlPoint"], "awaiting-selection")
            choose_fixture_experiment(
                database,
                "live-fork-1",
                "live-fork-1-b-output",
                "separate-agent",
            )
            bundle = advance_live_experiment(database, "live-fork-1")
            self.assertEqual(bundle["experiment"]["controlPoint"], "bundle-ready")
            prepared = prepare_live_delivery(database, "live-fork-1")
            self.assertEqual(prepared["experiment"]["deliveryStatus"], "prepared-not-sent")
            self.assertIn("live-fork-1-b-output", prepared["deliveryInput"])
            started_delivery = mark_live_delivery_started(
                database, "live-fork-1", "parent-turn-2"
            )
            self.assertEqual(
                started_delivery["experiment"]["deliveryStatus"], "native-turn-started"
            )
            completed = complete_live_delivery(
                database,
                "live-fork-1",
                native_turn_id="parent-turn-2",
                parent_output="Parent acknowledged B.",
            )
            self.assertEqual(completed["experiment"]["controlPoint"], "live-completed")
            self.assertEqual(completed["experiment"]["deliveryStatus"], "observed-completed")
            self.assertTrue(replay_interactions(database)["deterministic"])
            with open_database(database) as connection:
                connection.execute(
                    "UPDATE artifacts SET available_bytes = X'00' "
                    "WHERE artifact_id = 'live-fork-1-parent-response'"
                )
            corrupted = replay_interactions(database)
            self.assertFalse(corrupted["deterministic"])
            self.assertEqual(
                corrupted["artifactHashMismatchIds"],
                ["live-fork-1-parent-response"],
            )


if __name__ == "__main__":
    unittest.main()
