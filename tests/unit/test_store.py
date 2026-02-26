"""Unit tests for the store module.

All tests use a temporary .metaflow directory so they are fully isolated
from the user's real metadata and from each other.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from metaflow_local_service import store


@pytest.fixture(autouse=True)
def _isolated_metaflow(tmp_path):
    """Point LocalStorage at a fresh temp directory for every test."""
    # Reset the store's initialized flag so setup() runs fresh each test.
    store._initialized = False
    store._task_counters.clear()

    store.setup(str(tmp_path))

    # Also reset the LocalStorage datastore_root so the local provider
    # is pointed at our temp dir.
    from metaflow.plugins.datastores.local_storage import LocalStorage

    metadir = os.path.join(str(tmp_path), ".metaflow")
    os.makedirs(metadir, exist_ok=True)
    LocalStorage.datastore_root = metadir

    yield

    # Cleanup: reset for safety
    store._initialized = False
    store._task_counters.clear()


class TestSetup:
    def test_creates_metaflow_dir(self, tmp_path):
        store._initialized = False
        new_root = tmp_path / "new_project"
        new_root.mkdir()
        store.setup(str(new_root))
        assert (new_root / ".metaflow").is_dir()

    def test_idempotent(self, tmp_path):
        """Calling setup() twice must not raise."""
        store.setup(str(tmp_path))
        store.setup(str(tmp_path))  # second call is a no-op


class TestRunId:
    def test_is_numeric_string(self):
        run_id = store.new_run_id()
        assert run_id.isdigit()

    def test_monotonically_increases(self):
        ids = [int(store.new_run_id()) for _ in range(5)]
        assert ids == sorted(ids)

    def test_unique(self):
        ids = {store.new_run_id() for _ in range(20)}
        assert len(ids) == 20


class TestFlow:
    def test_create_and_get(self):
        obj, created = store.get_or_create_flow("MyFlow", {})
        assert created is True
        assert obj["flow_id"] == "MyFlow"

    def test_get_returns_none_for_missing(self):
        assert store.get_flow("NoSuchFlow") is None

    def test_idempotent_create(self):
        store.get_or_create_flow("MyFlow", {})
        obj2, created = store.get_or_create_flow("MyFlow", {})
        assert created is False
        assert obj2["flow_id"] == "MyFlow"


class TestRun:
    def test_create_run(self):
        run = store.create_run("MyFlow", {"tags": ["t1"], "system_tags": ["runtime:python"]})
        assert "run_number" in run
        assert run["flow_id"] == "MyFlow"
        assert run["tags"] == ["t1"]

    def test_get_run(self):
        run = store.create_run("MyFlow", {})
        fetched = store.get_run("MyFlow", run["run_number"])
        assert fetched is not None
        assert fetched["run_number"] == run["run_number"]

    def test_get_missing_run(self):
        assert store.get_run("MyFlow", "999999") is None

    def test_list_runs(self):
        store.create_run("MyFlow", {})
        store.create_run("MyFlow", {})
        runs = store.list_runs("MyFlow")
        assert len(runs) == 2

    def test_separate_flows_dont_mix(self):
        store.create_run("FlowA", {})
        store.create_run("FlowB", {})
        assert len(store.list_runs("FlowA")) == 1
        assert len(store.list_runs("FlowB")) == 1


class TestStep:
    def test_create_and_get_step(self):
        run = store.create_run("MyFlow", {})
        step, created = store.get_or_create_step("MyFlow", run["run_number"], "start", {})
        assert created is True
        assert step["step_name"] == "start"

    def test_step_is_idempotent(self):
        run = store.create_run("MyFlow", {})
        run_id = run["run_number"]
        store.get_or_create_step("MyFlow", run_id, "start", {})
        _, created = store.get_or_create_step("MyFlow", run_id, "start", {})
        assert created is False

    def test_list_steps(self):
        run = store.create_run("MyFlow", {})
        run_id = run["run_number"]
        store.get_or_create_step("MyFlow", run_id, "start", {})
        store.get_or_create_step("MyFlow", run_id, "end", {})
        assert len(store.list_steps("MyFlow", run_id)) == 2


class TestTask:
    def _setup_run(self, flow: str = "MyFlow") -> str:
        run = store.create_run(flow, {})
        return run["run_number"]

    def test_create_task(self):
        run_id = self._setup_run()
        task = store.create_task("MyFlow", run_id, "start", {})
        assert "task_id" in task
        assert task["step_name"] == "start"

    def test_task_ids_are_sequential(self):
        run_id = self._setup_run()
        t1 = store.create_task("MyFlow", run_id, "start", {})
        t2 = store.create_task("MyFlow", run_id, "start", {})
        assert int(t2["task_id"]) > int(t1["task_id"])

    def test_task_ids_unique_across_steps(self):
        run_id = self._setup_run()
        t1 = store.create_task("MyFlow", run_id, "start", {})
        t2 = store.create_task("MyFlow", run_id, "process", {})
        assert t1["task_id"] != t2["task_id"]

    def test_get_task(self):
        run_id = self._setup_run()
        task = store.create_task("MyFlow", run_id, "start", {})
        fetched = store.get_task("MyFlow", run_id, "start", task["task_id"])
        assert fetched is not None
        assert fetched["task_id"] == task["task_id"]

    def test_list_tasks(self):
        run_id = self._setup_run()
        store.create_task("MyFlow", run_id, "start", {})
        store.create_task("MyFlow", run_id, "start", {})
        tasks = store.list_tasks("MyFlow", run_id, "start")
        assert len(tasks) == 2

    def test_counter_seeded_from_disk_on_resume(self, tmp_path):
        """After a simulated restart, new task IDs must not collide with old ones."""
        run_id = self._setup_run()
        # Create some tasks
        for _ in range(3):
            store.create_task("MyFlow", run_id, "start", {})
        # Simulate service restart: clear in-memory counter
        store._task_counters.clear()
        # Next task should be 4, not 1
        task = store.create_task("MyFlow", run_id, "start", {})
        assert int(task["task_id"]) == 4


class TestArtifacts:
    def _setup(self) -> tuple[str, str]:
        run = store.create_run("MyFlow", {})
        run_id = run["run_number"]
        task = store.create_task("MyFlow", run_id, "start", {})
        return run_id, task["task_id"]

    def test_register_and_get_artifacts(self):
        run_id, task_id = self._setup()
        artifacts = [
            {
                "name": "my_var",
                "attempt_id": 0,
                "sha": "abc123",
                "ds_type": "local",
                "location": "/tmp/x",
                "content_type": "pickle",
                "type": "metaflow.artifact",
            }
        ]
        store.register_artifacts("MyFlow", run_id, "start", task_id, artifacts)
        result = store.get_artifacts("MyFlow", run_id, "start", task_id)
        assert len(result) == 1
        assert result[0]["name"] == "my_var"

    def test_get_artifacts_for_attempt(self):
        run_id, task_id = self._setup()
        for attempt in (0, 1):
            store.register_artifacts(
                "MyFlow",
                run_id,
                "start",
                task_id,
                [
                    {
                        "name": "x",
                        "attempt_id": attempt,
                        "sha": f"sha{attempt}",
                        "ds_type": "local",
                        "location": "/tmp/x",
                        "content_type": "pickle",
                        "type": "metaflow.artifact",
                    }
                ],
            )
        result = store.get_artifacts("MyFlow", run_id, "start", task_id, attempt=1)
        assert all(a.get("sha") == "sha1" for a in result)

    def test_empty_artifacts(self):
        run_id, task_id = self._setup()
        assert store.get_artifacts("MyFlow", run_id, "start", task_id) == []


class TestMetadata:
    def _setup(self) -> tuple[str, str]:
        run = store.create_run("MyFlow", {})
        run_id = run["run_number"]
        task = store.create_task("MyFlow", run_id, "start", {})
        return run_id, task["task_id"]

    def test_register_and_get_metadata(self):
        run_id, task_id = self._setup()
        metadata = [
            {
                "field_name": "runtime",
                "value": "python",
                "type": "runtime",
                "tags": ["attempt_id:0"],
            }
        ]
        store.register_metadata("MyFlow", run_id, "start", task_id, metadata)
        result = store.get_metadata("MyFlow", run_id, "start", task_id)
        fields = {m["field_name"] for m in result}
        assert "runtime" in fields

    def test_multiple_metadata_fields(self):
        run_id, task_id = self._setup()
        entries = [
            {"field_name": "a", "value": "1", "type": "t", "tags": []},
            {"field_name": "b", "value": "2", "type": "t", "tags": []},
        ]
        store.register_metadata("MyFlow", run_id, "start", task_id, entries)
        result = store.get_metadata("MyFlow", run_id, "start", task_id)
        fields = {m["field_name"] for m in result}
        assert {"a", "b"} <= fields

    def test_empty_metadata(self):
        run_id, task_id = self._setup()
        assert store.get_metadata("MyFlow", run_id, "start", task_id) == []


class TestTagMutation:
    def test_add_and_remove_tags(self):
        run = store.create_run("MyFlow", {"tags": ["existing"]})
        run_id = run["run_number"]
        final = store.mutate_tags("MyFlow", run_id, ["new_tag"], [])
        assert "new_tag" in final
        assert "existing" in final

    def test_remove_tag(self):
        run = store.create_run("MyFlow", {"tags": ["to_remove", "keep"]})
        run_id = run["run_number"]
        final = store.mutate_tags("MyFlow", run_id, [], ["to_remove"])
        assert "to_remove" not in final
        assert "keep" in final
