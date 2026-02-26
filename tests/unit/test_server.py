"""Unit tests for the FastAPI server routes.

Uses httpx.AsyncClient (via fastapi.testclient.TestClient) to send HTTP
requests without starting a real server. All storage is isolated to a
temp directory by resetting LocalStorage before each test.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from metaflow_local_service import store
from metaflow_local_service.server import create_app


@pytest.fixture()
def client(tmp_path):
    """Return a TestClient backed by a fresh temp .metaflow/ directory."""
    store._initialized = False
    store._task_counters.clear()

    from metaflow.plugins.datastores.local_storage import LocalStorage

    metadir = os.path.join(str(tmp_path), ".metaflow")
    os.makedirs(metadir, exist_ok=True)
    LocalStorage.datastore_root = metadir

    app = create_app(str(tmp_path))
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c

    store._initialized = False
    store._task_counters.clear()


# ---------------------------------------------------------------------------
# Ping
# ---------------------------------------------------------------------------


class TestPing:
    def test_ping_returns_200(self, client):
        resp = client.get("/ping")
        assert resp.status_code == 200

    def test_ping_has_version_header(self, client):
        resp = client.get("/ping")
        assert "METADATA_SERVICE_VERSION" in resp.headers
        assert resp.headers["METADATA_SERVICE_VERSION"] == "2.5.0"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


class TestFlowRoutes:
    def test_create_flow(self, client):
        resp = client.post("/flows/MyFlow", json={})
        assert resp.status_code == 201
        assert resp.json()["flow_id"] == "MyFlow"

    def test_create_flow_conflict(self, client):
        client.post("/flows/MyFlow", json={})
        resp = client.post("/flows/MyFlow", json={})
        assert resp.status_code == 409

    def test_get_flow(self, client):
        client.post("/flows/MyFlow", json={})
        resp = client.get("/flows/MyFlow")
        assert resp.status_code == 200
        assert resp.json()["flow_id"] == "MyFlow"

    def test_get_missing_flow(self, client):
        resp = client.get("/flows/NoSuchFlow")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


class TestRunRoutes:
    def test_create_run(self, client):
        resp = client.post("/flows/MyFlow/run", json={"tags": ["t1"]})
        assert resp.status_code == 201
        body = resp.json()
        assert "run_number" in body
        assert body["tags"] == ["t1"]

    def test_get_run(self, client):
        run_number = client.post("/flows/MyFlow/run", json={}).json()["run_number"]
        resp = client.get(f"/flows/MyFlow/runs/{run_number}")
        assert resp.status_code == 200
        assert resp.json()["run_number"] == run_number

    def test_get_missing_run(self, client):
        resp = client.get("/flows/MyFlow/runs/999999")
        assert resp.status_code == 404

    def test_list_runs(self, client):
        client.post("/flows/MyFlow/run", json={})
        client.post("/flows/MyFlow/run", json={})
        resp = client.get("/flows/MyFlow/runs")
        assert resp.status_code == 200
        assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# Step
# ---------------------------------------------------------------------------


class TestStepRoutes:
    def _run_id(self, client) -> str:
        return client.post("/flows/MyFlow/run", json={}).json()["run_number"]

    def test_create_step(self, client):
        run_id = self._run_id(client)
        resp = client.post(f"/flows/MyFlow/runs/{run_id}/steps/start/step", json={})
        assert resp.status_code == 201
        assert resp.json()["step_name"] == "start"

    def test_create_step_conflict(self, client):
        run_id = self._run_id(client)
        url = f"/flows/MyFlow/runs/{run_id}/steps/start/step"
        client.post(url, json={})
        resp = client.post(url, json={})
        assert resp.status_code == 409

    def test_get_step(self, client):
        run_id = self._run_id(client)
        client.post(f"/flows/MyFlow/runs/{run_id}/steps/start/step", json={})
        resp = client.get(f"/flows/MyFlow/runs/{run_id}/steps/start")
        assert resp.status_code == 200

    def test_list_steps(self, client):
        run_id = self._run_id(client)
        client.post(f"/flows/MyFlow/runs/{run_id}/steps/start/step", json={})
        client.post(f"/flows/MyFlow/runs/{run_id}/steps/end/step", json={})
        resp = client.get(f"/flows/MyFlow/runs/{run_id}/steps")
        assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


class TestTaskRoutes:
    def _run_id(self, client) -> str:
        return client.post("/flows/MyFlow/run", json={}).json()["run_number"]

    def test_create_task(self, client):
        run_id = self._run_id(client)
        resp = client.post(f"/flows/MyFlow/runs/{run_id}/steps/start/task", json={})
        assert resp.status_code == 201
        assert "task_id" in resp.json()

    def test_task_ids_increment(self, client):
        run_id = self._run_id(client)
        url = f"/flows/MyFlow/runs/{run_id}/steps/start/task"
        t1 = client.post(url, json={}).json()["task_id"]
        t2 = client.post(url, json={}).json()["task_id"]
        assert int(t2) > int(t1)

    def test_get_task(self, client):
        run_id = self._run_id(client)
        task_id = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/start/task", json={}
        ).json()["task_id"]
        resp = client.get(f"/flows/MyFlow/runs/{run_id}/steps/start/tasks/{task_id}")
        assert resp.status_code == 200

    def test_list_tasks(self, client):
        run_id = self._run_id(client)
        url = f"/flows/MyFlow/runs/{run_id}/steps/start/task"
        client.post(url, json={})
        client.post(url, json={})
        resp = client.get(f"/flows/MyFlow/runs/{run_id}/steps/start/tasks")
        assert len(resp.json()) == 2


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    def _run_id(self, client) -> str:
        return client.post("/flows/MyFlow/run", json={}).json()["run_number"]

    def test_run_heartbeat(self, client):
        run_id = self._run_id(client)
        resp = client.post(f"/flows/MyFlow/runs/{run_id}/heartbeat", json={})
        assert resp.status_code == 200
        assert "wait_time_in_seconds" in resp.json()

    def test_task_heartbeat(self, client):
        run_id = self._run_id(client)
        task_id = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/start/task", json={}
        ).json()["task_id"]
        resp = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/start/tasks/{task_id}/heartbeat",
            json={},
        )
        assert resp.status_code == 200
        assert "wait_time_in_seconds" in resp.json()


# ---------------------------------------------------------------------------
# Artifacts
# ---------------------------------------------------------------------------


class TestArtifactRoutes:
    def _task_path(self, client) -> tuple[str, str, str]:
        run_id = client.post("/flows/MyFlow/run", json={}).json()["run_number"]
        task_id = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/start/task", json={}
        ).json()["task_id"]
        return run_id, "start", task_id

    def test_register_and_list_artifacts(self, client):
        run_id, step, task_id = self._task_path(client)
        artifacts = [
            {
                "name": "x",
                "attempt_id": 0,
                "sha": "abc",
                "ds_type": "local",
                "location": "/tmp/x",
                "content_type": "pickle",
                "type": "metaflow.artifact",
            }
        ]
        resp = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/{step}/tasks/{task_id}/artifact",
            json=artifacts,
        )
        assert resp.status_code == 200

        resp = client.get(
            f"/flows/MyFlow/runs/{run_id}/steps/{step}/tasks/{task_id}/artifacts"
        )
        assert resp.status_code == 200
        assert len(resp.json()) == 1


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestMetadataRoutes:
    def _task_path(self, client) -> tuple[str, str, str]:
        run_id = client.post("/flows/MyFlow/run", json={}).json()["run_number"]
        task_id = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/start/task", json={}
        ).json()["task_id"]
        return run_id, "start", task_id

    def test_register_and_get_metadata(self, client):
        run_id, step, task_id = self._task_path(client)
        entries = [
            {"field_name": "runtime", "value": "python", "type": "runtime", "tags": []}
        ]
        resp = client.post(
            f"/flows/MyFlow/runs/{run_id}/steps/{step}/tasks/{task_id}/metadata",
            json=entries,
        )
        assert resp.status_code == 200

        resp = client.get(
            f"/flows/MyFlow/runs/{run_id}/steps/{step}/tasks/{task_id}/metadata"
        )
        assert resp.status_code == 200
        fields = {m["field_name"] for m in resp.json()}
        assert "runtime" in fields


# ---------------------------------------------------------------------------
# Tag mutation
# ---------------------------------------------------------------------------


class TestTagMutationRoutes:
    def test_add_tags(self, client):
        run_id = client.post(
            "/flows/MyFlow/run", json={"tags": ["existing"]}
        ).json()["run_number"]

        resp = client.patch(
            f"/flows/MyFlow/runs/{run_id}/tag/mutate",
            json={"tags_to_add": ["new"], "tags_to_remove": []},
        )
        assert resp.status_code == 200
        tags = resp.json()["tags"]
        assert "new" in tags
        assert "existing" in tags
