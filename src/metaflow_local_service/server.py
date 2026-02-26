"""FastAPI application — speaks the Metaflow service API.

Layer: HTTP
May only import from: .store, fastapi, stdlib

Each route is a thin adapter: parse request → call store → return response.
No business logic lives here. The store module owns all data operations.

Metaflow service API version advertised: 2.5.0 (enables heartbeats, attempt
gets, tag mutation, and filtered-tasks — all supported features).
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import Response

from metaflow_local_service import store

# ---------------------------------------------------------------------------
# Service metadata
# ---------------------------------------------------------------------------

_SERVICE_VERSION = "2.5.0"
_HEARTBEAT_INTERVAL_SECONDS = 10

# ---------------------------------------------------------------------------
# Idle timeout tracking
# ---------------------------------------------------------------------------
# Updated on every heartbeat POST. The daemon's idle monitor reads this value
# to decide when to shut down.

last_heartbeat_at: float = time.time()

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(metaflow_root: str) -> FastAPI:
    """Create and configure the FastAPI application.

    Parameters
    ----------
    metaflow_root:
        Parent directory of .metaflow/. Passed to store.setup().
    """
    store.setup(metaflow_root)

    app = FastAPI(title="metaflow-local-service", version=_SERVICE_VERSION)

    # -----------------------------------------------------------------------
    # Health / version
    # -----------------------------------------------------------------------

    @app.get("/ping")
    async def ping() -> Response:
        """Health check. Returns 200 with METADATA_SERVICE_VERSION header."""
        return Response(
            content="pong",
            media_type="text/plain",
            headers={"METADATA_SERVICE_VERSION": _SERVICE_VERSION},
        )

    # -----------------------------------------------------------------------
    # Flow
    # -----------------------------------------------------------------------

    @app.get("/flows/{flow_name}")
    async def get_flow(flow_name: str) -> JSONResponse:
        obj = store.get_flow(flow_name)
        if obj is None:
            raise HTTPException(status_code=404, detail="Flow not found")
        return JSONResponse(obj)

    @app.post("/flows/{flow_name}")
    async def create_flow(flow_name: str, request: Request) -> JSONResponse:
        body: dict[str, Any] = await _json_body(request)
        obj, created = store.get_or_create_flow(flow_name, body)
        if not created:
            # 409 → client falls back to GET /flows/{flow_name}
            raise HTTPException(status_code=409, detail="Flow already exists")
        return JSONResponse(obj, status_code=201)

    # -----------------------------------------------------------------------
    # Run
    # -----------------------------------------------------------------------

    @app.get("/flows/{flow_name}/runs")
    async def list_runs(flow_name: str) -> JSONResponse:
        return JSONResponse(store.list_runs(flow_name))

    @app.get("/flows/{flow_name}/runs/{run_id}")
    async def get_run(flow_name: str, run_id: str) -> JSONResponse:
        obj = store.get_run(flow_name, run_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return JSONResponse(obj)

    @app.post("/flows/{flow_name}/run")
    async def create_run(flow_name: str, request: Request) -> JSONResponse:
        body: dict[str, Any] = await _json_body(request)
        obj = store.create_run(flow_name, body)
        return JSONResponse(obj, status_code=201)

    # -----------------------------------------------------------------------
    # Run heartbeat
    # -----------------------------------------------------------------------

    @app.post("/flows/{flow_name}/runs/{run_id}/heartbeat")
    async def run_heartbeat(flow_name: str, run_id: str) -> JSONResponse:
        global last_heartbeat_at
        last_heartbeat_at = time.time()
        return JSONResponse({"wait_time_in_seconds": _HEARTBEAT_INTERVAL_SECONDS})

    # -----------------------------------------------------------------------
    # Tag mutation
    # -----------------------------------------------------------------------

    @app.patch("/flows/{flow_name}/runs/{run_id}/tag/mutate")
    async def mutate_tags(flow_name: str, run_id: str, request: Request) -> JSONResponse:
        body: dict[str, Any] = await _json_body(request)
        tags_to_add = list(body.get("tags_to_add") or [])
        tags_to_remove = list(body.get("tags_to_remove") or [])
        try:
            final_tags = store.mutate_tags(flow_name, run_id, tags_to_add, tags_to_remove)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse({"tags": sorted(final_tags)})

    # -----------------------------------------------------------------------
    # Step
    # -----------------------------------------------------------------------

    @app.get("/flows/{flow_name}/runs/{run_id}/steps")
    async def list_steps(flow_name: str, run_id: str) -> JSONResponse:
        return JSONResponse(store.list_steps(flow_name, run_id))

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}")
    async def get_step(flow_name: str, run_id: str, step_name: str) -> JSONResponse:
        obj = store.get_step(flow_name, run_id, step_name)
        if obj is None:
            raise HTTPException(status_code=404, detail="Step not found")
        return JSONResponse(obj)

    @app.post("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/step")
    async def create_step(
        flow_name: str, run_id: str, step_name: str, request: Request
    ) -> JSONResponse:
        body: dict[str, Any] = await _json_body(request)
        obj, created = store.get_or_create_step(flow_name, run_id, step_name, body)
        if not created:
            raise HTTPException(status_code=409, detail="Step already exists")
        return JSONResponse(obj, status_code=201)

    # -----------------------------------------------------------------------
    # Task
    # -----------------------------------------------------------------------

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks")
    async def list_tasks(flow_name: str, run_id: str, step_name: str) -> JSONResponse:
        return JSONResponse(store.list_tasks(flow_name, run_id, step_name))

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}")
    async def get_task(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        obj = store.get_task(flow_name, run_id, step_name, task_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return JSONResponse(obj)

    @app.post("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/task")
    async def create_task(
        flow_name: str, run_id: str, step_name: str, request: Request
    ) -> JSONResponse:
        body: dict[str, Any] = await _json_body(request)
        obj = store.create_task(flow_name, run_id, step_name, body)
        return JSONResponse(obj, status_code=201)

    # -----------------------------------------------------------------------
    # Task heartbeat
    # -----------------------------------------------------------------------

    @app.post(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/heartbeat"
    )
    async def task_heartbeat(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        global last_heartbeat_at
        last_heartbeat_at = time.time()
        return JSONResponse({"wait_time_in_seconds": _HEARTBEAT_INTERVAL_SECONDS})

    # -----------------------------------------------------------------------
    # Artifacts
    # -----------------------------------------------------------------------

    @app.post(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/artifact"
    )
    async def register_artifacts(
        flow_name: str, run_id: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        body = await _json_body(request)
        artifacts = body if isinstance(body, list) else [body]
        store.register_artifacts(flow_name, run_id, step_name, task_id, artifacts)
        return JSONResponse({}, status_code=200)

    @app.get(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/artifacts"
    )
    async def list_artifacts(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        return JSONResponse(store.get_artifacts(flow_name, run_id, step_name, task_id))

    @app.get(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}"
        "/attempt/{attempt}/artifacts"
    )
    async def list_artifacts_for_attempt(
        flow_name: str, run_id: str, step_name: str, task_id: str, attempt: int
    ) -> JSONResponse:
        return JSONResponse(
            store.get_artifacts(flow_name, run_id, step_name, task_id, attempt)
        )

    # -----------------------------------------------------------------------
    # Metadata
    # -----------------------------------------------------------------------

    @app.post(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata"
    )
    async def register_metadata(
        flow_name: str, run_id: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        body = await _json_body(request)
        entries = body if isinstance(body, list) else [body]
        store.register_metadata(flow_name, run_id, step_name, task_id, entries)
        return JSONResponse({}, status_code=200)

    @app.get(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata"
    )
    async def get_metadata(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        return JSONResponse(store.get_metadata(flow_name, run_id, step_name, task_id))

    # -----------------------------------------------------------------------
    # Filtered tasks
    # -----------------------------------------------------------------------

    @app.get(
        "/flows/{flow_name}/runs/{run_id}/steps/{step_name}/filtered_tasks"
    )
    async def filtered_tasks(
        flow_name: str,
        run_id: str,
        step_name: str,
        metadata_field_name: str = Query(default=""),
        pattern: str = Query(default=".*"),
    ) -> JSONResponse:
        pathspecs = store.filter_tasks_by_metadata(
            flow_name, run_id, step_name, metadata_field_name, pattern
        )
        return JSONResponse(pathspecs)

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _json_body(request: Request) -> Any:
    """Parse request body as JSON, returning empty dict on no-body requests."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    return body if body is not None else {}
