"""FastAPI application — speaks the Metaflow service API.

Layer: HTTP
May only import from: .store, fastapi, stdlib

Each route is a thin adapter: parse request → call store → return response.
No business logic lives here. The store module owns all data operations.

Metaflow service API version advertised: 2.5.0 (enables heartbeats, attempt
gets, tag mutation, and filtered-tasks — all supported features).
"""

from __future__ import annotations

import contextlib
import time
from typing import Any

from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import WebSocket
from fastapi.middleware.cors import CORSMiddleware
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
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

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
    async def get_task(flow_name: str, run_id: str, step_name: str, task_id: str) -> JSONResponse:
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

    @app.post("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/heartbeat")
    async def task_heartbeat(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        global last_heartbeat_at
        last_heartbeat_at = time.time()
        return JSONResponse({"wait_time_in_seconds": _HEARTBEAT_INTERVAL_SECONDS})

    # -----------------------------------------------------------------------
    # Artifacts
    # -----------------------------------------------------------------------

    @app.post("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/artifact")
    async def register_artifacts(
        flow_name: str, run_id: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        body = await _json_body(request)
        artifacts = body if isinstance(body, list) else [body]
        store.register_artifacts(flow_name, run_id, step_name, task_id, artifacts)
        return JSONResponse({}, status_code=200)

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/artifacts")
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
        return JSONResponse(store.get_artifacts(flow_name, run_id, step_name, task_id, attempt))

    # -----------------------------------------------------------------------
    # Metadata
    # -----------------------------------------------------------------------

    @app.post("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata")
    async def register_metadata(
        flow_name: str, run_id: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        body = await _json_body(request)
        entries = body if isinstance(body, list) else [body]
        store.register_metadata(flow_name, run_id, step_name, task_id, entries)
        return JSONResponse({}, status_code=200)

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/tasks/{task_id}/metadata")
    async def get_metadata(
        flow_name: str, run_id: str, step_name: str, task_id: str
    ) -> JSONResponse:
        return JSONResponse(store.get_metadata(flow_name, run_id, step_name, task_id))

    # -----------------------------------------------------------------------
    # Filtered tasks
    # -----------------------------------------------------------------------

    @app.get("/flows/{flow_name}/runs/{run_id}/steps/{step_name}/filtered_tasks")
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

    # -----------------------------------------------------------------------
    # UI-compatible API  (/api/*)
    # -----------------------------------------------------------------------
    # Speaks the metaflow-service UI backend protocol so the metaflow-ui Docker
    # container can be pointed here:
    #   docker run -p 3000:3000 \
    #     -e METAFLOW_SERVICE=http://host.docker.internal:<port>/api \
    #     netflix/metaflow-ui:latest
    # -----------------------------------------------------------------------

    ui = APIRouter(prefix="/api")

    def _ui_wrap(data: Any, request: Request) -> dict[str, Any]:
        return {
            "data": data,
            "status": 200,
            "links": {"self": str(request.url), "next": None},
            "pages": {"self": 1, "first": 1, "prev": None, "next": None},
            "query": dict(request.query_params),
        }

    @ui.get("/ping")
    async def ui_ping() -> JSONResponse:
        return JSONResponse({"version": _SERVICE_VERSION})

    @ui.get("/flows")
    async def ui_list_flows(request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_all_flows(), request))

    @ui.get("/flows/{flow_id}")
    async def ui_get_flow(flow_id: str, request: Request) -> JSONResponse:
        obj = store.get_flow(flow_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Flow not found")
        return JSONResponse(_ui_wrap(obj, request))

    @ui.get("/runs")
    async def ui_list_all_runs(request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_all_runs(), request))

    @ui.get("/flows/{flow_id}/runs")
    async def ui_list_runs(flow_id: str, request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_runs(flow_id), request))

    @ui.get("/flows/{flow_id}/runs/{run_number}")
    async def ui_get_run(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        obj = store.get_run(flow_id, run_number)
        if obj is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return JSONResponse(_ui_wrap(obj, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/tasks")
    async def ui_list_all_tasks_for_run(
        flow_id: str, run_number: str, request: Request
    ) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_all_tasks_for_run(flow_id, run_number), request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/artifacts")
    async def ui_run_artifacts(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps")
    async def ui_list_steps(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_steps(flow_id, run_number), request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}")
    async def ui_get_step(
        flow_id: str, run_number: str, step_name: str, request: Request
    ) -> JSONResponse:
        obj = store.get_step(flow_id, run_number, step_name)
        if obj is None:
            raise HTTPException(status_code=404, detail="Step not found")
        return JSONResponse(_ui_wrap(obj, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks")
    async def ui_list_tasks(
        flow_id: str, run_number: str, step_name: str, request: Request
    ) -> JSONResponse:
        return JSONResponse(_ui_wrap(store.list_tasks(flow_id, run_number, step_name), request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}")
    async def ui_get_task(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        obj = store.get_task(flow_id, run_number, step_name, task_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return JSONResponse(_ui_wrap(obj, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempts")
    async def ui_task_attempts(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        task = store.get_task(flow_id, run_number, step_name, task_id)
        if task is None:
            return JSONResponse(_ui_wrap([], request))
        attempt = {
            "flow_id": flow_id,
            "run_number": run_number,
            "step_name": step_name,
            "task_id": task_id,
            "attempt_id": task.get("attempt_id", 0),
            "ts_epoch": task.get("ts_epoch", 0),
            "status": task.get("status", "completed"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "duration": task.get("duration"),
        }
        return JSONResponse(_ui_wrap([attempt], request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata")
    async def ui_task_metadata(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        return JSONResponse(
            _ui_wrap(store.get_metadata(flow_id, run_number, step_name, task_id), request)
        )

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts")
    async def ui_task_artifacts(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        attempt_id: int | None = None
        raw = request.query_params.get("attempt_id")
        if raw is not None:
            with contextlib.suppress(ValueError):
                attempt_id = int(raw)
        data = store.get_artifacts(flow_id, run_number, step_name, task_id, attempt_id)
        return JSONResponse(_ui_wrap(data, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/cards")
    async def ui_task_cards(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out")
    async def ui_task_log_out(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        attempt = int(request.query_params.get("attempt_id", 0))
        logs = store.get_task_logs(flow_id, run_number, step_name, task_id, "out", attempt)
        return JSONResponse(_ui_wrap(logs, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/err")
    async def ui_task_log_err(
        flow_id: str, run_number: str, step_name: str, task_id: str, request: Request
    ) -> JSONResponse:
        attempt = int(request.query_params.get("attempt_id", 0))
        logs = store.get_task_logs(flow_id, run_number, step_name, task_id, "err", attempt)
        return JSONResponse(_ui_wrap(logs, request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/parameters")
    async def ui_run_parameters(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/metadata")
    async def ui_run_metadata(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    @ui.get("/flows/{flow_id}/runs/{run_number}/dag")
    async def ui_run_dag(flow_id: str, run_number: str, request: Request) -> JSONResponse:
        all_steps = store.list_steps(flow_id, run_number)
        # Exclude virtual _parameters step; sort by ts_epoch for linear ordering
        steps = [s for s in all_steps if s.get("step_name") != "_parameters"]
        steps.sort(key=lambda s: s.get("ts_epoch", 0))
        names = [s["step_name"] for s in steps]
        dag_steps: dict[str, Any] = {}
        for i, s in enumerate(steps):
            name = s["step_name"]
            is_last = i == len(steps) - 1
            dag_steps[name] = {
                "name": name,
                "type": "end" if is_last else "linear",
                "line": 0,
                "doc": "",
                "decorators": [],
                "next": [] if is_last else [names[i + 1]],
            }
        dag = {
            "file": "",
            "parameters": [],
            "constants": [],
            "steps": dag_steps,
            "graph_structure": names,
            "doc": "",
            "decorators": [],
            "extensions": {},
        }
        return JSONResponse(_ui_wrap(dag, request))

    # Features: raw JSON (not envelope) matching FEATURE_* env var convention
    @ui.get("/features")
    async def ui_features() -> JSONResponse:
        return JSONResponse({})

    # Plugin list: plain array (no envelope) — UI calls .filter() directly on this
    @ui.get("/plugin")
    async def ui_plugins() -> JSONResponse:
        return JSONResponse([])

    # Navigation links: plain array of {href, label}
    @ui.get("/links")
    async def ui_links() -> JSONResponse:
        return JSONResponse([])

    # Version: plain text
    @ui.get("/version")
    async def ui_version() -> Response:
        return Response(content=_SERVICE_VERSION, media_type="text/plain")

    # Notifications: envelope with empty list
    @ui.get("/notifications")
    async def ui_notifications(request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    # Autocomplete stubs
    @ui.get("/flows/autocomplete")
    async def ui_flows_autocomplete(request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    @ui.get("/artifacts/autocomplete")
    async def ui_artifacts_autocomplete(request: Request) -> JSONResponse:
        return JSONResponse(_ui_wrap([], request))

    app.include_router(ui)

    # WebSocket endpoint — accept and hold open so the UI stops polling
    @app.websocket("/api/ws")
    async def ui_ws(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.receive_text()
        except Exception:
            pass

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
