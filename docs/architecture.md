# Architecture

## Overview

`metaflow-local-service` has four modules arranged in a strict dependency hierarchy:

```
┌─────────────────────────────────────────────────────────────────┐
│  cli.py          Layer: CLI                                     │
│  Click commands: start / stop / status / url / run             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ imports
┌──────────────────────────▼──────────────────────────────────────┐
│  daemon.py       Layer: Process Management                      │
│  Manages the background uvicorn process via subprocess.Popen.  │
│  Persists state (PID, port, root) to ~/.metaflow-local-service/ │
└──────────────────────────┬──────────────────────────────────────┘
                           │ imports (at runtime, inside subprocess)
┌──────────────────────────▼──────────────────────────────────────┐
│  server.py       Layer: HTTP                                    │
│  FastAPI app factory. Thin adapters: parse → store → respond.  │
│  Advertises API version 2.5.0. Tracks last heartbeat timestamp. │
└──────────────────────────┬──────────────────────────────────────┘
                           │ imports
┌──────────────────────────▼──────────────────────────────────────┐
│  store.py        Layer: Storage                                 │
│  Wraps LocalMetadataProvider class methods. Owns task ID        │
│  counter. All data written to .metaflow/ in real-time.          │
└─────────────────────────────────────────────────────────────────┘
```

## Layer boundaries

Dependencies flow strictly **downward**. The structural tests in
`tests/structural/test_architecture.py` use AST analysis to enforce:

- `store.py` never imports `server` or `daemon`
- `server.py` never imports `daemon`
- `cli.py` never imports `server` directly

## Storage layer

`store.py` uses `LocalMetadataProvider` **class methods only** — it never
instantiates the class. The only setup required is pointing
`LocalStorage.datastore_root` at the `.metaflow/` directory.

Data is written using `LocalMetadataProvider._save_meta()`, which uses an
atomic tempfile/rename pattern. Reads go through `get_object()`.

### Task ID counter

Task IDs must be sequential integers scoped to each `(flow, run)` pair. The
counter lives in `store._task_counters`. On first use per `(flow, run)`:

1. `_scan_max_task_id()` globs `{root}/{flow}/{run}/*/*/_meta/_self.json`
2. Parses the task directory name (an integer) from each path
3. Seeds the counter at `max(found_ids)` — or 0 if no tasks exist

This makes resume work automatically: a restarted server reads the existing
task IDs from disk and continues from the right number.

## Process layer (daemon)

`daemon.start()` (called in the parent process):

1. Checks `~/.metaflow-local-service/state.json` for a running instance
2. Picks a free port (or uses the configured one)
3. Spawns `python -m metaflow_local_service.daemon` with `start_new_session=True`
4. Polls `GET /ping` up to 10 s; raises `RuntimeError` if unreachable

`_run_server()` (called in the daemon subprocess):

1. Creates the FastAPI app via `server.create_app(metaflow_root)`
2. Writes its own PID + port to `state.json`
3. Starts a background thread (`_idle_monitor`) that checks
   `server.last_heartbeat_at` every 30 s and sets `uvicorn.should_exit = True`
   when silence exceeds `idle_timeout`
4. Calls `uvicorn.Server.run()` (blocks until shutdown)
5. Clears state files in the `finally` block

## HTTP layer (server)

`create_app(metaflow_root)` returns a configured FastAPI instance. Routes
implement the Metaflow service API:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/ping` | Health check, returns `METADATA_SERVICE_VERSION` header |
| POST | `/flows/{flow}` | Create flow (409 if exists) |
| GET | `/flows/{flow}` | Get flow |
| POST | `/flows/{flow}/run` | Create run (returns `run_number`) |
| GET | `/flows/{flow}/runs` | List runs |
| GET | `/flows/{flow}/runs/{run}` | Get run |
| POST | `/flows/{flow}/runs/{run}/heartbeat` | Run heartbeat |
| PATCH | `/flows/{flow}/runs/{run}/tag/mutate` | Mutate user tags |
| POST | `.../steps/{step}/step` | Create step (409 if exists) |
| GET | `.../steps/{step}` | Get step |
| GET | `.../steps` | List steps |
| POST | `.../steps/{step}/task` | Create task (returns `task_id`) |
| GET | `.../tasks/{task}` | Get task |
| GET | `.../tasks` | List tasks |
| POST | `.../tasks/{task}/heartbeat` | Task heartbeat |
| POST | `.../tasks/{task}/artifact` | Register artifacts |
| GET | `.../tasks/{task}/artifacts` | List artifacts |
| GET | `.../attempt/{n}/artifacts` | List artifacts for attempt |
| POST | `.../tasks/{task}/metadata` | Register metadata |
| GET | `.../tasks/{task}/metadata` | Get metadata |
| GET | `.../filtered_tasks` | Filter tasks by metadata field |

## Idle timeout

The service shuts down after `IDLE_TIMEOUT_SECONDS` with no heartbeat activity.
This prevents orphaned daemons from accumulating.

Heartbeats are sent by Metaflow at `1 / HEARTBEAT_INTERVAL_SECONDS` Hz during
active runs. The idle monitor checks every 30 s — so shutdown latency is at
most `idle_timeout + 30` seconds after the last heartbeat.
