# metaflow-local-service — project guidelines

## Architecture

Four layers, each in its own module. Dependencies flow strictly downward:

```
cli.py  →  daemon.py  →  server.py  →  store.py
```

- **store.py** (`Layer: Storage`): wraps `LocalMetadataProvider` class methods.
  Never imports from server, daemon, or cli.
- **server.py** (`Layer: HTTP`): FastAPI app factory. Imports only from store
  and fastapi. Never imports from daemon or cli.
- **daemon.py** (`Layer: Process Management`): manages the background uvicorn
  process. Imports from server at runtime (inside `_run_server()`).
- **cli.py** (`Layer: CLI`): Click entry points. Imports from daemon only.

Layer boundaries are enforced by `tests/structural/test_architecture.py` using
AST analysis. Every module docstring must include `Layer: <name>`.

## Code style

- Python 3.9+ compatible (no `match`, no `3.10+` union syntax in runtime paths)
- Full type annotations on all public functions
- `from __future__ import annotations` in every module
- No external deps beyond: `fastapi`, `uvicorn[standard]`, `click`, `metaflow`
- Line length: 100 (ruff configured in pyproject.toml)

## Testing

```bash
pytest                         # all tests
pytest tests/unit/             # unit tests only
pytest -m structural           # architecture tests only
pytest --cov=metaflow_local_service --cov-report=term-missing
```

The structural tests use AST analysis and do **not** import the modules under
test, so they always run fast and never need metaflow installed.

## Key invariants

1. `store.setup()` must be called before any store operation. It is idempotent.
2. `store._task_counters` is the only in-memory state. It is seeded from disk on
   first use per (flow, run) to survive restarts.
3. `server.last_heartbeat_at` is updated on every heartbeat POST. The daemon's
   idle monitor reads this to decide when to shut down.
4. The daemon writes `~/.metaflow-local-service/state.json` before uvicorn
   starts, and clears it in the `finally` block of `_run_server()`.

## Do not

- Add any SQL database dependency.
- Import `daemon` from `server` or `store`.
- Import `server` from `cli` or `store`.
- Remove the `Layer:` declaration from any module docstring.
- Skip the structural tests — they are the architectural contract.
