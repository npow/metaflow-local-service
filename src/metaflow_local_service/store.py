"""Metadata store — thin wrapper over LocalMetadataProvider class methods.

Layer: Storage
May only import from: stdlib, metaflow (LocalMetadataProvider class methods only)

All HTTP handlers delegate to this module. Data is written directly to .metaflow/
in the LocalMetadataProvider on-disk format, so persistence is immediate and free —
there is no separate sync step.

Thread safety: reads are safe (filesystem is the source of truth). The task ID
counter is protected by a lock; all other writes use the atomic tempfile/rename
pattern from LocalMetadataProvider._save_meta().
"""

from __future__ import annotations

import contextlib
import glob
import os
import threading
import time
from typing import Any

# ---------------------------------------------------------------------------
# LocalMetadataProvider bootstrap
# ---------------------------------------------------------------------------
# We use class methods directly so we never need to instantiate the provider.
# The only setup required is pointing LocalStorage.datastore_root at the
# .metaflow directory before any calls are made.

_METAFLOW_DIR = ".metaflow"
_setup_lock = threading.Lock()
_initialized = False


def setup(metaflow_root: str) -> None:
    """Point LocalStorage at *metaflow_root*/.metaflow and create it if absent.

    Must be called once before any store operations. Idempotent.

    Parameters
    ----------
    metaflow_root:
        Parent directory of .metaflow/. Usually the project working directory.
    """
    global _initialized
    with _setup_lock:
        if _initialized:
            return
        from metaflow.plugins.datastores.local_storage import LocalStorage

        metadir = os.path.realpath(os.path.join(metaflow_root, _METAFLOW_DIR))
        os.makedirs(metadir, exist_ok=True)
        LocalStorage.datastore_root = metadir  # type: ignore[assignment]
        _initialized = True


def _local() -> Any:
    """Return the LocalMetadataProvider class (import deferred until setup())."""
    from metaflow.plugins.metadata_providers.local import LocalMetadataProvider

    return LocalMetadataProvider


# ---------------------------------------------------------------------------
# Task ID counter
# ---------------------------------------------------------------------------
# The service assigns sequential integer task IDs scoped to each (flow, run).
# On first use we scan the filesystem to seed the counter past any existing
# tasks, which makes resume work without any seeding step.

_task_counters: dict[str, int] = {}
_counter_lock = threading.Lock()


def _scan_max_task_id(flow_name: str, run_id: str) -> int:
    """Return the highest task_id currently stored for this run, or 0."""
    from metaflow.plugins.datastores.local_storage import LocalStorage

    if LocalStorage.datastore_root is None:
        return 0
    pattern = os.path.join(
        LocalStorage.datastore_root, flow_name, run_id, "*", "*", "_meta", "_self.json"
    )
    max_id = 0
    for path in glob.iglob(pattern):
        # path = .metaflow/{flow}/{run}/{step}/{task}/_meta/_self.json
        parts = path.split(os.sep)
        # task directory is 3 levels up from _self.json: _meta, task, step
        task_dir = parts[-3]
        with contextlib.suppress(ValueError):
            max_id = max(max_id, int(task_dir))
    return max_id


def _next_task_id(flow_name: str, run_id: str) -> str:
    """Return the next task ID for this flow/run, thread-safely."""
    key = f"{flow_name}/{run_id}"
    with _counter_lock:
        if key not in _task_counters:
            _task_counters[key] = _scan_max_task_id(flow_name, run_id)
        _task_counters[key] += 1
        return str(_task_counters[key])


# ---------------------------------------------------------------------------
# Run ID generation
# ---------------------------------------------------------------------------

_run_id_lock = threading.Lock()
_last_run_id: int = 0


def new_run_id() -> str:
    """Generate a monotonically-increasing, unique timestamp-based run ID.

    Uses a logical clock: the result is always strictly greater than the
    previous value, even when wall-clock resolution is insufficient.
    """
    global _last_run_id
    with _run_id_lock:
        wall = int(time.time() * 1e6)
        new_id = max(wall, _last_run_id + 1)
        _last_run_id = new_id
        return str(new_id)


# ---------------------------------------------------------------------------
# Object creation helpers
# ---------------------------------------------------------------------------


def _get_username() -> str:
    try:
        from metaflow.util import get_username

        return get_username() or "unknown"
    except Exception:
        return os.environ.get("USER", "unknown")


def _ts_now() -> int:
    return round(time.time() * 1000)


def _build_flow_record(flow_name: str, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "flow_id": flow_name,
        "user_name": body.get("user_name") or _get_username(),
        "tags": list(body.get("tags") or []),
        "system_tags": list(body.get("system_tags") or []),
        "ts_epoch": body.get("ts_epoch") or _ts_now(),
    }


def _build_run_record(flow_name: str, run_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "flow_id": flow_name,
        "run_number": run_id,
        "user_name": body.get("user_name") or _get_username(),
        "tags": list(body.get("tags") or []),
        "system_tags": list(body.get("system_tags") or []),
        "ts_epoch": body.get("ts_epoch") or _ts_now(),
    }


def _build_step_record(
    flow_name: str, run_id: str, step_name: str, body: dict[str, Any]
) -> dict[str, Any]:
    return {
        "flow_id": flow_name,
        "run_number": run_id,
        "step_name": step_name,
        "user_name": body.get("user_name") or _get_username(),
        "tags": list(body.get("tags") or []),
        "system_tags": list(body.get("system_tags") or []),
        "ts_epoch": body.get("ts_epoch") or _ts_now(),
    }


def _build_task_record(
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    return {
        "flow_id": flow_name,
        "run_number": run_id,
        "step_name": step_name,
        "task_id": task_id,
        "user_name": body.get("user_name") or _get_username(),
        "tags": list(body.get("tags") or []),
        "system_tags": list(body.get("system_tags") or []),
        "ts_epoch": body.get("ts_epoch") or _ts_now(),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_or_create_flow(flow_name: str, body: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Return (flow_record, created). Created=True if the flow did not exist."""
    provider = _local()
    existing = provider.get_object("flow", "self", {}, None, flow_name)
    if existing:
        return existing, False
    record = _build_flow_record(flow_name, body)
    meta_dir = provider._create_and_get_metadir(flow_name)
    provider._save_meta(meta_dir, {"_self": record})
    return record, True


def get_flow(flow_name: str) -> dict[str, Any] | None:
    return _local().get_object("flow", "self", {}, None, flow_name)  # type: ignore[no-any-return]


def create_run(flow_name: str, body: dict[str, Any]) -> dict[str, Any]:
    """Create a new run with a server-assigned ID. Always creates (never 409)."""
    provider = _local()
    # Ensure flow exists first
    get_or_create_flow(flow_name, body)
    run_id = new_run_id()
    record = _build_run_record(flow_name, run_id, body)
    meta_dir = provider._create_and_get_metadir(flow_name, run_id)
    provider._save_meta(meta_dir, {"_self": record})
    return record


def _enrich_run(run: dict[str, Any]) -> dict[str, Any]:
    """Add status, finished_at, and duration to a run by aggregating task statuses."""
    flow_name = run["flow_id"]
    run_id = run["run_number"]
    tasks = [
        t for t in list_all_tasks_for_run(flow_name, run_id) if t.get("step_name") != "_parameters"
    ]
    if not tasks:
        return {**run, "status": "running"}
    statuses = {t.get("status", "running") for t in tasks}
    if "failed" in statuses:
        status = "failed"
    elif "running" in statuses:
        status = "running"
    else:
        status = "completed"
    enriched: dict[str, Any] = {**run, "status": status}
    finished_ats = [t["finished_at"] for t in tasks if t.get("finished_at")]
    started_ats = [t["started_at"] for t in tasks if t.get("started_at")]
    if finished_ats and status != "running":
        enriched["finished_at"] = max(finished_ats)
        ts_start = run.get("ts_epoch") or (min(started_ats) if started_ats else None)
        if ts_start:
            enriched["duration"] = enriched["finished_at"] - ts_start
    return enriched


def get_run(flow_name: str, run_id: str) -> dict[str, Any] | None:
    raw = _local().get_object("run", "self", {}, None, flow_name, run_id)
    return _enrich_run(raw) if raw is not None else None


def list_runs(flow_name: str) -> list[dict[str, Any]]:
    result = _local().get_object("flow", "run", {}, None, flow_name)
    return [_enrich_run(r) for r in result] if result else []


def get_or_create_step(
    flow_name: str, run_id: str, step_name: str, body: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    """Return (step_record, created)."""
    provider = _local()
    existing = provider.get_object("step", "self", {}, None, flow_name, run_id, step_name)
    if existing:
        return existing, False
    record = _build_step_record(flow_name, run_id, step_name, body)
    meta_dir = provider._create_and_get_metadir(flow_name, run_id, step_name)
    provider._save_meta(meta_dir, {"_self": record})
    return record, True


def _enrich_step(step: dict[str, Any]) -> dict[str, Any]:
    """Add status and duration by aggregating tasks for this step."""
    flow_name = step["flow_id"]
    run_id = step["run_number"]
    step_name = step["step_name"]
    tasks = list_tasks(flow_name, run_id, step_name)
    if not tasks:
        return {**step, "status": "running"}
    statuses = {t.get("status", "running") for t in tasks}
    if "failed" in statuses:
        status = "failed"
    elif "running" in statuses:
        status = "running"
    else:
        status = "completed"
    enriched: dict[str, Any] = {**step, "status": status}
    finished_ats = [t["finished_at"] for t in tasks if t.get("finished_at")]
    if finished_ats and status != "running":
        max_finished = max(finished_ats)
        ts_start = step.get("ts_epoch")
        if ts_start:
            enriched["duration"] = max_finished - ts_start
        enriched["finished_at"] = max_finished
    return enriched


def get_step(flow_name: str, run_id: str, step_name: str) -> dict[str, Any] | None:
    raw = _local().get_object("step", "self", {}, None, flow_name, run_id, step_name)
    return _enrich_step(raw) if raw is not None else None


def list_steps(flow_name: str, run_id: str) -> list[dict[str, Any]]:
    result = _local().get_object("run", "step", {}, None, flow_name, run_id)
    return [_enrich_step(s) for s in result] if result else []


def create_task(
    flow_name: str, run_id: str, step_name: str, body: dict[str, Any]
) -> dict[str, Any]:
    """Create a task with a server-assigned ID. Always creates (never 409)."""
    provider = _local()
    # Ensure step exists first
    get_or_create_step(flow_name, run_id, step_name, body)
    task_id = _next_task_id(flow_name, run_id)
    record = _build_task_record(flow_name, run_id, step_name, task_id, body)
    meta_dir = provider._create_and_get_metadir(flow_name, run_id, step_name, task_id)
    provider._save_meta(meta_dir, {"_self": record})
    return record


def _enrich_task(task: dict[str, Any]) -> dict[str, Any]:
    """Add status, attempt_id, started_at, finished_at, duration from on-disk sysmeta."""
    provider = _local()
    meta_dir = provider._get_metadir(
        task["flow_id"], task["run_number"], task["step_name"], task["task_id"]
    )
    if not meta_dir:
        return {**task, "status": "running", "attempt_id": 0}

    # Read all sysmeta files relevant to attempt tracking
    attempt_start: int | None = None
    attempt_finish: int | None = None
    attempt_id: int = 0
    ok_value: str | None = None
    has_done = False

    for path in glob.iglob(os.path.join(meta_dir, "sysmeta_attempt*.json")):
        obj = provider._read_json_file(path)
        if obj is None:
            continue
        field = obj.get("field_name", "")
        ts = int(obj.get("ts_epoch", 0))
        if field == "attempt":
            attempt_id = int(obj.get("value", 0))
            attempt_start = ts
        elif field == "attempt_ok":
            ok_value = str(obj.get("value", ""))
            attempt_finish = ts
        elif field == "attempt-done":
            has_done = True

    if ok_value is not None:
        status = "completed" if ok_value == "True" else "failed"
    elif has_done:
        status = "failed"
    else:
        status = "running"

    started_at = attempt_start or task.get("ts_epoch")
    enriched: dict[str, Any] = {
        **task,
        "status": status,
        "attempt_id": attempt_id,
        "started_at": started_at,
    }
    if attempt_finish is not None:
        enriched["finished_at"] = attempt_finish
        if started_at:
            enriched["duration"] = attempt_finish - started_at
    return enriched


def get_task(flow_name: str, run_id: str, step_name: str, task_id: str) -> dict[str, Any] | None:
    raw = _local().get_object("task", "self", {}, None, flow_name, run_id, step_name, task_id)
    return _enrich_task(raw) if raw is not None else None


def list_tasks(flow_name: str, run_id: str, step_name: str) -> list[dict[str, Any]]:
    result = _local().get_object("step", "task", {}, None, flow_name, run_id, step_name)
    return [_enrich_task(t) for t in result] if result else []


def register_artifacts(
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    artifacts: list[dict[str, Any]],
) -> None:
    """Store a list of artifact records for a task attempt."""
    provider = _local()
    meta_dir = provider._create_and_get_metadir(flow_name, run_id, step_name, task_id)
    art_dict = {"{}_artifact_{}".format(a.get("attempt_id", 0), a["name"]): a for a in artifacts}
    provider._save_meta(meta_dir, art_dict)


def get_artifacts(
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    attempt: int | None = None,
) -> list[dict[str, Any]]:
    """Return artifacts for a task, optionally scoped to a specific attempt.

    Globs artifact files directly rather than using get_object(), which requires
    a sysmeta_attempt-done_* file to determine which attempt to read. Since the
    ephemeral service doesn't mandate that marker, we glob all matching files.
    """
    provider = _local()
    meta_dir = provider._get_metadir(flow_name, run_id, step_name, task_id)
    if not os.path.isdir(meta_dir):
        return []
    prefix = f"{attempt}_artifact_" if attempt is not None else "*_artifact_"
    pattern = os.path.join(meta_dir, f"{prefix}*.json")
    result: list[dict[str, Any]] = []
    for path in glob.iglob(pattern):
        obj = provider._read_json_file(path)
        if obj is not None:
            result.append(obj)
    return result


def register_metadata(
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    metadata: list[dict[str, Any]],
) -> None:
    """Store a list of metadata field records for a task."""
    provider = _local()
    meta_dir = provider._create_and_get_metadir(flow_name, run_id, step_name, task_id)
    ts = round(time.time() * 1000)
    meta_dict = {
        f"sysmeta_{m.get('field_name', 'unknown')}_{ts + i}": m for i, m in enumerate(metadata)
    }
    provider._save_meta(meta_dir, meta_dict)


def _enrich_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Add attempt_id as a direct field extracted from tags (e.g. 'attempt_id:0')."""
    if "attempt_id" in record:
        return record
    attempt_id = 0
    for tag in record.get("tags") or []:
        if isinstance(tag, str) and tag.startswith("attempt_id:"):
            with contextlib.suppress(ValueError):
                attempt_id = int(tag.split(":", 1)[1])
    return {**record, "attempt_id": attempt_id}


def get_metadata(flow_name: str, run_id: str, step_name: str, task_id: str) -> list[dict[str, Any]]:
    result = _local().get_object(
        "task", "metadata", {}, None, flow_name, run_id, step_name, task_id
    )
    return [_enrich_metadata(r) for r in result] if result else []


def mutate_tags(
    flow_name: str,
    run_id: str,
    tags_to_add: list[str],
    tags_to_remove: list[str],
) -> frozenset[str]:
    """Optimistically mutate user tags on a run. Returns the final tag set."""
    return _local()._mutate_user_tags_for_run(  # type: ignore[no-any-return]
        flow_name, run_id, tags_to_add=tags_to_add, tags_to_remove=tags_to_remove
    )


def list_all_tasks_for_run(flow_name: str, run_id: str) -> list[dict[str, Any]]:
    """Return all task records across all steps for a run."""
    result: list[dict[str, Any]] = []
    for step in list_steps(flow_name, run_id):
        result.extend(list_tasks(flow_name, run_id, step["step_name"]))
    result.sort(key=lambda t: t.get("ts_epoch", 0))
    return result


def list_all_flows() -> list[dict[str, Any]]:
    """Return all flow records stored on disk, sorted by name."""
    from metaflow.plugins.datastores.local_storage import LocalStorage

    if LocalStorage.datastore_root is None:
        return []
    pattern = os.path.join(str(LocalStorage.datastore_root), "*", "_meta", "_self.json")
    provider = _local()
    result = []
    for path in sorted(glob.iglob(pattern)):
        obj = provider._read_json_file(path)
        if obj is not None and "flow_id" in obj:
            result.append(obj)
    return result


def list_all_runs() -> list[dict[str, Any]]:
    """Return all run records across all flows, newest first."""
    result: list[dict[str, Any]] = []
    for flow in list_all_flows():
        result.extend(list_runs(flow["flow_id"]))
    result.sort(key=lambda r: r.get("ts_epoch", 0), reverse=True)
    return result


def get_task_logs(
    flow_name: str,
    run_id: str,
    step_name: str,
    task_id: str,
    stream: str,
    attempt: int = 0,
) -> list[dict[str, Any]]:
    """Return log lines for a task from the local datastore.

    Reads ``{ds_root}/{flow}/{run}/{step}/{task}/{attempt}.runtime_{stream}.log``
    and parses the mflog format into ``[{row, timestamp, line}, ...]``.
    Returns an empty list if the datastore is not local or the file doesn't exist.
    """
    meta = get_metadata(flow_name, run_id, step_name, task_id)
    meta_dict = {m["field_name"]: m["value"] for m in meta}
    if meta_dict.get("ds-type") != "local":
        return []
    ds_root = meta_dict.get("ds-root")
    if not ds_root:
        return []
    fname = f"{attempt}.runtime_{'stdout' if stream == 'out' else 'stderr'}.log"
    log_path = os.path.join(ds_root, flow_name, run_id, step_name, task_id, fname)
    if not os.path.isfile(log_path):
        return []
    try:
        from metaflow.mflog.mflog import parse
    except Exception:
        return []
    lines: list[dict[str, Any]] = []
    with open(log_path, "rb") as f:
        for raw in f:
            result = parse(raw)
            if result is None:
                continue
            msg = result.msg
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8", errors="replace")
            ts = int(result.utc_tstamp.timestamp() * 1000) if result.utc_tstamp else 0
            lines.append({"row": len(lines), "timestamp": ts, "line": msg})
    return lines


def filter_tasks_by_metadata(
    flow_name: str,
    run_id: str,
    step_name: str,
    field_name: str,
    pattern: str,
) -> list[str]:
    """Return task pathspecs whose metadata matches field_name/pattern."""
    return _local().filter_tasks_by_metadata(flow_name, run_id, step_name, field_name, pattern)  # type: ignore[no-any-return]
