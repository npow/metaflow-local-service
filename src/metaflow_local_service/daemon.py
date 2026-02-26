"""Daemon lifecycle management — start, stop, status, idle timeout.

Layer: Process Management
May only import from: stdlib, metaflow_local_service (server module)

The daemon is a background uvicorn process that owns the FastAPI server.
State (PID, port, metaflow_root) is written to ~/.metaflow-local-service/
so that any process on the machine can discover and talk to it.

Idle timeout: the daemon monitors server.last_heartbeat_at. After
IDLE_TIMEOUT_SECONDS with no heartbeat, it shuts itself down gracefully.
The default idle timeout is 300 s (5 minutes), configurable via CLI or
METAFLOW_LOCAL_SERVICE_IDLE_TIMEOUT.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

# ---------------------------------------------------------------------------
# State directory
# ---------------------------------------------------------------------------

_STATE_DIR = os.path.expanduser("~/.metaflow-local-service")
_STATE_FILE = os.path.join(_STATE_DIR, "state.json")
_PID_FILE = os.path.join(_STATE_DIR, "pid")

_DEFAULT_IDLE_TIMEOUT = int(os.environ.get("METAFLOW_LOCAL_SERVICE_IDLE_TIMEOUT", "300"))
_DEFAULT_PORT = int(os.environ.get("METAFLOW_LOCAL_SERVICE_PORT", "0"))  # 0 = random


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class DaemonState:
    """Snapshot of a running daemon's configuration."""

    pid: int
    port: int
    metaflow_root: str
    started_at: float = field(default_factory=time.time)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "port": self.port,
            "metaflow_root": self.metaflow_root,
            "started_at": self.started_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DaemonState":
        return cls(
            pid=int(d["pid"]),
            port=int(d["port"]),
            metaflow_root=str(d["metaflow_root"]),
            started_at=float(d.get("started_at", 0)),
        )


# ---------------------------------------------------------------------------
# Port helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    """Bind to port 0 and let the OS assign a free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _write_state(state: DaemonState) -> None:
    os.makedirs(_STATE_DIR, exist_ok=True)
    with open(_STATE_FILE, "w") as f:
        json.dump(state.to_dict(), f)
    with open(_PID_FILE, "w") as f:
        f.write(str(state.pid))


def _read_state() -> DaemonState | None:
    if not os.path.isfile(_STATE_FILE):
        return None
    try:
        with open(_STATE_FILE) as f:
            return DaemonState.from_dict(json.load(f))
    except Exception:
        return None


def _clear_state() -> None:
    for path in (_STATE_FILE, _PID_FILE):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def status() -> DaemonState | None:
    """Return the running daemon's state, or None if it is not running."""
    state = _read_state()
    if state is None:
        return None
    if not _is_alive(state.pid):
        _clear_state()
        return None
    return state


def start(
    port: int = _DEFAULT_PORT,
    metaflow_root: str | None = None,
    idle_timeout: int = _DEFAULT_IDLE_TIMEOUT,
) -> DaemonState:
    """Start the daemon in a background process and return its state.

    If the daemon is already running, returns the existing state without
    starting a new one.

    Parameters
    ----------
    port:
        Port to listen on. 0 means pick a random free port.
    metaflow_root:
        Parent directory of .metaflow/. Defaults to the current directory.
    idle_timeout:
        Seconds of heartbeat silence before auto-shutdown.

    Returns
    -------
    DaemonState
        The running daemon's state (URL, PID, port).
    """
    existing = status()
    if existing is not None:
        return existing

    if metaflow_root is None:
        metaflow_root = os.getcwd()
    if port == 0:
        port = _find_free_port()

    # Spawn the daemon as a detached subprocess that runs _daemon_main().
    cmd = [
        sys.executable,
        "-m",
        "metaflow_local_service.daemon",
        "--port",
        str(port),
        "--metaflow-root",
        metaflow_root,
        "--idle-timeout",
        str(idle_timeout),
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # detach from parent's process group
    )

    # Wait until the server is reachable (up to 10 s).
    deadline = time.time() + 10
    state = DaemonState(pid=proc.pid, port=port, metaflow_root=metaflow_root)
    while time.time() < deadline:
        try:
            import urllib.request

            urllib.request.urlopen(f"http://127.0.0.1:{port}/ping", timeout=1)
            break
        except Exception:
            time.sleep(0.2)
    else:
        proc.kill()
        raise RuntimeError(
            f"metaflow-local-service failed to start on port {port}.\n"
            "Check that metaflow is installed: pip install metaflow\n"
            "Run with METAFLOW_LOCAL_SERVICE_DEBUG=1 for verbose logs."
        )

    return state


def stop() -> bool:
    """Send SIGTERM to the running daemon. Returns True if a daemon was stopped."""
    state = status()
    if state is None:
        return False
    try:
        os.kill(state.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    _clear_state()
    return True


def ensure_running(
    port: int = _DEFAULT_PORT,
    metaflow_root: str | None = None,
    idle_timeout: int = _DEFAULT_IDLE_TIMEOUT,
) -> DaemonState:
    """Start the daemon if it is not already running, then return its state.

    Convenient for use in a ``run`` wrapper: call this before executing the
    flow command, and the service will be available at ``state.url``.
    """
    return start(port=port, metaflow_root=metaflow_root, idle_timeout=idle_timeout)


# ---------------------------------------------------------------------------
# Daemon entry point (runs inside the background process)
# ---------------------------------------------------------------------------


def _run_server(port: int, metaflow_root: str, idle_timeout: int) -> None:
    """Start uvicorn and run until idle timeout or SIGTERM.

    This function is called inside the daemon subprocess.
    """
    import threading

    import uvicorn

    from metaflow_local_service import server

    app = server.create_app(metaflow_root)
    state = DaemonState(pid=os.getpid(), port=port, metaflow_root=metaflow_root)
    _write_state(state)

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        access_log=False,
    )
    uv_server = uvicorn.Server(config)

    def _idle_monitor() -> None:
        """Shut down after *idle_timeout* seconds with no heartbeats."""
        while True:
            time.sleep(30)
            silence = time.time() - server.last_heartbeat_at
            if silence > idle_timeout:
                uv_server.should_exit = True
                break

    monitor_thread = threading.Thread(target=_idle_monitor, daemon=True)
    monitor_thread.start()

    try:
        uv_server.run()
    finally:
        _clear_state()


# ---------------------------------------------------------------------------
# __main__ — invoked by the subprocess spawned in start()
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="metaflow-local-service daemon")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--metaflow-root", required=True)
    parser.add_argument("--idle-timeout", type=int, default=_DEFAULT_IDLE_TIMEOUT)
    args = parser.parse_args()

    debug = os.environ.get("METAFLOW_LOCAL_SERVICE_DEBUG", "").lower() in (
        "1",
        "true",
        "yes",
    )
    if not debug:
        # Suppress all output from daemon process unless debugging.
        import logging

        logging.disable(logging.CRITICAL)

    _run_server(
        port=args.port,
        metaflow_root=args.metaflow_root,
        idle_timeout=args.idle_timeout,
    )
