"""CLI entry point — start, stop, status, url, run.

Layer: CLI
May only import from: .daemon, stdlib, click

Usage:
    metaflow-local-service start [--port PORT] [--metaflow-root PATH]
    metaflow-local-service stop
    metaflow-local-service status
    metaflow-local-service url
    metaflow-local-service run [--port PORT] [--metaflow-root PATH] CMD...

The ``run`` subcommand is the recommended way to use the service: it starts
the daemon, exports METAFLOW_SERVICE_URL and METAFLOW_DEFAULT_METADATA, then
exec's the given command. The daemon's idle timeout handles cleanup after the
flow finishes.
"""

from __future__ import annotations

import os
import subprocess
import sys

import click

from metaflow_local_service import daemon as _daemon

_DEFAULT_IDLE = _daemon._DEFAULT_IDLE_TIMEOUT
_DEFAULT_PORT = _daemon._DEFAULT_PORT


@click.group(
    help="Lightweight Metaflow metadata service backed by .metaflow/ on disk."
)
def cli() -> None:
    pass


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--port",
    default=_DEFAULT_PORT,
    show_default=True,
    help="Port to listen on. 0 = random free port.",
)
@click.option(
    "--metaflow-root",
    default=None,
    help="Parent directory of .metaflow/. Defaults to $PWD.",
)
@click.option(
    "--idle-timeout",
    default=_DEFAULT_IDLE,
    show_default=True,
    help="Seconds of heartbeat silence before auto-shutdown.",
)
def start(port: int, metaflow_root: str | None, idle_timeout: int) -> None:
    """Start the metadata service daemon in the background."""
    existing = _daemon.status()
    if existing is not None:
        click.echo(f"Already running: pid={existing.pid}  url={existing.url}")
        return

    state = _daemon.start(
        port=port, metaflow_root=metaflow_root, idle_timeout=idle_timeout
    )
    click.echo(f"Started: pid={state.pid}  url={state.url}")
    click.echo(f"  METAFLOW_SERVICE_URL={state.url}")
    click.echo(f"  METAFLOW_DEFAULT_METADATA=service")


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


@cli.command()
def stop() -> None:
    """Stop the running metadata service daemon."""
    stopped = _daemon.stop()
    if stopped:
        click.echo("Stopped.")
    else:
        click.echo("No running daemon found.")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@cli.command()
def status() -> None:
    """Print the status of the daemon."""
    state = _daemon.status()
    if state is None:
        click.echo("Not running.")
        raise SystemExit(1)
    click.echo(f"Running: pid={state.pid}  url={state.url}")
    click.echo(f"  metaflow_root={state.metaflow_root}")


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------


@cli.command()
def url() -> None:
    """Print the service URL (exits non-zero if not running).

    Useful for: export METAFLOW_SERVICE_URL=$(metaflow-local-service url)
    """
    state = _daemon.status()
    if state is None:
        click.echo("Not running.", err=True)
        raise SystemExit(1)
    click.echo(state.url, nl=False)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@cli.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
@click.argument("cmd", nargs=-1, required=True)
@click.option(
    "--port",
    default=_DEFAULT_PORT,
    show_default=True,
    help="Port to listen on. 0 = random free port.",
)
@click.option(
    "--metaflow-root",
    default=None,
    help="Parent directory of .metaflow/. Defaults to $PWD.",
)
@click.option(
    "--idle-timeout",
    default=_DEFAULT_IDLE,
    show_default=True,
    help="Seconds of heartbeat silence before auto-shutdown.",
)
def run(
    cmd: tuple[str, ...],
    port: int,
    metaflow_root: str | None,
    idle_timeout: int,
) -> None:
    """Start the service, run CMD with METAFLOW_SERVICE_URL set, then exit.

    The service keeps running after CMD exits (until idle timeout), so
    multiple flows can share a single daemon instance.

    Example:
        metaflow-local-service run python flow.py run
    """
    state = _daemon.ensure_running(
        port=port, metaflow_root=metaflow_root, idle_timeout=idle_timeout
    )

    env = os.environ.copy()
    env["METAFLOW_SERVICE_URL"] = state.url
    env["METAFLOW_DEFAULT_METADATA"] = "service"

    click.echo(
        f"metaflow-local-service running at {state.url} (pid={state.pid})",
        err=True,
    )

    result = subprocess.run(list(cmd), env=env)
    sys.exit(result.returncode)
