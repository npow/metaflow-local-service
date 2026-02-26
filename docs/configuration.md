# Configuration reference

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `METAFLOW_LOCAL_SERVICE_PORT` | `0` | Port for the daemon to bind. `0` means pick a random free port. Set to a fixed value if you need a stable URL across restarts. |
| `METAFLOW_LOCAL_SERVICE_IDLE_TIMEOUT` | `300` | Seconds of heartbeat silence before the daemon shuts itself down. Set to a large value (e.g. `86400`) to disable auto-shutdown. |
| `METAFLOW_LOCAL_SERVICE_DEBUG` | `""` | Set to `1`, `true`, or `yes` to enable verbose logging from the daemon process. |

## CLI flags

All environment variables can be overridden per-invocation via CLI flags:

```
metaflow-local-service start [OPTIONS]

  Options:
    --port INTEGER         Port to bind [default: 0 = random]
    --root DIRECTORY       Parent of .metaflow/ [default: current directory]
    --idle-timeout INTEGER Seconds before auto-shutdown [default: 300]
```

CLI flags take precedence over environment variables.

## Metaflow integration variables

When `metaflow-local-service run CMD` is used, the following variables are
automatically set in the child process's environment:

| Variable | Value set |
|---|---|
| `METAFLOW_SERVICE_URL` | `http://127.0.0.1:{port}` |
| `METAFLOW_DEFAULT_METADATA` | `service` |

You can set these manually if you prefer to manage the daemon separately:

```bash
metaflow-local-service start
export METAFLOW_SERVICE_URL=$(metaflow-local-service url)
export METAFLOW_DEFAULT_METADATA=service
python myflow.py run
```

## State file location

Daemon state is stored in `~/.metaflow-local-service/`:

| File | Contents |
|---|---|
| `state.json` | JSON object with `pid`, `port`, `metaflow_root`, `started_at` |
| `pid` | Raw PID integer |

These files are created when the daemon starts and removed when it stops.
If the daemon exits abnormally (SIGKILL, machine reboot), the stale state is
detected automatically: `status()` checks whether the PID is alive with
`os.kill(pid, 0)` and clears the files if the process is gone.

## Data location

All metadata is written to `{metaflow_root}/.metaflow/` in `LocalMetadataProvider`
format. This is the same directory structure that `METAFLOW_DEFAULT_METADATA=local`
uses, so you can switch between the two without any migration.

`metaflow_root` defaults to the current working directory when the daemon is
started. You can override it with `--root`:

```bash
metaflow-local-service start --root ~/projects/my-flow
```
