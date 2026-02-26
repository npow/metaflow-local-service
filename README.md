# metaflow-local-service

[![CI](https://github.com/npow/metaflow-local-service/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-local-service/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-local-service)](https://pypi.org/project/metaflow-local-service/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Track your Metaflow runs locally without setting up a database.

## The problem

When you want Metaflow's service metadata — run tracking, artifact indexing, heartbeats, tag
mutation — you need a running `metaflow-service` backed by PostgreSQL. That's fine for production,
but for local development, sandboxes, or CI you're forced to choose between `local` mode (no HTTP
API, no resume across machines) and a heavyweight Postgres deployment. There's no middle ground.

## Quick start

```bash
pip install metaflow-local-service

# Start the service and run your flow in one command
metaflow-local-service run python myflow.py run
```

The daemon starts automatically, sets `METAFLOW_SERVICE_URL` and
`METAFLOW_DEFAULT_METADATA=service` for your flow, and shuts down after 5 minutes of idle time.
Data is written to `.metaflow/` in the current directory — the same format as `local` mode.

## Install

```bash
pip install metaflow-local-service
```

Requires Python 3.9+ and Metaflow 2.12+.

## Usage

### Wrap a flow command

```bash
metaflow-local-service run python myflow.py run --max-workers 4
```

The daemon starts if it isn't already running, then runs your command with the service URL in the
environment.

### Manage the daemon manually

```bash
metaflow-local-service start               # start in background
metaflow-local-service status              # show PID, port, and URL
metaflow-local-service url                 # print just the URL, for scripting
metaflow-local-service stop                # send SIGTERM
```

### Scripting

```bash
export METAFLOW_SERVICE_URL=$(metaflow-local-service url)
export METAFLOW_DEFAULT_METADATA=service
python myflow.py run
```

## How it works

The daemon wraps Metaflow's own `LocalMetadataProvider` behind the standard service HTTP API
(v2.5.0). Data is written directly to `.metaflow/` on every request — no sync step, no separate
database. On resume, task IDs are seeded from existing files on disk so new tasks never collide
with previous ones.

See [docs/architecture.md](docs/architecture.md) for the full layer diagram.

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `METAFLOW_LOCAL_SERVICE_PORT` | `0` (random) | Port to bind |
| `METAFLOW_LOCAL_SERVICE_IDLE_TIMEOUT` | `300` | Seconds of heartbeat silence before shutdown |
| `METAFLOW_LOCAL_SERVICE_DEBUG` | `""` | Set to `1` to enable daemon logging |

See [docs/configuration.md](docs/configuration.md) for the full reference.

## Development

```bash
git clone https://github.com/npow/metaflow-local-service.git
cd metaflow-local-service
pip install -e ".[dev]"
pytest
```

Structural tests (AST-based layer boundary enforcement) run without metaflow installed:

```bash
pytest -m structural
```

## License

[Apache 2.0](LICENSE)
