"""Structural tests — enforce architectural invariants.

These tests use AST analysis to verify layer boundaries and coding standards
without importing the modules under test. They run on every CI push and
gate all pull requests.

Invariants enforced:
1. Every module declares its Layer in the module docstring.
2. The server module never imports from the daemon module (one-way dependency).
3. The store module never imports from server or daemon (bottom of the stack).
4. The CLI module never imports from the server directly.
5. Every public module has a non-empty module docstring.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Source file discovery
# ---------------------------------------------------------------------------

_SRC = Path(__file__).parent.parent.parent / "src" / "metaflow_local_service"

_MODULES = {
    "store": _SRC / "store.py",
    "server": _SRC / "server.py",
    "daemon": _SRC / "daemon.py",
    "cli": _SRC / "cli.py",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_docstring(filepath: Path) -> str:
    tree = ast.parse(filepath.read_text())
    return ast.get_docstring(tree) or ""


def _get_imports(filepath: Path) -> set[str]:
    tree = ast.parse(filepath.read_text())
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".")[0])
    return imports


def _get_local_imports(filepath: Path) -> set[str]:
    """Return local relative import names (module component after the package)."""
    tree = ast.parse(filepath.read_text())
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level > 0 and node.module:
                # relative import: from .store import ...  → "store"
                imports.add(node.module.split(".")[0])
            elif node.module and node.module.startswith("metaflow_local_service."):
                part = node.module[len("metaflow_local_service."):].split(".")[0]
                imports.add(part)
    return imports


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.structural
@pytest.mark.parametrize("name,path", list(_MODULES.items()))
def test_module_exists(name: str, path: Path) -> None:
    """Every declared module must exist on disk."""
    assert path.exists(), f"Module {name} not found at {path}"


@pytest.mark.structural
@pytest.mark.parametrize("name,path", list(_MODULES.items()))
def test_module_has_docstring(name: str, path: Path) -> None:
    """Every module must have a non-empty module docstring."""
    doc = _get_docstring(path)
    assert doc, (
        f"Module '{name}' is missing a module docstring. "
        "Add a docstring that describes the layer and what the module does."
    )


@pytest.mark.structural
@pytest.mark.parametrize("name,path", list(_MODULES.items()))
def test_module_docstring_declares_layer(name: str, path: Path) -> None:
    """Every module docstring must declare its architectural layer."""
    doc = _get_docstring(path)
    assert "Layer:" in doc, (
        f"Module '{name}' docstring does not declare its Layer. "
        "Add 'Layer: <name>' to the module docstring, e.g. 'Layer: Storage'."
    )


@pytest.mark.structural
def test_store_does_not_import_server() -> None:
    """store.py must not import from server.py — it is at the bottom of the stack."""
    local = _get_local_imports(_MODULES["store"])
    assert "server" not in local, (
        "store.py imports from server.py, which violates the layer boundary. "
        "store.py is at the bottom; only higher layers may import from it."
    )


@pytest.mark.structural
def test_store_does_not_import_daemon() -> None:
    """store.py must not import from daemon.py."""
    local = _get_local_imports(_MODULES["store"])
    assert "daemon" not in local, (
        "store.py imports from daemon.py, which violates the layer boundary."
    )


@pytest.mark.structural
def test_server_does_not_import_daemon() -> None:
    """server.py must not import from daemon.py — server is independent of lifecycle."""
    local = _get_local_imports(_MODULES["server"])
    assert "daemon" not in local, (
        "server.py imports from daemon.py. "
        "The FastAPI app must be independent of the process lifecycle."
    )


@pytest.mark.structural
def test_cli_does_not_import_server_directly() -> None:
    """cli.py should only talk to the daemon, not import the FastAPI app directly."""
    local = _get_local_imports(_MODULES["cli"])
    assert "server" not in local, (
        "cli.py imports from server.py directly. "
        "The CLI should interact with the service via daemon.py, not by importing FastAPI routes."
    )


@pytest.mark.structural
def test_all_modules_parse_without_error() -> None:
    """All source modules must be parseable Python (syntax check)."""
    for name, path in _MODULES.items():
        try:
            ast.parse(path.read_text())
        except SyntaxError as exc:
            pytest.fail(f"Module '{name}' has a syntax error: {exc}")
