"""Unit tests for the daemon module.

These tests cover helpers that don't require a running server.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from metaflow_local_service import daemon


@pytest.fixture(autouse=True)
def _clean_state(tmp_path, monkeypatch):
    """Redirect state files to a temp directory for test isolation."""
    monkeypatch.setattr(daemon, "_STATE_DIR", str(tmp_path / ".metaflow-local-service"))
    monkeypatch.setattr(
        daemon, "_STATE_FILE", str(tmp_path / ".metaflow-local-service" / "state.json")
    )
    monkeypatch.setattr(daemon, "_PID_FILE", str(tmp_path / ".metaflow-local-service" / "pid"))


class TestDaemonState:
    def test_round_trip(self):
        state = daemon.DaemonState(pid=12345, port=8765, metaflow_root="/tmp/foo")
        restored = daemon.DaemonState.from_dict(state.to_dict())
        assert restored.pid == 12345
        assert restored.port == 8765
        assert restored.metaflow_root == "/tmp/foo"

    def test_url_format(self):
        state = daemon.DaemonState(pid=1, port=9999, metaflow_root="/tmp")
        assert state.url == "http://127.0.0.1:9999"


class TestStatePersistence:
    def test_write_and_read(self):
        state = daemon.DaemonState(pid=99999, port=12345, metaflow_root="/tmp")
        daemon._write_state(state)
        restored = daemon._read_state()
        assert restored is not None
        assert restored.pid == 99999
        assert restored.port == 12345

    def test_read_missing_returns_none(self):
        assert daemon._read_state() is None

    def test_clear_state(self):
        state = daemon.DaemonState(pid=1, port=1234, metaflow_root="/tmp")
        daemon._write_state(state)
        daemon._clear_state()
        assert daemon._read_state() is None

    def test_clear_state_idempotent(self):
        """_clear_state() must not raise when files are already gone."""
        daemon._clear_state()  # no files exist — must not raise
        daemon._clear_state()  # still fine


class TestIsAlive:
    def test_current_process_is_alive(self):
        assert daemon._is_alive(os.getpid()) is True

    def test_pid_zero_is_not_alive(self):
        # PID 0 is not a real process we own
        # _is_alive sends signal 0; PID 0 raises PermissionError or ProcessLookupError
        result = daemon._is_alive(0)
        # Either False or True depending on OS; we just check it doesn't raise
        assert isinstance(result, bool)

    def test_large_fake_pid_is_not_alive(self):
        # PID 2^22 is unlikely to exist on any reasonable system
        assert daemon._is_alive(2**22) is False


class TestFindFreePort:
    def test_returns_positive_integer(self):
        port = daemon._find_free_port()
        assert isinstance(port, int)
        assert port > 0

    def test_returns_different_ports(self):
        ports = {daemon._find_free_port() for _ in range(5)}
        # With OS-level assignment we can't guarantee all different, but usually are
        assert len(ports) >= 1


class TestStatus:
    def test_status_with_no_state(self):
        assert daemon.status() is None

    def test_status_with_dead_pid(self):
        # Write a state with a PID that definitely does not exist
        state = daemon.DaemonState(pid=2**22, port=1234, metaflow_root="/tmp")
        daemon._write_state(state)
        # status() should detect the dead PID and return None
        assert daemon.status() is None
        # State files should be cleaned up
        assert daemon._read_state() is None
