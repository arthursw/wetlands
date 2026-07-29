from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from wetlands import cli


class FakeManager:
    instances: list[FakeManager] = []
    workers: tuple[SimpleNamespace, ...] = ()
    endpoint = SimpleNamespace(
        worker_id="worker-1",
        adapter="debugpy",
        host="127.0.0.1",
        port=43123,
    )

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.closed = False
        self.debug_call: tuple[str, str | None] | None = None
        type(self).instances.append(self)

    def running_workers(self, environment: str):
        assert environment == "analysis"
        return type(self).workers

    def start_debugger(self, environment: str, *, worker: str | None = None):
        self.debug_call = (environment, worker)
        return type(self).endpoint

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_manager(monkeypatch):
    FakeManager.instances = []
    FakeManager.workers = ()
    monkeypatch.setattr(cli, "EnvironmentManager", FakeManager)


def make_worker(
    worker_id: str = "worker-1",
    *,
    index: int = 0,
    process_id: int = 123,
    pool_id: str = "pool-1",
    persistent: bool = True,
    debugger=None,
):
    return SimpleNamespace(
        id=worker_id,
        environment="analysis",
        pool_id=pool_id,
        index=index,
        process_id=process_id,
        persistent=persistent,
        debugger=debugger,
    )


def test_workers_lists_live_workers(tmp_path, capsys) -> None:
    FakeManager.workers = (make_worker(),)

    result = cli.main(["workers", "--root", str(tmp_path), "--environment", "analysis"])

    assert result == 0
    assert "worker-1" in capsys.readouterr().out
    assert FakeManager.instances[-1].closed


def test_debug_selects_the_only_worker_and_prints_generic_endpoint(tmp_path, capsys) -> None:
    FakeManager.workers = (make_worker(),)

    result = cli.main(["debug", "--root", str(tmp_path), "--environment", "analysis"])

    assert result == 0
    assert FakeManager.instances[-1].debug_call == ("analysis", "worker-1")
    output = capsys.readouterr().out
    assert "127.0.0.1" in output
    assert "43123" in output
    assert "Debug Adapter Protocol" in output


def test_debug_requires_a_worker_selection_when_multiple_are_live(tmp_path, capsys) -> None:
    FakeManager.workers = (
        make_worker(),
        make_worker("worker-2", index=1, process_id=456, pool_id="pool-2"),
    )

    result = cli.main(["debug", "--root", str(tmp_path), "--environment", "analysis"])

    assert result == 2
    assert FakeManager.instances[-1].debug_call is None
    error = capsys.readouterr().err
    assert "--worker" in error
    assert "worker-1" in error
    assert "worker-2" in error


def test_no_launch_requires_an_editor_before_starting_debugger(tmp_path, capsys) -> None:
    FakeManager.workers = (make_worker(),)

    result = cli.main(
        [
            "debug",
            "--root",
            str(tmp_path),
            "--environment",
            "analysis",
            "--no-launch",
        ]
    )

    assert result == 2
    assert FakeManager.instances[-1].debug_call is None
    assert "--no-launch requires --editor" in capsys.readouterr().err


def test_vscode_workspace_is_generated_outside_project_configuration(tmp_path, capsys) -> None:
    FakeManager.workers = (make_worker(),)
    source = tmp_path / "source"
    source.mkdir()
    existing = source / ".vscode" / "launch.json"
    existing.parent.mkdir()
    existing.write_text('{"keep": true}\n', encoding="utf-8")

    result = cli.main(
        [
            "debug",
            "--root",
            str(tmp_path / "runtime"),
            "--environment",
            "analysis",
            "--editor",
            "vscode",
            "--source",
            str(source),
            "--no-launch",
        ]
    )

    assert result == 0
    assert existing.read_text(encoding="utf-8") == '{"keep": true}\n'
    workspace = next((tmp_path / "runtime" / "state" / "debug" / "workspaces").glob("*.code-workspace"))
    payload = json.loads(workspace.read_text(encoding="utf-8"))
    assert payload["folders"] == [{"path": str(source.resolve())}]
    configuration = payload["launch"]["configurations"][0]
    assert configuration["connect"] == {"host": "127.0.0.1", "port": 43123}
    assert str(workspace) in capsys.readouterr().out


def test_vscode_launch_uses_generated_workspace(tmp_path, monkeypatch) -> None:
    FakeManager.workers = (make_worker(),)
    launched: list[Path] = []
    monkeypatch.setattr(cli, "_launch_vscode", launched.append)

    result = cli.main(
        [
            "debug",
            "--root",
            str(tmp_path / "runtime"),
            "--environment",
            "analysis",
            "--editor",
            "vscode",
            "--source",
            str(tmp_path),
        ]
    )

    assert result == 0
    assert len(launched) == 1
    assert launched[0].suffix == ".code-workspace"
