"""Command-line tools for inspecting and debugging live Wetlands workers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from wetlands.environment_manager import EnvironmentManager


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wetlands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    workers = subparsers.add_parser("workers", help="list live workers")
    _add_location_arguments(workers)

    debug = subparsers.add_parser("debug", help="attach a debugger to a live worker")
    _add_location_arguments(debug)
    debug.add_argument("--worker", help="worker ID; may be omitted when exactly one worker is live")
    debug.add_argument(
        "--editor",
        choices=("vscode",),
        help="generate an editor configuration and open the editor",
    )
    debug.add_argument(
        "--source",
        type=Path,
        default=Path("."),
        help="source directory to open in the editor (default: current directory)",
    )
    debug.add_argument(
        "--no-launch",
        action="store_true",
        help="write the editor configuration without launching the editor",
    )
    return parser


def _add_location_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("wetlands"),
        help="Wetlands manager root (default: ./wetlands)",
    )
    parser.add_argument("--environment", required=True, help="managed environment name")


def _worker_summary(worker: Any) -> str:
    status = "debugging" if worker.debugger is not None else "not debugging"
    persistence = "persistent" if worker.persistent else "temporary"
    return f"{worker.id}  index={worker.index}  pid={worker.process_id}  pool={worker.pool_id}  {persistence}  {status}"


def _select_worker(workers: Sequence[Any], requested_id: str | None) -> Any:
    if not workers:
        raise ValueError("No live workers were found for this environment.")
    if requested_id is not None:
        for worker in workers:
            if worker.id == requested_id:
                return worker
        raise ValueError(f"No live worker has ID {requested_id!r}.")
    if len(workers) != 1:
        choices = "\n".join(f"  {_worker_summary(worker)}" for worker in workers)
        raise ValueError(f"More than one worker is live; select one with --worker:\n{choices}")
    return workers[0]


def _workspace_filename(environment: str, worker_id: str) -> str:
    readable = "".join(character if character.isalnum() or character in "-_" else "-" for character in environment)
    readable = readable.strip("-") or "environment"
    digest = hashlib.sha256(worker_id.encode("utf-8")).hexdigest()[:10]
    return f"{readable}-{digest}.code-workspace"


def _write_vscode_workspace(
    *,
    root: Path,
    environment: str,
    worker_id: str,
    source: Path,
    host: str,
    port: int,
) -> Path:
    source = source.expanduser().resolve(strict=True)
    if not source.is_dir():
        raise ValueError(f"Source path is not a directory: {source}")
    destination = root.expanduser().resolve(strict=False) / "state" / "debug" / "workspaces"
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / _workspace_filename(environment, worker_id)
    payload = {
        "folders": [{"path": str(source)}],
        "launch": {
            "version": "0.2.0",
            "configurations": [
                {
                    "name": f"Attach to Wetlands worker {worker_id}",
                    "type": "debugpy",
                    "request": "attach",
                    "connect": {"host": host, "port": port},
                    "justMyCode": False,
                }
            ],
        },
    }
    file_descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=destination)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as temporary:
            json.dump(payload, temporary, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    return path


def _launch_vscode(workspace: Path) -> None:
    executable = shutil.which("code")
    if executable is None:
        raise RuntimeError(
            "The VS Code command-line launcher 'code' was not found. "
            "Open the generated workspace manually or install the launcher from VS Code."
        )
    subprocess.Popen(
        [executable, "--reuse-window", str(workspace)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _run_workers(args: argparse.Namespace, manager: EnvironmentManager) -> int:
    workers = manager.running_workers(args.environment)
    if not workers:
        print(f"No live workers found for environment {args.environment!r}.")
        return 0
    print(f"Live workers for environment {args.environment!r}:")
    for worker in workers:
        print(_worker_summary(worker))
    return 0


def _run_debug(args: argparse.Namespace, manager: EnvironmentManager) -> int:
    if args.no_launch and args.editor is None:
        raise ValueError("--no-launch requires --editor")
    workers = manager.running_workers(args.environment)
    worker = _select_worker(workers, args.worker)
    endpoint = manager.start_debugger(args.environment, worker=worker.id)

    print(f"Debug adapter ready for worker {endpoint.worker_id}.")
    print(f"Adapter: {endpoint.adapter}")
    print(f"Host: {endpoint.host}")
    print(f"Port: {endpoint.port}")

    if args.editor == "vscode":
        workspace = _write_vscode_workspace(
            root=manager.root,
            environment=args.environment,
            worker_id=endpoint.worker_id,
            source=args.source,
            host=endpoint.host,
            port=endpoint.port,
        )
        print(f"VS Code workspace: {workspace}")
        if not args.no_launch:
            _launch_vscode(workspace)
    else:
        print("Configure a Debug Adapter Protocol client to attach to the host and port above.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Wetlands command-line interface."""

    parser = _parser()
    args = parser.parse_args(argv)
    manager = EnvironmentManager(root=args.root)
    try:
        if args.command == "workers":
            return _run_workers(args, manager)
        if args.command == "debug":
            return _run_debug(args, manager)
        parser.error(f"unknown command: {args.command}")
    except (OSError, RuntimeError, ValueError) as error:
        print(f"wetlands: error: {error}", file=sys.stderr)
        return 2
    finally:
        manager.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
