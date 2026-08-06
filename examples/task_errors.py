from __future__ import annotations

from pathlib import Path

from wetlands import (
    EnvironmentManager,
    EnvironmentSpec,
    ExecutionError,
    LocalPackage,
    PostInstallCommand,
    ProvisioningError,
)


def main(root: Path = Path("wetlands")) -> dict[str, object]:
    """Show structured worker and provisioning failures."""
    example_directory = Path(__file__).parent
    worker_spec = EnvironmentSpec(
        python="3.12.*",
        conda=("pip",),
        local=(LocalPackage(example_directory),),
    )
    remote_category = ""
    remote_target = ""
    remote_type = ""
    remote_message = ""
    remote_traceback = ""
    provisioning_stage = ""
    provisioning_command = ""
    provisioning_returncode: int | None = None
    provisioning_stderr: tuple[str, ...] = ()

    with EnvironmentManager(root=root) as manager:
        environment = manager.provision("docs-examples", worker_spec).wait_for()
        with environment.start() as pool:
            task = pool.submit_import("example_module:raise_example_error")
            try:
                task.wait_for()
            except ExecutionError as error:
                failure = error.failure
                remote = failure.remote_exception
                remote_category = failure.category.value
                remote_target = failure.call_target or ""
                remote_traceback = failure.traceback or ""
                print(f"Category: {remote_category}")
                print(f"Target: {failure.call_target}")
                if remote is not None:
                    remote_type = remote.type_name or ""
                    remote_message = remote.message or ""
                    print(f"Remote error: {remote.type_name}: {remote.message}")
                print(remote_traceback)

        failing_spec = EnvironmentSpec(
            python="3.12.*",
            post_install=(
                PostInstallCommand(
                    (
                        "python",
                        "-c",
                        "import sys; print('deliberate setup failure', file=sys.stderr); raise SystemExit(7)",
                    )
                ),
            ),
        )
        try:
            manager.provision("docs-provisioning-error", failing_spec).wait_for()
        except ProvisioningError as error:
            failure = error.failure
            provisioning_stage = failure.stage
            provisioning_command = failure.command or ""
            provisioning_returncode = failure.returncode
            provisioning_stderr = failure.stderr_tail
            print(f"Provisioning stage: {failure.stage}")
            print(f"Safe command: {failure.command}")
            print(f"Return code: {failure.returncode}")
            print(*failure.stderr_tail[-3:], sep="\n")

        corrected = manager.provision(
            "docs-provisioning-error",
            EnvironmentSpec(python="3.12.*"),
        ).wait_for()
        with corrected.start() as pool:
            retry_result = pool.execute_import("builtins:sum", args=([20, 22],), timeout=30)

    print(f"Corrected retry result: {retry_result}")
    return {
        "remote_category": remote_category,
        "remote_target": remote_target,
        "remote_type": remote_type,
        "remote_message": remote_message,
        "remote_traceback": remote_traceback,
        "provisioning_stage": provisioning_stage,
        "provisioning_command": provisioning_command,
        "provisioning_returncode": provisioning_returncode,
        "provisioning_stderr": provisioning_stderr,
        "retry_result": retry_result,
    }


if __name__ == "__main__":
    main()
