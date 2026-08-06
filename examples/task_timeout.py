from __future__ import annotations

from pathlib import Path

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage, OperationCanceled


def main(root: Path = Path("wetlands")) -> dict[str, object]:
    """Show that a waiting timeout does not cancel the remote task."""
    example_directory = Path(__file__).parent
    spec = EnvironmentSpec(
        python="3.12.*",
        conda=("pip",),
        local=(LocalPackage(example_directory),),
    )

    with EnvironmentManager(root=root, termination_grace=0.5) as manager:
        environment = manager.provision("docs-examples", spec).wait_for()
        with environment.start() as pool:
            task = pool.submit_import("example_module:slow_sum", args=([20, 22],))
            try:
                task.wait_for(timeout=0.1)
            except TimeoutError:
                state_after_timeout = task.state.value
                print(f"Wait timed out; task state is still {state_after_timeout}")
            else:
                raise RuntimeError("Task completed before the example timeout elapsed")

            running_after_timeout = not task.state.terminal
            task.cancel()
            try:
                task.wait_for(timeout=30)
            except OperationCanceled:
                print("Task canceled after cleanup")
            else:
                raise RuntimeError("Task completed instead of being canceled")

    return {
        "state_after_timeout": state_after_timeout,
        "running_after_timeout": running_after_timeout,
        "final_state": task.state.value,
    }


if __name__ == "__main__":
    main()
