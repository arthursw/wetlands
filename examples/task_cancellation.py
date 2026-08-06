from __future__ import annotations

import threading
from pathlib import Path

from wetlands import (
    EnvironmentManager,
    EnvironmentSpec,
    ExecutionEvent,
    ExecutionEventKind,
    LocalPackage,
    OperationCanceled,
)


def main(root: Path = Path("wetlands")) -> dict[str, object]:
    """Demonstrate cooperative and forced task cancellation."""
    example_directory = Path(__file__).parent
    spec = EnvironmentSpec(
        python="3.12.*",
        conda=("pip",),
        local=(LocalPackage(example_directory),),
    )

    with EnvironmentManager(root=root, termination_grace=0.5) as manager:
        environment = manager.provision("docs-examples", spec).wait_for()
        with environment.start() as pool:
            cooperative_started = threading.Event()

            def observe_cooperative(event: ExecutionEvent) -> None:
                if event.kind is ExecutionEventKind.UPDATE:
                    cooperative_started.set()

            cooperative = pool.submit_import(
                "example_module:cooperative_work",
                context_keyword="task",
            )
            cooperative.listen(observe_cooperative)
            if not cooperative_started.wait(timeout=30):
                raise TimeoutError("Cooperative task did not start")
            cooperative.cancel()
            try:
                cooperative.wait_for(timeout=30)
            except OperationCanceled:
                print("Cooperative task canceled after worker cleanup")
            else:
                raise RuntimeError("Cooperative task completed instead of acknowledging cancellation")

            stubborn_started = threading.Event()

            def observe_stubborn(event: ExecutionEvent) -> None:
                if event.kind is ExecutionEventKind.UPDATE:
                    stubborn_started.set()

            stubborn = pool.submit_import(
                "example_module:ignore_cancellation",
                context_keyword="task",
            )
            stubborn.listen(observe_stubborn)
            if not stubborn_started.wait(timeout=30):
                raise TimeoutError("Non-cooperative task did not start")
            original_worker_id = manager.running_workers("docs-examples")[0].id
            stubborn.cancel()
            try:
                stubborn.wait_for(timeout=30)
            except OperationCanceled:
                pass
            else:
                raise RuntimeError("Non-cooperative task completed instead of being canceled")

            follow_up = pool.execute_import("builtins:sum", args=([20, 22],), timeout=30)
            replacement_worker_id = manager.running_workers("docs-examples")[0].id
            print("Non-cooperative task canceled after worker replacement")

    print(f"Replacement worker result: {follow_up}")
    return {
        "cooperative_state": cooperative.state.value,
        "forced_state": stubborn.state.value,
        "worker_replaced": replacement_worker_id != original_worker_id,
        "follow_up": follow_up,
    }


if __name__ == "__main__":
    main()
