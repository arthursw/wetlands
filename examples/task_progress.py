from __future__ import annotations

import logging
from pathlib import Path

from wetlands import EnvironmentManager, EnvironmentSpec, ExecutionEvent, ExecutionEventKind, LocalPackage


def main(root: Path = Path("wetlands")) -> dict[str, object]:
    """Run a task that reports progress, output, and a worker log."""
    example_directory = Path(__file__).parent
    spec = EnvironmentSpec(
        python="3.12.*",
        conda=("pip",),
        local=(LocalPackage(example_directory),),
    )
    progress: list[tuple[int, int]] = []

    with EnvironmentManager(root=root) as manager:
        environment = manager.provision("docs-examples", spec).wait_for()
        with environment.start() as pool:
            task = pool.submit_import(
                "example_module:process_items",
                args=([1, 2, 3, 4],),
                context_keyword="task",
            )

            def report(event: ExecutionEvent) -> None:
                if event.kind is not ExecutionEventKind.UPDATE:
                    return
                if event.current is None or event.maximum is None or event.progress is None:
                    return
                point = (event.current, event.maximum)
                if progress and progress[-1] == point:
                    return
                progress.append(point)
                print(f"Progress: {event.current}/{event.maximum} ({event.progress:.0%})")

            task.listen(report)
            result = task.wait_for()
            outputs = task.outputs

    print(f"Intermediate output: {outputs['items_processed']} items")
    print(f"Result: {result}")
    return {"progress": progress, "outputs": outputs, "result": result}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
