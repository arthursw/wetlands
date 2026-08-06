from __future__ import annotations

import asyncio
from pathlib import Path

import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage


async def main(root: Path = Path("wetlands")) -> None:
    example_directory = Path(__file__).parent
    manager = EnvironmentManager(root=root)
    try:
        operation = manager.provision(
            "async-numpy-example",
            EnvironmentSpec(
                python="3.12.*",
                conda=("numpy>=2", "pip"),
                local=(LocalPackage(example_directory),),
            ),
        )

        async def report() -> None:
            async for event in operation.events():
                print(event.message)

        reporter = asyncio.create_task(report())
        environment = await operation
        await reporter

        workers = await asyncio.to_thread(environment.start)
        try:
            image = np.arange(16, dtype=np.float32).reshape(4, 4)
            mask = await workers.submit_import(
                "example_module:threshold",
                kwargs={"image": image, "value": 7.5},
            )
            print(mask)
        finally:
            await asyncio.to_thread(workers.close)
    finally:
        await asyncio.to_thread(manager.close)


if __name__ == "__main__":
    asyncio.run(main())
