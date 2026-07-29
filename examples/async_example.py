import asyncio
from pathlib import Path

import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage


async def main() -> None:
    manager = EnvironmentManager(root="wetlands")

    preparation = manager.prepare()

    async def report() -> None:
        async for event in preparation.events():
            print(event.message)

    reporter = asyncio.create_task(report())
    await preparation
    await reporter

    environment = await manager.provision(
        "async-numpy-example",
        EnvironmentSpec(
            python="3.12.*",
            conda=("numpy>=2", "pip"),
            local=(LocalPackage(Path(__file__).parent),),
        ),
    )

    image = np.arange(16, dtype=np.float32).reshape(4, 4)

    with environment.start() as pool:
        mask = await pool.submit_import(
            "example_module:threshold",
            kwargs={"image": image, "value": 7.5},
        )
        print(mask)

    manager.close()


asyncio.run(main())
