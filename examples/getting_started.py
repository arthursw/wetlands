from __future__ import annotations

from pathlib import Path

import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage


def main(root: Path = Path("wetlands")) -> None:
    example_directory = Path(__file__).parent
    spec = EnvironmentSpec(
        python="3.12.*",
        conda=("numpy>=2", "pip"),
        local=(LocalPackage(example_directory),),
    )

    with EnvironmentManager(root=root) as manager:
        environment = manager.provision("numpy-example", spec).wait_for()
        image = np.arange(16, dtype=np.float32).reshape(4, 4)

        with environment.start() as workers:
            mask = workers.execute_import(
                "example_module:threshold",
                kwargs={"image": image, "value": 7.5},
            )

    print(mask)


if __name__ == "__main__":
    main()
