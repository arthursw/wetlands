from pathlib import Path

import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage


def report(event) -> None:
    print(f"{event.kind.value}: {event.message}")


manager = EnvironmentManager(root="wetlands")

preparation = manager.prepare()
preparation.listen(report)
preparation.wait_for()

spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2", "pip"),
    local=(LocalPackage(Path(__file__).parent),),
)

provisioning = manager.provision("numpy-example", spec)
provisioning.listen(report)
environment = provisioning.wait_for()

image = np.arange(16, dtype=np.float32).reshape(4, 4)

with environment.start(workers=1) as pool:
    task = pool.submit_import(
        "example_module:threshold",
        kwargs={"image": image, "value": 7.5},
    )
    mask = task.wait_for()

print(mask)
manager.close()
