from pathlib import Path

from wetlands import EnvironmentManager, EnvironmentSpec, LocalPackage


manager = EnvironmentManager(root="wetlands")
spec = EnvironmentSpec(
    python="3.12.*",
    conda=("pip",),
    local=(LocalPackage(Path(__file__).parent),),
)
environment = manager.provision("minimal-example", spec).wait_for()

with environment.start() as pool:
    result = pool.execute_import(
        "minimal_module:sum_values",
        args=([1, 2, 3],),
    )
    print(result)

manager.close()
