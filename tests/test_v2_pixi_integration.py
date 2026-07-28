"""Representative real-Pixi acceptance coverage for Wetlands 2."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from wetlands import (
    EnvironmentManager,
    EnvironmentSpec,
    LocalPackage,
    PostInstallCommand,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.agent_integration,
    pytest.mark.slow,
]


def test_real_pixi_locked_local_package_and_qualified_worker(tmp_path: Path) -> None:
    package = tmp_path / "worker-package"
    module = package / "wetlands_acceptance_worker"
    module.mkdir(parents=True)
    (package / "pyproject.toml").write_text(
        """
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "wetlands-acceptance-worker"
version = "1.0.0"
""".lstrip(),
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        "def add(left, right):\n    return left + right\n",
        encoding="utf-8",
    )

    manager = EnvironmentManager(tmp_path / "manager")
    base_spec = EnvironmentSpec(
        python=f"{sys.version_info.major}.{sys.version_info.minor}.*",
        conda=("packaging",),
        pypi=("typing-extensions",),
        local=(LocalPackage(package, editable=True),),
        post_install=(
            PostInstallCommand(
                (
                    "python",
                    "-c",
                    "from pathlib import Path; Path('post-install.ok').write_text('ok')",
                )
            ),
        ),
    )

    first = manager.provision("acceptance", base_spec).wait_for(600)
    generated_lock = first.pixi_lock_path.read_bytes()
    assert generated_lock
    assert (first.path / "post-install.ok").read_text(encoding="utf-8") == "ok"

    locked_spec = EnvironmentSpec(
        python=base_spec.python,
        conda=base_spec.conda,
        pypi=base_spec.pypi,
        local=base_spec.local,
        post_install=base_spec.post_install,
        pixi_lock=generated_lock,
    )
    environment = manager.provision(
        "acceptance",
        locked_spec,
        replace_existing=True,
    ).wait_for(600)

    assert environment.pixi_lock_path.read_bytes() == generated_lock
    with environment.start() as pool:
        assert (
            pool.execute_import(
                "wetlands_acceptance_worker:add",
                args=(20, 22),
                timeout=60,
            )
            == 42
        )
