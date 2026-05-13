import pytest
import re
import platform
from pathlib import Path
from unittest.mock import MagicMock
from wetlands._internal.exceptions import IncompatibilityException
from wetlands._internal.dependency_manager import DependencyManager, Dependencies
from wetlands._internal.shell import shell_quote

# mock_settings_manager and mock_dependency_manager is defined in conftest.py


def test_platform_conda_format(mock_command_generator_micromamba):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    expected_platform = {
        "Darwin": "osx",
        "Windows": "win",
        "Linux": "linux",
    }[platform.system()]
    machine = platform.machine()
    machine = "64" if machine in ["x86_64", "AMD64"] else machine
    expected = f"{expected_platform}-{machine}"

    assert dependency_manager._platform_conda_format() == expected


def test_format_dependencies(mock_command_generator_micromamba):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": [
            "numpy",
            {
                "name": "tensorflow",
                "platforms": ["linux-64"],
                "optional": False,
                "dependencies": True,
            },
            {
                "name": "pandas",
                "platforms": ["win-64", "osx-64"],
                "optional": True,
                "dependencies": True,
            },
        ],
    }

    # Test case where platform is incompatible and optional
    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platform_conda_format = platform_mock

    deps, deps_no_deps, has_deps = dependency_manager.format_dependencies("conda", dependencies)

    assert shell_quote("numpy") in deps
    assert shell_quote("tensorflow") in deps  # tensorflow should be included as platform matches
    assert shell_quote("pandas") not in deps  # pandas should be excluded as platform does not match
    assert has_deps is True
    assert len(deps_no_deps) == 0

    # Test case where platform is incompatible and non-optional
    dependencies["conda"][2]["optional"] = False  # type: ignore
    with pytest.raises(IncompatibilityException):
        dependency_manager.format_dependencies("conda", dependencies)


def test_get_install_dependencies_commands_micromamba(mock_command_generator_micromamba):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": ["numpy", {"name": "stardist==0.9.1", "dependencies": False}],
        "pip": ["requests", {"name": "cellpose==3.1.0", "dependencies": False}],
    }

    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platform_conda_format = platform_mock

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert any(
        re.match(
            rf"{dependency_manager.settings_manager.conda_bin_config} install {re.escape(shell_quote('numpy'))} -y",
            cmd,
        )
        for cmd in commands
    )
    assert any(
        re.match(
            rf"{dependency_manager.settings_manager.conda_bin_config} install --no-deps {re.escape(shell_quote('stardist==0.9.1'))} -y",
            cmd,
        )
        for cmd in commands
    )
    assert any(re.match(rf"pip\s+install\s+{re.escape(shell_quote('requests'))}", cmd) for cmd in commands)
    assert any(
        re.match(rf"pip\s+install\s+--no-deps\s+{re.escape(shell_quote('cellpose==3.1.0'))}", cmd) for cmd in commands
    )


def test_get_install_dependencies_commands_pixi(mock_command_generator_pixi):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": ["numpy"],
        "pip": ["requests", {"name": "cellpose==3.1.0", "dependencies": False}],
    }

    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platform_conda_format = platform_mock

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert any("pixi add" in cmd and shell_quote("numpy") in cmd for cmd in commands)
    assert any("pixi add" in cmd and f"--pypi {shell_quote('requests')}" in cmd for cmd in commands)
    assert any(
        re.match(rf"pip\s+install\s+--no-deps\s+{re.escape(shell_quote('cellpose==3.1.0'))}", cmd) for cmd in commands
    )
