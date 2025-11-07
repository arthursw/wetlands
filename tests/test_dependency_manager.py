import pytest
import re
import platform
from pathlib import Path
from unittest.mock import MagicMock
from wetlands._internal.exceptions import IncompatibilityException
from wetlands._internal.dependency_manager import DependencyManager, Dependencies

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

    assert dependency_manager._platformCondaFormat() == expected


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
    dependency_manager._platformCondaFormat = platform_mock

    deps, deps_no_deps, has_deps = dependency_manager.formatDependencies("conda", dependencies)

    assert '"numpy"' in deps
    assert '"tensorflow"' in deps  # tensorflow should be included as platform matches
    assert '"pandas"' not in deps  # pandas should be excluded as platform does not match
    assert has_deps is True
    assert len(deps_no_deps) == 0

    # Test case where platform is incompatible and non-optional
    dependencies["conda"][2]["optional"] = False  # type: ignore
    with pytest.raises(IncompatibilityException):
        dependency_manager.formatDependencies("conda", dependencies)


def test_get_install_dependencies_commands_micromamba(mock_command_generator_micromamba):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": ["numpy", {"name": "stardist==0.9.1", "dependencies": False}],
        "pip": ["requests", {"name": "cellpose==3.1.0", "dependencies": False}],
    }

    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platformCondaFormat = platform_mock

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.getInstallDependenciesCommands(environment, dependencies)

    assert any(
        re.match(rf'{dependency_manager.settingsManager.condaBinConfig} install "numpy" -y', cmd) for cmd in commands
    )
    assert any(
        re.match(rf'{dependency_manager.settingsManager.condaBinConfig} install --no-deps "stardist==0.9.1" -y', cmd)
        for cmd in commands
    )
    assert any(re.match(r'pip\s+install\s+"requests"', cmd) for cmd in commands)
    assert any(re.match(r'pip\s+install\s+--no-deps\s+"cellpose==3.1.0"', cmd) for cmd in commands)


def test_get_install_dependencies_commands_pixi(mock_command_generator_pixi):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": ["numpy"],
        "pip": ["requests", {"name": "cellpose==3.1.0", "dependencies": False}],
    }

    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platformCondaFormat = platform_mock

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.getInstallDependenciesCommands(environment, dependencies)

    assert any("pixi add" in cmd and '"numpy"' in cmd for cmd in commands)
    assert any("pixi add" in cmd and '--pypi "requests"' in cmd for cmd in commands)
    assert any(re.match(r'pip\s+install\s+--no-deps\s+"cellpose==3.1.0"', cmd) for cmd in commands)
