import pytest
import platform
from unittest.mock import MagicMock
from cema.exceptions import IncompatibilityException
from cema.dependency_manager import DependencyManager, Dependencies

# mock_settings_manager and mock_dependency_manager is defined in conftest.py


def test_platform_conda_format(mock_settings_manager):
    dependency_manager = DependencyManager(mock_settings_manager)
    expected_platform = {
        "Darwin": "osx",
        "Windows": "win",
        "Linux": "linux",
    }[platform.system()]
    machine = platform.machine()
    machine = "64" if machine in ["x86_64", "AMD64"] else machine
    expected = f"{expected_platform}-{machine}"

    assert dependency_manager._platformCondaFormat() == expected


def test_format_dependencies(mock_settings_manager):
    dependency_manager = DependencyManager(mock_settings_manager)
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

    deps, deps_no_deps, has_deps = dependency_manager.formatDependencies(
        "conda", dependencies
    )

    assert '"numpy"' in deps
    assert '"tensorflow"' in deps  # tensorflow should be included as platform matches
    assert (
        '"pandas"' not in deps
    )  # pandas should be excluded as platform does not match
    assert has_deps is True
    assert len(deps_no_deps) == 0

    # Test case where platform is incompatible and non-optional
    dependencies["conda"][2]["optional"] = False # type: ignore
    with pytest.raises(IncompatibilityException):
        dependency_manager.formatDependencies("conda", dependencies)


def test_get_install_dependencies_commands(mock_settings_manager):
    dependency_manager = DependencyManager(mock_settings_manager)
    dependencies: Dependencies = {
        "python": "3.9",
        "conda": ["numpy"],
        "pip": ["requests"],
    }

    platform_mock = MagicMock()
    platform_mock.return_value = "linux-64"
    dependency_manager._platformCondaFormat = platform_mock

    commands = dependency_manager.getInstallDependenciesCommands(
        "envName", dependencies
    )

    assert any(
        f'{mock_settings_manager.condaBinConfig} install "numpy" -y' in cmd
        for cmd in commands
    )
    assert any('pip install  "requests"' in cmd for cmd in commands)
