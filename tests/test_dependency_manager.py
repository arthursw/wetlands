import pytest
import re
import platform
from pathlib import Path
from unittest.mock import MagicMock, patch
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


def test_get_install_dependencies_commands_micromamba_local_dependencies(mock_command_generator_micromamba, tmp_path):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    editable_path = tmp_path / "editable package"
    non_editable_path = tmp_path / "regular-package"
    editable_path.mkdir()
    non_editable_path.mkdir()
    dependencies: Dependencies = {
        "local": [
            {"name": "editable-package", "path": editable_path},
            {"name": "regular-package", "path": non_editable_path, "editable": False},
        ],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert 'echo "Installing local dependency..."' in commands
    assert any(cmd == f"pip install  -e {shell_quote(editable_path.resolve())}" for cmd in commands)
    assert any(cmd == f"pip install  {shell_quote(non_editable_path.resolve())}" for cmd in commands)


def test_get_install_dependencies_commands_local_dependency_validation(mock_command_generator_micromamba, tmp_path):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    with pytest.raises(Exception, match="local dependency.*name"):
        dependency_manager.get_install_dependencies_commands(environment, {"local": [{"path": tmp_path}]})  # type: ignore

    with pytest.raises(Exception, match="local dependency.*path"):
        dependency_manager.get_install_dependencies_commands(environment, {"local": [{"name": "missing-path"}]})  # type: ignore

    with pytest.raises(Exception, match="local dependency.*empty"):
        dependency_manager.get_install_dependencies_commands(environment, {"local": [{}]})  # type: ignore

    with pytest.raises(Exception, match="local dependency.*dictionary"):
        dependency_manager.get_install_dependencies_commands(environment, {"local": ["not-a-dict"]})  # type: ignore


def test_get_install_dependencies_commands_local_dependency_echo_does_not_interpolate_name(
    mock_command_generator_micromamba, tmp_path
):
    dependency_manager = DependencyManager(mock_command_generator_micromamba)
    local_path = tmp_path / "local-package"
    local_path.mkdir()
    dependencies: Dependencies = {"local": [{"name": "$(touch /tmp/wetlands-pwn)", "path": local_path}]}

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert 'echo "Installing local dependency..."' in commands
    assert not any(cmd.startswith('echo "Installing local dependency $(') for cmd in commands)


@pytest.mark.parametrize("manager_fixture_name", ["mock_command_generator_micromamba", "mock_command_generator_pixi"])
def test_get_install_dependencies_commands_progress_echoes_quote_multi_word_messages(
    manager_fixture_name, request, tmp_path
):
    dependency_manager = DependencyManager(request.getfixturevalue(manager_fixture_name))
    local_path = tmp_path / "local-package"
    local_path.mkdir()
    dependencies: Dependencies = {
        "conda": ["numpy"],
        "pip": ["requests", {"name": "cellpose==3.1.0", "dependencies": False}],
        "local": [{"name": "local-package", "path": local_path}],
    }
    dependency_manager._platform_conda_format = MagicMock(return_value="linux-64")

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    progress_echoes = [command for command in commands if command.startswith("echo ")]
    assert progress_echoes
    assert all(command.startswith('echo "') and command.endswith('"') for command in progress_echoes)


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


def test_get_install_dependencies_commands_pixi_infers_unquoted_channel(mock_command_generator_pixi):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "python": "3.13.2",
        "conda": ["bioimageit::atlas>=0"],
        "pip": [],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)
    channel_commands = [cmd for cmd in commands if "project channel add" in cmd]

    assert channel_commands == [
        f"pixi project channel add --manifest-path {shell_quote(environment.path)} --no-progress --prepend {shell_quote('bioimageit')}"
    ]
    assert shell_quote("'bioimageit") not in channel_commands[0]
    assert any(
        cmd == f"pixi add --manifest-path {shell_quote(environment.path)} {shell_quote('bioimageit::atlas>=0')}"
        for cmd in commands
    )


@patch("platform.system", return_value="Windows")
def test_get_install_dependencies_commands_pixi_infers_unquoted_channel_with_windows_shell_quotes(
    mock_platform,
    mock_command_generator_pixi,
):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "conda": ["bioimageit::atlas>=0"],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)
    channel_commands = [cmd for cmd in commands if "project channel add" in cmd]

    assert channel_commands == [
        f"pixi project channel add --manifest-path {shell_quote(environment.path)} --no-progress --prepend {shell_quote('bioimageit')}"
    ]
    assert shell_quote("'bioimageit") not in channel_commands[0]


def test_get_install_dependencies_commands_pixi_combines_explicit_and_inferred_channels(
    mock_command_generator_pixi,
):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "channels": ["conda-forge"],
        "conda": ["bioimageit::atlas>=0"],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)
    channel_commands = [cmd for cmd in commands if "project channel add" in cmd]

    assert len(channel_commands) == 1
    assert "conda-forge" in channel_commands[0]
    assert "bioimageit" in channel_commands[0]


def test_get_install_dependencies_commands_pixi_does_not_infer_channels_without_prefix(
    mock_command_generator_pixi,
):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependencies: Dependencies = {
        "conda": ["atlas>=0"],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert not any("project channel add" in cmd for cmd in commands)
    assert any(
        cmd == f"pixi add --manifest-path {shell_quote(environment.path)} {shell_quote('atlas>=0')}" for cmd in commands
    )


def test_get_install_dependencies_commands_pixi_quotes_special_conda_dependency(
    mock_command_generator_pixi,
):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    dependency = "conda-forge::some-pkg>=1.0,<2.0"
    dependencies: Dependencies = {
        "conda": [dependency],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)

    assert any(
        cmd == f"pixi add --manifest-path {shell_quote(environment.path)} {shell_quote(dependency)}" for cmd in commands
    )


def test_get_install_dependencies_commands_pixi_local_dependencies(mock_command_generator_pixi, tmp_path):
    dependency_manager = DependencyManager(mock_command_generator_pixi)
    editable_path = tmp_path / "editable package"
    non_editable_path = tmp_path / "regular-package"
    editable_path.mkdir()
    non_editable_path.mkdir()
    dependencies: Dependencies = {
        "local": [
            {"name": "editable-package", "path": editable_path},
            {"name": "regular-package", "path": non_editable_path, "editable": False},
        ],
    }

    environment = MagicMock()
    environment.name = "envName"
    environment.path = Path("/tmp/envName")

    commands = dependency_manager.get_install_dependencies_commands(environment, dependencies)
    editable_spec = f"editable-package @ {editable_path.resolve().as_uri()}"
    non_editable_spec = f"regular-package @ {non_editable_path.resolve().as_uri()}"

    assert 'echo "Installing local dependency..."' in commands
    assert any(
        cmd == f"pixi add --manifest-path {shell_quote(environment.path)} --pypi --editable {shell_quote(editable_spec)}"
        for cmd in commands
    )
    assert any(
        cmd == f"pixi add --manifest-path {shell_quote(environment.path)} --pypi {shell_quote(non_editable_spec)}"
        for cmd in commands
    )
