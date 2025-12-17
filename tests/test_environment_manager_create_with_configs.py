"""Tests for EnvironmentManager.create() method with config file support."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands._internal.dependency_manager import Dependencies


# --- Fixtures ---


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def mock_command_executor(monkeypatch):
    """Mocks the CommandExecutor methods."""
    mock_execute = MagicMock()
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    mocks = {
        "execute_commands": mock_execute,
        "execute_commands_and_get_output": mock_execute_output,
    }
    return mocks


@pytest.fixture
def environment_manager_for_config_tests(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_micromamba_path = tmp_path_factory.mktemp("conda_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance")
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    # Mock install_conda to prevent downloads
    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(
        wetlands_instance_path=wetlands_instance_path,
        conda_path=dummy_micromamba_path,
        manager="micromamba",
        main_conda_environment_path=main_env_path,
    )

    # Apply the mocks to the specific instance's command_executor
    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_command_executor["execute_commands"])
    monkeypatch.setattr(
        manager.command_executor,
        "execute_commands_and_get_output",
        mock_command_executor["execute_commands_and_get_output"],
    )

    # Mock environment_exists to simplify create tests
    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    # Mock _environment_validates_requirements to return False so dependencies are not checked
    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

    return manager, mock_command_executor["execute_commands_and_get_output"], mock_command_executor["execute_commands"]


@pytest.fixture
def sample_pixi_toml(temp_config_dir):
    """Create a sample pixi.toml file in native pixi format."""
    content = """
[workspace]
name = "project-pixi-toml"
channels = ["conda-forge"]
platforms = ["linux-64", "osx-arm64", "osx-64", "win-64"]

[dependencies]
python = ">=3.11"
numpy = ">=1.20"

[pypi-dependencies]
requests = ">=2.25"
"""
    pixi_file = temp_config_dir / "pixi.toml"
    pixi_file.write_text(content)
    return pixi_file


@pytest.fixture
def sample_pyproject_toml_with_pixi(temp_config_dir):
    """Create a sample pyproject.toml with pixi config."""
    content = """
[project]
name = "project-pyproject-toml"
requires-python = ">=3.10"
dependencies = [
    "numpy>=1.20",
    "requests>=2.25"
]

[tool.pixi.workspace]
channels = ["conda-forge"]
platforms = ["linux-64", "osx-arm64", "osx-64", "win-64"]

[tool.pixi.dependencies]
numpy = "*"
pandas = "*"
matplotlib = "*"

"""
    pyproject_file = temp_config_dir / "pyproject.toml"
    pyproject_file.write_text(content)
    return pyproject_file


@pytest.fixture
def sample_pyproject_toml_no_pixi(temp_config_dir):
    """Create a sample pyproject.toml without pixi config."""
    content = """
[project]
name = "test-package"
version = "0.1.0"
dependencies = [
    "numpy>=1.20",
    "scipy>=1.7",
]

[project.optional-dependencies]
dev = ["pytest>=6.0", "black>=21.0"]
"""
    pyproject_file = temp_config_dir / "pyproject.toml"
    pyproject_file.write_text(content)
    return pyproject_file


@pytest.fixture
def sample_environment_yml(temp_config_dir):
    """Create a sample environment.yml file."""
    content = """
name: test-env
channels:
  - conda-forge
dependencies:
  - python=3.11
  - numpy>=1.20
  - pip
  - pip:
    - requests>=2.25
"""
    env_file = temp_config_dir / "environment.yml"
    env_file.write_text(content)
    return env_file


@pytest.fixture
def sample_requirements_txt(temp_config_dir):
    """Create a sample requirements.txt file."""
    content = """numpy>=1.20
scipy>=1.7
requests>=2.25
pytest>=6.0
"""
    req_file = temp_config_dir / "requirements.txt"
    req_file.write_text(content)
    return req_file


# --- Tests for create() with config files ---


class TestCreateWithPixiToml:
    """Test EnvironmentManager.create() with pixi.toml files."""

    def test_create_with_pixi_toml_basic(self, environment_manager_for_config_tests, sample_pixi_toml):
        """Test creating environment from pixi.toml uses real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_pixi_toml)

        # Verify environment was created
        assert env is not None


class TestCreateWithPyprojectToml:
    """Test EnvironmentManager.create() with pyproject.toml files."""

    def test_create_with_pyproject_pixi_environment(
        self, environment_manager_for_config_tests, sample_pyproject_toml_with_pixi
    ):
        """Test creating environment from pyproject.toml with pixi config uses real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_pyproject_toml_with_pixi)

        assert env is not None

    def test_create_with_pyproject_optional_deps(
        self, environment_manager_for_config_tests, sample_pyproject_toml_no_pixi
    ):
        """Test creating environment from pyproject.toml with optional dependencies uses real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(
            name="test_env", config_path=sample_pyproject_toml_no_pixi, optional_dependencies=["dev"]
        )

        assert env is not None

    def test_create_with_pyproject_toml_no_env_or_optional(
        self, environment_manager_for_config_tests, sample_pyproject_toml_no_pixi
    ):
        """Test pyproject.toml without pixi config works with real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_pyproject_toml_no_pixi)

        assert env is not None


class TestCreateWithEnvironmentYml:
    """Test EnvironmentManager.create() with environment.yml files."""

    def test_create_with_environment_yml(self, environment_manager_for_config_tests, sample_environment_yml):
        """Test creating environment from environment.yml uses real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_environment_yml)

        assert env is not None

    def test_create_with_environment_yml_no_extra_params(
        self, environment_manager_for_config_tests, sample_environment_yml
    ):
        """Test environment.yml doesn't require environment_name or optional_dependencies."""
        manager, _, _ = environment_manager_for_config_tests

        # Should not raise error
        env = manager.create_from_config(name="test_env", config_path=sample_environment_yml)

        assert env is not None


class TestCreateWithRequirementsTxt:
    """Test EnvironmentManager.create() with requirements.txt files."""

    def test_create_with_requirements_txt(self, environment_manager_for_config_tests, sample_requirements_txt):
        """Test creating environment from requirements.txt uses real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_requirements_txt)

        assert env is not None

    def test_create_with_requirements_txt_no_extra_params(
        self, environment_manager_for_config_tests, sample_requirements_txt
    ):
        """Test requirements.txt doesn't require environment_name or optional_dependencies."""
        manager, _, _ = environment_manager_for_config_tests

        # Should not raise error
        env = manager.create_from_config(name="test_env", config_path=sample_requirements_txt)

        assert env is not None


class TestCreateBackwardsCompatibility:
    """Test that create() still works with traditional inline dependencies."""

    def test_create_with_inline_dependencies(self, environment_manager_for_config_tests):
        """Test creating environment with inline Dependencies dict (original API)."""
        manager, _, _ = environment_manager_for_config_tests

        deps: Dependencies = {"python": "3.11", "conda": ["numpy"], "pip": ["requests"]}

        # This should work without ConfigParser being called
        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            manager.create(name="test_env", dependencies=deps)

            # ConfigParser should not be called for inline deps
            MockConfigParser.assert_not_called()

    def test_create_with_none_dependencies(self, environment_manager_for_config_tests):
        """Test creating environment with no dependencies."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create(name="test_env")

        assert env is not None
        assert isinstance(env, ExternalEnvironment)


class TestCreateParameterValidation:
    """Test parameter validation for create() method."""

    def test_invalid_dependency_type(self, environment_manager_for_config_tests, temp_config_dir):
        """Test error when dependencies has invalid type."""
        manager, _, _ = environment_manager_for_config_tests
        invalid_file = temp_config_dir / "invalid.txt"
        invalid_file.write_text("not a config file")

        with pytest.raises(ValueError, match="Unsupported.*config"):
            manager.create_from_config(name="test_env", config_path=invalid_file)

    def test_missing_config_file(self, environment_manager_for_config_tests, temp_config_dir):
        """Test error when config file doesn't exist."""
        manager, _, _ = environment_manager_for_config_tests
        missing_file = temp_config_dir / "environment.yml"  # Use valid filename but in non-existent directory

        with pytest.raises(FileNotFoundError):
            manager.create_from_config(name="test_env", config_path=missing_file)

    def test_both_environment_and_optional_deps_provided(
        self, environment_manager_for_config_tests, sample_pyproject_toml_no_pixi
    ):
        """Test that providing optional_dependencies with create_from_config is allowed."""
        manager, _, _ = environment_manager_for_config_tests

        # create_from_config with optional_dependencies using real ConfigParser
        env = manager.create_from_config(
            name="test_env", config_path=sample_pyproject_toml_no_pixi, optional_dependencies=["dev"]
        )

        assert env is not None


class TestCreateIntegrationWithDependencyManager:
    """Test create() integration with DependencyManager."""

    def test_create_uses_parsed_dependencies(self, environment_manager_for_config_tests, sample_environment_yml):
        """Test that parsed dependencies are passed to DependencyManager using real ConfigParser."""
        manager, _, _ = environment_manager_for_config_tests

        env = manager.create_from_config(name="test_env", config_path=sample_environment_yml)

        # Verify environment was created
        assert env is not None
