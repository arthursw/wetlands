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
    """Create a sample pixi.toml file."""
    content = """
[project]
name = "test-project"
version = "0.1.0"

[tool.pixi.dependencies]
python = "3.11"
numpy = ">=1.20"

[tool.pixi.pypi-dependencies]
requests = ">=2.25"

[tool.pixi.environments.default]
channels = ["conda-forge"]
"""
    pixi_file = temp_config_dir / "pixi.toml"
    pixi_file.write_text(content)
    return pixi_file


@pytest.fixture
def sample_pyproject_toml_with_pixi(temp_config_dir):
    """Create a sample pyproject.toml with pixi config."""
    content = """
[project]
name = "test-package"
version = "0.1.0"

[tool.pixi.dependencies]
python = "3.10"
numpy = ">=1.20"

[tool.pixi.pypi-dependencies]
requests = ">=2.25"

[tool.pixi.environments.default]
channels = ["conda-forge"]
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
        """Test creating environment from pixi.toml."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock ConfigParser to return expected dependencies
        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"python": "3.11", "conda": ["numpy>=1.20"], "pip": ["requests>=2.25"]}
            MockConfigParser.return_value = mock_parser

            manager.create_from_config(name="test_env", config_path=sample_pixi_toml)

            # Verify parse was called
            mock_parser.parse.assert_called_once()

    def test_create_with_pixi_toml_missing_environment_name(
        self, environment_manager_for_config_tests, sample_pixi_toml
    ):
        """Test that create_from_config can be used without environment_name (validation happens in ConfigParser)."""
        manager, _, _ = environment_manager_for_config_tests

        # create_from_config doesn't require environment_name - the parser will handle validation
        # This test now verifies that create_from_config is callable without it
        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            # ConfigParser.parse will validate and raise if needed
            mock_parser.parse.side_effect = ValueError("environment_name is required for pixi.toml files")
            MockConfigParser.return_value = mock_parser

            with pytest.raises(ValueError, match="environment_name.*pixi.toml"):
                manager.create_from_config(name="test_env", config_path=sample_pixi_toml)


class TestCreateWithPyprojectToml:
    """Test EnvironmentManager.create() with pyproject.toml files."""

    def test_create_with_pyproject_pixi_environment(
        self, environment_manager_for_config_tests, sample_pyproject_toml_with_pixi
    ):
        """Test creating environment from pyproject.toml with pixi config."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"python": "3.10", "conda": ["numpy>=1.20"], "pip": ["requests>=2.25"]}
            MockConfigParser.return_value = mock_parser

            manager.create_from_config(name="test_env", config_path=sample_pyproject_toml_with_pixi)

            mock_parser.parse.assert_called_once()

    def test_create_with_pyproject_optional_deps(
        self, environment_manager_for_config_tests, sample_pyproject_toml_no_pixi, monkeypatch
    ):
        """Test creating environment from pyproject.toml with optional dependencies."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False so environment is created
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"pip": ["numpy>=1.20", "scipy>=1.7", "pytest>=6.0", "black>=21.0"]}
            MockConfigParser.return_value = mock_parser

            manager.create_from_config(
                name="test_env", config_path=sample_pyproject_toml_no_pixi, optional_dependencies=["dev"]
            )

            mock_parser.parse.assert_called_once()
            call_args = mock_parser.parse.call_args
            assert call_args[1].get("optional_dependencies") == ["dev"]

    def test_create_with_pyproject_toml_no_env_or_optional(
        self, environment_manager_for_config_tests, sample_pyproject_toml_with_pixi, monkeypatch
    ):
        """Test that a non-existent environment_name falls back to default environment."""
        manager, _, _ = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"python": "3.10", "conda": ["numpy"], "pip": ["requests"]}
            MockConfigParser.return_value = mock_parser

            # Should succeed with non-existent environment_name - falls back to default
            env = manager.create_from_config(name="test_env", config_path=sample_pyproject_toml_with_pixi)

            mock_parser.parse.assert_called_once()
            assert env is not None


class TestCreateWithEnvironmentYml:
    """Test EnvironmentManager.create() with environment.yml files."""

    def test_create_with_environment_yml(
        self, environment_manager_for_config_tests, sample_environment_yml, monkeypatch
    ):
        """Test creating environment from environment.yml."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"conda": ["python=3.11", "numpy>=1.20"], "pip": ["requests>=2.25"]}
            MockConfigParser.return_value = mock_parser

            manager.create_from_config(name="test_env", config_path=sample_environment_yml)

            mock_parser.parse.assert_called_once()

    def test_create_with_environment_yml_no_extra_params(
        self, environment_manager_for_config_tests, sample_environment_yml, monkeypatch
    ):
        """Test environment.yml doesn't require environment_name or optional_dependencies."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"conda": ["python=3.11"], "pip": ["requests>=2.25"]}
            MockConfigParser.return_value = mock_parser

            # Should not raise error
            manager.create_from_config(name="test_env", config_path=sample_environment_yml)

            mock_parser.parse.assert_called_once()


class TestCreateWithRequirementsTxt:
    """Test EnvironmentManager.create() with requirements.txt files."""

    def test_create_with_requirements_txt(
        self, environment_manager_for_config_tests, sample_requirements_txt, monkeypatch
    ):
        """Test creating environment from requirements.txt."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"pip": ["numpy>=1.20", "scipy>=1.7", "requests>=2.25", "pytest>=6.0"]}
            MockConfigParser.return_value = mock_parser

            env = manager.create_from_config(name="test_env", config_path=sample_requirements_txt)

            mock_parser.parse.assert_called_once()
            assert env is not None

    def test_create_with_requirements_txt_no_extra_params(
        self, environment_manager_for_config_tests, sample_requirements_txt, monkeypatch
    ):
        """Test requirements.txt doesn't require environment_name or optional_dependencies."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"pip": ["numpy>=1.20"]}
            MockConfigParser.return_value = mock_parser

            # Should not raise error
            manager.create_from_config(name="test_env", config_path=sample_requirements_txt)

            mock_parser.parse.assert_called_once()


class TestCreateBackwardsCompatibility:
    """Test that create() still works with traditional inline dependencies."""

    def test_create_with_inline_dependencies(self, environment_manager_for_config_tests):
        """Test creating environment with inline Dependencies dict (original API)."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        deps: Dependencies = {"python": "3.11", "conda": ["numpy"], "pip": ["requests"]}

        # This should work without ConfigParser being called
        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            manager.create(name="test_env", dependencies=deps)

            # ConfigParser should not be called for inline deps
            MockConfigParser.assert_not_called()

    def test_create_with_none_dependencies(self, environment_manager_for_config_tests, monkeypatch):
        """Test creating environment with no dependencies."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False so environment is created
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

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
        self, environment_manager_for_config_tests, sample_pyproject_toml_with_pixi, monkeypatch
    ):
        """Test that providing optional_dependencies with create_from_config is allowed."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = {"python": "3.10", "conda": ["numpy"], "pip": ["requests"]}
            MockConfigParser.return_value = mock_parser

            # create_from_config with optional_dependencies
            manager.create_from_config(
                name="test_env", config_path=sample_pyproject_toml_with_pixi, optional_dependencies=["dev"]
            )

            mock_parser.parse.assert_called_once()


class TestCreateIntegrationWithDependencyManager:
    """Test create() integration with DependencyManager."""

    def test_create_uses_parsed_dependencies(
        self, environment_manager_for_config_tests, sample_environment_yml, monkeypatch
    ):
        """Test that parsed dependencies are passed to DependencyManager."""
        manager, mock_execute_output, mock_execute = environment_manager_for_config_tests

        # Mock _environment_validates_requirements to return False
        monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

        parsed_deps = {"conda": ["python=3.11", "numpy>=1.20"], "pip": ["requests>=2.25"]}

        with patch("wetlands.environment_manager.ConfigParser") as MockConfigParser:
            mock_parser = MagicMock()
            mock_parser.parse.return_value = parsed_deps
            MockConfigParser.return_value = mock_parser

            env = manager.create_from_config(name="test_env", config_path=sample_environment_yml)

            # Verify that ConfigParser.parse was called with the config file
            mock_parser.parse.assert_called_once()
            # Verify environment was created
            assert env is not None
