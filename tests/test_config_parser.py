"""Tests for ConfigParser class."""

import pytest
import tempfile
from pathlib import Path

from wetlands._internal.config_parser import ConfigParser


# --- Test Fixtures ---


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_pixi_toml(temp_config_dir):
    """Create a sample pixi.toml file."""
    content = """
[build]
targets = ["linux-64", "osx-arm64"]

[project]
name = "my-project"
version = "0.1.0"

[tool.pixi.dependencies]
python = "3.11"
numpy = ">=1.20"
scipy = ">=1.7"

[tool.pixi.pypi-dependencies]
requests = ">=2.25"
pandas = ">=1.3"

[tool.pixi.feature.dev.dependencies]
pytest = ">=6.0"
black = ">=21.0"

[tool.pixi.feature.dev.pypi-dependencies]
pytest-cov = ">=2.0"

[tool.pixi.environments.default]
features = ["dev"]
channels = ["conda-forge"]

[tool.pixi.environments.prod]
solve-group = "default"
channels = ["conda-forge"]
"""
    pixi_file = temp_config_dir / "pixi.toml"
    pixi_file.write_text(content)
    return pixi_file


@pytest.fixture
def sample_pyproject_toml_with_pixi(temp_config_dir):
    """Create a sample pyproject.toml with pixi config."""
    content = """
[build-system]
requires = ["flit_core >=3.2,<4"]
build-backend = "flit_core.buildapi"

[project]
name = "my-package"
version = "0.1.0"
description = "A test package"

[project.optional-dependencies]
dev = ["pytest>=6.0", "black>=21.0"]
docs = ["sphinx>=4.0", "sphinx-rtd-theme>=1.0"]

[tool.pixi.dependencies]
python = "3.10"
numpy = ">=1.20"

[tool.pixi.pypi-dependencies]
requests = ">=2.25"

[tool.pixi.feature.dev.dependencies]
pytest = ">=6.0"

[tool.pixi.feature.dev.pypi-dependencies]
pytest-cov = ">=2.0"

[tool.pixi.feature.docs.pypi-dependencies]
sphinx = ">=4.0"
sphinx-rtd-theme = ">=1.0"

[tool.pixi.environments.default]
features = ["dev"]

[tool.pixi.environments.docs]
features = ["docs"]
"""
    pyproject_file = temp_config_dir / "pyproject.toml"
    pyproject_file.write_text(content)
    return pyproject_file


@pytest.fixture
def sample_pyproject_toml_no_pixi(temp_config_dir):
    """Create a sample pyproject.toml without pixi config."""
    content = """
[build-system]
requires = ["flit_core >=3.2,<4"]
build-backend = "flit_core.buildapi"

[project]
name = "my-package"
version = "0.1.0"
description = "A test package"
dependencies = [
    "numpy>=1.20",
    "scipy>=1.7",
]

[project.optional-dependencies]
dev = ["pytest>=6.0", "black>=21.0"]
docs = ["sphinx>=4.0"]
"""
    pyproject_file = temp_config_dir / "pyproject.toml"
    pyproject_file.write_text(content)
    return pyproject_file


@pytest.fixture
def sample_environment_yml(temp_config_dir):
    """Create a sample environment.yml file."""
    content = """
name: my-env
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.11
  - numpy>=1.20
  - scipy>=1.7
  - pytest=6.0
  - pip
  - pip:
    - requests>=2.25
    - pandas>=1.3
"""
    env_file = temp_config_dir / "environment.yml"
    env_file.write_text(content)
    return env_file


@pytest.fixture
def sample_requirements_txt(temp_config_dir):
    """Create a sample requirements.txt file."""
    content = """# This is a requirements.txt file
numpy>=1.20
scipy>=1.7
requests>=2.25
pandas>=1.3

# More dependencies
pytest>=6.0
black>=21.0
"""
    req_file = temp_config_dir / "requirements.txt"
    req_file.write_text(content)
    return req_file


@pytest.fixture
def sample_requirements_txt_with_markers(temp_config_dir):
    """Create a requirements.txt file with environment markers."""
    content = """numpy>=1.20
scipy>=1.7; python_version >= '3.10'
requests>=2.25
pytest>=6.0; extra == 'dev'
"""
    req_file = temp_config_dir / "requirements.txt"
    req_file.write_text(content)
    return req_file


# --- ConfigParser Tests ---


class TestConfigParserPixiToml:
    """Test parsing pixi.toml files."""

    def test_parse_pixi_toml_basic(self, sample_pixi_toml):
        """Test parsing basic pixi.toml file."""
        parser = ConfigParser()
        deps = parser.parsePixiToml(sample_pixi_toml, "default")

        assert deps is not None
        assert "python" in deps
        assert deps["python"] == "3.11"
        assert "conda" in deps
        assert "numpy" in deps["conda"]
        assert "pip" in deps
        assert "requests" in deps["pip"]

    def test_parse_pixi_toml_with_features(self, sample_pixi_toml):
        """Test parsing pixi.toml with features."""
        parser = ConfigParser()
        deps = parser.parsePixiToml(sample_pixi_toml, "default")

        # default environment includes dev feature
        assert "pytest" in deps["conda"]
        assert "pytest-cov" in deps["pip"]

    def test_parse_pixi_toml_missing_environment(self, sample_pixi_toml):
        """Test parsing pixi.toml with missing environment name falls back to default."""
        parser = ConfigParser()

        # Non-existent environment should fall back to default
        deps = parser.parsePixiToml(sample_pixi_toml, "nonexistent")
        assert isinstance(deps, dict)
        assert "python" in deps or "conda" in deps or "pip" in deps

    def test_parse_pixi_toml_missing_file(self, temp_config_dir):
        """Test parsing non-existent pixi.toml file."""
        parser = ConfigParser()
        missing_file = temp_config_dir / "nonexistent.toml"

        with pytest.raises(FileNotFoundError):
            parser.parsePixiToml(missing_file, "default")

    def test_parse_pixi_toml_invalid_toml(self, temp_config_dir):
        """Test parsing invalid TOML file."""
        bad_toml = temp_config_dir / "bad.toml"
        bad_toml.write_text("this is [not valid toml {")

        parser = ConfigParser()
        with pytest.raises(Exception):  # TOML parsing error
            parser.parsePixiToml(bad_toml, "default")


class TestConfigParserPyprojectToml:
    """Test parsing pyproject.toml files."""

    def test_parse_pyproject_with_pixi_dependencies(self, sample_pyproject_toml_with_pixi):
        """Test parsing pyproject.toml with pixi dependencies."""
        parser = ConfigParser()
        deps = parser.parsePyprojectToml(sample_pyproject_toml_with_pixi, environmentName="default")

        assert deps is not None
        assert "python" in deps
        assert deps["python"] == "3.10"
        assert "conda" in deps
        assert "numpy" in deps["conda"]
        assert "requests" in deps["pip"]

    def test_parse_pyproject_with_pixi_features(self, sample_pyproject_toml_with_pixi):
        """Test parsing pyproject.toml with pixi features."""
        parser = ConfigParser()
        deps = parser.parsePyprojectToml(sample_pyproject_toml_with_pixi, environmentName="default")

        # default environment includes dev feature
        assert "pytest" in deps["conda"]
        assert "pytest-cov" in deps["pip"]

    def test_parse_pyproject_with_optional_dependencies(self, sample_pyproject_toml_with_pixi):
        """Test parsing pyproject.toml with optional dependencies group."""
        parser = ConfigParser()
        deps = parser.parsePyprojectToml(sample_pyproject_toml_with_pixi, optionalDependencies=["dev", "docs"])

        assert deps is not None
        # Should include dev dependencies
        assert "pytest" in deps["conda"] or "pytest" in deps.get("pip", [])
        # Should include docs dependencies
        assert "sphinx" in str(deps)

    def test_parse_pyproject_no_pixi_standard_deps(self, sample_pyproject_toml_no_pixi):
        """Test parsing pyproject.toml without pixi config (standard deps)."""
        parser = ConfigParser()
        deps = parser.parsePyprojectToml(sample_pyproject_toml_no_pixi, optionalDependencies=["dev"])

        assert deps is not None
        # Standard pyproject.toml uses pip for main dependencies
        assert "pip" in deps
        assert "numpy>=1.20" in deps["pip"]

    def test_parse_pyproject_missing_environment(self, sample_pyproject_toml_with_pixi):
        """Test parsing pyproject.toml with missing environment name falls back to default."""
        parser = ConfigParser()

        # Non-existent environment should fall back to default
        deps = parser.parsePyprojectToml(sample_pyproject_toml_with_pixi, environmentName="nonexistent")
        assert isinstance(deps, dict)
        assert "python" in deps or "conda" in deps or "pip" in deps

    def test_parse_pyproject_missing_optional_deps(self, sample_pyproject_toml_no_pixi):
        """Test parsing pyproject.toml with missing optional dependency group."""
        parser = ConfigParser()

        with pytest.raises(ValueError, match="Optional dependency group.*not found"):
            parser.parsePyprojectToml(sample_pyproject_toml_no_pixi, optionalDependencies=["nonexistent"])

    def test_parse_pyproject_missing_file(self, temp_config_dir):
        """Test parsing non-existent pyproject.toml file."""
        parser = ConfigParser()
        missing_file = temp_config_dir / "nonexistent.toml"

        with pytest.raises(FileNotFoundError):
            parser.parsePyprojectToml(missing_file)


class TestConfigParserEnvironmentYml:
    """Test parsing environment.yml files."""

    def test_parse_environment_yml_basic(self, sample_environment_yml):
        """Test parsing basic environment.yml file."""
        parser = ConfigParser()
        deps = parser.parseEnvironmentYml(sample_environment_yml)

        assert deps is not None
        assert "conda" in deps
        assert "python=3.11" in deps["conda"]
        assert "numpy>=1.20" in deps["conda"]
        assert "pip" in deps
        assert "requests>=2.25" in deps["pip"]

    def test_parse_environment_yml_conda_only(self, temp_config_dir):
        """Test parsing environment.yml with only conda dependencies."""
        env_yml = temp_config_dir / "environment.yml"
        env_yml.write_text("""
name: test-env
channels:
  - conda-forge
dependencies:
  - python=3.10
  - numpy
  - scipy
""")
        parser = ConfigParser()
        deps = parser.parseEnvironmentYml(env_yml)

        assert "conda" in deps
        assert "python=3.10" in deps["conda"]
        assert "numpy" in deps["conda"]
        assert "pip" not in deps or len(deps.get("pip", [])) == 0

    def test_parse_environment_yml_pip_only(self, temp_config_dir):
        """Test parsing environment.yml with only pip dependencies."""
        env_yml = temp_config_dir / "environment.yml"
        env_yml.write_text("""
name: test-env
dependencies:
  - pip
  - pip:
    - requests
    - numpy
""")
        parser = ConfigParser()
        deps = parser.parseEnvironmentYml(env_yml)

        assert "pip" in deps
        assert "requests" in deps["pip"]
        assert "numpy" in deps["pip"]

    def test_parse_environment_yml_missing_file(self, temp_config_dir):
        """Test parsing non-existent environment.yml file."""
        parser = ConfigParser()
        missing_file = temp_config_dir / "nonexistent.yml"

        with pytest.raises(FileNotFoundError):
            parser.parseEnvironmentYml(missing_file)

    def test_parse_environment_yml_invalid_yaml(self, temp_config_dir):
        """Test parsing invalid YAML file."""
        bad_yml = temp_config_dir / "bad.yml"
        bad_yml.write_text("this is: [not valid yaml {")

        parser = ConfigParser()
        with pytest.raises(Exception):  # YAML parsing error
            parser.parseEnvironmentYml(bad_yml)


class TestConfigParserRequirementsTxt:
    """Test parsing requirements.txt files."""

    def test_parse_requirements_txt_basic(self, sample_requirements_txt):
        """Test parsing basic requirements.txt file."""
        parser = ConfigParser()
        deps = parser.parseRequirementsTxt(sample_requirements_txt)

        assert deps is not None
        assert "pip" in deps
        assert "numpy>=1.20" in deps["pip"]
        assert "scipy>=1.7" in deps["pip"]
        assert "requests>=2.25" in deps["pip"]
        assert "pandas>=1.3" in deps["pip"]
        assert "pytest>=6.0" in deps["pip"]
        assert "black>=21.0" in deps["pip"]

    def test_parse_requirements_txt_with_markers(self, sample_requirements_txt_with_markers):
        """Test parsing requirements.txt with environment markers."""
        parser = ConfigParser()
        deps = parser.parseRequirementsTxt(sample_requirements_txt_with_markers)

        assert deps is not None
        assert "pip" in deps
        # Should parse dependencies without markers
        assert "numpy>=1.20" in deps["pip"]
        assert "requests>=2.25" in deps["pip"]
        # Should strip markers from dependencies
        assert "scipy>=1.7" in deps["pip"]
        assert "pytest>=6.0" in deps["pip"]
        # Should not contain marker syntax
        assert not any(";" in dep for dep in deps["pip"])

    def test_parse_requirements_txt_missing_file(self, temp_config_dir):
        """Test parsing non-existent requirements.txt file."""
        parser = ConfigParser()
        missing_file = temp_config_dir / "nonexistent.txt"

        with pytest.raises(FileNotFoundError):
            parser.parseRequirementsTxt(missing_file)

    def test_parse_requirements_txt_empty(self, temp_config_dir):
        """Test parsing empty requirements.txt file."""
        empty_req = temp_config_dir / "requirements.txt"
        empty_req.write_text("")

        parser = ConfigParser()
        deps = parser.parseRequirementsTxt(empty_req)

        assert deps == {} or "pip" not in deps or len(deps.get("pip", [])) == 0

    def test_parse_requirements_txt_only_comments(self, temp_config_dir):
        """Test parsing requirements.txt with only comments."""
        req_file = temp_config_dir / "requirements.txt"
        req_file.write_text("""
# This is a comment
# Another comment
""")

        parser = ConfigParser()
        deps = parser.parseRequirementsTxt(req_file)

        assert deps == {} or "pip" not in deps or len(deps.get("pip", [])) == 0


class TestConfigParserDetection:
    """Test file type detection."""

    def test_detect_pixi_toml(self, sample_pixi_toml):
        """Test detection of pixi.toml file."""
        parser = ConfigParser()
        file_type = parser.detectConfigFileType(sample_pixi_toml)
        assert file_type == "pixi"

    def test_detect_pyproject_toml(self, sample_pyproject_toml_no_pixi):
        """Test detection of pyproject.toml file."""
        parser = ConfigParser()
        file_type = parser.detectConfigFileType(sample_pyproject_toml_no_pixi)
        assert file_type == "pyproject"

    def test_detect_environment_yml(self, sample_environment_yml):
        """Test detection of environment.yml file."""
        parser = ConfigParser()
        file_type = parser.detectConfigFileType(sample_environment_yml)
        assert file_type == "environment"

    def test_detect_pyproject_with_pixi(self, sample_pyproject_toml_with_pixi):
        """Test detection of pyproject.toml with pixi config."""
        parser = ConfigParser()
        file_type = parser.detectConfigFileType(sample_pyproject_toml_with_pixi)
        assert file_type == "pyproject"

    def test_detect_requirements_txt(self, sample_requirements_txt):
        """Test detection of requirements.txt file."""
        parser = ConfigParser()
        file_type = parser.detectConfigFileType(sample_requirements_txt)
        assert file_type == "requirements"

    def test_detect_invalid_file_type(self, temp_config_dir):
        """Test detection of invalid file type."""
        invalid_file = temp_config_dir / "config.json"
        invalid_file.write_text('{"invalid": "config"}')

        parser = ConfigParser()
        with pytest.raises(ValueError, match="Unsupported config file type"):
            parser.detectConfigFileType(invalid_file)


class TestConfigParserUnifiedInterface:
    """Test the unified parsing interface."""

    def test_parse_config_pixi(self, sample_pixi_toml):
        """Test unified parse() method with pixi.toml."""
        parser = ConfigParser()
        deps = parser.parse(sample_pixi_toml, environmentName="default")

        assert deps is not None
        assert "conda" in deps

    def test_parse_config_pyproject_with_env(self, sample_pyproject_toml_with_pixi):
        """Test unified parse() method with pyproject.toml (environment)."""
        parser = ConfigParser()
        deps = parser.parse(sample_pyproject_toml_with_pixi, environmentName="default")

        assert deps is not None
        assert "python" in deps

    def test_parse_config_pyproject_with_optional(self, sample_pyproject_toml_no_pixi):
        """Test unified parse() method with pyproject.toml (optional deps)."""
        parser = ConfigParser()
        deps = parser.parse(sample_pyproject_toml_no_pixi, optionalDependencies=["dev"])

        assert deps is not None
        assert "pip" in deps

    def test_parse_config_environment_yml(self, sample_environment_yml):
        """Test unified parse() method with environment.yml."""
        parser = ConfigParser()
        deps = parser.parse(sample_environment_yml)

        assert deps is not None
        assert "conda" in deps
        assert "pip" in deps

    def test_parse_config_requirements_txt(self, sample_requirements_txt):
        """Test unified parse() method with requirements.txt."""
        parser = ConfigParser()
        deps = parser.parse(sample_requirements_txt)

        assert deps is not None
        assert "pip" in deps
        assert "numpy>=1.20" in deps["pip"]
