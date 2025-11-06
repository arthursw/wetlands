"""Parser for configuration files (pixi.toml, pyproject.toml, environment.yml)."""

from pathlib import Path
from typing import Optional, Union

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

import yaml

from wetlands._internal.dependency_manager import Dependencies


class ConfigParser:
    """Parse dependency configurations from various file formats."""

    def parse(
        self,
        config_path: Union[str, Path],
        environmentName: Optional[str] = None,
        optionalDependencies: Optional[list[str]] = None,
    ) -> Dependencies:
        """Parse configuration file and extract dependencies.

        Args:
            config_path: Path to configuration file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt)
            environmentName: Environment name to use (for pixi.toml and pyproject.toml)
            optionalDependencies: Optional dependency groups to include (for pyproject.toml)

        Returns:
            Dependencies dict with conda, pip, and python keys

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config format is unsupported or parameters invalid
            Exception: If file parsing fails
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        file_type = self.detectConfigFileType(config_path)

        if file_type == "pixi":
            return self.parsePixiToml(config_path, environmentName)
        elif file_type == "pyproject":
            return self.parsePyprojectToml(
                config_path,
                environmentName=environmentName,
                optionalDependencies=optionalDependencies,
            )
        elif file_type == "environment":
            return self.parseEnvironmentYml(config_path)
        elif file_type == "requirements":
            return self.parseRequirementsTxt(config_path)
        else:
            raise ValueError(f"Unsupported config file type: {file_type}")

    def detectConfigFileType(self, config_path: Union[str, Path]) -> str:
        """Detect the type of configuration file.

        Args:
            config_path: Path to configuration file

        Returns:
            "pixi", "pyproject", "environment", or "requirements"

        Raises:
            ValueError: If file type is not supported
        """
        config_path = Path(config_path)
        name = config_path.name.lower()

        if name == "pixi.toml":
            return "pixi"
        elif name == "pyproject.toml":
            return "pyproject"
        elif name in ["environment.yml", "environment.yaml"]:
            return "environment"
        elif name == "requirements.txt":
            return "requirements"
        else:
            raise ValueError(
                f"Unsupported config file type: {name}. "
                "Expected pixi.toml, pyproject.toml, environment.yml, or requirements.txt"
            )

    def parsePixiToml(
        self,
        pixi_path: Union[str, Path],
        environmentName: Optional[str] = None,
    ) -> Dependencies:
        """Parse pixi.toml file and extract dependencies.

        Args:
            pixi_path: Path to pixi.toml file
            environmentName: Name of environment to extract (optional - falls back to default)

        Returns:
            Dependencies dict

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If no valid environment found
            Exception: If TOML parsing fails
        """
        pixi_path = Path(pixi_path)

        if not pixi_path.exists():
            raise FileNotFoundError(f"pixi.toml not found: {pixi_path}")

        with open(pixi_path, "rb") as f:
            config = tomllib.load(f)

        # Get environment configuration
        environments = config.get("tool", {}).get("pixi", {}).get("environments", {})

        # Determine which environment to use
        env_to_use = environmentName
        if env_to_use and env_to_use not in environments:
            # Fall back to default if requested environment doesn't exist
            env_to_use = "default"

        if not env_to_use:
            # No environmentName provided, use default
            env_to_use = "default"

        if env_to_use not in environments:
            available = list(environments.keys())
            raise ValueError(f"Environment '{env_to_use}' not found in pixi.toml. Available environments: {available}")

        env_config = environments[env_to_use]
        features = env_config.get("features", [])

        # Start with base dependencies
        dependencies: Dependencies = {}

        # Extract python version
        pixi_config = config.get("tool", {}).get("pixi", {})
        if "python" in pixi_config.get("dependencies", {}):
            dependencies["python"] = pixi_config["dependencies"]["python"]

        # Collect dependencies from features and base
        conda_deps = []
        pip_deps = []

        # Base dependencies
        conda_deps.extend(pixi_config.get("dependencies", {}).keys())
        pip_deps.extend(pixi_config.get("pypi-dependencies", {}).keys())

        # Feature dependencies
        features_config = pixi_config.get("feature", {})
        for feature_name in features:
            if feature_name in features_config:
                feature = features_config[feature_name]
                conda_deps.extend(feature.get("dependencies", {}).keys())
                pip_deps.extend(feature.get("pypi-dependencies", {}).keys())

        # Remove duplicates and python key if present
        conda_deps = list(set(d for d in conda_deps if d != "python"))
        pip_deps = list(set(pip_deps))

        if conda_deps:
            dependencies["conda"] = conda_deps
        if pip_deps:
            dependencies["pip"] = pip_deps

        return dependencies

    def parsePyprojectToml(
        self,
        pyproject_path: Union[str, Path],
        environmentName: Optional[str] = None,
        optionalDependencies: Optional[list[str]] = None,
    ) -> Dependencies:
        """Parse pyproject.toml file and extract dependencies.

        Args:
            pyproject_path: Path to pyproject.toml file
            environmentName: Name of pixi environment to extract
            optionalDependencies: List of optional dependency groups to include

        Returns:
            Dependencies dict

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If required parameters missing or not found
            Exception: If TOML parsing fails
        """
        pyproject_path = Path(pyproject_path)

        if not pyproject_path.exists():
            raise FileNotFoundError(f"pyproject.toml not found: {pyproject_path}")

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        dependencies: Dependencies = {}
        pixi_config = config.get("tool", {}).get("pixi", {})

        # If pixi config exists, use it (like pixi.toml)
        if pixi_config:
            if environmentName or not optionalDependencies:
                # Use pixi environment if environmentName provided, or if no optionalDependencies
                environments = pixi_config.get("environments", {})

                # Determine which environment to use
                env_to_use = environmentName
                if env_to_use and env_to_use not in environments:
                    # Fall back to default if requested environment doesn't exist
                    env_to_use = "default"

                if not env_to_use:
                    # No environmentName provided, use default
                    env_to_use = "default"

                if env_to_use not in environments:
                    available = list(environments.keys())
                    raise ValueError(
                        f"Environment '{env_to_use}' not found in pyproject.toml. Available environments: {available}"
                    )

                env_config = environments[env_to_use]
                features = env_config.get("features", [])

                # Extract python version
                if "python" in pixi_config.get("dependencies", {}):
                    dependencies["python"] = pixi_config["dependencies"]["python"]

                # Collect dependencies from features and base
                conda_deps = []
                pip_deps = []

                # Base dependencies
                conda_deps.extend(pixi_config.get("dependencies", {}).keys())
                pip_deps.extend(pixi_config.get("pypi-dependencies", {}).keys())

                # Feature dependencies
                features_config = pixi_config.get("feature", {})
                for feature_name in features:
                    if feature_name in features_config:
                        feature = features_config[feature_name]
                        conda_deps.extend(feature.get("dependencies", {}).keys())
                        pip_deps.extend(feature.get("pypi-dependencies", {}).keys())

                # Remove duplicates and python key
                conda_deps = list(set(d for d in conda_deps if d != "python"))
                pip_deps = list(set(pip_deps))

                if conda_deps:
                    dependencies["conda"] = conda_deps
                if pip_deps:
                    dependencies["pip"] = pip_deps

            elif optionalDependencies:
                # Use pixi features as optional dependencies
                features_config = pixi_config.get("feature", {})
                conda_deps = []
                pip_deps = []

                for feature_name in optionalDependencies:
                    if feature_name not in features_config:
                        available = list(features_config.keys())
                        raise ValueError(
                            f"Feature '{feature_name}' not found in pyproject.toml. Available features: {available}"
                        )

                    feature = features_config[feature_name]
                    conda_deps.extend(feature.get("dependencies", {}).keys())
                    pip_deps.extend(feature.get("pypi-dependencies", {}).keys())

                # Remove duplicates
                conda_deps = list(set(d for d in conda_deps if d != "python"))
                pip_deps = list(set(pip_deps))

                if conda_deps:
                    dependencies["conda"] = conda_deps
                if pip_deps:
                    dependencies["pip"] = pip_deps
            else:
                raise ValueError(
                    "For pyproject.toml with pixi config, provide either environmentName or optionalDependencies"
                )
        else:
            # Standard PEP 621 pyproject.toml (no pixi config)
            project_config = config.get("project", {})

            # Main dependencies go to pip
            main_deps = project_config.get("dependencies", [])
            if main_deps:
                dependencies["pip"] = main_deps

            # Optional dependencies
            if optionalDependencies:
                optional_deps = project_config.get("optional-dependencies", {})
                pip_deps = dependencies.get("pip", [])

                for group_name in optionalDependencies:
                    if group_name not in optional_deps:
                        available = list(optional_deps.keys())
                        raise ValueError(
                            f"Optional dependency group '{group_name}' not found. Available groups: {available}"
                        )

                    pip_deps.extend(optional_deps[group_name])

                if pip_deps:
                    dependencies["pip"] = list(set(pip_deps))

        return dependencies

    def parseEnvironmentYml(
        self,
        env_path: Union[str, Path],
    ) -> Dependencies:
        """Parse environment.yml file and extract dependencies.

        Args:
            env_path: Path to environment.yml file

        Returns:
            Dependencies dict with conda and pip keys

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If YAML parsing fails
        """
        env_path = Path(env_path)

        if not env_path.exists():
            raise FileNotFoundError(f"environment.yml not found: {env_path}")

        with open(env_path, "r") as f:
            config = yaml.safe_load(f)

        dependencies: Dependencies = {}

        if not config or not isinstance(config, dict):
            return dependencies

        # Process dependencies list
        deps_list = config.get("dependencies", [])
        conda_deps = []
        pip_deps = []

        for dep in deps_list:
            if isinstance(dep, str):
                if dep == "pip":
                    # Skip the 'pip' marker itself
                    continue
                else:
                    # Regular conda dependency
                    conda_deps.append(dep)
            elif isinstance(dep, dict):
                # Nested pip dependencies
                for key, value in dep.items():
                    if key == "pip" and isinstance(value, list):
                        pip_deps.extend(value)

        if conda_deps:
            dependencies["conda"] = conda_deps
        if pip_deps:
            dependencies["pip"] = pip_deps

        return dependencies

    def parseRequirementsTxt(
        self,
        requirements_path: Union[str, Path],
    ) -> Dependencies:
        """Parse requirements.txt file and extract dependencies.

        Args:
            requirements_path: Path to requirements.txt file

        Returns:
            Dependencies dict with pip key containing all dependencies

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If file reading fails
        """
        requirements_path = Path(requirements_path)

        if not requirements_path.exists():
            raise FileNotFoundError(f"requirements.txt not found: {requirements_path}")

        pip_deps = []

        with open(requirements_path, "r") as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Skip environment markers (e.g., lines with ; python_version)
                if ";" in line:
                    line = line.split(";")[0].strip()

                if line:
                    pip_deps.append(line)

        dependencies: Dependencies = {}
        if pip_deps:
            dependencies["pip"] = pip_deps

        return dependencies
