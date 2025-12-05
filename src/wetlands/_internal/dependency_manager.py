import platform
from typing import TYPE_CHECKING

from wetlands._internal.command_generator import CommandGenerator

try:
    from typing import NotRequired, TypedDict, Literal  # type: ignore
except ImportError:
    from typing_extensions import NotRequired, TypedDict, Literal  # type: ignore

from wetlands._internal.exceptions import IncompatibilityException

if TYPE_CHECKING:
    from wetlands.environment import Environment

Platform = Literal["osx-64", "osx-arm64", "win-64", "win-arm64", "linux-64", "linux-arm64"]


class Dependency(TypedDict):
    name: str
    platforms: NotRequired[list[Platform]]
    optional: NotRequired[bool]
    dependencies: NotRequired[bool]


class Dependencies(TypedDict):
    python: NotRequired[str]
    conda: NotRequired[list[str | Dependency]]
    channels: NotRequired[list[str]]
    pip: NotRequired[list[str | Dependency]]


class DependencyManager:
    """Manage pip and conda dependencies."""

    def __init__(self, command_generator: CommandGenerator):
        self.installed_packages: dict[str, dict[str, str]] = {}
        self.settings_manager = command_generator.settings_manager
        self.command_generator = command_generator

    def _platform_conda_format(self) -> str:
        """Get conda-compatible platform string (e.g., 'linux-64', 'osx-arm64', 'win-64')."""
        machine = platform.machine()
        machine = "64" if machine == "x86_64" or machine == "AMD64" else machine
        system = dict(Darwin="osx", Windows="win", Linux="linux")[platform.system()]
        return f"{system}-{machine}"

    def format_dependencies(
        self,
        package_manager: str,
        dependencies: Dependencies,
        raise_incompatibility_error: bool = True,
        quotes: bool = True,
    ) -> tuple[list[str], list[str], bool]:
        """Formats dependencies for installation with platform checks.

        Args:
                package_manager: 'conda' or 'pip'.
                dependencies: Dependencies to process.
                raise_incompatibility_error: Whether to raise on incompatible platforms.
                quotes: Whether to put dependencies in quotes (required when installing extras on mac, e.g. `pip install "napari[pyqt5]"`)

        Returns:
                Tuple of (dependencies, no-deps dependencies, has_dependencies).

        Raises:
                IncompatibilityException: For non-optional incompatible dependencies.
        """
        dependency_list: list[str | Dependency] = dependencies.get(package_manager, [])  # type: ignore
        final_dependencies: list[str] = []
        final_dependencies_no_deps: list[str] = []
        for dependency in dependency_list:
            if isinstance(dependency, str):
                final_dependencies.append(dependency)
            else:
                current_platform = self._platform_conda_format()
                platforms = dependency.get("platforms", "all")
                if (
                    current_platform in platforms
                    or platforms == "all"
                    or len(platforms) == 0
                    or not raise_incompatibility_error
                ):
                    if "dependencies" not in dependency or dependency["dependencies"]:
                        final_dependencies.append(dependency["name"])
                    else:
                        final_dependencies_no_deps.append(dependency["name"])
                elif not dependency.get("optional", False):
                    platforms_string = ", ".join(platforms)
                    raise IncompatibilityException(
                        f"Error: the library {dependency['name']} is not available on this platform ({current_platform}). It is only available on the following platforms: {platforms_string}."
                    )
        if quotes:
            final_dependencies = [f'"{d}"' for d in final_dependencies]
            final_dependencies_no_deps = [f'"{d}"' for d in final_dependencies_no_deps]
        return (
            final_dependencies,
            final_dependencies_no_deps,
            len(final_dependencies) + len(final_dependencies_no_deps) > 0,
        )

    def get_install_dependencies_commands(self, environment: "Environment", dependencies: Dependencies) -> list[str]:
        """Generates commands to install dependencies in the given environment. Note: this does not activate conda, use self.get_activate_conda_commands() first.

        Args:
                environment: Target environment name. If none, no conda environment will be activated, only pip dependencies will be installed in the current python environemnt ; conda dependencies will be ignored.
                dependencies: Dependencies to install.

        Returns:
                list of installation commands.

        Raises:
                Exception: If pip dependencies contain Conda channel syntax.
        """
        conda_dependencies, condaDependenciesNoDeps, hasCondaDependencies = self.format_dependencies(
            "conda", dependencies
        )
        pipDependencies, pipDependenciesNoDeps, hasPipDependencies = self.format_dependencies("pip", dependencies)

        if hasCondaDependencies and not environment:
            raise Exception(
                "Conda dependencies can only be installed in a Conda environment. Please provide an existing conda environment to install dependencies."
            )
        if any("::" in d for d in pipDependencies + pipDependenciesNoDeps):
            raise Exception(
                f'One pip dependency has a channel specifier "::". Is it a conda dependency?\n\n({dependencies.get("pip")})'
            )
        install_deps_commands = self.settings_manager.get_proxy_environment_variables_commands()

        install_deps_commands += self.command_generator.get_activate_conda_commands()

        if environment:
            install_deps_commands += self.command_generator.get_activate_environment_commands(
                environment, activate_conda=False
            )
            install_deps_commands += self.command_generator.get_add_channels_commands(
                environment, dependencies.get("channels", []), conda_dependencies, activate_conda=False
            )

        proxy_string = self.settings_manager.get_proxy_string()
        proxy_args = f"--proxy {proxy_string}" if proxy_string is not None else ""
        if self.settings_manager.use_pixi:
            if environment is None:
                raise Exception(
                    "Use micromamba if you want to install a pip dependency without specifying a conda environment."
                )
            if hasPipDependencies:
                install_deps_commands += [
                    f'echo "Installing pip dependencies..."',
                    f'{self.settings_manager.conda_bin} add --manifest-path "{environment.path}" --pypi {" ".join(pipDependencies)}',
                ]
            if hasCondaDependencies:
                install_deps_commands += [
                    f'echo "Installing conda dependencies..."',
                    f'{self.settings_manager.conda_bin} add --manifest-path "{environment.path}" {" ".join(conda_dependencies)}',
                ]
            if len(condaDependenciesNoDeps) > 0:
                raise Exception(f"Use micromamba to be able to install conda packages without their dependencies.")
            if len(pipDependenciesNoDeps) > 0:
                install_deps_commands += [
                    f'echo "Installing pip dependencies without their dependencies..."',
                    f"pip install {proxy_args} --no-deps {' '.join(pipDependenciesNoDeps)}",
                ]
            return install_deps_commands

        if len(conda_dependencies) > 0:
            install_deps_commands += [
                f'echo "Installing conda dependencies..."',
                f"{self.settings_manager.conda_bin_config} install {' '.join(conda_dependencies)} -y",
            ]
        if len(condaDependenciesNoDeps) > 0:
            install_deps_commands += [
                f'echo "Installing conda dependencies without their dependencies..."',
                f"{self.settings_manager.conda_bin_config} install --no-deps {' '.join(condaDependenciesNoDeps)} -y",
            ]

        if len(pipDependencies) > 0:
            install_deps_commands += [
                f'echo "Installing pip dependencies..."',
                f"pip install {proxy_args} {' '.join(pipDependencies)}",
            ]
        if len(pipDependenciesNoDeps) > 0:
            install_deps_commands += [
                f'echo "Installing pip dependencies without their dependencies..."',
                f"pip install {proxy_args} --no-deps {' '.join(pipDependenciesNoDeps)}",
            ]
        return install_deps_commands
