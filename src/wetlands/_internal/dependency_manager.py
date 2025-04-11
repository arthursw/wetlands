import platform

try:
    from typing import NotRequired, TypedDict, Literal              # type: ignore
except ImportError:
    from typing_extensions import NotRequired, TypedDict, Literal   # type: ignore

from wetlands._internal.exceptions import IncompatibilityException
from wetlands._internal.settings_manager import SettingsManager

Platform = Literal["osx-64", "osx-arm64", "win-64", "win-arm64", "linux-64", "linux-arm64"]


class Dependency(TypedDict):
    name: str
    platforms: NotRequired[list[Platform]]
    optional: NotRequired[bool]
    dependencies: NotRequired[bool]


class Dependencies(TypedDict):
    python: NotRequired[str]
    conda: NotRequired[list[str | Dependency]]
    pip: NotRequired[list[str | Dependency]]


class DependencyManager:
    """Manage pip and conda dependencies."""

    def __init__(self, settingsManager: SettingsManager):
        self.installedPackages: dict[str, dict[str, str]] = {}
        self.settingsManager = settingsManager

    def _platformCondaFormat(self) -> str:
        """Get conda-compatible platform string (e.g., 'linux-64', 'osx-arm64', 'win-64')."""
        machine = platform.machine()
        machine = "64" if machine == "x86_64" or machine == "AMD64" else machine
        system = dict(Darwin="osx", Windows="win", Linux="linux")[platform.system()]
        return f"{system}-{machine}"

    def formatDependencies(
        self,
        package_manager: str,
        dependencies: Dependencies,
        raiseIncompatibilityError: bool = True,
        quotes: bool = True,
    ) -> tuple[list[str], list[str], bool]:
        """Formats dependencies for installation with platform checks.

        Args:
                package_manager: 'conda' or 'pip'.
                dependencies: Dependencies to process.
                raiseIncompatibilityError: Whether to raise on incompatible platforms.
                quotes: Whether to put dependencies in quotes (required when installing extras on mac, e.g. `pip install "napari[pyqt5]"`)

        Returns:
                Tuple of (dependencies, no-deps dependencies, has_dependencies).

        Raises:
                IncompatibilityException: For non-optional incompatible dependencies.
        """
        dependencyList: list[str | Dependency] = dependencies.get(package_manager, [])  # type: ignore
        finalDependencies: list[str] = []
        finalDependenciesNoDeps: list[str] = []
        for dependency in dependencyList:
            if isinstance(dependency, str):
                finalDependencies.append(dependency)
            else:
                currentPlatform = self._platformCondaFormat()
                platforms = dependency.get("platforms", "all")
                if (
                    currentPlatform in platforms
                    or platforms == "all"
                    or len(platforms) == 0
                    or not raiseIncompatibilityError
                ):
                    if "dependencies" not in dependency or dependency["dependencies"]:
                        finalDependencies.append(dependency["name"])
                    else:
                        finalDependenciesNoDeps.append(dependency["name"])
                elif not dependency.get("optional", False):
                    platformsString = ", ".join(platforms)
                    raise IncompatibilityException(
                        f"Error: the library {dependency['name']} is not available on this platform ({currentPlatform}). It is only available on the following platforms: {platformsString}."
                    )
        if quotes:
            finalDependencies = [f'"{d}"' for d in finalDependencies]
            finalDependenciesNoDeps = [f'"{d}"' for d in finalDependenciesNoDeps]
        return (
            finalDependencies,
            finalDependenciesNoDeps,
            len(finalDependencies) + len(finalDependenciesNoDeps) > 0,
        )

    def getInstallDependenciesCommands(self, environment: str | None, dependencies: Dependencies) -> list[str]:
        """Generates commands to install dependencies in the given environment. Note: this does not activate conda, use self.getActivateCondaCommands() first.

        Args:
                environment: Target environment name. If none, no conda environment will be activated, only pip dependencies will be installed in the current python environemnt ; conda dependencies will be ignored.
                dependencies: Dependencies to install.

        Returns:
                list of installation commands.

        Raises:
                Exception: If pip dependencies contain Conda channel syntax.
        """
        condaDependencies, condaDependenciesNoDeps, hasCondaDependencies = self.formatDependencies(
            "conda", dependencies
        )
        pipDependencies, pipDependenciesNoDeps, hasPipDependencies = self.formatDependencies("pip", dependencies)

        if hasCondaDependencies and not environment:
            raise Exception(
                "Conda dependencies can only be installed in a Conda environment. Please provide an existing conda environment to install dependencies."
            )
        if any("::" in d for d in pipDependencies + pipDependenciesNoDeps):
            raise Exception(
                f'One pip dependency has a channel specifier "::". Is it a conda dependency?\n\n({dependencies.get("pip")})'
            )
        installDepsCommands = self.settingsManager.getProxyEnvironmentVariablesCommands()
        installDepsCommands += (
            [
                f'echo "Activating environment {environment}..."',
                f"{self.settingsManager.condaBin} activate {environment}",
            ]
            if environment and (hasCondaDependencies or hasPipDependencies)
            else []
        )
        installDepsCommands += (
            [
                f'echo "Installing conda dependencies..."',
                f"{self.settingsManager.condaBinConfig} install {' '.join(condaDependencies)} -y",
            ]
            if len(condaDependencies) > 0
            else []
        )
        installDepsCommands += (
            [
                f'echo "Installing conda dependencies without their dependencies..."',
                f"{self.settingsManager.condaBinConfig} install --no-deps {' '.join(condaDependenciesNoDeps)} -y",
            ]
            if len(condaDependenciesNoDeps) > 0
            else []
        )
        proxyString = self.settingsManager.getProxyString()
        proxyArgs = f"--proxy {proxyString}" if proxyString is not None else ""
        installDepsCommands += (
            [
                f'echo "Installing pip dependencies..."',
                f"pip install {proxyArgs} {' '.join(pipDependencies)}",
            ]
            if len(pipDependencies) > 0
            else []
        )
        installDepsCommands += (
            [
                f'echo "Installing pip dependencies without their dependencies..."',
                f"pip install {proxyArgs} --no-deps {' '.join(pipDependenciesNoDeps)}",
            ]
            if len(pipDependenciesNoDeps) > 0
            else []
        )
        return installDepsCommands
