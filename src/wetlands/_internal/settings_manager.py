from pathlib import Path
import platform


class SettingsManager:
    usePixi = True
    condaBin = "pixi"  # "micromamba"
    condaBinConfig = "pixi --manifest-path .pixi/project.toml"  # "micromamba --rc-file ~/.mambarc"
    proxies: dict[str, str] | None = None

    def __init__(self, condaPath: str | Path = Path("pixi"), usePixi=True) -> None:
        self.setCondaPath(condaPath, usePixi)

    def setCondaPath(self, condaPath: str | Path, usePixi=True) -> None:
        """Updates the micromamba path and loads proxy settings if exists.

        Args:
                condaPath: New path to micromamba binary.

        Side Effects:
                Updates condaBinConfig and proxies from the .mambarc file.
        """
        self.usePixi = usePixi
        self.condaBin = (
            "pixi.exe" if platform.system() == "Windows" and usePixi else "pixi" if usePixi else "micromamba"
        )
        self.condaPath = Path(condaPath).resolve()
        # condaBinConfig is only used with micromamba but let's initialize it for pixi as well
        condaConfigPath = self.condaPath / "pixi.toml" if self.usePixi else self.condaPath / ".mambarc"
        self.condaBinConfig = (
            f'{self.condaBin} --manifest-path "{condaConfigPath}"'
            if self.usePixi
            else f'{self.condaBin} --rc-file "{condaConfigPath}"'
        )

        if self.usePixi:
            return
        import yaml

        if condaConfigPath.exists():
            with open(condaConfigPath, "r") as f:
                condaConfig = yaml.safe_load(f)
                if condaConfig is not None and "proxies" in condaConfig:
                    self.proxies = condaConfig["proxies"]

    def setProxies(self, proxies: dict[str, str]) -> None:
        """Configures proxy settings for Conda operations (see [Using Anaconda behind a company proxy](https://www.anaconda.com/docs/tools/working-with-conda/reference/proxy)).

        Args:
                proxies: Proxy configuration dictionary (e.g., {'http': 'http://username:password@corp.com:8080', 'https': 'https://username:password@corp.com:8080'}).

        Side Effects:
                Updates .mambarc configuration file with proxy settings.
        """
        self.proxies = proxies
        if self.usePixi:
            return
        condaConfigPath = self.condaPath / ".mambarc"
        condaConfig = dict()
        import yaml

        if condaConfigPath.exists():
            with open(condaConfigPath, "r") as f:
                condaConfig = yaml.safe_load(f)
            if proxies:
                condaConfig["proxy_servers"] = proxies
            else:
                del condaConfig["proxy_servers"]
            with open(condaConfigPath, "w") as f:
                yaml.safe_dump(condaConfig, f)

    def getCondaPaths(self) -> tuple[Path, Path]:
        """Gets micromamba root path and binary path.

        Returns:
                Tuple of (conda directory path, binary relative path).
        """
        condaName = "pixi" if self.usePixi else "micromamba"
        suffix = ".exe" if platform.system() == "Windows" else ""
        condaBinPath = f"bin/{condaName}{suffix}"
        return self.condaPath.resolve(), Path(condaBinPath)

    def getEnvironmentPathFromName(self, environmentName: str) -> Path:
        return (
            self.condaPath / "workspaces" / environmentName / "pixi.toml"
            if self.usePixi
            else self.condaPath / "envs" / environmentName
        )

    def getProxyEnvironmentVariablesCommands(self) -> list[str]:
        """Generates proxy environment variable commands.

        Returns:
                List of OS-specific proxy export commands.
        """
        if self.proxies is None:
            return []
        return [
            f'export {name.lower()}_proxy="{value}"'
            if platform.system() != "Windows"
            else f'$Env:{name.upper()}_PROXY="{value}"'
            for name, value in self.proxies.items()
        ]

    def getProxyString(self) -> str | None:
        """Gets active proxy string from configuration (HTTPS preferred, fallback to HTTP)."""
        if self.proxies is None:
            return None
        return self.proxies.get("https", self.proxies.get("http", None))
