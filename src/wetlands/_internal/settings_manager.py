from pathlib import Path
import platform


class SettingsManager:
    use_pixi = True
    conda_bin = "pixi"  # "micromamba"
    conda_bin_config = "pixi --manifest-path .pixi/project.toml"  # "micromamba --rc-file ~/.mambarc"
    proxies: dict[str, str] | None = None

    def __init__(self, conda_path: str | Path = Path("pixi"), use_pixi=True) -> None:
        self.set_conda_path(conda_path, use_pixi)

    def set_conda_path(self, conda_path: str | Path, use_pixi=True) -> None:
        """Updates the micromamba path and loads proxy settings if exists.

        Args:
                conda_path: New path to micromamba binary.

        Side Effects:
                Updates conda_bin_config and proxies from the .mambarc file.
        """
        self.use_pixi = use_pixi
        self.conda_bin = (
            "pixi.exe" if platform.system() == "Windows" and use_pixi else "pixi" if use_pixi else "micromamba"
        )
        self.conda_path = Path(conda_path).resolve()
        # conda_bin_config is only used with micromamba but let's initialize it for pixi as well
        conda_config_path = self.conda_path / "pixi.toml" if self.use_pixi else self.conda_path / ".mambarc"
        self.conda_bin_config = (
            f'{self.conda_bin} --manifest-path "{conda_config_path}"'
            if self.use_pixi
            else f'{self.conda_bin} --rc-file "{conda_config_path}"'
        )

        if self.use_pixi:
            return
        import yaml

        if conda_config_path.exists():
            with open(conda_config_path, "r") as f:
                conda_config = yaml.safe_load(f)
                if conda_config is not None and "proxies" in conda_config:
                    self.proxies = conda_config["proxies"]

    def set_proxies(self, proxies: dict[str, str]) -> None:
        """Configures proxy settings for Conda operations (see [Using Anaconda behind a company proxy](https://www.anaconda.com/docs/tools/working-with-conda/reference/proxy)).

        Args:
                proxies: Proxy configuration dictionary (e.g., {'http': 'http://username:password@corp.com:8080', 'https': 'https://username:password@corp.com:8080'}).

        Side Effects:
                Updates .mambarc configuration file with proxy settings.
        """
        self.proxies = proxies
        if self.use_pixi:
            return
        conda_config_path = self.conda_path / ".mambarc"
        conda_config = dict()
        import yaml

        if conda_config_path.exists():
            with open(conda_config_path, "r") as f:
                conda_config = yaml.safe_load(f)
            if proxies:
                conda_config["proxy_servers"] = proxies
            else:
                del conda_config["proxy_servers"]
            with open(conda_config_path, "w") as f:
                yaml.safe_dump(conda_config, f)

    def get_conda_paths(self) -> tuple[Path, Path]:
        """Gets micromamba root path and binary path.

        Returns:
                Tuple of (conda directory path, binary relative path).
        """
        conda_name = "pixi" if self.use_pixi else "micromamba"
        suffix = ".exe" if platform.system() == "Windows" else ""
        conda_bin_path = f"bin/{conda_name}{suffix}"
        return self.conda_path.resolve(), Path(conda_bin_path)

    def get_environment_path_from_name(self, environment_name: str) -> Path:
        return (
            self.conda_path / "workspaces" / environment_name / "pixi.toml"
            if self.use_pixi
            else self.conda_path / "envs" / environment_name
        )

    def get_proxy_environment_variables_commands(self) -> list[str]:
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

    def get_proxy_string(self) -> str | None:
        """Gets active proxy string from configuration (HTTPS preferred, fallback to HTTP)."""
        if self.proxies is None:
            return None
        return self.proxies.get("https", self.proxies.get("http", None))
