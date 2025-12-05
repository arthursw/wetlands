from pathlib import Path
import platform
from unittest.mock import patch, mock_open
from wetlands._internal.settings_manager import SettingsManager


def test_initialization():
    sm = SettingsManager()
    assert sm.conda_path == Path("pixi").resolve()
    sm = SettingsManager("micromamba", False)
    assert sm.conda_path == Path("micromamba").resolve()


def test_set_conda_path():
    sm = SettingsManager()
    new_path = Path("/custom/path")
    with patch("pathlib.Path.exists", return_value=False):
        sm.set_conda_path(new_path)
        assert sm.conda_path == new_path.resolve()
        assert str(new_path) in sm.conda_bin_config


def test_set_conda_path_with_proxies():
    mock_config = """proxies:
      http: http://proxy.com:8080
      https: https://secure-proxy.com:8443"""
    new_path = Path("/custom/path")
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("builtins.open", mock_open(read_data=mock_config)),
    ):
        sm = SettingsManager(new_path, False)
        assert sm.proxies == {
            "http": "http://proxy.com:8080",
            "https": "https://secure-proxy.com:8443",
        }


def test_set_proxies():
    sm = SettingsManager("micromamba", False)
    proxies = {
        "http": "http://proxy.com:8080",
        "https": "https://secure-proxy.com:8443",
    }
    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("builtins.open", mock_open(read_data="{}")) as m_open,
    ):
        sm.set_proxies(proxies)
        m_open.assert_called_with(sm.conda_path / ".mambarc", "w")


def test_get_conda_paths():
    sm = SettingsManager("/some/path", False)
    root, bin_path = sm.get_conda_paths()
    assert root == Path("/some/path").resolve()
    expected_bin = "bin/micromamba" if platform.system() != "Windows" else "bin/micromamba.exe"
    assert bin_path == Path(expected_bin)

    sm = SettingsManager("/some/path")
    root, bin_path = sm.get_conda_paths()
    assert root == Path("/some/path").resolve()
    expected_bin = "bin/pixi" if platform.system() != "Windows" else "bin/pixi.exe"
    assert bin_path == Path(expected_bin)


def test_get_proxy_environment_variables_commands():
    sm = SettingsManager()
    sm.proxies = {
        "http": "http://proxy.com:8080",
        "https": "https://secure-proxy.com:8443",
    }
    expected_cmds = [
        'export http_proxy="http://proxy.com:8080"'
        if platform.system() != "Windows"
        else '$Env:HTTP_PROXY="http://proxy.com:8080"',
        'export https_proxy="https://secure-proxy.com:8443"'
        if platform.system() != "Windows"
        else '$Env:HTTPS_PROXY="https://secure-proxy.com:8443"',
    ]
    assert sm.get_proxy_environment_variables_commands() == expected_cmds


def test_get_proxy_string():
    sm = SettingsManager()
    sm.proxies = {
        "http": "http://proxy.com:8080",
        "https": "https://secure-proxy.com:8443",
    }
    assert sm.get_proxy_string() == "https://secure-proxy.com:8443"
    sm.proxies.pop("https")
    assert sm.get_proxy_string() == "http://proxy.com:8080"
    sm.proxies.pop("http")
    assert sm.get_proxy_string() is None
