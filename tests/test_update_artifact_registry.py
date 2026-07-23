from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

UPDATER_PATH = Path(__file__).parents[1] / "tools" / "update_artifact_registry.py"
UPDATER_SPEC = importlib.util.spec_from_file_location("update_artifact_registry", UPDATER_PATH)
assert UPDATER_SPEC is not None and UPDATER_SPEC.loader is not None
updater = importlib.util.module_from_spec(UPDATER_SPEC)
UPDATER_SPEC.loader.exec_module(updater)


class FakeDownloader:
    def __init__(self, pixi_artifacts=None, micromamba_artifacts=None):
        self.artifact_content = {}
        for repository, version, names in (
            (
                updater.PIXI_REPOSITORY,
                "v0.48.2",
                pixi_artifacts if pixi_artifacts is not None else updater.PIXI_ARTIFACTS,
            ),
            (
                updater.MICROMAMBA_REPOSITORY,
                "2.3.0-1",
                micromamba_artifacts if micromamba_artifacts is not None else updater.MICROMAMBA_ARTIFACTS,
            ),
        ):
            for name in names:
                self.artifact_content[(repository, version, name)] = f"content:{name}".encode()
        self.vc_content = b"vc-redist"

    def read_bytes(self, url, allowed_hosts, limit=1024 * 1024):
        parsed = updater.urllib.parse.urlsplit(url)
        if parsed.hostname == "api.github.com":
            parts = parsed.path.split("/")
            repository = "/".join(parts[2:4])
            if parsed.path.endswith("/releases/latest"):
                versions = {
                    updater.PIXI_REPOSITORY: "v0.48.2",
                    updater.MICROMAMBA_REPOSITORY: "2.3.0-1",
                }
                return json.dumps(
                    {
                        "tag_name": versions[repository],
                        "draft": False,
                        "prerelease": False,
                    }
                ).encode()
            version = updater.urllib.parse.unquote(parts[-1])
            assets = [
                {"name": name}
                for repo, release, name in self.artifact_content
                if repo == repository and release == version
                for name in (name, f"{name}.sha256")
            ]
            return json.dumps({"tag_name": version, "draft": False, "immutable": False, "assets": assets}).encode()

        repository, version, asset_name = self._github_parts(url)
        if not asset_name.endswith(".sha256"):
            raise AssertionError(f"Unexpected read_bytes URL: {url}")
        artifact_name = asset_name.removesuffix(".sha256")
        content = self.artifact_content[(repository, version, artifact_name)]
        digest = hashlib.sha256(content).hexdigest()
        return f"{digest} *{artifact_name}\n".encode()

    def download(self, url, destination, allowed_hosts, expected_filename):
        parsed = updater.urllib.parse.urlsplit(url)
        if parsed.hostname == "download.visualstudio.microsoft.com":
            content = self.vc_content
        else:
            repository, version, artifact_name = self._github_parts(url)
            assert artifact_name == expected_filename
            content = self.artifact_content[(repository, version, artifact_name)]
        destination.write_bytes(content)
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def _github_parts(url):
        parts = updater.urllib.parse.urlsplit(url).path.split("/")
        repository = "/".join(parts[1:3])
        version = updater.urllib.parse.unquote(parts[5])
        asset_name = updater.urllib.parse.unquote(parts[6])
        return repository, version, asset_name


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"a" * 64, "a" * 64),
        (f"{'B' * 64} *artifact.bin\n".encode(), "b" * 64),
        (f"{'C' * 64}  artifact.bin\n".encode(), "c" * 64),
    ],
)
def test_checksum_parser_accepts_supported_formats(content, expected):
    assert updater.parse_checksum(content, "artifact.bin") == expected


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"not-a-hash", "Malformed"),
        (f"{'a' * 63}\n".encode(), "Malformed"),
        (f"{'a' * 64} artifact.bin\n".encode(), "Malformed"),
        (f"{'a' * 64} *other.bin\n".encode(), "filename mismatch"),
        (f"{'a' * 64}\n{'b' * 64}\n".encode(), "exactly one"),
    ],
)
def test_checksum_parser_rejects_malformed_or_mismatched_entries(content, message):
    with pytest.raises(updater.RegistryUpdateError, match=message):
        updater.parse_checksum(content, "artifact.bin")


def test_incomplete_release_is_rejected():
    downloader = FakeDownloader(pixi_artifacts=updater.PIXI_ARTIFACTS[:-1])

    with pytest.raises(updater.RegistryUpdateError, match="incomplete"):
        updater.fetch_github_hashes(
            downloader,
            updater.PIXI_REPOSITORY,
            "v0.48.2",
            updater.PIXI_ARTIFACTS,
        )


@pytest.mark.parametrize(
    ("tool", "repository", "pattern", "expected"),
    [
        ("Pixi", updater.PIXI_REPOSITORY, updater.PIXI_VERSION_RE, "v0.48.2"),
        (
            "Micromamba",
            updater.MICROMAMBA_REPOSITORY,
            updater.MICROMAMBA_VERSION_RE,
            "2.3.0-1",
        ),
    ],
)
def test_latest_resolves_to_an_exact_stable_release(tool, repository, pattern, expected):
    resolved = updater.resolve_release_version(
        FakeDownloader(),
        tool,
        repository,
        "latest",
        pattern,
    )

    assert resolved == expected
    assert resolved != "latest"


def test_latest_resolution_rejects_ambiguous_returned_tag():
    class AmbiguousLatestDownloader(FakeDownloader):
        def read_bytes(self, url, allowed_hosts, limit=1024 * 1024):
            return json.dumps(
                {
                    "tag_name": "main",
                    "draft": False,
                    "prerelease": False,
                }
            ).encode()

    with pytest.raises(updater.RegistryUpdateError, match="not an exact supported release tag"):
        updater.resolve_release_version(
            AmbiguousLatestDownloader(),
            "Pixi",
            updater.PIXI_REPOSITORY,
            "latest",
            updater.PIXI_VERSION_RE,
        )


def test_generator_output_is_deterministic_and_sorted():
    pixi_hashes = {"z": "a" * 64, "a": "b" * 64}
    micromamba_hashes = {"y": "c" * 64, "b": "d" * 64}
    arguments = (
        "v1.2.3",
        pixi_hashes,
        "1.2.3-1",
        micromamba_hashes,
        "VC_redist.x64.exe",
        "https://download.visualstudio.microsoft.com/VC_redist.x64.exe",
        "e" * 64,
    )

    first = updater.render_registry(*arguments)
    second = updater.render_registry(*arguments)

    assert first == second
    assert first.index('"a":') < first.index('"z":')
    assert first.index('"b":') < first.index('"y":')
    assert "Generated by tools/update_artifact_registry.py" in first
    assert "timestamp" not in first.lower()


def test_generation_failure_leaves_existing_registry_unchanged(tmp_path):
    registry = tmp_path / "artifact_registry.py"
    original = "VC_REDIST_ARTIFACT_NAME = 'VC_redist.x64.exe'\nVC_REDIST_URL = 'bad-url'\n"
    registry.write_text(original)

    with pytest.raises(updater.RegistryUpdateError):
        updater.generate_registry(
            "v0.48.2",
            "2.3.0-1",
            None,
            registry_path=registry,
            downloader=FakeDownloader(),
        )

    assert registry.read_text() == original


def test_generate_registry_builds_complete_source_in_memory(tmp_path):
    registry = tmp_path / "artifact_registry.py"
    vc_url = "https://download.visualstudio.microsoft.com/VC_redist.x64.exe"
    registry.write_text(f"VC_REDIST_ARTIFACT_NAME = 'VC_redist.x64.exe'\nVC_REDIST_URL = {vc_url!r}\n")

    generated = updater.generate_registry(
        "v0.48.2",
        "2.3.0-1",
        None,
        registry_path=registry,
        downloader=FakeDownloader(),
    )

    assert generated != registry.read_text()
    assert all(name in generated for name in updater.PIXI_ARTIFACTS)
    assert all(name in generated for name in updater.MICROMAMBA_ARTIFACTS)
    assert hashlib.sha256(b"vc-redist").hexdigest() in generated


def test_generate_registry_writes_resolved_versions_not_latest(tmp_path):
    registry = tmp_path / "artifact_registry.py"
    vc_url = "https://download.visualstudio.microsoft.com/VC_redist.x64.exe"
    registry.write_text(f"VC_REDIST_ARTIFACT_NAME = 'VC_redist.x64.exe'\nVC_REDIST_URL = {vc_url!r}\n")

    generated = updater.generate_registry(
        "latest",
        "latest",
        None,
        registry_path=registry,
        downloader=FakeDownloader(),
    )

    assert 'PIXI_VERSION = "v0.48.2"' in generated
    assert 'MICROMAMBA_VERSION = "2.3.0-1"' in generated
    assert 'VERSION = "latest"' not in generated


def test_check_mode_detects_stale_registry(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("stale\n")
    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", lambda *args, **kwargs: "fresh\n")

    result = updater.main(["--pixi-version", "v0.48.2", "--micromamba-version", "2.3.0-1", "--check"])

    assert result == 1
    assert registry.read_text() == "stale\n"


def test_check_mode_accepts_current_registry(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("current\n")
    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", lambda *args, **kwargs: "current\n")

    result = updater.main(["--pixi-version", "v0.48.2", "--micromamba-version", "2.3.0-1", "--check"])

    assert result == 0


def test_latest_flag_requests_latest_for_both_tools(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("current\n")
    requested = {}

    def fake_generate(pixi_version, micromamba_version, vc_redist_url):
        requested["versions"] = (pixi_version, micromamba_version)
        return "current\n"

    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", fake_generate)

    result = updater.main(["--latest", "--check"])

    assert result == 0
    assert requested["versions"] == ("latest", "latest")
