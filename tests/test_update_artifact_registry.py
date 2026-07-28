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
    def __init__(self, pixi_artifacts=None):
        artifacts = pixi_artifacts if pixi_artifacts is not None else updater.PIXI_ARTIFACTS
        self.artifact_content = {
            (updater.PIXI_REPOSITORY, "v0.48.2", name): f"content:{name}".encode() for name in artifacts
        }

    def read_bytes(self, url, allowed_hosts, limit=1024 * 1024):
        parsed = updater.urllib.parse.urlsplit(url)
        if parsed.hostname == "api.github.com":
            parts = parsed.path.split("/")
            repository = "/".join(parts[2:4])
            if parsed.path.endswith("/releases/latest"):
                return json.dumps({"tag_name": "v0.48.2", "draft": False, "prerelease": False}).encode()
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


def test_latest_resolves_to_an_exact_stable_release():
    resolved = updater.resolve_release_version(
        FakeDownloader(),
        "Pixi",
        updater.PIXI_REPOSITORY,
        "latest",
        updater.PIXI_VERSION_RE,
    )

    assert resolved == "v0.48.2"


def test_latest_resolution_rejects_ambiguous_returned_tag():
    class AmbiguousLatestDownloader(FakeDownloader):
        def read_bytes(self, url, allowed_hosts, limit=1024 * 1024):
            return json.dumps({"tag_name": "main", "draft": False, "prerelease": False}).encode()

    with pytest.raises(updater.RegistryUpdateError, match="not an exact supported release tag"):
        updater.resolve_release_version(
            AmbiguousLatestDownloader(),
            "Pixi",
            updater.PIXI_REPOSITORY,
            "latest",
            updater.PIXI_VERSION_RE,
        )


def test_generator_output_is_deterministic_and_sorted():
    hashes = {"z": "a" * 64, "a": "b" * 64}

    first = updater.render_registry("v1.2.3", hashes)
    second = updater.render_registry("v1.2.3", hashes)

    assert first == second
    assert first.index('"a":') < first.index('"z":')
    assert "Generated by tools/update_artifact_registry.py" in first
    assert "timestamp" not in first.lower()


def test_generate_registry_builds_complete_source_in_memory():
    generated = updater.generate_registry("v0.48.2", downloader=FakeDownloader())

    assert all(name in generated for name in updater.PIXI_ARTIFACTS)


def test_generate_registry_writes_resolved_version_not_latest():
    generated = updater.generate_registry("latest", downloader=FakeDownloader())

    assert 'PIXI_VERSION = "v0.48.2"' in generated
    assert 'PIXI_VERSION = "latest"' not in generated


def test_check_mode_detects_stale_registry(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("stale\n")
    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", lambda *args, **kwargs: "fresh\n")

    result = updater.main(["--pixi-version", "v0.48.2", "--check"])

    assert result == 1
    assert registry.read_text() == "stale\n"


def test_check_mode_accepts_current_registry(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("current\n")
    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", lambda *args, **kwargs: "current\n")

    result = updater.main(["--pixi-version", "v0.48.2", "--check"])

    assert result == 0


def test_latest_flag_requests_latest(tmp_path, monkeypatch):
    registry = tmp_path / "artifact_registry.py"
    registry.write_text("current\n")
    requested = {}

    def fake_generate(pixi_version):
        requested["version"] = pixi_version
        return "current\n"

    monkeypatch.setattr(updater, "REGISTRY_PATH", registry)
    monkeypatch.setattr(updater, "generate_registry", fake_generate)

    result = updater.main(["--latest", "--check"])

    assert result == 0
    assert requested["version"] == "latest"
