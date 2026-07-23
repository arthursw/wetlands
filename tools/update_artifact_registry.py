#!/usr/bin/env python3
"""Validate upstream artifacts and generate Wetlands' embedded checksum registry.

The generated hashes are intentionally embedded in Python for freezer and zip-import
compatibility. Expected hashes are trusted only after this command has validated exact
release tags, downloaded every allowlisted artifact and sidecar, and the
generated source change has been reviewed and committed. Fetching a checksum beside a
binary at application runtime would not provide the same pinning property.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "src" / "wetlands" / "_internal" / "artifact_registry.py"

PIXI_REPOSITORY = "prefix-dev/pixi"
MICROMAMBA_REPOSITORY = "mamba-org/micromamba-releases"
GITHUB_DOWNLOAD_HOSTS = {
    "github.com",
    "release-assets.githubusercontent.com",
    "objects.githubusercontent.com",
}
GITHUB_API_HOSTS = {"api.github.com"}
MICROSOFT_DOWNLOAD_HOSTS = {"download.visualstudio.microsoft.com"}

PIXI_ARTIFACTS = (
    "pixi-aarch64-apple-darwin.tar.gz",
    "pixi-aarch64-pc-windows-msvc.zip",
    "pixi-aarch64-unknown-linux-musl.tar.gz",
    "pixi-x86_64-apple-darwin.tar.gz",
    "pixi-x86_64-pc-windows-msvc.zip",
    "pixi-x86_64-unknown-linux-musl.tar.gz",
)
MICROMAMBA_ARTIFACTS = (
    "micromamba-linux-64",
    "micromamba-linux-aarch64",
    "micromamba-linux-ppc64le",
    "micromamba-osx-64",
    "micromamba-osx-arm64",
    "micromamba-win-64",
)

PIXI_VERSION_RE = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
MICROMAMBA_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
CHECKSUM_LINE_RE = re.compile(r"^([0-9a-fA-F]{64})(?:[ \t]+(?:\*|[ \t])([^ \t].*))?$")


class RegistryUpdateError(RuntimeError):
    """A safe, user-facing registry update failure."""


class _RestrictedRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, allowed_hosts: Set[str]) -> None:
        super().__init__()
        self.allowed_hosts = allowed_hosts

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        validate_url(newurl, self.allowed_hosts)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Downloader:
    """HTTPS downloader that rejects redirects outside an explicit host allowlist."""

    def _open(self, url: str, allowed_hosts: Set[str]):  # type: ignore[no-untyped-def]
        validate_url(url, allowed_hosts)
        opener = urllib.request.build_opener(_RestrictedRedirectHandler(allowed_hosts))
        request = urllib.request.Request(url, headers={"User-Agent": "wetlands-artifact-registry-updater"})
        try:
            response = opener.open(request, timeout=120)
        except (OSError, urllib.error.URLError) as e:
            raise RegistryUpdateError(f"Failed to download {url}: {e}") from e
        validate_url(response.geturl(), allowed_hosts)
        return response

    def read_bytes(self, url: str, allowed_hosts: Set[str], limit: int = 1024 * 1024) -> bytes:
        with self._open(url, allowed_hosts) as response:
            content = response.read(limit + 1)
        if len(content) > limit:
            raise RegistryUpdateError(f"Response from {url} exceeded the {limit}-byte safety limit.")
        return content

    def download(
        self,
        url: str,
        destination: Path,
        allowed_hosts: Set[str],
        expected_filename: str,
    ) -> str:
        digest = hashlib.sha256()
        with self._open(url, allowed_hosts) as response, open(destination, "wb") as output:
            content_filename = response.headers.get_filename()
            if content_filename is not None and Path(content_filename).name != expected_filename:
                raise RegistryUpdateError(
                    f"Artifact filename mismatch for {url}: expected {expected_filename}, got {content_filename}."
                )
            while True:
                block = response.read(1024 * 1024)
                if not block:
                    break
                output.write(block)
                digest.update(block)
        return digest.hexdigest()


def validate_url(url: str, allowed_hosts: Set[str]) -> None:
    parsed = urllib.parse.urlsplit(url)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or hostname not in allowed_hosts or parsed.username or parsed.password:
        allowed = ", ".join(sorted(allowed_hosts))
        raise RegistryUpdateError(f"Refusing unexpected URL {url!r}; expected HTTPS on one of: {allowed}.")


def validate_version(tool: str, version: str, pattern: re.Pattern[str]) -> None:
    if not pattern.fullmatch(version):
        raise RegistryUpdateError(
            f"{tool} version {version!r} is not an exact supported release tag; branches and ambiguous "
            "identifiers are not allowed."
        )


def resolve_release_version(
    downloader: Downloader,
    tool: str,
    repository: str,
    requested_version: str,
    pattern: re.Pattern[str],
) -> str:
    """Resolve the developer-only ``latest`` alias to an exact release tag."""
    if requested_version != "latest":
        validate_version(tool, requested_version, pattern)
        return requested_version

    url = f"https://api.github.com/repos/{repository}/releases/latest"
    try:
        metadata = json.loads(downloader.read_bytes(url, GITHUB_API_HOSTS).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RegistryUpdateError(f"Malformed latest-release metadata for {repository}.") from e

    if not isinstance(metadata, dict) or not isinstance(metadata.get("tag_name"), str):
        raise RegistryUpdateError(f"GitHub returned no exact latest release tag for {repository}.")
    if metadata.get("draft") or metadata.get("prerelease"):
        raise RegistryUpdateError(f"GitHub returned a draft or prerelease as the latest release for {repository}.")

    resolved_version = metadata["tag_name"]
    validate_version(tool, resolved_version, pattern)
    print(f"Resolved latest {tool} release to exact tag {resolved_version}.")
    return resolved_version


def parse_checksum(content: bytes, expected_filename: str) -> str:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as e:
        raise RegistryUpdateError(f"Checksum for {expected_filename} is not valid UTF-8.") from e

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RegistryUpdateError(
            f"Checksum for {expected_filename} must contain exactly one non-empty entry; found {len(lines)}."
        )

    match = CHECKSUM_LINE_RE.fullmatch(lines[0])
    if match is None:
        raise RegistryUpdateError(f"Malformed SHA-256 checksum for {expected_filename}: {lines[0]!r}.")

    digest, filename = match.groups()
    if filename is not None and filename != expected_filename:
        raise RegistryUpdateError(f"Checksum filename mismatch: expected {expected_filename}, got {filename}.")
    return digest.lower()


def _github_api_url(repository: str, version: str) -> str:
    quoted_version = urllib.parse.quote(version, safe="")
    return f"https://api.github.com/repos/{repository}/releases/tags/{quoted_version}"


def _release_download_url(repository: str, version: str, asset_name: str) -> str:
    quoted_version = urllib.parse.quote(version, safe="")
    quoted_asset = urllib.parse.quote(asset_name, safe="")
    return f"https://github.com/{repository}/releases/download/{quoted_version}/{quoted_asset}"


def load_release_metadata(
    downloader: Downloader,
    repository: str,
    version: str,
    expected_assets: Iterable[str],
) -> Tuple[Mapping[str, Mapping[str, object]], bool]:
    url = _github_api_url(repository, version)
    try:
        metadata = json.loads(downloader.read_bytes(url, GITHUB_API_HOSTS).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RegistryUpdateError(f"Malformed GitHub release metadata for {repository} {version}.") from e

    if not isinstance(metadata, dict) or metadata.get("tag_name") != version:
        actual_tag = metadata.get("tag_name") if isinstance(metadata, dict) else None
        raise RegistryUpdateError(
            f"GitHub returned a mismatched release tag for {repository}: expected {version}, got {actual_tag!r}."
        )
    if metadata.get("draft"):
        raise RegistryUpdateError(f"Refusing draft release {repository} {version}.")

    assets = metadata.get("assets")
    if not isinstance(assets, list):
        raise RegistryUpdateError(f"GitHub release {repository} {version} has malformed asset metadata.")

    by_name: Dict[str, Mapping[str, object]] = {}
    duplicate_names: Set[str] = set()
    for asset in assets:
        if not isinstance(asset, dict) or not isinstance(asset.get("name"), str):
            raise RegistryUpdateError(f"GitHub release {repository} {version} has a malformed asset entry.")
        name = asset["name"]
        if name in by_name:
            duplicate_names.add(name)
        by_name[name] = asset
    if duplicate_names:
        raise RegistryUpdateError(
            f"GitHub release {repository} {version} has duplicate assets: {', '.join(sorted(duplicate_names))}."
        )

    missing = sorted(set(expected_assets) - set(by_name))
    if missing:
        raise RegistryUpdateError(
            f"GitHub release {repository} {version} is incomplete; missing assets: {', '.join(missing)}."
        )
    return by_name, bool(metadata.get("immutable", False))


def _gh_supports_release_verification() -> bool:
    gh = shutil.which("gh")
    if gh is None:
        return False
    result = subprocess.run(
        [gh, "release", "verify-asset", "--help"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def verify_immutable_release_asset(repository: str, version: str, artifact_path: Path) -> None:
    gh = shutil.which("gh")
    if gh is None or not _gh_supports_release_verification():
        print(
            f"GitHub marks {repository} {version} immutable, but this gh version cannot verify release assets; "
            "continuing with the upstream sidecar and release digest.",
            file=sys.stderr,
        )
        return
    result = subprocess.run(
        [gh, "release", "verify-asset", version, str(artifact_path), "--repo", repository],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown gh error"
        raise RegistryUpdateError(
            f"GitHub immutable-release verification failed for {repository} {version} "
            f"asset {artifact_path.name}: {detail}"
        )


def _validate_release_digest(asset_metadata: Mapping[str, object], artifact_name: str, digest: str) -> None:
    release_digest = asset_metadata.get("digest")
    if release_digest is None:
        return
    if not isinstance(release_digest, str) or not release_digest.startswith("sha256:"):
        raise RegistryUpdateError(f"Malformed GitHub release digest for {artifact_name}: {release_digest!r}.")
    if release_digest.removeprefix("sha256:").lower() != digest:
        raise RegistryUpdateError(
            f"GitHub release digest mismatch for {artifact_name}: "
            f"expected {release_digest.removeprefix('sha256:').lower()}, calculated {digest}."
        )


def fetch_github_hashes(
    downloader: Downloader,
    repository: str,
    version: str,
    artifact_names: Sequence[str],
) -> Dict[str, str]:
    sidecar_names = tuple(f"{name}.sha256" for name in artifact_names)
    expected_assets = tuple(artifact_names) + sidecar_names
    release_assets, immutable = load_release_metadata(downloader, repository, version, expected_assets)
    hashes: Dict[str, str] = {}

    with tempfile.TemporaryDirectory(prefix="wetlands-artifacts-") as temp_dir:
        temp_path = Path(temp_dir)
        for artifact_name in artifact_names:
            artifact_url = _release_download_url(repository, version, artifact_name)
            sidecar_url = _release_download_url(repository, version, f"{artifact_name}.sha256")
            try:
                expected_digest = parse_checksum(
                    downloader.read_bytes(sidecar_url, GITHUB_DOWNLOAD_HOSTS),
                    artifact_name,
                )
                artifact_path = temp_path / artifact_name
                calculated_digest = downloader.download(
                    artifact_url,
                    artifact_path,
                    GITHUB_DOWNLOAD_HOSTS,
                    artifact_name,
                )
            except RegistryUpdateError as e:
                raise RegistryUpdateError(
                    f"Failed to verify {repository} {version} artifact {artifact_name}: {e}"
                ) from e

            if calculated_digest != expected_digest:
                raise RegistryUpdateError(
                    f"SHA-256 mismatch for {repository} {version} artifact {artifact_name}: "
                    f"sidecar {expected_digest}, calculated {calculated_digest}."
                )
            _validate_release_digest(release_assets[artifact_name], artifact_name, calculated_digest)
            if immutable:
                verify_immutable_release_asset(repository, version, artifact_path)
            hashes[artifact_name] = calculated_digest

    if set(hashes) != set(artifact_names):
        missing = sorted(set(artifact_names) - set(hashes))
        raise RegistryUpdateError(
            f"Refusing partial mapping for {repository} {version}; missing: {', '.join(missing)}."
        )
    return hashes


def fetch_vc_redist_hash(downloader: Downloader, url: str, artifact_name: str) -> str:
    validate_url(url, MICROSOFT_DOWNLOAD_HOSTS)
    parsed = urllib.parse.urlsplit(url)
    if Path(parsed.path).name != artifact_name:
        raise RegistryUpdateError(
            f"Visual C++ Redistributable URL must end with {artifact_name}; got {Path(parsed.path).name!r}."
        )

    with tempfile.TemporaryDirectory(prefix="wetlands-vc-redist-") as temp_dir:
        artifact_path = Path(temp_dir) / artifact_name
        digest = downloader.download(url, artifact_path, MICROSOFT_DOWNLOAD_HOSTS, artifact_name)

    digest_components = [part.lower() for part in parsed.path.split("/") if SHA256_RE.fullmatch(part)]
    if digest_components and any(component != digest for component in digest_components):
        raise RegistryUpdateError(
            f"Visual C++ Redistributable URL digest does not match {artifact_name}: "
            f"URL contains {', '.join(digest_components)}, calculated {digest}."
        )
    return digest


def load_current_vc_redist(registry_path: Path) -> Tuple[str, str]:
    if not registry_path.is_file():
        raise RegistryUpdateError(f"{registry_path} does not exist; pass --vc-redist-url when creating the registry.")
    spec = importlib.util.spec_from_file_location("_wetlands_artifact_registry", registry_path)
    if spec is None or spec.loader is None:
        raise RegistryUpdateError(f"Could not load current registry from {registry_path}.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        return module.VC_REDIST_ARTIFACT_NAME, module.VC_REDIST_URL
    except AttributeError as e:
        raise RegistryUpdateError(f"Current registry {registry_path} has no VC Redistributable entry.") from e


def render_registry(
    pixi_version: str,
    pixi_hashes: Mapping[str, str],
    micromamba_version: str,
    micromamba_hashes: Mapping[str, str],
    vc_redist_artifact_name: str,
    vc_redist_url: str,
    vc_redist_hash: str,
) -> str:
    lines: List[str] = [
        "# Generated by tools/update_artifact_registry.py.",
        "# Do not edit manually.",
        "#",
        "# Embedded hashes keep artifact verification compatible with freezers and zip imports.",
        "# They become trusted only after this generated file is reviewed and committed.",
        "",
        f"# Source: https://github.com/{PIXI_REPOSITORY}/releases/tag/{pixi_version}",
        f'PIXI_VERSION = "{pixi_version}"',
        "PIXI_SHA256 = {",
    ]
    lines.extend(f'    "{name}": "{pixi_hashes[name]}",' for name in sorted(pixi_hashes))
    lines.extend(
        [
            "}",
            "",
            f"# Source: https://github.com/{MICROMAMBA_REPOSITORY}/releases/tag/{micromamba_version}",
            f'MICROMAMBA_VERSION = "{micromamba_version}"',
            "MICROMAMBA_SHA256 = {",
        ]
    )
    lines.extend(f'    "{name}": "{micromamba_hashes[name]}",' for name in sorted(micromamba_hashes))
    lines.extend(
        [
            "}",
            "",
            "# Source: Microsoft Visual C++ Redistributable for Visual Studio 2015-2022.",
            f'VC_REDIST_ARTIFACT_NAME = "{vc_redist_artifact_name}"',
            f'VC_REDIST_URL = "{vc_redist_url}"',
            f'VC_REDIST_SHA256 = "{vc_redist_hash}"',
            "",
        ]
    )
    return "\n".join(lines)


def generate_registry(
    pixi_version: str,
    micromamba_version: str,
    vc_redist_url: Optional[str],
    registry_path: Path = REGISTRY_PATH,
    downloader: Optional[Downloader] = None,
) -> str:
    if downloader is None:
        downloader = Downloader()

    resolved_pixi_version = resolve_release_version(
        downloader,
        "Pixi",
        PIXI_REPOSITORY,
        pixi_version,
        PIXI_VERSION_RE,
    )
    resolved_micromamba_version = resolve_release_version(
        downloader,
        "Micromamba",
        MICROMAMBA_REPOSITORY,
        micromamba_version,
        MICROMAMBA_VERSION_RE,
    )
    current_artifact_name, current_vc_url = load_current_vc_redist(registry_path)
    selected_vc_url = vc_redist_url or current_vc_url

    pixi_hashes = fetch_github_hashes(
        downloader,
        PIXI_REPOSITORY,
        resolved_pixi_version,
        PIXI_ARTIFACTS,
    )
    micromamba_hashes = fetch_github_hashes(
        downloader,
        MICROMAMBA_REPOSITORY,
        resolved_micromamba_version,
        MICROMAMBA_ARTIFACTS,
    )
    vc_redist_hash = fetch_vc_redist_hash(downloader, selected_vc_url, current_artifact_name)
    return render_registry(
        resolved_pixi_version,
        pixi_hashes,
        resolved_micromamba_version,
        micromamba_hashes,
        current_artifact_name,
        selected_vc_url,
        vc_redist_hash,
    )


def write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Resolve and use the latest stable Pixi and Micromamba releases.",
    )
    parser.add_argument(
        "--pixi-version",
        help="Exact prefix-dev/pixi release tag, including v, or latest.",
    )
    parser.add_argument(
        "--micromamba-version",
        help="Exact mamba-org/micromamba-releases tag, or latest.",
    )
    parser.add_argument(
        "--vc-redist-url",
        help="Deliberately replace the registered Microsoft VC Redistributable URL.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate upstream artifacts and fail if the generated registry would differ.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if args.latest:
        if args.pixi_version is not None or args.micromamba_version is not None:
            parser.error("--latest cannot be combined with --pixi-version or --micromamba-version.")
        pixi_version = "latest"
        micromamba_version = "latest"
    else:
        if args.pixi_version is None or args.micromamba_version is None:
            parser.error("provide both --pixi-version and --micromamba-version, or use --latest.")
        pixi_version = args.pixi_version
        micromamba_version = args.micromamba_version

    try:
        generated = generate_registry(
            pixi_version,
            micromamba_version,
            args.vc_redist_url,
        )
        current = REGISTRY_PATH.read_text(encoding="utf-8") if REGISTRY_PATH.exists() else ""
        if args.check:
            if generated != current:
                print(
                    f"{REGISTRY_PATH} is stale; run the updater without --check and review the result.",
                    file=sys.stderr,
                )
                return 1
            print(f"{REGISTRY_PATH} is current and all upstream artifacts were revalidated.")
            return 0
        if generated == current:
            print(f"{REGISTRY_PATH} is already current.")
            return 0
        write_atomic(REGISTRY_PATH, generated)
        print(f"Updated {REGISTRY_PATH}. Review and commit the version and complete checksum sets together.")
        return 0
    except (OSError, RegistryUpdateError) as e:
        print(f"Artifact registry update failed: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
