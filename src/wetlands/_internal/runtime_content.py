"""One stdlib-only capture kernel, also executed by the managed interpreter."""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.metadata
import importlib.util
import json
import io
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shlex
import stat
import sys
import sysconfig
from typing import Any
import urllib.parse
import urllib.request
import zipfile


def _admitted_python(text: str) -> bool:
    return text in {sys.executable, str(Path(sys.executable).resolve())}


def _python_script(content: bytes) -> bytes:
    first, separator, rest = content.partition(b"\n")
    if separator and first.startswith(b"#!") and _admitted_python(first[2:].decode("utf-8", errors="replace")):
        return b"#!<admitted-python>\n" + rest
    lines = content.split(b"\n", 3)
    if len(lines) == 4 and lines[0] == b"#!/bin/sh" and lines[2] == b"' '''":
        match = re.fullmatch(r"'''exec' (.+) \"\$0\" \"\$@\"", lines[1].decode("utf-8", errors="replace"))
        if match:
            arguments = shlex.split(match.group(1))
            if len(arguments) == 1 and _admitted_python(arguments[0]):
                return b"#!<admitted-python>\n" + lines[3]
    return content


def _windows_entrypoint(content: bytes) -> bytes:
    """Normalize only a verified executable+Python-shebang+entrypoint ZIP."""
    if not content.startswith(b"MZ"):
        return content
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.infolist()
            if len(members) != 1 or members[0].filename != "__main__.py":
                return content
            start = members[0].header_offset
            archive.read(members[0])
        prefix, payload = content[:start], content[start:]
        marker = prefix.rfind(b"#!")
        if marker < 0 or not prefix.endswith(b"\n"):
            return content
        executable = prefix[marker + 2 : -1].decode("utf-8")
        if executable.startswith('"') and executable.endswith('"'):
            executable = executable[1:-1]
        if _admitted_python(executable):
            return prefix[:marker] + b"#!<admitted-python>\n" + payload
    except (ValueError, UnicodeError, zipfile.BadZipFile):
        pass
    return content


def _name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _file_content(path: Path, *, materialize: bool = False, python_shebang: bool = False) -> tuple[int, bytes]:
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"Runtime content member is not a regular file: {path}")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        digest = hashlib.sha256()
        chunks = []
        delta = 0
        first_chunk = True
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            if first_chunk and python_shebang:
                normalized = _python_script(chunk)
                delta = len(normalized) - len(chunk)
                chunk = normalized
            first_chunk = False
            digest.update(chunk)
            if materialize:
                chunks.append(chunk)
                if stream.tell() > 1024 * 1024:
                    raise ValueError(f"Runtime activation metadata exceeds its bound: {path}")
        after_open = os.fstat(stream.fileno())

    def identity(value: os.stat_result) -> tuple[int, int, int, int]:
        mode = value.st_mode
        if os.name == "nt":
            # Path stat synthesizes execute bits from .exe/.bat/.cmd/.com names;
            # a descriptor has no filename from which to derive those bits.
            mode &= ~0o111
        return value.st_dev, value.st_ino, mode, value.st_size

    def signature(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
        return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns

    # Windows path stat and fstat can expose different ctime semantics.
    # Match physical identity across APIs, then fence each API's full signature.
    after = path.stat()
    if (
        identity(before) != identity(opened)
        or signature(opened) != signature(after_open)
        or signature(after) != signature(before)
    ):
        raise ValueError(
            f"Runtime content changed while being captured: {path}; "
            f"path_before={signature(before)!r}, opened={signature(opened)!r}, "
            f"after_open={signature(after_open)!r}, path_after={signature(after)!r}"
        )
    return opened.st_size + delta, b"".join(chunks) if materialize else digest.digest()


def _members_digest(members: list[tuple[str, int, bytes]]) -> str:
    digest = hashlib.sha256(b"wetlands-runtime-members-v1\0")
    names = [name for name, _, _ in members]
    if len(set(names)) != len(names):
        raise ValueError("Duplicate runtime content member")
    for name, size, content_digest in sorted(members):
        label = name.encode("utf-8")
        digest.update(len(label).to_bytes(8, "big"))
        digest.update(label)
        digest.update(size.to_bytes(8, "big"))
        digest.update(content_digest)
    return digest.hexdigest()


def editable_content_digest(roots: list[str]) -> str:
    """Capture bounded actual import roots; paths themselves are operational."""
    members = []
    for index, text in enumerate(roots):
        root = Path(text)
        if root.is_symlink() or root.resolve(strict=True) != root:
            raise ValueError(f"Editable import root changed its physical authority: {root}")
        if not root.exists():
            raise ValueError(f"Editable import root disappeared: {root}")
        files = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in files:
            if any(
                part in {"__pycache__", ".git"} or part.endswith((".egg-info", ".dist-info"))
                for part in path.relative_to(root.parent if root.is_file() else root).parts
            ):
                continue
            if path.is_symlink():
                raise ValueError(f"Editable import content cannot contain an unowned link: {path}")
            if path.suffix in {".pyc", ".pyo"} or path.is_dir():
                continue
            relative = path.name if root.is_file() else path.relative_to(root).as_posix()
            size, digest = _file_content(path)
            members.append((f"{index}/{relative}", size, digest))
    return _members_digest(members)


def _conda_members(
    distribution: Any, prefix: Path, records: list[dict[str, Any]]
) -> list[importlib.metadata.PackagePath] | None:
    """Admit a complete installed manifest by its exact owned metadata anchor."""
    base = Path(distribution.locate_file("")).resolve(strict=True)
    base.relative_to(prefix)
    owners = []
    for record in records:
        files = record.get("files", [])
        if not isinstance(files, list) or any(
            not isinstance(member, str)
            or not member
            or "\\" in member
            or ":" in member
            or PurePosixPath(member).is_absolute()
            or any(part in {"", ".", ".."} for part in member.split("/"))
            for member in files
        ):
            raise ValueError("Invalid installed Conda member authority")
        anchored = False
        for member in files:
            path = prefix / member
            if (
                not (
                    (path.name == "METADATA" and path.parent.suffix == ".dist-info")
                    or (path.name == "PKG-INFO" and path.parent.suffix == ".egg-info")
                )
                or path.parent.parent.resolve(strict=True) != base
            ):
                continue
            path = path.resolve(strict=True)
            path.relative_to(prefix)
            public_text = distribution.read_text(path.name)
            if (
                public_text is None
                or _file_content(path, materialize=True)[1].decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
                != public_text
            ):
                continue
            metadata = importlib.metadata.Distribution.at(path.parent).metadata
            if metadata["Name"] == distribution.metadata["Name"] and metadata["Version"] == distribution.version:
                anchored = True
        if anchored:
            owners.append(files)
    if not owners:
        return None
    if len(owners) != 1:
        raise ValueError("Installed distribution has ambiguous installed Conda owners")
    return [
        importlib.metadata.PackagePath(Path(os.path.relpath(prefix / member, base)).as_posix()) for member in owners[0]
    ]


def _distribution_members(
    distribution: Any, prefix: Path, records: list[dict[str, Any]]
) -> list[importlib.metadata.PackagePath]:
    """Capture complete installed authority before stdlib existence filtering."""
    conda = _conda_members(distribution, prefix, records)
    if conda is not None:
        return conda
    record = distribution.read_text("RECORD")
    if not record:
        raise ValueError("Installed distribution has no complete RECORD authority or installed Conda owner")
    members = []
    for row in csv.reader(io.StringIO(record)):
        if not row:
            continue
        if len(row) != 3 or not row[0]:
            raise ValueError("Invalid installed RECORD member")
        if row[2]:
            int(row[2])
        members.append(importlib.metadata.PackagePath(row[0]))
    if not members:
        raise ValueError("Installed RECORD has no member authority")
    return members


def _editable_layout(
    distribution: Any, source: Path, files: list[importlib.metadata.PackagePath]
) -> tuple[set[str], set[str]]:
    """Use owned activation members, not a guessed distribution/import name."""
    names = set((distribution.read_text("top_level.txt") or "").split())
    finders: set[str] = set()
    for member in files:
        path = Path(distribution.locate_file(member))
        if path.suffix != ".pth":
            continue
        content = _file_content(path, materialize=True)[1].decode("utf-8")
        for line in content.splitlines():
            if not line or line.startswith("#"):
                continue
            match = re.fullmatch(r"import (__editable__[\w]+); ?\1\.install\(\)", line)
            if match:
                finder_name = match.group(1)
                finder = path.parent / (finder_name + ".py")
                if finder.name not in {Path(str(item)).name for item in files}:
                    raise ValueError("Editable finder has no owned member authority")
                tree = ast.parse(_file_content(finder, materialize=True)[1].decode("utf-8"))
                mappings = _finder_mappings(tree)
                names.update(mappings)
                finders.add(finder.name)
                continue
            if line.startswith("import "):
                continue
            activated = Path(line)
            if not activated.is_absolute():
                continue
            activated = activated.resolve(strict=True)
            activated.relative_to(source)
            children = list(activated.iterdir())
            if len(children) > 1024:
                raise ValueError("Editable activation layout exceeds its bound")
            for child in children:
                if child.is_dir() and child.name.isidentifier():
                    names.add(child.name)
                elif child.suffix == ".py" and child.stem.isidentifier():
                    names.add(child.stem)
    return names, finders


def _finder_mappings(tree: ast.Module) -> dict[str, Any]:
    mappings: dict[str, Any] = {}
    for statement in tree.body:
        if isinstance(statement, ast.Assign):
            targets, value = statement.targets, statement.value
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            targets, value = [statement.target], statement.value
        else:
            continue
        if not any(isinstance(target, ast.Name) and target.id in {"MAPPING", "NAMESPACES"} for target in targets):
            continue
        selected = ast.literal_eval(value)
        if not isinstance(selected, dict) or any(not isinstance(key, str) for key in selected):
            raise ValueError("Invalid generated editable finder mapping")
        mappings.update(selected)
    if not mappings:
        raise ValueError("Editable finder lacks a supported generated mapping")
    return mappings


def _editable_roots(distribution: Any, source: Path, files: list[importlib.metadata.PackagePath]) -> list[str]:
    names, _ = _editable_layout(distribution, source, files)
    if not names:
        raise ValueError(f"Editable distribution lacks bounded import-root metadata: {distribution.metadata['Name']}")
    roots = []
    for name in sorted(names):
        if not name.isidentifier():
            raise ValueError("Invalid editable top-level import name")
        selected = importlib.util.find_spec(name)
        if selected is None:
            continue
        paths = list(selected.submodule_search_locations or ())
        if not paths and selected.origin:
            paths = [selected.origin]
        for text in paths:
            path = Path(text).resolve(strict=True)
            try:
                path.relative_to(source)
            except ValueError:
                continue
            roots.append(str(path))
    if not roots:
        raise ValueError("Editable activation has no actual import-member authority")
    return sorted(set(roots))


def _editable_activation(content: bytes, source: Path, *, finder: bool) -> bytes:
    def normalize(value: str) -> str:
        if not Path(value).is_absolute():
            return value
        try:
            relative = Path(value).relative_to(source)
        except ValueError:
            return value
        return "<editable-source>/" + relative.as_posix()

    if finder:
        tree = ast.parse(content.decode("utf-8"))
        _finder_mappings(tree)
        for statement in tree.body:
            if isinstance(statement, ast.Assign):
                targets, value = statement.targets, statement.value
            elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                targets, value = [statement.target], statement.value
            else:
                continue
            if any(isinstance(target, ast.Name) and target.id in {"MAPPING", "NAMESPACES"} for target in targets):
                for node in ast.walk(value):
                    if isinstance(node, ast.Constant) and isinstance(node.value, str):
                        node.value = normalize(node.value)
        return ast.dump(tree, include_attributes=False).encode()
    lines = content.decode("utf-8").splitlines()
    return "\n".join(normalize(line) for line in lines).encode()


def capture_runtime_content(
    *, distribution_paths: list[str] | None = None, prefix: Path | None = None
) -> dict[str, Any]:
    """Hash actual installed Python members once, without importing tool packages."""
    prefix = (prefix or Path(sys.prefix)).resolve()
    records = [json.loads(metadata.read_text()) for metadata in sorted((prefix / "conda-meta").glob("*.json"))]
    distributions = (
        importlib.metadata.distributions(path=distribution_paths)
        if distribution_paths is not None
        else importlib.metadata.distributions()
    )
    installed, editable, operational = [], [], []
    names = set()
    for distribution in distributions:
        name = _name(distribution.metadata["Name"] or "")
        if not name or name in names:
            raise ValueError(f"Missing or duplicate installed distribution: {name!r}")
        names.add(name)
        files = _distribution_members(distribution, prefix, records)
        direct_url = distribution.read_text("direct_url.json")
        url = json.loads(direct_url) if direct_url else {}
        is_editable = url.get("dir_info", {}).get("editable") is True
        roots = []
        source = None
        finders: set[str] = set()
        if is_editable:
            parsed = urllib.parse.urlsplit(url["url"])
            if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
                raise ValueError(f"Editable distribution {name!r} has no local source authority")
            source = Path(urllib.request.url2pathname(urllib.parse.unquote(parsed.path))).resolve(strict=True)
            roots = _editable_roots(distribution, source, files)
            _, finders = _editable_layout(distribution, source, files)
            content_digest = editable_content_digest(roots)
            editable.append({"name": name, "content_digest": content_digest})
            operational.append(
                {"name": name, "source_root": str(source), "import_roots": roots, "content_digest": content_digest}
            )
        entrypoints = {
            item.name for item in distribution.entry_points if item.group in {"console_scripts", "gui_scripts"}
        }
        members = []
        for file in files:
            # Operational bytecode caches can be recorded without still existing.
            if file.suffix in {".pyc", ".pyo"} or "__pycache__" in file.parts:
                continue
            path = Path(str(distribution.locate_file(file))).resolve(strict=True)
            relative = path.relative_to(prefix).as_posix()
            if path.suffix in {".pyc", ".pyo"} or "__pycache__" in path.parts:
                continue
            if any(part.endswith(".dist-info") for part in path.parts) and path.name in {
                "RECORD",
                "INSTALLER",
                "REQUESTED",
                "direct_url.json",
                "WHEEL",
            }:
                continue
            normalized = None
            if is_editable and path.suffix == ".pth":
                assert source is not None
                normalized = _editable_activation(_file_content(path, materialize=True)[1], source, finder=False)
            elif is_editable and path.name in finders:
                assert source is not None
                normalized = _editable_activation(_file_content(path, materialize=True)[1], source, finder=True)
            if path.parent.name == "Scripts" and path.suffix == ".exe" and path.stem in entrypoints:
                normalized = _windows_entrypoint(_file_content(path, materialize=True)[1])
            size, file_digest = (
                (len(normalized), hashlib.sha256(normalized).digest())
                if normalized is not None
                else _file_content(path, python_shebang=path.parent.name in {"bin", "Scripts"})
            )
            members.append((relative, size, file_digest))
        installed.append({"name": name, "version": distribution.version, "content_digest": _members_digest(members)})
    artifacts = []
    for value in records:
        artifacts.append(
            {
                "name": _name(value["name"]),
                "version": str(value["version"]),
                "build": str(value.get("build", "")),
                "subdir": str(value.get("subdir", "")),
                "sha256": value.get("sha256"),
            }
        )
    facts = {
        "schema_version": 1,
        "python": {
            "implementation": sys.implementation.name,
            "version": list(sys.version_info[:3]),
            "cache_tag": sys.implementation.cache_tag or "",
            "soabi": sysconfig.get_config_var("SOABI") or "",
            "platform": sys.platform,
            "machine": platform.machine(),
            "executable_digest": _file_content(Path(sys.executable).resolve())[1].hex(),
        },
        "distributions": sorted(installed, key=lambda item: item["name"]),
        "resolved_artifacts": sorted(artifacts, key=lambda item: item["name"]),
        "editable_sources": sorted(editable, key=lambda item: item["name"]),
    }
    return {"scientific_facts": facts, "editable_roots": sorted(operational, key=lambda item: item["name"])}


if __name__ == "__main__":
    print(json.dumps(capture_runtime_content(), sort_keys=True, separators=(",", ":")))
