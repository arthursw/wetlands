# Release checklist

Wetlands releases are published to PyPI by [`.github/workflows/release.yml`](.github/workflows/release.yml) when a GitHub release is published.
Maintainers and agents can create releases with `gh`; they do not need a PyPI API token.

Wetlands uses semantic versioning.
Breaking public API, managed-metadata, or worker-protocol changes require a new major version.
Backward-compatible features use a minor release, and backward-compatible fixes use a patch release.

The package version and managed worker-runtime version are released together and must be identical.
Host and worker execution and management protocol versions must match exactly.

1. Confirm that CI is green on Linux, macOS, and Windows, including the real-Pixi acceptance jobs.
2. Update the project version, `wetlands.protocol.WORKER_RUNTIME_VERSION`, and `CHANGELOG.md` together. If a protocol changed, update its protocol version constant and compatibility tests in the same commit.
3. Run `uv lock --check`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy src/wetlands`, and the full test suite.
4. Build the documentation with the same strict command as CI: `uv run --frozen --python 3.14 --extra docs --no-dev mkdocs build --strict`.
5. From a clean checkout of the release commit, install the locked release tools with `uv sync --frozen --python 3.14 --only-group release --no-install-project`, then build both artifacts with `uv build --no-build-isolation`.
6. Inspect the source distribution and confirm that it contains the source, tests, examples, documentation, `RELEASING.md`, and release metadata, but not generated `site/`, `dist/`, `.vscode/`, virtual environments, or worktrees.
7. Validate the artifacts with `uv run --no-sync twine check dist/*`.
8. Install the wheel and source distribution separately into clean environments and verify `import wetlands`, `wetlands.__version__`, and the `wetlands` CLI.
9. Stop all persistent workers created by the release candidate before testing an upgrade or downgrade.
10. Create and push the version tag, then publish the GitHub release with the commands below.
11. Watch the publishing workflow and confirm that the new version appears on PyPI.

## One-time trusted publisher setup

In the [Wetlands PyPI publishing settings](https://pypi.org/manage/project/wetlands/settings/publishing/), add a **GitHub** trusted publisher with these exact values:

| Field | Value |
| --- | --- |
| Owner | `arthursw` |
| Repository name | `wetlands` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

The workflow name is the filename, without the `.github/workflows/` prefix.
See [PyPI's trusted publisher instructions](https://docs.pypi.org/trusted-publishers/adding-a-publisher/) for the form details.
Create a GitHub environment named `pypi` in [repository settings](https://github.com/arthursw/wetlands/settings/environments).
For unattended agent releases, leave required reviewers disabled; agents still need GitHub credentials with permission to push tags and create releases.
If you restrict deployment branches and tags, allow version tags (`v*`) and the default branch (`main`) so both release events and manual dispatches can publish.
No PyPI secret is needed in GitHub.
Merge and push this workflow to the default branch before creating the first release that uses it.

## Publish with gh

After completing the checklist, run from a clean checkout of the release commit on `main`:

```sh
version="$(uv run --no-sync python -c 'import tomllib; from pathlib import Path; print(tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"])')"
tag="v$version"
git push origin main
git tag -a "$tag" -m "Wetlands $version"
git push origin "$tag"
gh release create "$tag" --repo arthursw/wetlands --verify-tag --title "Wetlands $version" --generate-notes
gh run list --repo arthursw/wetlands --workflow release.yml --limit 5
gh run watch RUN_ID --repo arthursw/wetlands --exit-status
```

Replace `RUN_ID` with the release workflow run ID shown by `gh run list`.
Use `--prerelease` when creating a GitHub release for a prerelease package version such as `2.5.0rc1`.
Draft releases and tag pushes alone do not publish to PyPI.
The workflow builds the tagged commit, requires the tag to equal `v<project.version>`, checks the worker runtime version, runs static checks and fast tests, validates wheel and source distribution metadata, and smoke tests both installed distributions before publishing.
The complete platform and real-Pixi CI checks in step 1 remain required before releasing.
Only the separate publishing job has permission to obtain the PyPI identity token.

## Retry or publish an existing GitHub release

If a workflow failed before uploading, retry it with `gh run rerun RUN_ID --repo arthursw/wetlands`.
To explicitly publish an existing, non-draft release, use the manual trigger on the default branch:

```sh
gh workflow run release.yml --repo arthursw/wetlands --ref main -f tag=v2.4.3
gh run list --repo arthursw/wetlands --workflow release.yml --limit 5
gh run watch RUN_ID --repo arthursw/wetlands --exit-status
```

Replace `v2.4.3` with the intended release tag.
Manual dispatch also works for releases created by another workflow using `GITHUB_TOKEN`, because those release events do not trigger a second workflow automatically.
PyPI does not allow replacing uploaded distributions; publish a new version for code changes and check PyPI before retrying an upload that partially succeeded.
