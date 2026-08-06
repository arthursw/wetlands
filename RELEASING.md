# Release checklist

Wetlands releases are published manually.

Wetlands uses semantic versioning.
Breaking public API, managed-metadata, or worker-protocol changes require a new major version.
Backward-compatible features use a minor release, and backward-compatible fixes use a patch release.

The package version and managed worker-runtime version are released together and must be identical.
Host and worker execution and management protocol versions must match exactly.

1. Confirm that CI is green on Linux, macOS, and Windows, including the real-Pixi acceptance jobs.
2. Update the project version, `wetlands.protocol.WORKER_RUNTIME_VERSION`, and `CHANGELOG.md` together. If a protocol changed, update its protocol version constant and compatibility tests in the same commit.
3. Run `uv lock --check`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy src/wetlands`, and the full test suite.
4. Build the documentation with the same strict command as CI: `uv run --frozen --python 3.14 --extra docs --no-dev mkdocs build --strict`.
5. From a clean checkout of the release commit, build both artifacts with `uv build`.
6. Inspect the source distribution and confirm that it contains the source, tests, examples, documentation, `RELEASING.md`, and release metadata, but not generated `site/`, `dist/`, `.vscode/`, virtual environments, or worktrees.
7. Validate the artifacts with `uv run --group release --no-dev twine check dist/*`.
8. Install the wheel and source distribution separately into clean environments and verify `import wetlands`, `wetlands.__version__`, and the `wetlands` CLI.
9. Stop all persistent workers created by the release candidate before testing an upgrade or downgrade.
10. Create and push the version tag.
11. Publish the checked artifacts to PyPI.

Publication is not performed by GitHub Actions.
