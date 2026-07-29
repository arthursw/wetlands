# Release checklist

Wetlands releases are published manually.

1. Confirm that CI is green on Linux, macOS, and Windows, including the real-Pixi acceptance jobs.
2. Update the project version, `wetlands.protocol.WORKER_RUNTIME_VERSION`, and `CHANGELOG.md` together.
3. Run `uv lock --check`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy src/wetlands`, and the full test suite.
4. Build the documentation strictly with `uv run --extra docs mkdocs build --strict`.
5. From a clean checkout of the release commit, build both artifacts with `uv build`.
6. Inspect the source distribution contents and confirm that it contains the source, tests, examples, documentation, and release metadata, but not generated `site/`, `dist/`, `.vscode/`, virtual environments, or worktrees.
7. Validate the artifacts with `uv run --group release --no-dev twine check dist/*`.
8. Install the wheel and source distribution separately into clean environments and verify `import wetlands`, `wetlands.__version__`, and the `wetlands` CLI.
9. Create and push the version tag.
10. Publish the checked artifacts to PyPI.

Publication is not performed by GitHub Actions.
