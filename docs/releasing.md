# Release checklist

Wetlands releases are published manually.

1. Confirm that CI is green on Linux, macOS, and Windows, including the real-Pixi acceptance jobs.
2. Update the version and `CHANGELOG.md`.
3. Run `uv lock --check`, `uv run ruff check`, `uv run ruff format --check`, and the full test suite.
4. Build the documentation strictly with `uv run --extra docs mkdocs build --strict`.
5. Build both artifacts with `uv build`.
6. Validate the artifacts with `uv run --group release --no-dev twine check dist/*`.
7. Install the wheel and source distribution separately into clean environments and verify `import wetlands` and `wetlands.__version__`.
8. Create and push the version tag.
9. Publish the checked artifacts to PyPI.

Publication is not performed by GitHub Actions.
