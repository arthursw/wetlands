"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()  # type: ignore

root = Path(__file__).parent.parent
src = root / "src"
package = src / "wetlands"

references = [
    ("wetlands", "Public API", package / "__init__.py"),
]

for identifier, display_name, path in references:
    doc_path = Path(f"{identifier}.md")
    full_doc_path = Path("reference", doc_path)
    nav[(display_name,)] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        print("# Public API\n", file=fd)
        print("The supported API is exported from `wetlands`.\n", file=fd)
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
