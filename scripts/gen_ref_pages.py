"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()  # type: ignore

root = Path(__file__).parent.parent
src = root / "src"

# files = sorted(src.rglob("wetlands/*.py"))
file_names = [
    "environment_manager.py",
    "environment.py",
    "internal_environment.py",
    "external_environment.py",
]  # define manually to force rendering order
files = [src / "wetlands" / f for f in file_names] + sorted(src.rglob("wetlands/_internal/*.py"))

for path in files:
    module_path = path.relative_to(src).with_suffix("")
    doc_path = path.relative_to(src).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)

    if parts[-1] in ["__init__", "__main__", "logger"]:
        continue

    display_name = parts[-1].replace("_", " ").capitalize()
    display_parts = parts[1:-1] + (display_name,)
    display_parts = tuple([dp.replace("_", "") for dp in display_parts])
    nav[display_parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
