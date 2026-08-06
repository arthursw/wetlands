"""Generate grouped public API reference pages."""

from pathlib import Path

import mkdocs_gen_files

from wetlands import __all__ as public_names


root = Path(__file__).parent.parent
package = root / "src" / "wetlands"

groups = {
    "Environment management": (
        "__version__",
        "EnvironmentManager",
        "EnvironmentSpec",
        "LocalPackage",
        "ManagedEnvironment",
        "ManagedEnvironmentInfo",
        "ManagedEnvironmentState",
        "PostInstallCommand",
        "PixiInfo",
        "ProvisioningStage",
        "WorkerPool",
        "local_package_content_identity",
    ),
    "Operations and tasks": (
        "ExecutionEvent",
        "ExecutionEventKind",
        "ExecutionState",
        "ExecutionTask",
        "Operation",
        "OperationEvent",
        "OperationEventKind",
        "OperationState",
        "PreparationOperation",
        "ProvisioningOperation",
        "RemovalOperation",
    ),
    "Errors and diagnostics": (
        "EnvironmentGenerationChangedError",
        "EnvironmentInUseError",
        "EnvironmentNotFoundError",
        "EnvironmentRecipeConflictError",
        "EnvironmentNotReadyError",
        "ExecutionError",
        "ExecutionFailure",
        "ExecutionFailureCategory",
        "InvalidStateError",
        "LocalPackageValidationError",
        "ManagerCloseError",
        "OperationCanceled",
        "OperationError",
        "OperationFailure",
        "PreparationError",
        "ProvisioningError",
        "RemoteExceptionInfo",
        "RemovalError",
        "UnmanagedTargetError",
        "ValueDecodingError",
        "ValueEncodingError",
        "WorkerInfo",
        "WorkerStartError",
    ),
    "Debugging": (
        "DebugEndpoint",
        "RunningWorker",
    ),
}

assigned_names = [name for names in groups.values() for name in names]
if len(public_names) != len(set(public_names)):
    raise RuntimeError("wetlands.__all__ contains a duplicate public API name")
if len(assigned_names) != len(set(assigned_names)):
    raise RuntimeError("A public API name appears in more than one reference group")
if set(assigned_names) != set(public_names):
    missing = sorted(set(public_names) - set(assigned_names))
    extra = sorted(set(assigned_names) - set(public_names))
    raise RuntimeError(f"Public API reference grouping is incomplete: missing={missing}, extra={extra}")

nav = mkdocs_gen_files.Nav()  # type: ignore

for title, names in groups.items():
    filename = title.lower().replace(" ", "-") + ".md"
    full_path = Path("reference", filename)
    nav[(title,)] = filename

    with mkdocs_gen_files.open(full_path, "w") as page:
        print(f"# {title}\n", file=page)
        print("::: wetlands", file=page)
        print("    options:", file=page)
        print("      members:", file=page)
        for name in names:
            print(f"        - {name}", file=page)

    mkdocs_gen_files.set_edit_path(full_path, (package / "__init__.py").relative_to(root))

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
