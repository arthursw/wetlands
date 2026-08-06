# Environment specifications

Wetlands 2 provisions Pixi projects from immutable `EnvironmentSpec` values.

```python
from wetlands import EnvironmentSpec

spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2", "scikit-image"),
    pypi=("example-worker==1.4.0",),
    channels=("conda-forge",),
)
```

## Fields

`python`
: A Pixi-compatible Python version constraint.

`conda`
: Conda package constraints installed by Pixi.
Declare channels separately with `channels`; `channel::package` requirements are rejected.

`pypi`
: Python package requirements installed through the Pixi project.

`channels`
: Ordered Conda channels.
Duplicate entries are removed.

`local`
: Local Python packages described by `LocalPackage`.

`post_install`
: Explicit commands run after dependency installation.

`pixi_lock`
: Optional bytes or a path containing an existing `pixi.lock`.

## Managed worker runtime

Wetlands adds its own pinned worker-runtime dependencies to every generated Pixi project.
The runtime currently includes `debugpy`, which remains inactive until post-hoc debugger attachment is requested.
Do not declare or override `debugpy` in `EnvironmentSpec`; Wetlands owns its compatible version.

The managed runtime and its exact pins participate in recipe identity.
Changing them requires a rebuild rather than silently reusing an older ready environment.
After post-install commands finish, provisioning verifies the installed managed-runtime versions before publishing the environment as ready.

## Pinned Git packages

Declare a Git-hosted Python package with a `git+https` direct reference pinned to its full commit SHA:

```python
from wetlands import EnvironmentSpec

spec = EnvironmentSpec(
    pypi=("sampleproject @ git+https://github.com/pypa/sampleproject.git@621e4974ca25ce531773def586ba3ed8e736b3fc",),
)
```

Wetlands accepts full 40-character SHA-1 and 64-character SHA-256 commit identifiers.
Branches, tags, abbreviated hashes, embedded credentials, query parameters, URL fragments, and non-HTTPS Git transports are rejected.
Pinning makes the selected repository state immutable, but does not establish that its code is trustworthy; review Git dependencies before provisioning them.

## Local packages

```python
from pathlib import Path

from wetlands import EnvironmentSpec, LocalPackage

spec = EnvironmentSpec(
    conda=("pip",),
    local=(
        LocalPackage(
            source=Path("../worker-package"),
            editable=True,
            extras=("models",),
        ),
    ),
)
```

Each local source must be an existing directory with a valid `pyproject.toml` and a non-empty `[project].name`.
Wetlands uses that normalized distribution name to register the local requirement with Pixi.
Invalid local package metadata raises `LocalPackageValidationError` when `LocalPackage` is constructed.

Local paths are resolved when the specification is constructed, and the path and distribution name participate in the recipe identity.
Use editable installs for development, not for immutable production recipes.

## Post-install commands

```python
from wetlands import EnvironmentSpec, PostInstallCommand

spec = EnvironmentSpec(
    post_install=(
        PostInstallCommand(
            argv=("python", "-m", "worker_package.download_assets"),
        ),
    ),
)
```

Arguments are passed without a shell by default.

Shell execution requires an explicit safe display string so logs and structured failures do not expose command secrets:

```python
PostInstallCommand(
    argv=('worker-package configure --token "$WORKER_TOKEN"',),
    shell=True,
    display="worker-package configure --token <redacted>",
)
```

Applications should pass credentials through a controlled child-process environment and avoid embedding them in recipes.
Wetlands redacts common credential and proxy forms from command output, but the application remains responsible for not placing secrets in arbitrary output.

## Lockfile behavior

Without `pixi_lock`, Wetlands materializes `pixi.toml`, asks Pixi to resolve the project, and keeps the resulting `pixi.lock` beside the manifest.

With `pixi_lock`, Wetlands copies the supplied bytes into the project and uses locked installation semantics.
Local packages are registered in the Pixi manifest before installation, so a supplied lockfile must already resolve the same local dependency sources and editable settings.
It must also contain the exact Wetlands-managed runtime dependencies, including the managed `debugpy` pin.
Pixi validates the supplied lock against the complete generated manifest during locked installation.
Wetlands rejects any lockfile change during locked provisioning.

```python
spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy==2.2.6",),
    pixi_lock="pixi.lock",
)
```

The normalized recipe includes a SHA-256 digest of the supplied lockfile.
Ready metadata records the resulting lockfile digest.

## Ready and rebuild semantics

An environment is ready only after:

1. the Pixi project has been materialized;
2. lock resolution or validation has succeeded;
3. Conda, PyPI, local, and post-install stages have succeeded;
4. the environment has passed validation;
5. final managed metadata has been published atomically.

Failure or cancellation removes the incomplete environment.
A later provisioning attempt also detects and removes crash-interrupted state before rebuilding.
Wetlands never returns an environment that lacks successful managed metadata.

`replace_existing=True` may remove a currently ready environment before building its replacement.
Applications that must preserve the old recipe during an upgrade should provision the new recipe under a different managed environment name and switch their own mapping after success.

## Environment names

Environment names are single portable path components.
Path traversal, rooted paths, control characters, trailing dots or spaces, and Windows device aliases are rejected.
