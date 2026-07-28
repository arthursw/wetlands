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

Local paths are resolved when the specification is constructed and participate in its recipe identity.
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
    argv=("worker-package configure --token \"$WORKER_TOKEN\"",),
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
Applications that must preserve the old recipe during an upgrade should provision the new recipe under a different physical name and switch their own mapping after success.

## Environment names

Environment names are single portable path components.
Path traversal, rooted paths, control characters, trailing dots or spaces, and Windows device aliases are rejected.
