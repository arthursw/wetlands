# Environment identity, reuse, and rebuilding

Wetlands reuses an environment only when it can prove that the ready environment matches the requested recipe.

## Recipe identity

The normalized `EnvironmentSpec`, supplied lockfile digest, local package settings, and Wetlands-managed worker runtime contribute to recipe identity.
Changing any of them requires a rebuild rather than silently using a different environment.

## Ready publication

Provisioning performs dependency resolution, installation, post-install commands, and runtime validation before publishing ready metadata.
The environment becomes usable only after that final publication succeeds.

If provisioning fails or is canceled, Wetlands removes the incomplete target.
If the host crashes, the next provisioning attempt detects missing or inconsistent ready metadata and rebuilds instead of resuming unknown state.

## Lockfiles

Without `pixi_lock`, Pixi resolves the generated project and Wetlands keeps the resulting `pixi.lock` in the managed environment.

With `pixi_lock`, Wetlands installs exactly from the supplied lock and rejects changes during provisioning.
The lock must match the complete generated project, including local packages and Wetlands' managed runtime dependencies.

## Replacement strategy

`replace_existing=True` removes an existing different recipe before the new recipe is ready.

Applications that require rollback should put a release or recipe identity in the managed environment name, provision the new name, and update their own logical mapping only after success.

See [Discover, replace, and remove environments](../how-to/environment_management.md) for the relevant calls.
