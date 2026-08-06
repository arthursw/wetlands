# Upgrade within Wetlands 2

Wetlands follows semantic versioning for its public API.
Backward-compatible features use a minor release, and backward-compatible fixes use a patch release.

## Before upgrading

Stop all persistent Wetlands worker pools before upgrading or downgrading the package.
A controller must not attach to workers created by a different Wetlands release.

Use `pool.close()` when persistent workers are no longer needed.
If the original controller has detached, attach to the pool from the matching installed release and close it before changing versions.

## Upgrade the package

Upgrade Wetlands using the same tool that manages your host application environment:

```sh
pip install --upgrade wetlands
```

Read the [changelog](https://github.com/arthursw/wetlands/blob/main/CHANGELOG.md) for changes that affect environment recipes or worker behavior.

## Rebuild managed environments when required

The Wetlands-managed worker runtime participates in environment recipe identity.
When a release changes that runtime, provisioning detects the mismatch instead of reusing an incompatible ready environment.

Use a new environment name for rollback-safe application upgrades.
Use `replace_existing=True` only when removing the old environment before the new one succeeds is acceptable.

The latest Wetlands 2.x release is the supported release line.
See the [security policy](https://github.com/arthursw/wetlands/security/policy) for vulnerability reporting and support details.

Wetlands 1 users should follow [Migrate from Wetlands 1](../migration_v2.md) instead.
