# Compatibility and releases

## Supported platforms

Wetlands 2 supports:

- Python 3.9 through 3.14;
- Linux, macOS, and Windows;
- the Pixi version whose executable and SHA-256 checksums are registered in the Wetlands release.

The test suite covers every supported Python version on all three operating systems.
Real-Pixi acceptance tests run on all three operating systems before changes are merged.

Pixi version and checksum updates are deliberate source changes.
They are accepted only after the real-Pixi acceptance suite passes on Linux, macOS, and Windows.

## Versioning

Wetlands uses [semantic versioning](https://semver.org/).
Breaking public API, managed-metadata, or worker-protocol changes require a new major version.
Backward-compatible features use a minor release, and backward-compatible fixes use a patch release.

Host and worker execution- and management-protocol versions must match exactly.
A host fails the corresponding connection clearly rather than attempting to communicate with an incompatible worker.
The package version and managed worker-runtime version are released together and are required to be identical.

Stop all persistent Wetlands workers before upgrading or downgrading Wetlands.
An upgraded controller must not attach to workers created by another Wetlands release.

## Supported release line

The latest Wetlands 2.x release is supported.
See the [security policy](https://github.com/arthursw/wetlands/security/policy) for the trusted-code model and vulnerability reporting.
