# Transport codecs

Wetlands encodes task arguments and results so worker functions can exchange ordinary values without transport-specific code.
The codec implementation is internal in Wetlands 2.0.

## Supported boundary

The initial codec set supports:

- `None`, booleans, integers, floats, strings, and bytes;
- nested lists, tuples, and dictionaries with simple scalar keys;
- NumPy arrays without object dtype.

Recursive containers describe their children in the execution envelope.
NumPy array data travels through shared memory while its dtype, shape, order, and lease identity travel in a protocol descriptor.

Every encoded value identifies its codec and version where required.
Startup capability negotiation prevents the host from dispatching a descriptor that the worker cannot decode.
Unknown, unsupported, cyclic, or malformed values fail explicitly at the execution boundary.

## Lease ownership

An input NumPy lease is created and owned by the host.
The worker attaches, copies into private contiguous storage, and closes its attachment.
The host unlinks the segment after the task reaches a terminal state.

An output NumPy lease is created and owned by the worker.
The host attaches, copies the result into independently owned storage, and acknowledges receipt.
The worker then unlinks the segment and acknowledges release.

Cleanup is idempotent because task completion, cancellation, connection loss, timeout, and worker death may race.
The process that survives a failed exchange reconciles the leases it owns.
Resource-tracker registration and unregistration remain transport internals because their behavior differs between Python releases.

Intermediate progress metadata is limited to small simple values.
NumPy arrays are accepted as terminal results, not as intermediate outputs, because intermediate lease lifetime has no single terminal acknowledgement.

## Adding a built-in codec

Before adding a codec, define:

1. the exact Python types and recursive values it accepts;
2. a stable codec identifier and descriptor version;
3. host and worker capability requirements;
4. copy, mutation, and ownership semantics;
5. creator, attachment, acknowledgement, and terminal cleanup responsibilities;
6. maximum sizes and validation applied before allocation;
7. behavior under cancellation, disconnection, decoding failure, and worker death;
8. cross-version and cross-platform tests.

A codec must reject values it cannot represent safely rather than falling back to arbitrary pickling.
Descriptors must be strictly validated before opening files, attaching memory, allocating buffers, or importing optional dependencies.
Logs and failure payloads must not contain data contents or credentials.

Tests must cover nested values, malformed descriptors, capability mismatch, resource exhaustion boundaries, cleanup races, and creator death on Linux, macOS, and Windows.

## Extension boundary

Wetlands 2.0 does not expose third-party codec registration.
The current registry coordinates concrete shared-memory leases and process-local attachment state, so publishing it would make those implementation details part of the public compatibility contract.

Keep codec selection and registration private until another production transport demonstrates the common boundary between value semantics, transfer transport, and resource leases.
Adding a built-in codec must not change task submission, task state, or worker-call semantics.
