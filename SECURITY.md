# Security policy

## Supported versions

The latest Wetlands 2.x release is the supported security line.
Older 2.x releases and Wetlands 1.x do not receive security fixes.

## Trusted-code model

Wetlands isolates Python dependencies, not operating-system permissions.
Worker functions, package installers, and post-install commands run as the current user and can generally access that user's files, environment variables, network, local services, processes, CPU, memory, and disk.

The authenticated local connection prevents accidental or unauthenticated connections to a worker.
It does not restrict code already running as the same user.
Only use Wetlands with worker code and dependency sources you trust.

Post-hoc debugger startup is authenticated, but the resulting `debugpy` endpoint is an unauthenticated loopback service that can control the worker.
Use it only on a machine whose local users and processes you trust, and never expose its port beyond the local host.

Use an operating-system sandbox, container, virtual machine, or separate user account when executing untrusted code.

## Reporting a vulnerability

Please report vulnerabilities privately through [GitHub private vulnerability reporting](https://github.com/arthursw/wetlands/security/advisories/new).
If that is unavailable, email [arthur.masson@inria.fr](mailto:arthur.masson@inria.fr).

Include the affected Wetlands version, operating system, Python version, reproduction steps, and potential impact.
Please do not open a public issue before the report has been assessed.
