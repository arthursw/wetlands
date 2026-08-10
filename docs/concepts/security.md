# Trusted-code security model

Wetlands environments solve dependency conflicts.
They are not security sandboxes.

Worker functions, managed commands, package installers, post-install commands, and attached debuggers run with the current operating-system user's permissions.
They can generally read and modify that user's files, inspect environment variables, use the network, start processes, and consume CPU, memory, and disk.

Use Wetlands only with code and dependency sources you trust.

Authenticated loopback connections prevent accidental or unauthenticated control messages from reaching workers.
They do not restrict code that is already running as the same user.

The debugger adapter listens on the local loopback interface after it is requested.
Another process on the same machine may be able to attach to it and control the worker.

Use an operating-system sandbox, container, virtual machine, or separate user account when untrusted code must run.

Read the [security policy](https://github.com/arthursw/wetlands/security/policy) for supported releases and vulnerability reporting.
