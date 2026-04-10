## 🎓 Step by Step

Wetlands leverages **Pixi**, a package management tool for developers, or **Micromamba**, a fast, native reimplementation of the Conda package manager.

1.  **Pixi or Micromamba Setup:** When `EnvironmentManager` is initialized, it checks for a `pixi` or `micromamba` executable at the specified path (e.g., `"micromamba/"`). If not found, it downloads a self-contained Pixi or Micromamba binary suitable for the current operating system and architecture into that directory. This means Wetlands doesn't require a pre-existing Conda/Mamba installation.
2.  **Environment Creation:** `create(envName, dependencies)` uses Pixi or Micromamba commands (`pixi init /path/to/envName` or  `micromamba create -n envName -c channel package ...`) to build a new, isolated Conda environment within the Pixi or Micromamba prefix (e.g., `pixi/workspaces/envName/envs/default/` or `micromamba/envs/envName`). When using Pixi, Wetlands also creates a workspace for the environment (e.g. `pixi/workspace/envName/`). Note that the main environment is returned if it already satisfies the required dependencies.
3.  **Dependency Installation:** Dependencies (Conda packages, Pip packages) are installed into the target environment using `pixi add ...` or `micromamba install ...` and `pip install ...` (executed within the activated environment).
4.  **Launching Workers (`launch`):**
    *   `launch(max_workers=N)` starts one or more `module_executor` worker processes *within* the activated target environment using `subprocess.Popen`. All workers share the same Conda environment on disk — no duplication.
    *   Each worker listens on its own local socket using `multiprocessing.connection.Listener`.
    *   The main process connects to each worker using `multiprocessing.connection.Client`.
    *   A dedicated IPC reader daemon thread is started per worker to receive messages asynchronously.
    *   A health monitor daemon thread is started to periodically check all workers for liveness and inactivity timeouts (see below).
5.  **Execution (`submit`/`execute`/`import_module`):**
    *   `submit(module, func, args)` creates a `Task[T]` object, dispatches the function call to an idle worker, and returns the `Task` immediately. If all workers are busy, the task is queued internally and dispatched when the next worker becomes available.
    *   `execute(module, func, args)` is a blocking shortcut: it submits the call and waits for the result before returning.
    *   `import_module(module)` creates a proxy object in the main process. When methods are called on this proxy, it triggers the `execute` mechanism described above.
    *   Each worker imports the target module, executes the function with the provided arguments, and sends the result (or exception) back to the main process via its IPC connection.
6.  **Task Lifecycle:**
    *   A `Task` goes through states: `PENDING → RUNNING → COMPLETED` (or `FAILED` / `CANCELED`).
    *   The worker sends typed IPC messages: `execution finished`, `error`, `update` (progress), and `canceled`.
    *   The reader thread dispatches these messages to the `Task` object, which notifies registered listeners and resolves its internal `concurrent.futures.Future`.
    *   When a worker finishes a task, it is returned to the idle pool and the next queued task (if any) is dispatched to it.
7.  **Progress and Cancellation:**
    *   Remote code can report progress by declaring a `task` parameter in the function signature. Wetlands detects it via `inspect.signature()` and injects a `RemoteTaskHandle` automatically.
    *   The handle provides `task.update()` for progress, `task.set_output()` for intermediate results, `task.cancel_requested` for cooperative cancellation, and `task.log()` for remote logging.
8.  **Worker Health Monitoring:**
    *   A background daemon thread monitors all workers every few seconds.
    *   If a worker process has exited (crash, OOM kill, etc.), the monitor fails the active task, removes the worker, and launches a replacement with the same configuration.
    *   If `worker_timeout` is set and a worker has not sent any IPC message within that duration, it is treated as hung: the active task is failed, the worker is killed and replaced.
    *   On `env.exit()`, the health monitor stops and any tasks still in the queue are failed with a descriptive error.
9.  **Direct Execution (`execute_commands`):** This method directly activates the target environment and runs the provided shell commands using `subprocess.Popen` (no worker processes involved here). The user is responsible for managing the launched process and any necessary communication.
10.  **Isolation:** Each environment created by Wetlands is fully isolated, preventing dependency conflicts between different environments or with the main application's environment.


## 🔀 Worker Pool Architecture

When `max_workers > 1` is passed to `launch()`, Wetlands starts multiple `module_executor` processes, all activating the **same conda environment**. This provides true process-level parallelism without duplicating the environment on disk.

```
                                ┌─ module_executor (pid 1001) ─ port 5001
env.launch(max_workers=4) ──────├─ module_executor (pid 1002) ─ port 5002
  (one conda env on disk)       ├─ module_executor (pid 1003) ─ port 5003
                                └─ module_executor (pid 1004) ─ port 5004
```

Each worker holds its own subprocess, port, IPC connection, and a dedicated reader thread. Tasks are dispatched to idle workers from an internal pool; when all workers are busy, tasks queue and are dispatched as workers become available. A health monitor thread runs alongside the pool, detecting crashed or hung workers and replacing them transparently.

Multi-process is preferred over multi-thread because Wetlands' primary use case is scientific computing (numpy, torch, cellpose, stardist). Separate processes provide true parallelism (no GIL), failure isolation (one crash doesn't kill other tasks), and separate `sys.path`/`sys.argv` per worker. The only cost is memory (~200–400 MB per worker for a typical scientific stack), controlled via `max_workers`.

With `max_workers=1` (the default), the pool is a trivial pass-through: one worker, one connection, one reader thread. Behavior is identical to a single-worker setup.

## ⚙️ Under the Hood


Wetlands uses the `EnvironmentManager.execute_commands()` for different operations (to create environments, install dependencies, etc). 
Behind the scenes, this method creates and executes a temporary script (a bash script on Linux and Mac, and a PowerShell script on Windows) which looks like the following:

```bash
# Initialize Micromamba
cd "/path/to/examples/micromamba"
export MAMBA_ROOT_PREFIX="/path/to/examples/micromamba"
eval "$(micromamba shell hook -s posix)"

# Create the cellpose environment
cd "/Users/amasson/Travail/wetlands/examples"
micromamba --rc-file "/path/to/examples/micromamba/.mambarc" create -n cellpose python=3.12.7 -y

# Activate the environment
cd "/path/to/examples/"
micromamba activate cellpose

# Install the dependencies
echo "Installing conda dependencies..."
micromamba --rc-file "/path/to/examples/micromamba/.mambarc" install "cellpose==3.1.0" -y

# Execute optional custom commands
python -u example_module.py
```
