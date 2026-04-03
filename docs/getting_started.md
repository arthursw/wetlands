
### Executing Code in an Environment

Wetlands provides two ways to execute functions in an isolated environment:

- **[`env.submit()`][wetlands.environment.Environment.submit]** returns a **[`Task[T]`][wetlands.task.Task]** immediately, letting you monitor progress, cancel execution, or wait for the result at your convenience.
- **[`env.execute()`][wetlands.environment.Environment.execute]** and **[`env.import_module()`][wetlands.environment.Environment.import_module]** are blocking shortcuts that wait for the result before returning — convenient when you don't need cancellation.

To demonstrate, we will create an environment, install `cellpose`, and run a segmentation function defined in a separate file ([`example_module.py`](https://github.com/arthursw/wetlands/blob/main/examples/example_module.py)) within that isolated environment.

Let's see the main script [`getting_started.py`](https://github.com/arthursw/wetlands/blob/main/examples/getting_started.py) step by step. 

We will segment the image `img02.png` (available [here](https://www.cellpose.org/static/images/img02.png)).

```python
from pathlib import Path
image_path = Path("img02.png")
segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.png"
```

#### 1. Initialize the Environment Manager

We start by initializing the [EnvironmentManager][wetlands.environment_manager.EnvironmentManager]. We provide:

- A `wetlands_instance_path` where Wetlands stores logs and debug information (defaults to `"wetlands/"`).
- Optionally, a `conda_path` where Wetlands should look for an existing Pixi (or Micromamba) installation or where it should download and set up a new one. If not provided, it defaults to `wetlands_instance_path / "pixi"`.

```python
from wetlands.environment_manager import EnvironmentManager

environment_manager = EnvironmentManager()
# Or with explicit paths:
# environment_manager = EnvironmentManager(
#     wetlands_instance_path="wetlands_state",
#     conda_path="path/to/pixi/"
# )
```

!!! note

    EnvironmentManager also accepts a `main_conda_environment_path` argument, useful if Wetlands is used in a conda environment (e.g. `environment_manager = EnvironmentManager(main_conda_environment_path="/path/to/project/environment/")`). Wetlands will activate this main environment and check if the installed packages satisfy the requirements when creating new environments. If the required dependencies are already installed in the main environment, EnvironmentManager.create() will return the main enviroment instead of creating a new one. The modules will be called directly, bypassing the Wetlands communication server.

!!! warning

    On Windows, spaces are not allowed in the `conda_path` argument of `EnvironmentManager()`.

#### 2. Create (or get) an Environment and Install Dependencies

Next, we define and create the Conda environment. We give it a name (`"cellpose_env"`) and specify its dependencies using a dictionary. Here, we require `cellpose` version 3.1.0, to be installed via Conda. If an environment with this name already exists, Wetlands will use it (and *ignore the dependencies*); otherwise, it will create it and install the specified dependencies. The `create` method returns an `Environment` object.

```python
env = environment_manager.create(
    "cellpose_env",
    {"conda": ["cellpose==3.1.0"]}
)
```

!!! note

    If a `main_conda_environment_path` was provided when instanciating the `EnvironmentManager`, Wetlands will check if `cellpose==3.1.0` is already installed in the main environment and return it if it is the case. If `main_conda_environment_path` is not provided but the required dependencies are only pip packages, Wetlands will check if the dependencies are installed in the current python environment and return it if it is the case.

!!! note "Reusing existing environments with `use_existing=True`"

    You can pass `use_existing=True` to `create()` to search for and reuse an existing environment that satisfies the dependencies. This includes the main environment. If no environment satisfies the requirements, a new one will be created. By default, `use_existing=False`, which always creates a new environment.

    ```python
    # Return main or existing environment if it satisfies the dependencies
    env = environment_manager.create(
        "cellpose_env",
        {"conda": ["cellpose==3.1.0"]},
        use_existing=True  # Check if any existing env satisfies the dependencies
    )
    ```

!!! note "Specifying dependencies"

    See the [dependencies page](dependencies.md) to learn more on specifying dependencies.
    Wetlands supports [PEP 440 version specifiers](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#version-specifiers), so you can use flexible version constraints like `"numpy>=1.20,<2.0"`, `"scipy~=1.5"`, or `"pandas!=1.0.0"`.
    You can also use [`EnvironmentManager.create_from_config()`][wetlands.environment_manager.EnvironmentManager.create_from_config] and provide a `requirements.txt`, `environment.yml`, `pyproject.toml` or `pixi.toml` file for your dependencies.

!!! note "Load an existing environment"

    You can also load an existing environment with `environment.load("env_name", Path("Path/to/existing/environment/pyproject.toml"))`. See [`EnvironmentManager.load()`][wetlands.environment_manager.EnvironmentManager.load].


#### 3. Launch the Environment's Worker Processes

For Wetlands to execute code within the isolated environment, we need to launch its worker processes. Each worker runs *inside* the `cellpose_env` and listens for commands from our main script.

```python
env.launch()
```

!!! note

    By default, `launch()` starts a single worker process. For parallel execution, you can start multiple workers sharing the same Conda environment:

    ```python
    env.launch(max_workers=4)  # 4 worker processes, same conda env on disk
    ```

    You can also assign specific environment variables per worker (e.g. for GPU assignment):

    ```python
    env.launch(max_workers=4, worker_env=lambda i: {"CUDA_VISIBLE_DEVICES": str(i)})
    ```

#### 4. Execute Code in the Environment

##### Non-blocking execution with `submit()`

[`env.submit()`][wetlands.environment.Environment.submit] sends a function call to a worker and returns a [`Task`][wetlands.task.Task] object immediately. You can then monitor progress, attach listeners, or wait for the result:

```python
task = env.submit("example_module.py", "segment",
                  args=(str(image_path), str(segmentation_path)))

# Do other work while segmentation runs...
print(f"Task status: {task.status}")

# Block until the result is ready
task.wait_for()
diameters = task.result
print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```

Tasks support progress reporting, cooperative cancellation, event listeners, context managers, `concurrent.futures.Future` interop, and `async/await`. See [Tasks and parallel execution](tasks.md) for the full API.

##### Blocking shortcuts: `import_module()` and `execute()`

For simple cases where you just need the result, Wetlands provides blocking shortcuts.

[`env.import_module()`][wetlands.environment.Environment.import_module] returns a *proxy object*. When you call a method on this proxy (like `example_module.segment(...)`), Wetlands sends the function name and arguments to the worker running in the `cellpose_env`, executes the function there, and returns the result:

```python
example_module = env.import_module("example_module.py")
diameters = example_module.segment(str(image_path), str(segmentation_path))
print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```


[`env.execute()`][wetlands.environment.Environment.execute] calls a function directly without a proxy object:

```python
args = (str(image_path), str(segmentation_path))
diameters = env.execute("example_module.py", "segment", args)
print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```

!!! tip "Tracking progress with blocking calls"

    Even when using the blocking API, you can track what happens inside the environment through Wetlands' [logging system](logging.md). The environment's stdout is captured in real time and routed through Python's `logging` module, so `print()` statements in remote code appear as log messages that you can filter, redirect, or display in a GUI. See [Logging](logging.md) for details.

!!! note "Function arguments must be serializable"

    The arguments of the segment function will be sent to the other process via [`multiprocessing.connection.Connection.send()`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection.send) so the objects must be picklable.

#### 5. Clean Up

Finally, we tell Wetlands to shut down the communication server and clean up resources associated with the launched environment.

```python
print("Exiting environment...")
env.exit()

print("Done.")
```

---

??? note "`getting_started.py` source code"

    ```python
    {% include "../examples/getting_started.py" %}
    ```

Now, let's look at the [`example_module.py`](https://github.com/arthursw/wetlands/blob/main/examples/example_module.py) file. This code contains the actual segmentation logic and is executed *inside* the isolated `cellpose_env` when called via the proxy object.


#### Define the Segmentation Function

The module defines a `segment` function that takes input/output paths and other parameters. It uses a global variable `model` to potentially cache the loaded Cellpose model between calls within the same environment process lifetime.

```python
# example_module.py
from pathlib import Path
from typing import Any, cast

model = None

def segment(
    input_image: Path | str,
    segmentation: Path | str,
    model_type="cyto",
    use_gpu=False,
    channels=[0, 0],
    auto_diameter=True,
    diameter=30,
):
    """Performs cell segmentation using Cellpose."""
    global model

    input_image = Path(input_image)
    if not input_image.exists():
        raise FileNotFoundError(f"Error: input image {input_image}"\
                                "does not exist.")
```

#### Import Dependencies (Inside the Environment)

Crucially, the necessary libraries (`cellpose`, `numpy`) are imported *within this function*, meaning they are resolved using the packages installed inside the isolated `cellpose_env`, not the main script's environment. This is important to enable the main script to import `example_module.py` without raising a `ModuleNotFoundError`. In this way, the main script can see the functions defined in `example_module.py`. This is only necessary when using the proxy object ([`env.import_module("example_module.py")`][wetlands.environment.Environment.import_module] then `example_module.function(args)`) but it is not required when using [`env.execute("example_module.py", "function", (args))`][wetlands.environment.Environment.execute] directly.

```python
    print(f"[[1/4]] Load libraries and model '{model_type}'")
    import cellpose.models
    import cellpose.io
    import numpy as np
```

!!! note "Using try catch to prevent `ModuleNotFoundError`"

    A better approach is to use a try statement at the beginning of `example_module.py` to fail silently when importing modules which are not accessible in the main environment, like so:

    ```python
    try:
        import cellpose.models
        import cellpose.io
        import numpy as np
    except ModuleNotFoundError:
        pass
    ...
    ```

    This allows:
     - to access the function definitions in the main environment (even if we won't be able to execute them in the main environment),
     - to import the modules for all functions defined in `example_module.py` in the `cellpose_env`.

#### Load Model and Process Image

The code proceeds to load the Cellpose model (if not already cached) and the input image. All this happens within the context of the `cellpose_env`.

```python
    if model is None or model.cp.model_type != model_type:
        print("Loading model...")
        gpu_flag = str(use_gpu).lower() == 'true'
        model = cellpose.models.Cellpose(gpu=gpu_flag, model_type=model_type)

    print(f"[[2/4]] Load image {input_image}")
    image = cast(np.ndarray, cellpose.io.imread(str(input_image)))
```

#### Perform Segmentation

The core segmentation task is performed using the loaded model and image. Any exceptions raised here will be captured by Wetlands and re-raised in the main script.

```python
    print(f"[[3/4]] Compute segmentation for image shape {image.shape}")
    try:
        kwargs: Any = dict(diameter=int(diameter)) if auto_diameter else {}
        masks, _, _, diams = model.eval(image, channels=channels, **kwargs)
    except Exception as e:
        print(f"Error during segmentation: {e}")
        raise e
    print("Segmentation finished (inside environment).")
```

#### Save Results and Return Value

The segmentation results (masks) are saved to disk, potentially renaming the output file. The function then returns the calculated cell diameters (`diams`). This return value is serialized by Wetlands and sent back to the main script.

```python

    if segmentation is None:                # If segmentation is None: return all results
        return masks, flows, styles, diams

    segmentation_path = Path(segmentation)
    print(f"[[4/4]] Save segmentation to {segmentation_path}")

    cellpose.io.save_masks(image, masks, flows, str(input_image), png=True)
    default_output = input_image.parent / f"{input_image.stem}_cp_masks.png"

    if default_output.exists():
        if segmentation_path.exists():
            segmentation_path.unlink()
        default_output.rename(segmentation_path)
        print(f"Saved mask: {segmentation_path}")
    else:
        print("Warning: Segmentation mask file was not generated by cellpose.")

    return diams
```

??? note "`example_module.py` source code"

    ```python
    {% include "../examples/example_module.py" %}
    ```


#### Summary of Example 1 Flow:

The main script uses [`EnvironmentManager`][wetlands.environment_manager.EnvironmentManager] to prepare an isolated environment. [`env.launch()`][wetlands.environment.Environment.launch] starts one or more worker processes inside that environment. [`env.submit()`][wetlands.environment.Environment.submit] dispatches work and returns a [`Task`][wetlands.task.Task] for non-blocking control, while [`env.import_module()`][wetlands.environment.Environment.import_module] and [`env.execute()`][wetlands.environment.Environment.execute] provide blocking shortcuts. [`env.exit()`][wetlands.environment.Environment.exit] cleans up all worker processes.

### Next Steps

- See [Tasks and parallel execution](tasks.md) for the full task API: progress, cancellation, events, parallel workers
- See [Shared memory](shared_memory.md) to share memory between environments
- See [Manual communication](manual_communication.md) for low-level control over environment processes
- See [Wetlands logging system](logging.md)
- See [Debugging Guide](debugging.md) to understand how to debug within environments