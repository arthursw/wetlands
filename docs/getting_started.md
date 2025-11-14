
### Simplified Execution with [`env.importModule`][wetlands.environment.Environment.importModule]

To demonstrates the most straightforward way to use Wetlands, we will create an environment, install `cellpose`, and run a segmentation function defined in a separate file ([`example_module.py`](https://github.com/arthursw/wetlands/blob/main/examples/example_module.py)) within that isolated environment.

Let's see the main script [`getting_started.py`](https://github.com/arthursw/wetlands/blob/main/examples/getting_started.py) step by step. 

We will segment the image `img02.png` (available [here](https://www.cellpose.org/static/images/img02.png)).

```python
from pathlib import Path
imagePath = Path("img02.png")
segmentationPath = imagePath.parent / f"{imagePath.stem}_segmentation.png"
```

#### 1. Initialize the Environment Manager

We start by initializing the [EnvironmentManager][wetlands.environment_manager.EnvironmentManager]. We provide:
- A `wetlandsInstancePath` where Wetlands stores logs and debug information (defaults to `"wetlands/"`).
- Optionally, a `condaPath` where Wetlands should look for an existing Pixi (or Micromamba) installation or where it should download and set up a new one. If not provided, it defaults to `wetlandsInstancePath / "pixi"`.

```python
from wetlands.environment_manager import EnvironmentManager

environmentManager = EnvironmentManager()
# Or with explicit paths:
# environmentManager = EnvironmentManager(
#     wetlandsInstancePath="wetlands_state",
#     condaPath="path/to/pixi/"
# )
```

!!! note

    EnvironmentManager also accepts a `mainCondaEnvironmentPath` argument, useful if Wetlands is used in a conda environment (e.g. `environmentManager = EnvironmentManager(mainCondaEnvironmentPath="/path/to/project/environment/")`). Wetlands will activate this main environment and check if the installed packages satisfy the requirements when creating new environments. If the required dependencies are already installed in the main environment, EnvironmentManager.create() will return the main enviroment instead of creating a new one. The modules will be called directly, bypassing the Wetlands communication server.

!!! warning

    On Windows, spaces are not allowed in the `condaPath` argument of `EnvironmentManager()`.

#### 2. Create (or get) an Environment and Install Dependencies

Next, we define and create the Conda environment. We give it a name (`"cellpose_env"`) and specify its dependencies using a dictionary. Here, we require `cellpose` version 3.1.0, to be installed via Conda. If an environment with this name already exists, Wetlands will use it (and *ignore the dependencies*); otherwise, it will create it and install the specified dependencies. The `create` method returns an `Environment` object.

```python
env = environmentManager.create(
    "cellpose_env",
    {"conda": ["cellpose==3.1.0"]}
)
```

!!! note

    If a `mainCondaEnvironmentPath` was provided when instanciating the `EnvironmentManager`, Wetlands will check if `cellpose==3.1.0` is already installed in the main environment and return it if it is the case. If `mainCondaEnvironmentPath` is not provided but the required dependencies are only pip packages, Wetlands will check if the dependencies are installed in the current python environment and return it if it is the case.

!!! note "Reusing existing environments with `useExisting=True`"

    You can pass `useExisting=True` to `create()` to search for and reuse an existing environment that satisfies the dependencies. This includes the main environment. If no environment satisfies the requirements, a new one will be created. By default, `useExisting=False`, which always creates a new environment.

    ```python
    # Return main or existing environment if it satisfies the dependencies
    env = environmentManager.create(
        "cellpose_env",
        {"conda": ["cellpose==3.1.0"]},
        useExisting=True  # Check if any existing env satisfies the dependencies
    )
    ```

!!! note "Specifying dependencies"

    See the [dependencies page](dependencies.md) to learn more on specifying dependencies.
    Wetlands supports [PEP 440 version specifiers](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#version-specifiers), so you can use flexible version constraints like `"numpy>=1.20,<2.0"`, `"scipy~=1.5"`, or `"pandas!=1.0.0"`.
    You can also use [`EnvironmentManager.createFromConfig()`][wetlands.environment_manager.EnvironmentManager.createFromConfig] and provide a `requirements.txt`, `environment.yml`, `pyproject.toml` or `pixi.toml` file for your dependencies.

!!! note "Load an existing environment"

    You can also load an existing environment with `environment.load("env_name", Path("Path/to/existing/environment/pyproject.toml"))`. See [`EnvironmentManager.load()`][wetlands.environment_manager.EnvironmentManager.load].


#### 3. Launch the Environment's Communication Server

For Wetlands to execute code within the isolated environment (using [`importModule`][wetlands.environment.Environment.importModule] or [`execute`][wetlands.environment.Environment.execute]), we need to launch its background communication server. This server runs as a separate process *inside* the `cellpose_env` and listens for commands from our main script.

```python
env.launch()
```

#### 4. Import and Execute Code in the Environment via Proxy

This is where the core Wetlands interaction happens. We use [`env.importModule("example_module.py")`][wetlands.environment.Environment.importModule] to gain access to the functions defined in `example_module.py`. Wetlands doesn't actually import the module into the main process; instead, it returns a *proxy object*. When we call a method on this proxy object (like `example_module.segment(...)`), Wetlands intercepts the call, sends the function name and arguments to the server running in the `cellpose_env`, executes the *real* function there, and returns the result back to the main script. File paths and other pickleable arguments are automatically transferred.

```python
print("Importing module in environment...")
example_module = env.importModule("example_module.py")

print(f"Running segmentation on {imagePath}...")
diameters = example_module.segment(str(imagePath), str(segmentationPath))

print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```


Alternatively, we could use [`env.execute()`][wetlands.environment.Environment.execute] directly:

```python
print(f"Running segmentation on {imagePath}...")
args = (str(imagePath), str(segmentationPath))
diameters = env.execute("example_module.py", "segment", args)

print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```

!!! note "Function arguments must be serializable"

    The arguments of the segment function will be send to the other process via [`multiprocessing.connection.Connection.send()`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection.send) so the objects must be picklable.

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
    inputImage: Path | str,
    segmentation: Path | str,
    modelType="cyto",
    useGPU=False,
    channels=[0, 0],
    autoDiameter=True,
    diameter=30,
):
    """Performs cell segmentation using Cellpose."""
    global model

    inputImage = Path(inputImage)
    if not inputImage.exists():
        raise FileNotFoundError(f"Error: input image {inputImage}"\
                                "does not exist.")
```

#### Import Dependencies (Inside the Environment)

Crucially, the necessary libraries (`cellpose`, `numpy`) are imported *within this function*, meaning they are resolved using the packages installed inside the isolated `cellpose_env`, not the main script's environment. This is important to enable the main script to import `example_module.py` without raising a `ModuleNotFoundError`. In this way, the main script can see the functions defined in `example_module.py`. This is only necessary when using the proxy object ([`env.importModule("example_module.py")`][wetlands.environment.Environment.importModule] then `example_module.function(args)`) but it is not required when using [`env.execute("example_module.py", "function", (args))`][wetlands.environment.Environment.execute] directly.

```python
    print(f"[[1/4]] Load libraries and model '{modelType}'")
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
    if model is None or model.cp.model_type != modelType:
        print("Loading model...")
        gpu_flag = str(useGPU).lower() == 'true'
        model = cellpose.models.Cellpose(gpu=gpu_flag, model_type=modelType)

    print(f"[[2/4]] Load image {inputImage}")
    image = cast(np.ndarray, cellpose.io.imread(str(inputImage)))
```

#### Perform Segmentation

The core segmentation task is performed using the loaded model and image. Any exceptions raised here will be captured by Wetlands and re-raised in the main script.

```python
    print(f"[[3/4]] Compute segmentation for image shape {image.shape}")
    try:
        kwargs: Any = dict(diameter=int(diameter)) if autoDiameter else {}
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

    cellpose.io.save_masks(image, masks, flows, str(inputImage), png=True)
    default_output = inputImage.parent / f"{inputImage.stem}_cp_masks.png"

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

The main script uses [`EnvironmentManager`][wetlands.environment_manager.EnvironmentManager] to prepare an isolated environment. [`env.launch()`][wetlands.environment_manager.Environment.launch] starts a hidden server in that environment. [`env.importModule()`][wetlands.environment.Environment.importModule] provides a proxy, and calling functions on the proxy executes the code (like `example_module.segment`) within the isolated environment, handling data transfer automatically. [`env.exit()`][wetlands.environment.Environment.exit] cleans up the server process.
