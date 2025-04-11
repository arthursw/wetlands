
### Simplified Execution with [`env.importModule`][wetlands.environment.Environment.importModule]

To demonstrates the most straightforward way to use Wetlands, we will create an environment, install `cellpose`, and run a segmentation function defined in a separate file ([`example_module.py`](https://github.com/arthursw/wetlands/blob/main/examples/example_module.py)) within that isolated environment.

Let's see the main script [`getting_started.py`](https://github.com/arthursw/wetlands/blob/main/examples/getting_started.py) step by step. 

We will segment the image `img02.png` (available [here](https://www.cellpose.org/static/images/img02.png)).

```python
imagePath = "img02.png"
segmentationPath = "img02_segmentation.png"
```

#### 1. Initialize the Environment Manager

We start by initializing the [EnvironmentManager][wetlands.environment_manager.EnvironmentManager]. We provide a path (`"micromamba/"`) where Wetlands should look for an existing Micromamba installation or where it should download and set up a new one if it's not found.

```python
from wetlands.environment_manager import EnvironmentManager
from pathlib import Path

environmentManager = EnvironmentManager("micromamba/")
```

!!! note
    
    EnvironmentManager also accepts a `mainCondaEnvironmentPath` argument, useful if Wetlands is used in a conda environment (e.g. `environmentManager = EnvironmentManager("micromamba/", "/path/to/project/environment/")`). Wetlands will activate this main environment and check if the installed packages satisfy the requirements when creating new environments. If the required dependencies are already installed in the main environment, EnvironmentManager.create() will return the main enviroment instead of creating a new one. The modules will be called directly, bypassing the Wetlands communication server.

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
diameters = example_module.segment(imagePath, segmentationPath)

print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```


Alternatively, we could use [`env.execute()`][wetlands.environment.Environment.execute] directly:

```python
print(f"Running segmentation on {imagePath}...")
args = (imagePath, segmentationPath)
diameters = env.execute("example_module.py", "segment", args)

print(f"Segmentation complete. Found diameters of {diameters} pixels.")
```

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
    from wetlands.environment_manager import EnvironmentManager

    # Declare our input and output paths
    imagePath = "img02.png"
    segmentationPath = "img02_segmentation.png"

    # Initialize the environment manager
    # Wetlands will use the existing Micromamba installation at the specified path (e.g., "micromamba/") if available;
    # otherwise it will automatically download and install Micromamba in a self-contained manner.
    environmentManager = EnvironmentManager("micromamba/")

    # Create and launch an isolated Conda environment named "cellpose"
    env = environmentManager.create("cellpose", {"conda": ["cellpose==3.1.0"]})
    env.launch()

    # Import example_module in the environment
    example_module = env.importModule("example_module.py")
    # example_module is a proxy to example_module.py in the environment,
    # calling example_module.function_name(args) will run env.execute(module_name, function_name, args)
    diameters = example_module.segment(imagePath, segmentationPath)

    # Or use env.execute() directly
    # diameters = env.execute("example_module.py", "segment", (imagePath, segmentationPath))

    print(f"Found diameters of {diameters} pixels.")

    # Clean up and exit the environment
    env.exit()
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

Crucially, the necessary libraries (`cellpose`, `numpy`) are imported *within this function*, meaning they are resolved using the packages installed inside the isolated `cellpose_env`, not the main script's environment. This is important to enable the main script to import `example_module.py` without raising a `ModuleNotFoundError`. In this way, the main script can see the functions defined in `example_module.py`. This is only necessary when using the proxy object ([`env.importModule("example_module.py")`][wetlands.environment.Environment.importModule] then `example_module.function(args)`) but it is not required when using [`env.execute("example_module.py", "function", (args))`][wetlands.environment.Environment.execute] directly.

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
        global model

        input_image = Path(input_image)
        if not input_image.exists():
            raise Exception(f"Error: input image {input_image} does not exist.")

        print(f"[[1/4]] Load libraries and model {model_type}")
        print("Loading libraries...")
        import cellpose.models  # type: ignore
        import cellpose.io  # type: ignore
        import numpy as np  # type: ignore

        if model is None or model.cp.model_type != model_type:
            print("Loading model...")
            model = cellpose.models.Cellpose(gpu=True if use_gpu == "True" else False, model_type=model_type)

        print(f"[[2/4]] Load image {input_image}")
        image = cast(np.ndarray, cellpose.io.imread(str(input_image)))

        print("[[3/4]] Compute segmentation", image.shape)
        try:
            kwargs: Any = dict(diameter=int(diameter)) if auto_diameter else {}
            masks, flows, styles, diams = model.eval(image, channels=channels, **kwargs)
        except Exception as e:
            print(e)
            raise e
        print("segmentation finished.")

        segmentation = Path(segmentation)
        print(f"[[4/4]] Save segmentation {segmentation}")
        # save results as png
        cellpose.io.save_masks(image, masks, flows, str(input_image), png=True)
        output_mask = input_image.parent / f"{input_image.stem}_cp_masks.png"
        if output_mask.exists():
            if segmentation.exists():
                segmentation.unlink()
            (output_mask).rename(segmentation)
            print(f"Saved out: {segmentation}")
        else:
            print("Segmentation was not generated because no masks were found.")
        return diams

    ```

#### Summary of Example 1 Flow:

The main script uses [`EnvironmentManager`][wetlands.environment_manager.EnvironmentManager] to prepare an isolated environment. [`env.launch()`][wetlands.environment_manager.Environment.launch] starts a hidden server in that environment. [`env.importModule()`][wetlands.environment.Environment.importModule] provides a proxy, and calling functions on the proxy executes the code (like `example_module.segment`) within the isolated environment, handling data transfer automatically. [`env.exit()`][wetlands.environment.Environment.exit] cleans up the server process.
