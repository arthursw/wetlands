from wetlands.environment_manager import EnvironmentManager

# Initialize the environment manager
# Wetlands will use the existing Pixi or Micromamba installation at the specified path (e.g., "pixi/" or "micromamba/") if available;
# otherwise it will automatically download and install Pixi or Micromamba in a self-contained manner.
environmentManager = EnvironmentManager("pixi/")

# Create and launch an isolated Conda environment named "numpy"
env = environmentManager.create("numpy", {"pip": ["numpy==2.2.4"]})
env.launch()

# Import minimal_module in the environment, see minimal_module.py below
minimal_module = env.importModule("minimal_module.py")
# minimal_module is a proxy to minimal_module.py in the environment
array = [1, 2, 3]
result = minimal_module.sum(array)

print(f"Sum of {array} is {result}.")

# Clean up and exit the environment
env.exit()
