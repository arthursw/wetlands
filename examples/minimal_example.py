from wetlands.environment_manager import EnvironmentManager

# Initialize the environment manager
# Wetlands will store logs and state in the wetlandsInstancePath (defaults to "wetlands/")
# Pixi/Micromamba will be installed in wetlandsInstancePath/pixi by default
environmentManager = EnvironmentManager()

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
