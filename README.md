# Cema

Cema (Conda Environment MAnager) is a library to manage conda environments.

It can create any conda environment, install its dependencies, and execute arbitrary code in them. This is useful to create a plugin system for any application. Since each plugin is isolated in its own environment, there are no dependency conflicts.

## Example usage

```
import cema

cema.initialize(condaPath='path') # if existing conda: use it, otherwise download micromamba

env = cema.launch('envName', ['deps']) # use existing conda
# env.install('libName')
result = env.execute('module.py', 'function', ['arg1', 'arg2'])
# env.uninstall('libName')

# Or directly 
cema.executeCommandsInEnvironment('envName', ['command1'])
cema.executeModuleInEnvironment('envName', 'module.py', 'function', ['arg1', 'arg2'])

env.remove()


```