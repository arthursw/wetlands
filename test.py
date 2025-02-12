
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
