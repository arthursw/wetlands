import cema._logging
from .environment import Environment, DirectEnvironment, ClientEnvironment
from .dependencies import Dependencies
from .exceptions import ExecutionException, IncompatibilityException
from .environment_manager import EnvironmentManager

logger = cema._logging.getLogger('cema')
environmentManager = EnvironmentManager()