import threading
import subprocess
from importlib import import_module
from abc import abstractmethod
from multiprocessing.connection import Client
import psutil
from cema import logger, ExecutionException

class Environment():
	
	def __init__(self, name) -> None:
		self.name = name
		self.process = None
		self.installedDependencies = {}

	@abstractmethod
	def execute(self, module:str, function: str, args: list):
		return
	
	@abstractmethod
	def _exit(self):
		return

	def launched(self):
		return True
		
class ClientEnvironment(Environment):
	def __init__(self, name, port, process: subprocess.Popen) -> None:
		super().__init__(name)
		self.port = port
		self.process = process
		self.stopEvent = threading.Event()
		self.connection = None

	def initialize(self):
		self.connection = Client(('localhost', self.port))
	
	def execute(self, module:str, function: str, args: list):
		if self.connection.closed:
			logger.warning(f'Connection not ready. Skipping execute {module}.{function}({args})')
			return
		try:
			self.connection.send(dict(action='execute', module=module, function=function, args=args))
			while message := self.connection.recv():
				if message['action'] == 'execution finished':
					logger.info('execution finished')
					return message['result'] if 'result' in message else None
				elif message['action'] == 'error':
					raise ExecutionException(message)
				else:
					logger.warning('Got an unexpected message: ', message)
		# If the connection was closed (subprocess killed): catch and ignore the exception, otherwise: raise it
		except EOFError:
			print("Connection closed gracefully by the peer.")
		except BrokenPipeError as e:
			print("Broken pipe. The peer process might have terminated.")
		# except (PicklingError, TypeError) as e:
		# 	print(f"Failed to serialize the message: {e}")
		except OSError as e:
			if e.errno == 9:  # Bad file descriptor
				print("Connection closed abruptly by the peer.")
			else:
				print(f"Unexpected OSError: {e}")
				raise e
	
	def launched(self):
		return self.process.poll() is None and self.connection is not None and self.connection.writable and self.connection.readable and not self.connection.closed
	
	def _exit(self):
		if self.connection is not None:
			try:
				self.connection.send(dict(action='exit'))
			except OSError as e:
				if e.args[0] == 'handle is closed': pass
			self.connection.close()
		self.stopEvent.set()

		# Terminate the process and its children
		parent = psutil.Process(self.process.pid)
		for child in parent.children(recursive=True):  # Get all child processes
			if child.is_running():
				child.kill()
		if parent.is_running():
			parent.kill()

		return
	
class DirectEnvironment(Environment):
	def __init__(self, name) -> None:
		super().__init__(name)
		self.modules = {}

	def execute(self, module:str, function: str, args: list):
		if module not in self.modules:
			self.modules[module] = import_module(module)
		if not hasattr(self.modules[module], function):
			raise Exception(f'Module {module} has no function {function}.')
		return getattr(self.modules[module], function)(*args)

	def _exit(self):
		return
