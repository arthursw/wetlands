# Wetlands Logging Guide

Wetlands provides a comprehensive logging system that tracks operations across environments with rich context metadata. This guide shows you how to integrate and customize logging in your applications.

## Table of Contents

- [Overview](#overview)
- [Log Context](#log-context)
- [Basic Usage](#basic-usage)
- [Advanced Examples](#advanced-examples)
  - [Filtering Logs by Context](#filtering-logs-by-context)
  - [Per-Execution Log Files](#per-execution-log-files)
  - [GUI Integration](#gui-integration)
  - [Custom Log Processing](#custom-log-processing)

---

## Overview

Wetlands automatically logs all operations (environment creation, installation, execution) with rich context metadata:

- By default, logs are written to `"wetlands/wetlands.log"` file (when using `EnvironmentManager`)
- Use `logging.basicConfig()` or add handlers to enable console output
- Most logs include context fields (environment name, operation type, etc.)
- ProcessLogger reads subprocess output in background threads for real-time logging

!!! note
    By default, `execute_commands()` functions read process stdout in a background thread via ProcessLogger. If you need to read stdout manually, pass `log=False` to disable automatic logging.

## Log Context

Every log record in Wetlands includes metadata that helps track operations. This metadata is stored in the LogRecord's attributes and can be accessed via custom handlers and filters.

1. **Global** - General application operations
   ```python
   {
       "log_source": "global",
       "stage": None
   }
   ```

2. **Environment** - Environment creation, installation, launching
   ```python
   {
       "log_source": "environment",
       "env_name": "cellpose",           # Environment name
       "stage": "create"                 # One of: "create", "install", "launch"
   }
   ```

3. **Execution** - Function/script execution within environments
   ```python
   {
       "log_source": "execution",
       "env_name": "cellpose",
       "call_target": "segment:detect"   # Format: "module:function" or "script.py"
   }
   ```

## Basic Usage

### Default Behavior

By default, when you create an `EnvironmentManager`, it automatically enables logging to `"wetlands/wetlands.log"`:

```python
from wetlands.environment_manager import EnvironmentManager


# To enable console logging: use basicConfig (simplest)
logging.basicConfig(level=logging.INFO)

# You can also add a console handler manually
logging.getLogger("wetlands").addHandler(logging.StreamHandler())
logging.getLogger("wetlands").setLevel(logging.INFO)


# Logs are automatically written to "wetlands/wetlands.log"
env_manager = EnvironmentManager()
# Change log path with:
# env_manager = EnvironmentManager(log_file_path=Path("my_logs/operation.log"))
# Disable file logging with:
# env_manager = EnvironmentManager(log_file_path=None)
env = env_manager.create("cellpose", {"conda": ["cellpose==3.1.0"]})
env.launch()

# All operations are now logged to wetlands/wetlands.log
```

## Advanced Examples

### Per-Execution Log Files

Capture logs from individual function/script executions to separate files. Here's a simple context manager that routes all logs during execution to a file:

```python
from pathlib import Path
from contextlib import contextmanager
from wetlands.environment_manager import EnvironmentManager
import logging

@contextmanager
def capture_execution_logs(output_file: Path):
    """Context manager to capture all logs during execution to a file."""
    logger = logging.getLogger("wetlands")
    handler = logging.FileHandler(output_file)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(handler)

    try:
        yield
    finally:
        logger.removeHandler(handler)
        handler.close()

# Usage: route logs from different executions to different files
env_manager = EnvironmentManager()
env = env_manager.create("analysis", {"conda": ["pandas", "scikit-learn"]})
env.launch()

with capture_execution_logs(Path("preprocess.log")):
    env.execute("analysis.py", "preprocess", args=("data.csv",))

with capture_execution_logs(Path("train.log")):
    env.execute("analysis.py", "train_model", args=(50,))

with capture_execution_logs(Path("evaluate.log")):
    env.execute("analysis.py", "evaluate")
```

You can also use Wetlands ProcessLogger:

```python

# Retrieve the ProcessLogger that was created by execute_commands
process_logger = self.environment_manager.get_process_logger(env.process.pid)

# Subscribe to the process output
def check_output(line: str, _context: dict) -> None:
    if "Special message" in line:
        print(line)

# Be aware of the include_history arg to apply the callback on the entire log history, or only the futur logs
process_logger.subscribe(check_output, include_history=False)

# Wait for port announcement with timeout
def port_predicate(line: str) -> bool:
    return line.startswith("Listening port ")

port_line = process_logger.wait_for_line(port_predicate, timeout=30)

if port_line:
    port = int(port_line.replace("Listening port ", ""))
    connection = Client(("localhost", self.port))

```

If you want to capture only logs from a specific execution (filtering by `call_target`), use a filter:

```python
@contextmanager
def capture_execution_logs_filtered(env_name: str, call_target: str, output_file: Path):
    """Context manager that captures only logs from a specific execution."""
    logger = logging.getLogger("wetlands")
    handler = logging.FileHandler(output_file)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    def filter_execution(record):
        return (getattr(record, "log_source") == "execution" and
            getattr(record, "env_name") == env_name and
            getattr(record, "call_target") == call_target
        )

    handler.addFilter(filter_execution)
    logger.addHandler(handler)

    try:
        yield
    finally:
        logger.removeHandler(handler)
        handler.close()

# Usage with filtering
with capture_execution_logs_filtered("analysis", "preprocess:run", Path("preprocess.log")):
    env.execute("preprocess.py", "run", args=("data.csv",))
```

### Filtering Logs by Context

Route different log types to separate files:

```python
import logging
from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import enable_file_logging

# Enable main log file
enable_file_logging(Path("wetlands.log"))

# Get the wetlands logger
logger = logging.getLogger("wetlands")

# Create separate handlers for different log sources
env_handler = logging.FileHandler("environment.log")
exec_handler = logging.FileHandler("execution.log")

# Create filters
def filter_environment(record):
    return getattr(record, "log_source", None) == "environment"

def filter_execution(record):
    return getattr(record, "log_source", None) == "execution"

# Add filters and attach handlers
env_handler.addFilter(filter_environment)
exec_handler.addFilter(filter_execution)

logger.addHandler(env_handler)
logger.addHandler(exec_handler)

# Now operations are routed to appropriate files
env_manager = EnvironmentManager()
env = env_manager.create("analysis", {"conda": ["numpy", "pandas"]})  # → environment.log
env.launch()                                                            # → environment.log
result = env.execute("process.py", "analyze", args=("data.csv",))     # → execution.log
```

**Result:**
```
wetlands.log      # All logs (environment + execution)
environment.log   # Only environment operations
execution.log     # Only function/script executions
```

### GUI Integration

Display real-time logs in a GUI. **Important:** Log callbacks run in background threads, so use thread-safe mechanisms.

**Tkinter Example:**
```python
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from queue import Queue
import threading

from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import attach_log_handler

class LogViewer:
    def __init__(self, root):
        self.root = root
        self.log_queue = Queue()  # Thread-safe queue

        self.log_text = ScrolledText(root, height=20, width=80)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

        # Attach logging handler - runs in background thread
        attach_log_handler(self.on_log)

        # Poll queue from main thread
        self.poll_queue()

    def on_log(self, message):
        """Called from ProcessLogger thread - queue the message."""
        self.log_queue.put(message)

    def poll_queue(self):
        """Process queued messages on main thread."""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.log_text.insert("end", f"{message}\n")
                self.log_text.see("end")
        except:
            pass
        # Poll again after 100ms
        self.root.after(100, self.poll_queue)

if __name__ == "__main__":
    root = tk.Tk()
    root.title("Wetlands Operations")
    viewer = LogViewer(root)

    # Run operations in background thread
    def run_ops():
        env_mgr = EnvironmentManager()
        env = env_mgr.create("demo", {"conda": ["numpy"]})
        env.launch()
        env.execute("script.py", "main")

    threading.Thread(target=run_ops, daemon=True).start()
    root.mainloop()
```

**PyQt6 Example:**
```python
from PyQt6.QtWidgets import QApplication, QMainWindow, QTextEdit
from PyQt6.QtCore import pyqtSignal, QObject
import threading

from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import attach_log_handler

class LogSignals(QObject):
    log_signal = pyqtSignal(str)  # Signal for thread-safe communication

class LogViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.signals = LogSignals()
        self.text_edit = QTextEdit()
        self.setCentralWidget(self.text_edit)
        self.setWindowTitle("Wetlands Operations")
        self.setGeometry(100, 100, 800, 600)

        # Connect signal to slot (main thread)
        self.signals.log_signal.connect(self.append_log)

        # Attach handler - runs in background thread
        attach_log_handler(self.on_log)

    def on_log(self, message):
        """Called from ProcessLogger thread - emit signal."""
        self.signals.log_signal.emit(message)

    def append_log(self, message):
        """Called on main thread (slot)."""
        self.text_edit.append(message)

if __name__ == "__main__":
    app = QApplication([])
    viewer = LogViewer()
    viewer.show()

    # Run operations in background
    def run_ops():
        env_mgr = EnvironmentManager()
        env = env_mgr.create("demo", {"conda": ["numpy"]})
        env.launch()
        env.execute("script.py", "main")

    threading.Thread(target=run_ops, daemon=True).start()
    app.exec()
```

## Tips & Tricks

4. **Be thread-safe** when updating UI from log callbacks - use queues or signals