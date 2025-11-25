# Wetlands Logging Guide

Wetlands provides a rich logging system that emits logs in real-time with context metadata. This guide shows you how to integrate logging into your applications.

## Table of Contents

- [Log Context](#log-context)
- [Simple Examples](#simple-examples)
  - [Basic Logging and Filtering](#basic-logging-and-filtering)
- [Advanced Examples](#advanced-examples)
  - [Per-Execution Log Files](#per-execution-log-files)
  - [GUI Integration](#gui-integration)
  - [Custom Callback Processing](#custom-callback-processing)

---

## Log Context

Every log record in Wetlands includes context metadata that helps you track where logs come from and what operation generated them. This metadata is passed through the entire logging system and allows you to filter and route logs intelligently.

### Understanding log_context

The `log_context` is a dictionary attached to each log record under the `extra` field. You access it in log filters and handlers like this:

```python
def my_filter(record):
    extra = getattr(record, 'extra', {})
    log_source = extra.get("log_source")
    env_name = extra.get("env_name")
    call_target = extra.get("call_target")
    # ... filter logic ...
```

### Context Types

**Global operations** (setup, cleanup, general tasks):
```python
{
    "log_source": "global",
}
```

**Environment operations** (create, install, launch):
```python
{
    "log_source": "environment",
    "env_name": "cellpose",           # Name of the environment
    "stage": "create",                # One of: "create", "install", "launch"
}
```

**Execution operations** (running functions or scripts):
```python
{
    "log_source": "execution",
    "env_name": "cellpose",           # Name of the environment
    "call_target": "segment:detect",  # Either "module:function" or "script.py" format
}
```

### call_target Format

The `call_target` field dynamically changes based on what's executing:

- During `execute("segment.py", "detect")`: `call_target = "segment:detect"`
- During `runScript("process.py")`: `call_target = "process.py"`
- During environment launch: `call_target = "module_executor"` (internal process)


## Simple Examples

### Basic Logging and Filtering

By default, Wetlands logs to the console and a log file. You can customize the log file location and route different operation types to separate files:

```python
from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import logger, setLogFilePath
import logging

# Set custom main log file location
setLogFilePath(Path("my_wetlands.log"))

# Optionally: Create separate handlers for different log sources
env_file = logging.FileHandler("environment.log")
exec_file = logging.FileHandler("execution.log")

# Create filters to separate log types
def filter_environment(record):
    extra = getattr(record, 'extra', {})
    return extra.get("log_source") == "environment"

def filter_execution(record):
    extra = getattr(record, 'extra', {})
    return extra.get("log_source") == "execution"

# Apply filters and add handlers
env_file.addFilter(filter_environment)
exec_file.addFilter(filter_execution)
logger.addHandler(env_file)
logger.addHandler(exec_file)

# Now execute operations - logs will be automatically routed
env_manager = EnvironmentManager()
env = env_manager.create("cellpose", {"conda": ["cellpose==3.1.0"]})  # → environment.log, my_wetlands.log
env.launch()                                                            # → environment.log, my_wetlands.log
result = env.execute("segment.py", "segment", args=("image.png",))    # → execution.log, my_wetlands.log
```

**Files created:**
```
my_wetlands.log       # All logs (environment + execution)
environment.log       # Only environment creation/launch
execution.log         # Only function execution
```

**Example output in execution.log:**
```
2024-11-25 10:31:15 [INFO] Executing segment:segment
2024-11-25 10:31:20 [INFO] Processing image...
2024-11-25 10:32:00 [INFO] Execution complete
```

---

## Advanced Examples

### Per-Execution Log Files

Capture logs from individual function/script executions to separate files. Here's a simple context manager that routes all logs during execution to a file:

```python
from pathlib import Path
from contextlib import contextmanager
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import logger
import logging

@contextmanager
def capture_execution_logs(output_file: Path):
    """Context manager to capture all logs during execution to a file."""
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

If you want to capture only logs from a specific execution (filtering by `call_target`), use a filter:

```python
@contextmanager
def capture_execution_logs_filtered(env_name: str, call_target: str, output_file: Path):
    """Context manager that captures only logs from a specific execution."""
    handler = logging.FileHandler(output_file)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    def filter_execution(record):
        extra = getattr(record, 'extra', {})
        return (
            extra.get("log_source") == "execution" and
            extra.get("env_name") == env_name and
            extra.get("call_target") == call_target
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

---

### GUI Integration

Display real-time logs in a GUI window. **Important:** Log callbacks run in a background thread, so use thread-safe mechanisms to update the GUI on the main thread.

**Tkinter (thread-safe with `after()`):**
```python
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import attachLogHandler
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from queue import Queue
import threading

class LogViewer:
    def __init__(self, root):
        self.root = root
        self.log_queue = Queue()  # Thread-safe queue
        self.log_text = ScrolledText(root, height=20, width=80)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

        # Attach handler - this runs in background thread
        attachLogHandler(self.on_log)

        # Poll queue from main thread
        self.poll_queue()

    def on_log(self, record):
        # This runs in ProcessLogger's thread - just queue the message
        message = record.getMessage()
        self.log_queue.put(message)

    def poll_queue(self):
        # Process queued messages on main thread
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
    root.title("Wetlands Logs")
    viewer = LogViewer(root)

    # Run operations in background
    def run_ops():
        env_mgr = EnvironmentManager()
        env = env_mgr.create("demo", {"conda": ["numpy"]})
        env.launch()
        env.execute("script.py", "main")

    threading.Thread(target=run_ops, daemon=True).start()
    root.mainloop()
```

**PyQt6 (thread-safe with signals):**
```python
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import attachLogHandler
from PyQt6.QtWidgets import QApplication, QMainWindow, QTextEdit
from PyQt6.QtCore import pyqtSignal, QObject
import threading

class LogSignals(QObject):
    log_signal = pyqtSignal(str)  # Signal for thread-safe communication

class LogViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.signals = LogSignals()
        self.text_edit = QTextEdit()
        self.setCentralWidget(self.text_edit)
        self.setWindowTitle("Wetlands Logs")
        self.setGeometry(100, 100, 800, 600)

        # Connect signal to slot (executes on main thread)
        self.signals.log_signal.connect(self.append_log)

        # Attach handler - this runs in background thread
        attachLogHandler(self.on_log)

    def on_log(self, record):
        # This runs in ProcessLogger's thread - emit signal
        message = record.getMessage()
        self.signals.log_signal.emit(message)

    def append_log(self, message):
        # This runs on main thread (slot)
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

---

### Custom Callback Processing

Process logs with custom logic (parsing, analytics, filtering):

```python
from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
from wetlands.logger import logger, attachLogHandler
import logging
from collections import defaultdict
import re

class LogAnalyzer:
    """Analyzes logs in real-time for metrics and patterns."""

    def __init__(self):
        self.packages_installed = []
        self.errors = []
        self.warnings = []
        self.execution_times = {}
        self.current_execution = None
        self.timings = defaultdict(list)

    def on_log(self, record: logging.LogRecord):
        """Process each log record."""
        message = record.getMessage()
        level = record.levelname
        extra = getattr(record, 'extra', {})
        source = extra.get("log_source")
        env_name = extra.get("env_name")
        call_target = extra.get("call_target")

        # Track errors and warnings
        if level == "ERROR":
            self.errors.append((env_name, message))
            print(f"❌ ERROR [{env_name}]: {message}")
        elif level == "WARNING":
            self.warnings.append((env_name, message))
            print(f"⚠️  WARNING [{env_name}]: {message}")

        # Track package installations
        if source == "environment" and "Installing" in message:
            match = re.search(r'Installing\s+(\S+)', message)
            if match:
                package = match.group(1)
                self.packages_installed.append(package)
                print(f"📦 Package: {package}")

        # Track execution start/end
        if source == "execution":
            if call_target and call_target != "module_executor":
                if message.startswith("Executing"):
                    self.current_execution = call_target
                    print(f"▶️  Starting: {call_target}")
                elif message.startswith("Execution complete"):
                    if self.current_execution:
                        print(f"✓ Completed: {self.current_execution}")
                    self.current_execution = None

    def print_summary(self):
        """Print analysis summary."""
        print("\n" + "=" * 60)
        print("📊 WETLANDS OPERATION SUMMARY")
        print("=" * 60)

        if self.packages_installed:
            print(f"\n📦 Packages Installed ({len(self.packages_installed)}):")
            for pkg in self.packages_installed:
                print(f"   ✓ {pkg}")

        if self.errors:
            print(f"\n❌ Errors ({len(self.errors)}):")
            for env, msg in self.errors:
                print(f"   [{env}] {msg}")
        else:
            print(f"\n✓ No errors encountered")

        if self.warnings:
            print(f"\n⚠️  Warnings ({len(self.warnings)}):")
            for env, msg in self.warnings[:3]:  # Show first 3
                print(f"   [{env}] {msg}")

        print("\n" + "=" * 60)

# Usage
analyzer = LogAnalyzer()
attachLogHandler(analyzer.on_log)

env_manager = EnvironmentManager()
env = env_manager.create("analysis", {"conda": ["numpy", "pandas", "matplotlib"]})
env.launch()
result = env.execute("analysis.py", "main")

# Print summary report
analyzer.print_summary()
```

**Sample Output:**
```
📦 Package: numpy-1.24.0
📦 Package: pandas-2.0.0
📦 Package: matplotlib-3.7.0
▶️  Starting: analysis:main
✓ Completed: analysis:main

============================================================
📊 WETLANDS OPERATION SUMMARY
============================================================

📦 Packages Installed (3):
   ✓ numpy-1.24.0
   ✓ pandas-2.0.0
   ✓ matplotlib-3.7.0

✓ No errors encountered

============================================================
```

---

## Common Patterns

### Log only errors from a specific environment

```python
handler = logging.FileHandler("cellpose_errors.log")
def filter_cellpose_errors(record):
    extra = getattr(record, 'extra', {})
    return (
        record.levelname == "ERROR" and
        extra.get("env_name") == "cellpose"
    )
handler.addFilter(filter_cellpose_errors)
logger.addHandler(handler)
```

### Log only during environment creation

```python
handler = logging.FileHandler("creation_trace.log")
def filter_creation(record):
    extra = getattr(record, 'extra', {})
    return extra.get("stage") == "create"
handler.addFilter(filter_creation)
logger.addHandler(handler)
```

### Track multiple executions separately

```python
executions = {}
for func_name in ["preprocess", "train", "evaluate"]:
    handler = logging.FileHandler(f"logs/{func_name}.log")
    def make_filter(target):
        def f(record):
            extra = getattr(record, 'extra', {})
            return extra.get("call_target") == target
        return f
    handler.addFilter(make_filter(func_name))
    logger.addHandler(handler)
    executions[func_name] = handler
```

---

## Tips & Tricks

1. **Real-time monitoring**: Use `attachLogHandler()` for live updates in GUI applications
2. **Per-operation logs**: Use context managers with filters to capture individual operations
3. **Log rotation**: Use `RotatingFileHandler` for large logs
4. **Structured logging**: Access context via `getattr(record, 'extra', {})` in custom handlers
5. **Thread-safe**: All logging operations are thread-safe - no locks needed

---

## Next Steps

- See [Advanced Examples](advanced_example.md) for more complex workflows
- See [Debugging Guide](debugging.md) to understand how to debug within environments
