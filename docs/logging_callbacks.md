# Logging Callbacks Guide

The wetlands library provides a callback-based logging system to capture and process subprocess output in real-time. This guide explains how to use callbacks for logging and monitoring environment operations.

## Basic Concepts

### Callback Types

1. **Global Callbacks**: Set during `launch()`, receive all output from the environment
2. **Per-Execution Callbacks**: Set during `execute()` or `runScript()`, receive output specific to that execution

### Thread Safety

- Callbacks are invoked from daemon threads reading subprocess output
- All environment methods are thread-safe (protected by `@synchronized` decorator)
- Callback execution should be fast; long-running callbacks will block the output reader thread

## Usage Examples

Both callbacks work together—the global callback receives all output, and per-execution callbacks receive output specific to that operation:

```python
global_logs = []
def global_callback(line: str):
    global_logs.append(("global", line))

execution_logs = []
def execution_callback(line: str):
    execution_logs.append(("execution", line))

env = manager.create("my_env")
env.launch(log_callback=global_callback)

# After launch, global_callback continues to receive output
# During execute, both global_callback and execution_callback are called
result = env.execute(
    "my_module.py",
    "my_function",
    log_callback=execution_callback
)

print(f"Global logs: {len(global_logs)}")
print(f"Execution logs: {len(execution_logs)}")
```

## Best Practices

1. **Keep Callbacks Fast**: Callbacks are called from daemon threads. Long-running operations block output reading.
2. **Handle Exceptions**: Always catch exceptions in callbacks—the environment will catch unhandled ones and log them.
3. **Avoid Modifying State**: Callbacks can modify module-level state, but be aware of thread-safety implications.
4. **Use Global + Per-Execution**: Use global callbacks for environment monitoring and per-execution callbacks for specific operation logging.
5. **Log Selectively**: Filter logs at the callback level to reduce overhead and clutter.


### **Warning for GUI Developers:**

The `log_callback` is executed on a background thread. If you are updating a GUI (e.g., PyQt, Tkinter), you must dispatch the update to the main thread.
*   **PyQt/PySide:** Use `Signal.emit` as the callback.
*   **Tkinter:** Use a `queue.Queue` and poll it using `root.after()`.

Here are examples for the two most common Python GUI frameworks.

#### The Golden Rule
> "The `log_callback` is called from a background thread. You must **signal** the main thread to update the GUI. Do not modify widgets directly inside the callback."

---

#### 1. If the user uses PyQt or PySide (Most Common)
Qt has a built-in "Signal/Slot" mechanism which is thread-safe. The user should create a Signal, connect it to their widget, and pass the **signal's emit function** as the callback.

**User Code Example:**

```python
from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtWidgets import QTextEdit, QApplication

# 1. Create a bridge class (Signals must be defined on QObjects)
class LogBridge(QObject):
    new_log = pyqtSignal(str)

app = QApplication([])
console = QTextEdit()
bridge = LogBridge()

# 2. Connect the signal to the widget's update method
# (Qt automatically handles the thread context switch here)
bridge.new_log.connect(console.append)

# 3. Pass the 'emit' method to your library
# When the library calls this on the bg thread, Qt puts the event 
# onto the main thread's event loop.
stardist.execute(
    "script.py", 
    "run", 
    im, 
    log_callback=bridge.new_log.emit  # <--- The Magic Link
)
```

#### 2. If the user uses Tkinter
Tkinter does not have built-in signals. The standard Pythonic way to handle this is using a `queue.Queue`. The library pushes to the queue, and the GUI periodically checks it.

**User Code Example:**

```python
import tkinter as tk
import queue

root = tk.Tk()
text_area = tk.Text(root)
text_area.pack()

# 1. Create a thread-safe queue
log_queue = queue.Queue()

# 2. Define the callback that puts data into the queue
def my_log_callback(line):
    log_queue.put(line)

# 3. Create a poller function on the main thread
def process_log_queue():
    while not log_queue.empty():
        line = log_queue.get_nowait()
        text_area.insert(tk.END, line + "\n")
        text_area.see(tk.END) # Scroll to bottom
    
    # Check again in 100ms
    root.after(100, process_log_queue)

# Start polling
process_log_queue()

# 4. Pass the callback to your library
# (Note: stardist.execute must be run in a thread itself if using Tkinter, 
# otherwise it blocks the GUI entirely)
import threading
threading.Thread(target=lambda: stardist.execute(..., log_callback=my_log_callback)).start()

root.mainloop()
```

#### 3. If the user just wants to use `print`
If the user is running a GUI but also wants to see logs in the terminal/console (stdout), `print` is **thread-safe** in Python. They can do this directly:

```python
# This is safe even from a background thread
stardist.execute(..., log_callback=print)
```