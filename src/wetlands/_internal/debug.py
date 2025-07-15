import json
from pathlib import Path
import subprocess
import time
import argparse

def setup_and_launch_vscode(wetlands_sources: Path, python_exe: str, env_name: str, port_number: int):
    launch_json_path = wetlands_sources / "launch.json"

    launch_config = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "Module executor",
                "type": "debugpy",
                "request": "launch",
                "program": "${workspaceFolder}/src/wetlands/_internal/module_executor.py",
                "console": "integratedTerminal",
                "python": python_exe,
                "justMyCode": False,
                "autoReload": {
                    "enable": True
                },
                "windows": {
                    "python": python_exe
                },
                "args": [env_name, port_number]
            }
        ]
    }

    with open(launch_json_path, "w") as f:
        json.dump(launch_config, f, indent=4)

    # Open VS Code in new window
    subprocess.run(["code", "--new-window", str(wetlands_sources)])
    
    # Wait for VS Code to start
    time.sleep(1)

    # Send backtick (key code 96) to open the terminal
    apple_script = '''
    tell application "System Events"
        tell process "Visual Studio Code"
            key code 96
        end tell
    end tell
    '''
    subprocess.run(["osascript", "-e", apple_script])
import socket

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))  # Bind to a free port provided by the OS
        return s.getsockname()[1]  # Return the port number

# Example usage
if __name__ == "__main__":

    parser = argparse.ArgumentParser("Debug Wetlands environments", description="This script launches environments servers with VS Code python debugger.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s","--sources", help="The wetlands sources path (clone with `git clone git@github.com:arthursw/wetlands.git`)", type=Path, default=Path(__file__).parent.parent.parent.parent)
    parser.add_argument("-py", "--python", help="The python executable.", required=True)
    parser.add_argument("-e", "--environment", help="Name of the environment to launch.", required=True)
    parser.add_argument("-p", "--port", help="The port number.", type=int, default=find_free_port())
    args = parser.parse_args()

    setup_and_launch_vscode(
        wetlands_sources=args.sources,
        python_exe=args.python,
        env_name=args.environment,
        port_number=args.port
    )
