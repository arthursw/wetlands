import json
import json5
from pathlib import Path
import subprocess
import argparse

def setup_and_launch_vscode(args):
    launch_json_path = args.sources / '.vscode' / "launch.json"
    launch_json_path.parent.mkdir(exist_ok=True, parents=True)

    configuration_name = "Python Debugger: Remote Attach Wetlands"
    new_config = {
        "name": configuration_name,
        "type": "debugpy",
        "request": "attach",
        "connect": {
            "host": "localhost",
            "port": args.port
        },
        "pathMappings": [
            {
                "localRoot": "${workspaceFolder}",
                "remoteRoot": "."
            }
        ]
    }
    launch_configs = {
        "version": "0.2.0",
        "configurations": [ new_config ]
    }

    # If the vscode launch.json exists: update it with the new config
    if launch_json_path.exists():
        with open(launch_json_path, "r") as f:
            try:
                existing_launch_configs = json5.load(f)
            except Exception as e:
                e.add_note(f"The launch config file {launch_json_path} cannot be read. Try deleting or fixing it before debugging.")
                raise e
            # Find the config "Python Debugger: Remote Attach Wetlands" and replace it
            # If the config does not exist: append it to the configs
            if "configuration" in existing_launch_configs:
                found = False
                for i, configuration in enumerate(existing_launch_configs["configuration"]):
                    if "name" in configuration and configuration["name"] == configuration_name:
                        existing_launch_configs[i] = new_config
                        found = True
                        break
                if not found:
                    existing_launch_configs["configurations"].append(new_config)
            else:
                existing_launch_configs["configurations"] = [new_config]
                launch_configs = existing_launch_configs

    with open(launch_json_path, "w") as f:
        json.dump(launch_configs, f, indent=4)

    # Open VS Code in new window
    subprocess.run(["code", "--new-window", str(args.sources)])
    
    # # Wait for VS Code to start
    # time.sleep(1)

    # # Send backtick (key code 96) to open the terminal
    # apple_script = '''
    # tell application "System Events"
    #     tell process "Visual Studio Code"
    #         key code 96
    #     end tell
    # end tell
    # '''
    # subprocess.run(["osascript", "-e", apple_script])

def list_environments(args):
    debug_ports_path = args.wetlandsInstancePath / 'debug_ports.json'
    if not debug_ports_path.exists():
        print(f"The file {debug_ports_path} does not exists.")
        return
    with open(debug_ports_path, 'r') as f:
        debug_ports = json5.load(f)
    print(f'Here are the available debug ports for the environments of the wetlands instance {args.wetlandsInstancePath}:\n')
    print('Environment: Debug port')
    print('---')
    for environment, port in debug_ports.items():
        print(environment, port)
    return


def main():
    main_parser = argparse.ArgumentParser("wetlands", description="List and debug Wetlands environments.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = main_parser.add_subparsers()
    debug_parser = subparsers.add_parser('debug', help="Debug Wetlands environments: opens VS Code at the given path to debug the environment at the given port")
    debug_parser.add_argument("-s","--sources", help="Path of the sources to debug", type=Path, required=True)
    debug_parser.add_argument("-p", "--port", help="The debug port number.", type=int, required=True)
    debug_parser.set_defaults(func=setup_and_launch_vscode)
    list_parser = subparsers.add_parser('list', help="List the running Wetlands environments and their debug ports.")
    list_parser.add_argument("-wip", "--wetlandsInstancePath", help="The Wetlands instance folder path.", default=Path("pixi/wetlands"), type=Path)
    list_parser.set_defaults(func=list_environments)
    args = main_parser.parse_args()
    return args.func(args)

if __name__ == "__main__":
    main()