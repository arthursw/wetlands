from pathlib import Path
import subprocess
import argparse
import json5
import psutil
import xml.etree.ElementTree as ET
from xml.dom import minidom


def process_match(process_args_list: list[str], name: str | None = None):
    # Ignore non python processes
    if process_args_list[0] != "python":
        return False
    # --wetlandsInstancePath must be given
    if "--wetlandsInstancePath" not in process_args_list:
        return False
    # For all processes: find wetlands ones in debug mode, and matching with the name
    for i, item in enumerate(process_args_list):
        if "wetlands" in item and "module_executor.py" in item:
            if i + 1 < len(process_args_list):
                # If the process environment name is not the one we are looking for: return None
                if name is None or name == process_args_list[i + 1]:
                    return True
    return False


def get_matching_processes(name: str | None = None):
    processes = []
    for process in psutil.process_iter():
        try:
            process_args_list = process.cmdline()
            if process_match(process_args_list, name):
                processes.append(dict(process=process, args=process_args_list, name=name))
        except Exception:
            pass

    return processes


def get_wetlands_instance_paths(processes: list[dict]):
    return [p["args"][p["args"].index("--wetlandsInstancePath") + 1] for p in processes]


def setup_and_launch_vscode(args):
    launch_json_path = args.sources / ".vscode" / "launch.json"
    launch_json_path.parent.mkdir(exist_ok=True, parents=True)

    # Check if the environment can be found in the running processes
    processes = get_matching_processes(args.name)
    wetlands_instance_paths = get_wetlands_instance_paths(processes)

    wetlands_instance_path = args.wetlandsInstancePath.resolve()
    # If a single process matches the given environment: use it for wetlandsInstancePath
    if len(wetlands_instance_paths) == 1:
        wetlands_instance_path = Path(wetlands_instance_paths[0])
        print(
            f'Found a single process matching with environment {args.name} with wetlandsInstancePath "{wetlands_instance_path}".'
        )
        if args.wetlandsInstancePath.resolve() != wetlands_instance_path:
            print("Ignoring {args.wetlandsInstancePath}, using {wetlands_instance_path} instead.")

    # Read the debug port for the given environment
    debug_ports_path = wetlands_instance_path / "debug_ports.json"
    if not debug_ports_path.exists():
        print(f"The file {debug_ports_path} does not exist.")
        return

    with open(debug_ports_path, "r") as f:
        debug_ports = json5.load(f)

    port = None
    moduleExecutorPath = None

    for env, detail in debug_ports.items():
        if env == args.name:
            port = detail["debugPort"]
            moduleExecutorPath = Path(detail["moduleExecutorPath"])
            break

    if port is None or moduleExecutorPath is None:
        print(f"Error, debug port not found in {debug_ports_path} for environment {args.name}.")
        return

    # Create a new debug launch configuration
    configuration_name = "Python Debugger: Remote Attach Wetlands"
    new_config = {
        "name": configuration_name,
        "type": "debugpy",
        "request": "attach",
        "justMyCode": args.justMyCode,
        "connect": {"host": "localhost", "port": port},
        "pathMappings": [{"localRoot": str(moduleExecutorPath.parent), "remoteRoot": str(moduleExecutorPath.parent)}],
    }
    launch_configs = {"version": "0.2.0", "configurations": [new_config]}

    # If the vscode launch.json exists: update it with the new config
    if launch_json_path.exists():
        with open(launch_json_path, "r") as f:
            try:
                existing_launch_configs = json5.load(f)
            except Exception as e:
                e.add_note(
                    f"The launch config file {launch_json_path} cannot be read. Try deleting or fixing it before debugging."
                )
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
        json5.dump(launch_configs, f, indent=4)

    # Open VS Code in new window
    subprocess.run(["code", "--new-window", str(args.sources), str(moduleExecutorPath)])

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


def setup_and_launch_pycharm(args):
    run_configs_dir = args.sources / ".idea" / "runConfigurations"
    run_configs_dir.mkdir(exist_ok=True, parents=True)

    # Check if the environment can be found in the running processes
    processes = get_matching_processes(args.name)
    wetlands_instance_paths = get_wetlands_instance_paths(processes)

    wetlands_instance_path = args.wetlandsInstancePath.resolve()
    # If a single process matches the given environment: use it for wetlandsInstancePath
    if len(wetlands_instance_paths) == 1:
        wetlands_instance_path = Path(wetlands_instance_paths[0])
        print(
            f'Found a single process matching with environment {args.name} with wetlandsInstancePath "{wetlands_instance_path}".'
        )
        if args.wetlandsInstancePath.resolve() != wetlands_instance_path:
            print("Ignoring {args.wetlandsInstancePath}, using {wetlands_instance_path} instead.")

    # Read the debug port for the given environment
    debug_ports_path = wetlands_instance_path / "debug_ports.json"
    if not debug_ports_path.exists():
        print(f"The file {debug_ports_path} does not exist.")
        return

    with open(debug_ports_path, "r") as f:
        debug_ports = json5.load(f)

    port = None
    moduleExecutorPath = None

    for env, detail in debug_ports.items():
        if env == args.name:
            port = detail["debugPort"]
            moduleExecutorPath = Path(detail["moduleExecutorPath"])
            break

    if port is None or moduleExecutorPath is None:
        print(f"Error, debug port not found in {debug_ports_path} for environment {args.name}.")
        return

    # Create a new PyCharm debug launch configuration (XML format)
    configuration_name = "Remote Attach Wetlands"

    # Create the root configuration element
    root = ET.Element("component", name="ProjectRunConfigurationManager")
    config = ET.SubElement(
        root,
        "configuration",
        name=configuration_name,
        type="Python",
        factoryName="Python",
        show_console_on_std_err="false",
        show_console_on_std_out="false",
    )

    # Add module reference
    ET.SubElement(config, "module", name="$PROJECT_NAME")

    # Add debug parameters
    ET.SubElement(config, "option", name="INTERPRETER_OPTIONS", value="")
    ET.SubElement(config, "option", name="PARENT_ENVS", value="true")
    ET.SubElement(config, "envs", {"PYTHONUNBUFFERED": "1"})
    ET.SubElement(config, "option", name="SDK_HOME", value="")
    ET.SubElement(config, "option", name="WORKING_DIRECTORY", value="")
    ET.SubElement(config, "option", name="IS_MODULE_SDK", value="false")
    ET.SubElement(config, "option", name="ADD_CONTENT_ROOTS", value="true")
    ET.SubElement(config, "option", name="ADD_SOURCE_ROOTS", value="true")

    # This is the key part: debugger configuration for remote attach
    ET.SubElement(config, "option", name="MODULE_NAME", value="")
    ET.SubElement(config, "option", name="SCRIPT_NAME", value="")
    ET.SubElement(config, "option", name="PARAMETERS", value="")

    # Remote debugging configuration
    method = ET.SubElement(config, "method", v="2")
    ET.SubElement(
        method,
        "option",
        name="RunConfigurationTask",
        enabled="true",
        run_configuration_name=configuration_name,
        run_configuration_type="Python",
    )

    # Add path mappings as method options (PyCharm stores these in a special way)
    path_mapping = ET.SubElement(config, "option", name="PATH_MAPPINGS")
    path_pair = ET.SubElement(path_mapping, "list")
    ET.SubElement(path_pair, "item", index="0", itemvalue=f"{moduleExecutorPath.parent}:{moduleExecutorPath.parent}")

    # Format XML nicely
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    # Remove XML declaration and extra blank lines
    xml_lines = xml_str.split("\n")[1:]  # Skip XML declaration
    xml_str = "\n".join([line for line in xml_lines if line.strip()])

    # Write configuration file
    config_file = run_configs_dir / f"{configuration_name.replace(' ', '_')}.xml"
    with open(config_file, "w") as f:
        f.write(xml_str)

    print(f"Created PyCharm run configuration at {config_file}")

    # Open PyCharm
    subprocess.run(["pycharm", str(args.sources)])


def list_environments(args):
    processes = get_matching_processes()
    process_strings = []
    for process in processes:
        pa = " ".join(process["args"])
        process_strings.append(f"{pa} | {process['process'].pid} | {process['process'].ppid()}")

    if len(processes) == 0:
        print("No running wetlands environment process found.")
    else:
        print(f"Running wetlands environments (for all wetlands instance):\n")
        print("Command line | Process ID | Parent process ID")
        print("---")
        for ps in process_strings:
            print(ps)

    debug_ports_path = args.wetlandsInstancePath.resolve() / "debug_ports.json"
    if not debug_ports_path.exists():
        print(f"The file {debug_ports_path} does not exist.")
        return

    with open(debug_ports_path, "r") as f:
        debug_ports = json5.load(f)
    if len(debug_ports) == 0:
        print(f"No environments for instance {args.wetlandsInstancePath.resolve()} ({debug_ports_path} is empty).")
        return
    print(f"\n\nEnvironments of the wetlands instance {args.wetlandsInstancePath.resolve()}:\n")
    print("Environment | Debug Port | Path")
    print("---")
    new_debug_ports = {}
    for environment, detail in debug_ports.items():
        # Only keep environments matching with the running processes
        if process_match(processes, environment):
            print(environment, "|", detail["debugPort"], "|", detail["moduleExecutorPath"])
            new_debug_ports[environment] = detail
    # Update the debug_ports.json to only keep running environments
    with open(debug_ports_path, "w") as f:
        json5.dump(new_debug_ports, f, indent=4)

    return


def kill_environment(args):
    processes = get_matching_processes(args.name)
    wetlands_instance_paths = get_wetlands_instance_paths(processes)
    process = None
    if len(processes) == 1:
        process = processes[0]["process"]
    elif len(processes) > 1:
        for i, wetlands_instance_path in enumerate(wetlands_instance_paths):
            if wetlands_instance_path == str(args.wetlandsInstancePath.resolve()):
                process = processes[i]["process"]
                break
    if process is None:
        print(
            f"No wetlands process with environment name {args.name} for instance {args.wetlandsInstancePath.resolve()} found."
        )
        return

    parent = psutil.Process(process.pid)
    for child in parent.children(recursive=True):  # Get all child processes
        if child.is_running():
            child.kill()
    if parent.is_running():
        parent.kill()
    return


def main():
    main_parser = argparse.ArgumentParser(
        "wetlands",
        description="List and debug Wetlands environments.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    main_parser.add_argument(
        "-wip",
        "--wetlandsInstancePath",
        help="The Wetlands instance folder path. Required only when multiple wetlands instances are running and two environments have the same name.",
        default=Path("pixi/wetlands"),
        type=Path,
    )
    subparsers = main_parser.add_subparsers()
    debug_parser = subparsers.add_parser(
        "debug",
        help="Debug Wetlands environments: opens an IDE at the given path to debug the environment at the given port",
    )
    debug_parser.add_argument("-s", "--sources", help="Path of the sources to debug", type=Path, required=True)
    debug_parser.add_argument("-n", "--name", help="Name of the environment to debug.", type=str, required=True)
    debug_parser.add_argument(
        "-ide",
        "--ide",
        help="IDE to use for debugging (vscode or pycharm)",
        type=str,
        choices=["vscode", "pycharm"],
        default="vscode",
    )
    debug_parser.add_argument(
        "-jmc",
        "--justMyCode",
        help="Only debug the given sources files, not used libraries. Sets the justMyCode property to true in the VS Code launch.json configuration.",
        action="store_true",
    )

    def launch_debugger(args):
        if args.ide == "pycharm":
            return setup_and_launch_pycharm(args)
        else:
            return setup_and_launch_vscode(args)

    debug_parser.set_defaults(func=launch_debugger)
    list_parser = subparsers.add_parser("list", help="List the running Wetlands environments and their debug ports.")
    list_parser.set_defaults(func=list_environments)
    kill_parser = subparsers.add_parser("kill", help="Kill a running Wetlands environment.")
    kill_parser.add_argument("-n", "--name", help="Name of the environment to kill.", type=str, required=True)
    kill_parser.set_defaults(func=kill_environment)
    args = main_parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    main()
