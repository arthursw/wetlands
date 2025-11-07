from pathlib import Path
from unittest.mock import MagicMock, patch

from wetlands.main import (
    process_match,
    get_matching_processes,
    get_wetlands_instance_paths,
    setup_and_launch_vscode,
    setup_and_launch_pycharm,
    list_environments,
    kill_environment,
)


class TestProcessMatch:
    def test_process_match_valid_python_process(self):
        """Test matching a valid Python process with wetlands module_executor"""
        process_args = [
            "python",
            "/path/to/wetlands/module_executor.py",
            "test_env",
            "--wetlandsInstancePath",
            "/tmp/wetlands",
        ]
        assert process_match(process_args, name="test_env") is True

    def test_process_match_no_python(self):
        """Test that non-Python processes are not matched"""
        process_args = ["bash", "script.sh"]
        assert process_match(process_args) is False

    def test_process_match_no_wetlands_instance_path(self):
        """Test that processes without --wetlandsInstancePath are not matched"""
        process_args = ["python", "/path/to/wetlands/module_executor.py", "test_env"]
        assert process_match(process_args) is False

    def test_process_match_no_module_executor(self):
        """Test that processes without module_executor are not matched"""
        process_args = ["python", "script.py", "--wetlandsInstancePath", "/tmp/wetlands"]
        assert process_match(process_args) is False

    def test_process_match_wrong_env_name(self):
        """Test that processes with different env name are not matched"""
        process_args = [
            "python",
            "/path/to/wetlands/module_executor.py",
            "other_env",
            "--wetlandsInstancePath",
            "/tmp/wetlands",
        ]
        assert process_match(process_args, name="test_env") is False

    def test_process_match_any_env_name(self):
        """Test matching with None env name matches any"""
        process_args = [
            "python",
            "/path/to/wetlands/module_executor.py",
            "any_env",
            "--wetlandsInstancePath",
            "/tmp/wetlands",
        ]
        assert process_match(process_args, name=None) is True


class TestGetMatchingProcesses:
    @patch("wetlands.main.psutil.process_iter")
    def test_get_matching_processes_found(self, mock_process_iter):
        """Test finding matching processes"""
        mock_process = MagicMock()
        mock_process.cmdline.return_value = [
            "python",
            "/path/to/wetlands/module_executor.py",
            "test_env",
            "--wetlandsInstancePath",
            "/tmp/wetlands",
        ]
        mock_process_iter.return_value = [mock_process]

        processes = get_matching_processes(name="test_env")
        assert len(processes) == 1
        assert processes[0]["process"] == mock_process
        assert processes[0]["name"] == "test_env"

    @patch("wetlands.main.psutil.process_iter")
    def test_get_matching_processes_none_found(self, mock_process_iter):
        """Test when no matching processes are found"""
        mock_process = MagicMock()
        mock_process.cmdline.return_value = ["bash", "script.sh"]
        mock_process_iter.return_value = [mock_process]

        processes = get_matching_processes()
        assert len(processes) == 0

    @patch("wetlands.main.psutil.process_iter")
    def test_get_matching_processes_handles_exceptions(self, mock_process_iter):
        """Test that exceptions from process.cmdline() are handled"""
        mock_process1 = MagicMock()
        mock_process1.cmdline.side_effect = Exception("Permission denied")
        mock_process2 = MagicMock()
        mock_process2.cmdline.return_value = [
            "python",
            "/path/to/wetlands/module_executor.py",
            "test_env",
            "--wetlandsInstancePath",
            "/tmp/wetlands",
        ]
        mock_process_iter.return_value = [mock_process1, mock_process2]

        processes = get_matching_processes()
        assert len(processes) == 1


class TestGetWetlandsInstancePaths:
    def test_get_wetlands_instance_paths_single(self):
        """Test extracting wetlands instance paths from processes"""
        processes = [
            {
                "args": [
                    "python",
                    "/path/to/wetlands/module_executor.py",
                    "env1",
                    "--wetlandsInstancePath",
                    "/tmp/wetlands1",
                ]
            }
        ]
        paths = get_wetlands_instance_paths(processes)
        assert paths == ["/tmp/wetlands1"]

    def test_get_wetlands_instance_paths_multiple(self):
        """Test extracting multiple wetlands instance paths"""
        processes = [
            {
                "args": [
                    "python",
                    "/path/to/wetlands/module_executor.py",
                    "env1",
                    "--wetlandsInstancePath",
                    "/tmp/wetlands1",
                ]
            },
            {
                "args": [
                    "python",
                    "/path/to/wetlands/module_executor.py",
                    "env2",
                    "--wetlandsInstancePath",
                    "/tmp/wetlands2",
                ]
            },
        ]
        paths = get_wetlands_instance_paths(processes)
        assert paths == ["/tmp/wetlands1", "/tmp/wetlands2"]


class TestSetupAndLaunchVscode:
    @patch("wetlands.main.subprocess.run")
    @patch("wetlands.main.json5.load")
    @patch("builtins.open", create=True)
    @patch("wetlands.main.get_matching_processes")
    @patch("wetlands.main.get_wetlands_instance_paths")
    def test_setup_and_launch_vscode_debug_ports_not_found(
        self, mock_get_paths, mock_get_processes, mock_file, mock_json5_load, mock_subprocess
    ):
        """Test VS Code setup when debug ports file is missing"""
        args = MagicMock()
        args.sources = MagicMock()
        args.sources.resolve = MagicMock(return_value=Path("/sources"))
        args.name = "test_env"
        args.wetlandsInstancePath = MagicMock()
        args.wetlandsInstancePath.resolve = MagicMock(return_value=Path("/tmp/wetlands"))

        mock_get_processes.return_value = []
        mock_get_paths.return_value = []

        # Simulate file not existing
        mock_file.side_effect = FileNotFoundError()

        result = setup_and_launch_vscode(args)
        assert result is None


class TestListEnvironments:
    @patch("builtins.print")
    @patch("wetlands.main.json5.load")
    @patch("builtins.open", create=True)
    @patch("wetlands.main.get_matching_processes")
    def test_list_environments_with_running_processes(self, mock_get_processes, mock_open, mock_json5_load, mock_print):
        """Test listing environments when processes are running"""
        args = MagicMock()
        args.wetlandsInstancePath = MagicMock()
        args.wetlandsInstancePath.resolve = MagicMock(return_value=Path("/tmp/wetlands"))

        debug_ports = {
            "env1": {"debugPort": 5678, "moduleExecutorPath": "/path/to/executor1"},
            "env2": {"debugPort": 5679, "moduleExecutorPath": "/path/to/executor2"},
        }
        mock_json5_load.return_value = debug_ports
        mock_get_processes.return_value = []

        mock_open_file = MagicMock()
        mock_open.return_value.__enter__ = MagicMock(return_value=mock_open_file)
        mock_open.return_value.__exit__ = MagicMock(return_value=None)

        list_environments(args)
        # Should print information about environments
        assert mock_print.called

    @patch("builtins.print")
    @patch("wetlands.main.get_matching_processes")
    def test_list_environments_no_debug_ports_file(self, mock_get_processes, mock_print):
        """Test when debug_ports.json does not exist"""
        args = MagicMock()
        args.wetlandsInstancePath = MagicMock()
        args.wetlandsInstancePath.resolve = MagicMock(return_value=Path("/nonexistent/path"))

        mock_get_processes.return_value = []

        with patch("builtins.open", side_effect=FileNotFoundError()):
            result = list_environments(args)
            assert result is None


class TestKillEnvironment:
    @patch("wetlands.main.psutil.Process")
    @patch("wetlands.main.get_matching_processes")
    @patch("wetlands.main.get_wetlands_instance_paths")
    def test_kill_environment_single_process(self, mock_get_paths, mock_get_processes, mock_process_class):
        """Test killing a single matching process"""
        args = MagicMock()
        args.name = "test_env"
        args.wetlandsInstancePath = MagicMock()
        args.wetlandsInstancePath.resolve = MagicMock(return_value=Path("/tmp/wetlands"))

        mock_process = MagicMock()
        mock_process.pid = 1234
        mock_parent_process = MagicMock()
        mock_child_process = MagicMock()

        mock_parent_process.children.return_value = [mock_child_process]
        mock_parent_process.is_running.return_value = True
        mock_child_process.is_running.return_value = True
        mock_process_class.return_value = mock_parent_process

        mock_get_processes.return_value = [
            {
                "process": mock_process,
                "args": [
                    "python",
                    "/path/to/wetlands/module_executor.py",
                    "test_env",
                    "--wetlandsInstancePath",
                    "/tmp/wetlands",
                ],
            }
        ]
        mock_get_paths.return_value = ["/tmp/wetlands"]

        kill_environment(args)
        mock_child_process.kill.assert_called_once()
        mock_parent_process.kill.assert_called_once()

    @patch("wetlands.main.get_matching_processes")
    def test_kill_environment_no_process_found(self, mock_get_processes):
        """Test when no matching process is found"""
        args = MagicMock()
        args.name = "test_env"
        args.wetlandsInstancePath = MagicMock()
        args.wetlandsInstancePath.resolve = MagicMock(return_value=Path("/tmp/wetlands"))

        mock_get_processes.return_value = []

        with patch("builtins.print"):
            result = kill_environment(args)
            assert result is None


class TestSetupAndLaunchPycharm:
    @patch("wetlands.main.subprocess.run")
    @patch("builtins.print")
    @patch("wetlands.main.get_matching_processes")
    @patch("wetlands.main.get_wetlands_instance_paths")
    def test_setup_and_launch_pycharm_debug_ports_not_found(
        self, mock_get_paths, mock_get_processes, mock_print, mock_subprocess
    ):
        """Test PyCharm setup when debug ports file is missing"""
        args = MagicMock()
        args.sources = Path("/tmp/sources")
        args.name = "test_env"
        args.wetlandsInstancePath = Path("/nonexistent/path")

        mock_get_processes.return_value = []
        mock_get_paths.return_value = []

        with patch("builtins.open", side_effect=FileNotFoundError()):
            with patch.object(Path, "mkdir"):
                result = setup_and_launch_pycharm(args)
                assert result is None

    @patch("wetlands.main.subprocess.run")
    def test_setup_and_launch_pycharm_creates_xml_config(self, mock_subprocess):
        """Test that PyCharm setup creates XML configuration file"""
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            sources_dir = Path(tmpdir) / "sources"
            sources_dir.mkdir()
            wetlands_dir = Path(tmpdir) / "wetlands"
            wetlands_dir.mkdir()

            # Create args that work with real Path objects
            args = MagicMock()
            args.sources = sources_dir
            args.name = "test_env"
            args.wetlandsInstancePath = wetlands_dir

            # Create debug_ports.json file
            debug_ports_file = wetlands_dir / "debug_ports.json"
            with open(debug_ports_file, "w") as f:
                import json5

                json5.dump(
                    {
                        "test_env": {
                            "debugPort": 5678,
                            "moduleExecutorPath": "/path/to/module_executor.py",
                        }
                    },
                    f,
                )

            with patch("wetlands.main.get_matching_processes", return_value=[]):
                with patch("wetlands.main.get_wetlands_instance_paths", return_value=[]):
                    setup_and_launch_pycharm(args)

            # Check that XML config file was created
            config_file = sources_dir / ".idea" / "runConfigurations" / "Remote_Attach_Wetlands.xml"
            assert config_file.exists()

            # Check that file contains expected XML structure
            with open(config_file, "r") as f:
                content = f.read()
                assert "Remote Attach Wetlands" in content
                assert "component" in content
                assert "configuration" in content

            # Check that pycharm command was called
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0][0]
            assert call_args[0] == "pycharm"
            assert str(sources_dir) in call_args

    @patch("wetlands.main.subprocess.run")
    @patch("builtins.print")
    @patch("wetlands.main.get_matching_processes")
    @patch("wetlands.main.get_wetlands_instance_paths")
    def test_setup_and_launch_pycharm_debug_port_not_found(
        self, mock_get_paths, mock_get_processes, mock_print, mock_subprocess
    ):
        """Test PyCharm setup when debug port is not found for env"""
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            sources_dir = Path(tmpdir) / "sources"
            sources_dir.mkdir()
            wetlands_dir = Path(tmpdir) / "wetlands"
            wetlands_dir.mkdir()

            args = MagicMock()
            args.sources = sources_dir
            args.name = "unknown_env"
            args.wetlandsInstancePath = wetlands_dir

            mock_get_processes.return_value = []
            mock_get_paths.return_value = []

            # Create debug_ports.json with different env
            debug_ports_file = wetlands_dir / "debug_ports.json"
            with open(debug_ports_file, "w") as f:
                import json5

                json5.dump(
                    {
                        "other_env": {
                            "debugPort": 5678,
                            "moduleExecutorPath": "/path/to/executor",
                        }
                    },
                    f,
                )

            result = setup_and_launch_pycharm(args)
            assert result is None
