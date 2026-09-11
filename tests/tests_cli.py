import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from kantrip import APP_VERSION
from kantrip.cli import cli
from kantrip.config import load_configuration
from kantrip.ping import PingError, PingResult


class TestCli(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_help_describes_current_cli(self) -> None:
        result = self.runner.invoke(cli, ["--help"])

        self.assertEqual(0, result.exit_code)
        self.assertIn("Kantrip securely manages local Kafka profiles", result.output)
        self.assertIn("--no-color", result.output)

    def test_version_uses_package_metadata(self) -> None:
        result = self.runner.invoke(cli, ["--version"])

        self.assertEqual(0, result.exit_code)
        self.assertIn(APP_VERSION, result.output)

    def test_profile_commands_use_resolved_file(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_REGISTRY_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}

            listed = self.runner.invoke(cli, ["list"], env=environment)
            shown = self.runner.invoke(cli, ["show", "local"], env=environment)

        self.assertIn("Profile", listed.output)
        self.assertIn("Description", listed.output)
        self.assertIn("Kafka", listed.output)
        self.assertIn("Schema Registry", listed.output)
        self.assertIn("local", listed.output)
        self.assertIn("Local development", listed.output)
        self.assertIn("localhost:8081", listed.output)
        self.assertNotIn("\x1b[", listed.output)
        self.assertEqual(0, shown.exit_code, shown.output)
        self.assertIn("bootstrapServers:", shown.output)

    def test_add_and_remove_manage_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "kantrip" / "config.yaml"
            environment = {"KANTRIP_CONFIG": str(config_path)}
            added = self.runner.invoke(
                cli,
                [
                    "add",
                    "development",
                    "-b",
                    "broker-1.example.com:9092,broker-2.example.com:9092",
                    "-d",
                    "Development cluster",
                    "--schema-registry-url",
                    "http://registry.example.com:8081",
                ],
                env=environment,
            )
            profile = load_configuration(config_path).profile("development")
            listed = self.runner.invoke(cli, ["list"], env=environment)
            removed = self.runner.invoke(cli, ["remove", "development"], env=environment)
            empty = self.runner.invoke(cli, ["list"], env=environment)

        self.assertEqual(0, added.exit_code, added.output)
        self.assertIn("Profile", listed.output)
        self.assertIn("development", listed.output)
        self.assertIn("Development cluster", listed.output)
        self.assertEqual(
            ["broker-1.example.com:9092", "broker-2.example.com:9092"],
            profile["kafka"]["bootstrapServers"],
        )
        self.assertEqual("http://registry.example.com:8081", profile["schemaRegistry"]["url"])
        self.assertEqual(0, removed.exit_code, removed.output)
        self.assertEqual("", empty.output)

    def test_add_rejects_empty_comma_separated_bootstrap_server(self) -> None:
        result = self.runner.invoke(
            cli, ["add", "invalid", "-b", "localhost:9092,"], env={"KANTRIP_CONFIG": "x"}
        )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("comma-separated list of host:port addresses", result.output)

    def test_list_is_empty_when_configuration_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "missing.yaml"

            result = self.runner.invoke(cli, ["list"], env={"KANTRIP_CONFIG": str(config_path)})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("", result.output)

    def test_current_reports_active_profile(self) -> None:
        active = self.runner.invoke(cli, ["current"], env={"KANTRIP_PROFILE": "local"})
        inactive = self.runner.invoke(cli, ["current"], env={"KANTRIP_PROFILE": ""})

        self.assertEqual(0, active.exit_code, active.output)
        self.assertEqual("local\n", active.output)
        self.assertNotEqual(0, inactive.exit_code)
        self.assertIn("no profile is active", inactive.output)

    def test_doctor_uses_readable_status_markers_without_color(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)

            with patch("kantrip.cli.run_doctor") as run:
                from kantrip.doctor import DoctorCheck, DoctorReport

                run.return_value = DoctorReport(
                    (
                        DoctorCheck("success", "configuration is valid"),
                        DoctorCheck("warning", "kcat was not found"),
                    )
                )
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "doctor"],
                    env={"KANTRIP_CONFIG": str(config_path)},
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[passed] configuration is valid", result.output)
        self.assertIn("[warning] kcat was not found", result.output)

    def test_doctor_exits_nonzero_for_failed_checks(self) -> None:
        with patch("kantrip.cli.run_doctor") as run:
            from kantrip.doctor import DoctorCheck, DoctorReport

            run.return_value = DoctorReport((DoctorCheck("error", "configuration is invalid"),))
            result = self.runner.invoke(cli, ["--no-color", "doctor"])

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("[failed] configuration is invalid", result.output)

    def test_ping_reports_kafka_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(broker_count=2),
            ) as ping:
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn("[passed] Connected to Kafka (2 brokers)", result.output)
        ping.assert_called_once_with(unittest.mock.ANY, timeout=1.5)

    def test_ping_reports_schema_registry_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_REGISTRY_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(broker_count=2, schema_registry_subject_count=3),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn("[passed] Connected to Kafka (2 brokers)", result.output)
        self.assertIn("[passed] Connected to Schema Registry (3 subjects)", result.output)

    def test_ping_exits_nonzero_when_kafka_is_unreachable(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                side_effect=PingError("the Kafka cluster did not return metadata"),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local"],
                    env=environment,
                )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("[failed] Could not connect for profile 'local'", result.stderr)

    def test_exec_preserves_command_arguments_and_exit_status(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}
            with patch("kantrip.cli.run_profile_session", return_value=17) as run:
                result = self.runner.invoke(
                    cli, ["exec", "local", "--", "kcat", "-L"], env=environment
                )

        self.assertEqual(17, result.exit_code, result.output)
        self.assertEqual(("kcat", "-L"), run.call_args.args[2])

    def test_exec_rejects_a_nested_session(self) -> None:
        result = self.runner.invoke(
            cli,
            ["exec", "local"],
            env={"KANTRIP_SESSION_ID": "existing-session"},
        )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("session is already active", result.output)

    def test_import_has_no_filesystem_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            environment = os.environ | {
                "HOME": str(temporary_path),
                "XDG_CONFIG_HOME": str(temporary_path / "config"),
                "XDG_STATE_HOME": str(temporary_path / "state"),
                "XDG_RUNTIME_DIR": str(temporary_path / "runtime"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }

            result = subprocess.run(
                [sys.executable, "-c", "import kantrip"],
                capture_output=True,
                text=True,
                check=False,
                env=environment,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual([], list(temporary_path.iterdir()))


_VALID_CONFIG = """\
profiles:
  local:
    id: 018f8f13-7c21-7cee-8000-000000000001
    description: Local development
    kafka:
      bootstrapServers:
        - localhost:9092
      transport: plaintext
      auth:
        type: none
"""

_VALID_REGISTRY_CONFIG = _VALID_CONFIG + """\
    schemaRegistry:
      url: http://localhost:8081
      auth:
        type: none
"""


if __name__ == "__main__":
    unittest.main()
