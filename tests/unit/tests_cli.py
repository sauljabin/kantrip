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

    def test_configuration_commands_use_resolved_file(self) -> None:
        with self.runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            environment = {"KANTRIP_CONFIG": str(config_path.resolve())}

            validated = self.runner.invoke(cli, ["config", "validate"], env=environment)
            listed = self.runner.invoke(cli, ["list"], env=environment)
            shown = self.runner.invoke(cli, ["show", "local"], env=environment)

        self.assertEqual(0, validated.exit_code, validated.output)
        self.assertIn("Configuration is valid", validated.output)
        self.assertEqual("local\tLocal development\n", listed.output)
        self.assertEqual(0, shown.exit_code, shown.output)
        self.assertIn("bootstrapServers:", shown.output)

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
version: 1
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


if __name__ == "__main__":
    unittest.main()
