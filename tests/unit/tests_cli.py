import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
