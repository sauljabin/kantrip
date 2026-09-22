import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner
from rich.console import Console

from tests.e2e.adapters import (
    SmokeFailure,
    _failure_details,
    _kantrip_cli,
    _show_section,
    _write_shell_driver,
    main,
)


class TestSmoke(unittest.TestCase):
    def test_writes_one_sourced_driver_for_interactive_shell_commands(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            driver = _write_shell_driver("bash", ("first", "second"), root)

            self.assertEqual(f". {root / 'commands'} < /dev/null; exit $?", driver)
            self.assertEqual("first\nsecond\n", (root / "commands").read_text())
            self.assertEqual(0o600, (root / "commands").stat().st_mode & 0o777)

    def test_uses_fish_source_syntax_for_the_shell_driver(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            driver = _write_shell_driver("fish", (), root)

            self.assertEqual(
                f"source '{root / 'commands'}' < /dev/null; "
                "set -l kantrip_status $status; exit $kantrip_status",
                driver,
            )

    def test_sections_are_separated_from_the_sandbox_title(self) -> None:
        stream = io.StringIO()
        console = Console(file=stream, color_system=None)

        console.print("Kantrip Sandbox")
        _show_section(console, "Setup")
        _show_section(console, "Kafka CLI")

        self.assertEqual("Kantrip Sandbox\n\nSetup\n\nKafka CLI\n", stream.getvalue())

    def test_captured_kantrip_commands_explicitly_disable_color(self) -> None:
        with patch("tests.e2e.adapters.shutil.which", return_value="/opt/bin/kantrip"):
            command = _kantrip_cli("ping", "sandbox")

        self.assertEqual(
            ["/opt/bin/kantrip", "--no-color", "ping", "sandbox"],
            command,
        )

    def test_failure_details_remove_nested_status_presentation(self) -> None:
        result = subprocess.CompletedProcess(
            ["kantrip", "ping", "sandbox"],
            1,
            stdout="[running] Checking profile 'sandbox'\n",
            stderr=(
                "[failed] Could not connect for profile 'sandbox': the Kafka cluster "
                "did not return metadata\n"
            ),
        )

        self.assertEqual(
            "Could not connect for profile 'sandbox': the Kafka cluster did not return metadata",
            _failure_details(result),
        )

    @patch("tests.e2e.adapters.smoke", side_effect=SmokeFailure("connectivity failed"))
    def test_main_renders_failures_with_its_selected_presentation(self, _smoke: object) -> None:
        result = CliRunner().invoke(main, ["--no-color"])

        self.assertEqual(1, result.exit_code)
        self.assertEqual("[failed] connectivity failed\n", result.output)
        self.assertNotIn("Error:", result.output)


if __name__ == "__main__":
    unittest.main()
