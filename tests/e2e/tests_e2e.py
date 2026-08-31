import subprocess
import sys
import unittest

from kantrip import APP_VERSION


class TestInstalledCli(unittest.TestCase):
    def test_module_entry_point_reports_version(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "kantrip.cli", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn(APP_VERSION, result.stdout)


if __name__ == "__main__":
    unittest.main()
