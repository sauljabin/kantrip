import unittest
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

from tests.e2e.preconditions import E2ESetupError, _require_librdkafka_version


class TestE2EPreconditions(unittest.TestCase):
    @patch("tests.e2e.preconditions.subprocess.run")
    def test_accepts_compatible_librdkafka(self, run: MagicMock) -> None:
        run.return_value = CompletedProcess(("kcat", "-V"), 0, "librdkafka 2.15.1", "")

        _require_librdkafka_version("2.11.0")

    @patch("tests.e2e.preconditions.subprocess.run")
    def test_rejects_old_librdkafka(self, run: MagicMock) -> None:
        run.return_value = CompletedProcess(("kcat", "-V"), 0, "librdkafka 2.6.1", "")

        with self.assertRaisesRegex(E2ESetupError, "librdkafka >= 2.11.0"):
            _require_librdkafka_version("2.11.0")

    @patch("tests.e2e.preconditions.subprocess.run")
    def test_rejects_unidentified_librdkafka(self, run: MagicMock) -> None:
        run.return_value = CompletedProcess(("kcat", "-V"), 0, "kcat 1.7.1", "")

        with self.assertRaisesRegex(E2ESetupError, "librdkafka >= 2.11.0"):
            _require_librdkafka_version("2.11.0")
