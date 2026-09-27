import unittest
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

from kantrip.adapters import KASKADE_ADAPTER
from tests.e2e.preconditions import (
    E2ESetupError,
    _require_librdkafka_version,
    _require_pinned_version,
)


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

    @patch("tests.e2e.preconditions.subprocess.run")
    def test_local_clients_may_be_newer_than_the_pin(self, run: MagicMock) -> None:
        gate = KASKADE_ADAPTER.minimum_version
        for output in ("kaskade, version 5.0.1", "kaskade, version 5.0.2"):
            with self.subTest(output=output):
                run.return_value = CompletedProcess(("kaskade", "--version"), 0, output, "")

                _require_pinned_version("kaskade", "kaskade", gate, "5.0.1")

    @patch("tests.e2e.preconditions.subprocess.run")
    def test_rejects_clients_older_than_the_pin(self, run: MagicMock) -> None:
        gate = KASKADE_ADAPTER.minimum_version
        for output, found in (("kaskade, version 5.0.0", "found 5.0.0"), ("", "unreadable")):
            with self.subTest(output=output):
                run.return_value = CompletedProcess(("kaskade", "--version"), 0, output, "")

                with self.assertRaisesRegex(E2ESetupError, f"kaskade 5.0.1 or newer .*{found}"):
                    _require_pinned_version("kaskade", "kaskade", gate, "5.0.1")
