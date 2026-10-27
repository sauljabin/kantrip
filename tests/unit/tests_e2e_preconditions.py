import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

from kantrip.adapters import KAFUI_ADAPTER, KASKADE_ADAPTER
from tests.e2e.preconditions import (
    E2ESetupError,
    _require_librdkafka_version,
    _require_pinned_version,
)
from tests.unit.client_versions import fake_go_executable


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

    @patch("tests.e2e.preconditions.subprocess.run")
    def test_go_build_information_gates_are_read_without_running_the_client(
        self, run: MagicMock
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = Path(directory) / "kafui"
            client.write_bytes(
                fake_go_executable(
                    "#!/bin/sh\nexit 1\n", "mod\tgithub.com/Benny93/kafui\tv0.1.38\t\n"
                )
            )
            client.chmod(0o700)
            with patch("tests.e2e.preconditions.shutil.which", return_value=str(client)):
                _require_pinned_version("kafui", "kafui", KAFUI_ADAPTER.minimum_version, "0.1.38")
                with self.assertRaisesRegex(E2ESetupError, "kafui 0.1.39 or newer .*found 0.1.38"):
                    _require_pinned_version(
                        "kafui", "kafui", KAFUI_ADAPTER.minimum_version, "0.1.39"
                    )
        run.assert_not_called()
