"""Black-box acceptance against the explicitly provisioned sandbox."""

from __future__ import annotations

import fcntl
import os
import secrets
import unittest
from typing import TextIO

from rich.console import Console

from sandbox.__main__ import STATE_ROOT
from tests.e2e import adapters, authentication
from tests.e2e.preconditions import check_preconditions


class TestSandboxEndToEnd(unittest.TestCase):
    """Exercise released external clients through the installed Kantrip command."""

    lock_stream: TextIO

    @classmethod
    def setUpClass(cls) -> None:
        check_preconditions(os.environ)
        lock_path = STATE_ROOT / "e2e.lock"
        cls.lock_stream = lock_path.open("a+", encoding="utf-8")
        lock_path.chmod(0o600)
        try:
            fcntl.flock(cls.lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            cls.lock_stream.close()
            raise RuntimeError("another E2E run owns the shared OAuth identities") from error

    @classmethod
    def tearDownClass(cls) -> None:
        fcntl.flock(cls.lock_stream.fileno(), fcntl.LOCK_UN)
        cls.lock_stream.close()

    def test_10_plaintext_adapter_operations(self) -> None:
        run_id = secrets.token_hex(6)
        adapters.smoke(
            Console(color_system=None),
            profile=f"e2e-{run_id}",
            bootstrap_servers=("localhost:9092",),
            topic=f"kantrip-smoke-e2e-{run_id}",
            keep_topic=False,
            registry_provider="confluent",
            registry_url="http://localhost:8081",
            environment=os.environ,
            shells=("bash", "zsh", "fish"),
        )

    def test_20_authenticated_matrix(self) -> None:
        authentication.main()

    def test_30_registry_oauth_lifetime(self) -> None:
        authentication.exercise_registry_oauth()


if __name__ == "__main__":
    unittest.main()
