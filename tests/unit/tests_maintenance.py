import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.maintenance import run_repair
from kantrip.profile_storage import inspect_pending_secret_cleanup, inspect_profile_database
from kantrip.profiles import add_profile
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime
from kantrip.secret_store import SecretStoreError, VaultError, secret_reference

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"


class TestMaintenance(unittest.TestCase):
    def setUp(self) -> None:
        # Repair inspects the vault registration; never the developer's real one.
        self.vault = _VaultRegistration()
        patcher = patch("kantrip.maintenance.load_secret_store", return_value=self.vault)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_missing_vault_is_removed_from_the_keychain_search_list(self) -> None:
        self.vault.forgotten = True
        with tempfile.TemporaryDirectory() as directory:
            environment = {
                "KANTRIP_DATABASE": str(Path(directory) / "profiles.db"),
                "XDG_RUNTIME_DIR": directory,
            }

            report = run_repair(environment)

        self.assertTrue(report.healthy)
        self.assertIn(
            "Removed the missing credential vault from the keychain search list",
            [action.message for action in report.actions],
        )

    def test_vault_registration_failure_is_a_repair_error(self) -> None:
        self.vault.error = VaultError("synthetic search list failure")
        with tempfile.TemporaryDirectory() as directory:
            environment = {
                "KANTRIP_DATABASE": str(Path(directory) / "profiles.db"),
                "XDG_RUNTIME_DIR": directory,
            }

            report = run_repair(environment)

        self.assertFalse(report.healthy)
        self.assertIn(
            "Credential vault registration could not be repaired",
            [action.message for action in report.actions],
        )

    def test_unavailable_store_leaves_nothing_to_repair(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch(
                "kantrip.maintenance.load_secret_store",
                side_effect=SecretStoreError("synthetic unavailable backend"),
            ),
        ):
            report = run_repair(
                {
                    "KANTRIP_DATABASE": str(Path(directory) / "profiles.db"),
                    "XDG_RUNTIME_DIR": directory,
                }
            )

        self.assertTrue(report.healthy)

    def test_missing_database_and_runtime_are_not_created(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "data" / "profiles.db"
            environment = {
                "KANTRIP_DATABASE": str(database),
                "XDG_RUNTIME_DIR": directory,
            }

            report = run_repair(environment)

            self.assertTrue(report.healthy)
            self.assertFalse(database.parent.exists())
            self.assertFalse((root / "kantrip").exists())

    def test_repair_keeps_current_schema_and_removes_all_stale_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "profiles.db"
            environment = {
                "KANTRIP_DATABASE": str(database),
                "XDG_RUNTIME_DIR": directory,
            }
            add_profile("local", database)
            with patch("kantrip.runtime.time.time", return_value=1000):
                runtime = create_session_runtime(PROFILE_ID, 1, environment)
            runtime_path = runtime.path
            runtime._closed = True
            os.close(runtime._lock_descriptor)
            os.close(runtime._session_descriptor)
            os.close(runtime._root_descriptor)

            with patch(
                "kantrip.runtime.time.time",
                return_value=1000 + SESSION_STALE_SECONDS,
            ):
                report = run_repair(environment)

            self.assertTrue(report.healthy)
            self.assertFalse(runtime_path.exists())
            self.assertFalse(inspect_profile_database(database).requires_migration)
            messages = [action.message for action in report.actions]
            self.assertTrue(any("schema is current" in message for message in messages))
            self.assertTrue(any("Sessions: removed 1 stale" in message for message in messages))

    def test_repair_reconciles_exact_pending_credential_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "profiles.db"
            environment = {
                "KANTRIP_DATABASE": str(database),
                "XDG_RUNTIME_DIR": directory,
            }
            profile = add_profile("local", database).profile("local")
            reference = secret_reference(profile["id"], "kafka/oauth/client-secret")
            with sqlite3.connect(database) as connection:
                queue_secret_cleanup(connection, reference)
            store = _RecordingSecretStore()

            report = run_repair(environment, secret_store=store)

            self.assertTrue(report.healthy)
            self.assertEqual([reference], store.deleted)
            self.assertEqual((), inspect_pending_secret_cleanup(database))
            self.assertTrue(
                any(
                    action.status == "cleanup"
                    and action.message == "Credential reconciliation: removed 1"
                    for action in report.actions
                )
            )

    def test_repair_retains_failed_credential_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "profiles.db"
            environment = {
                "KANTRIP_DATABASE": str(database),
                "XDG_RUNTIME_DIR": directory,
            }
            profile = add_profile("local", database).profile("local")
            reference = secret_reference(profile["id"], "registry/password")
            with sqlite3.connect(database) as connection:
                queue_secret_cleanup(connection, reference)

            report = run_repair(
                environment,
                secret_store=_RecordingSecretStore(fail=True),
            )

            self.assertFalse(report.healthy)
            self.assertEqual(1, len(inspect_pending_secret_cleanup(database)))
            self.assertTrue(any("failed 1" in action.message for action in report.actions))


class _VaultRegistration:
    def __init__(self) -> None:
        self.forgotten = False
        self.error: VaultError | None = None

    def forget_missing_vault(self) -> bool:
        if self.error is not None:
            raise self.error
        return self.forgotten


class _RecordingSecretStore:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.deleted: list[str] = []

    def get(self, reference: str) -> str:
        del reference
        raise NotImplementedError

    def set(self, reference: str, value: str) -> None:
        del reference, value
        raise NotImplementedError

    def delete(self, reference: str) -> None:
        if self.fail:
            raise SecretStoreError("synthetic failure")
        self.deleted.append(reference)


if __name__ == "__main__":
    unittest.main()
