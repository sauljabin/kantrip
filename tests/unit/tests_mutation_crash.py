import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from kantrip.profile_storage import load_profiles, reconcile_pending_secrets
from kantrip.reconciliation import pending_secret_cleanup
from tests.unit.mutation_worker import FIRST_SECRET, SECOND_SECRET, FileSecretStore


@unittest.skipUnless(hasattr(signal, "SIGKILL"), "SIGKILL is unavailable")
class TestMutationCrashRecovery(unittest.TestCase):
    def test_kill_after_durable_intent_before_store_write(self) -> None:
        database, store_root, output = self._kill_at("before-store-write")

        self.assertEqual({}, load_profiles(database).profiles)
        self.assertEqual(1, len(_cleanup_records(database)))
        self.assertEqual((), FileSecretStore(store_root).references())
        self._assert_private_and_secret_free(database, output)

    def test_kill_after_store_readback_before_profile_commit(self) -> None:
        database, store_root, output = self._kill_at("after-store-readback")

        self.assertEqual({}, load_profiles(database).profiles)
        records = _cleanup_records(database)
        self.assertEqual(1, len(records))
        self.assertEqual((records[0].secret_reference,), FileSecretStore(store_root).references())
        self._assert_private_and_secret_free(database, output)

    def test_kill_after_profile_commit_before_reload(self) -> None:
        database, store_root, output = self._kill_at("after-profile-commit")

        profile = load_profiles(database).profile("local")
        reference = profile["kafka"]["auth"]["passwordRef"]
        self.assertEqual((), _cleanup_records(database))
        self.assertEqual((reference,), FileSecretStore(store_root).references())
        self._assert_private_and_secret_free(database, output)

    def test_kill_after_secret_delete_before_journal_removal_is_repairable(self) -> None:
        database, store_root, output = self._kill_at("after-store-delete")

        profiles = load_profiles(database)
        profile = profiles.profile("local")
        self.assertEqual(2, profiles.revision("local"))
        active = profile["kafka"]["auth"]["passwordRef"]
        records = _cleanup_records(database)
        self.assertEqual(1, len(records))
        self.assertNotEqual(active, records[0].secret_reference)
        self.assertEqual((active,), FileSecretStore(store_root).references())

        repaired = reconcile_pending_secrets(
            database,
            store=FileSecretStore(store_root),
        )
        repeated = reconcile_pending_secrets(
            database,
            store=FileSecretStore(store_root),
        )

        self.assertEqual((1, 1, 0), (repaired.pending, repaired.removed, repaired.failed))
        self.assertEqual((0, 0, 0), (repeated.pending, repeated.removed, repeated.failed))
        self.assertEqual((), _cleanup_records(database))
        self._assert_private_and_secret_free(database, output)

    def _kill_at(self, scenario: str) -> tuple[Path, Path, str]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        database = root / "state" / "profiles.db"
        store_root = root / "secrets"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.unit.mutation_worker",
                scenario,
                str(database),
                str(store_root),
            ],
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={
                key: value
                for key, value in os.environ.items()
                if key not in {"KANTRIP_DATABASE", "KANTRIP_KEYRING_BACKEND"}
            },
        )
        self.addCleanup(_terminate, process)
        assert process.stdout is not None
        barrier = process.stdout.readline().strip()
        self.assertEqual(f"BARRIER {scenario}", barrier)
        os.kill(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        self.assertEqual(-signal.SIGKILL, process.returncode)
        return database, store_root, f"{barrier}\n{stdout}\n{stderr}"

    def _assert_private_and_secret_free(self, database: Path, output: str) -> None:
        self.assertEqual(0o700, database.parent.stat().st_mode & 0o777)
        self.assertEqual(0o600, database.stat().st_mode & 0o777)
        self.assertNotIn(FIRST_SECRET, output)
        self.assertNotIn(SECOND_SECRET, output)


def _cleanup_records(database: Path):
    with closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        return pending_secret_cleanup(connection)


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
        process.wait()


if __name__ == "__main__":
    unittest.main()
