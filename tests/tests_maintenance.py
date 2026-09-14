import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.maintenance import run_repair
from kantrip.profiles import add_profile, inspect_profile_database
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime


class TestMaintenance(unittest.TestCase):
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
                runtime = create_session_runtime(environment)
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


if __name__ == "__main__":
    unittest.main()
