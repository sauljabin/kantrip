import sqlite3
import unittest
from unittest.mock import patch

from kantrip.migrations import Migration, apply_migrations


class TestMigrations(unittest.TestCase):
    def test_failed_migration_rolls_back_schema_and_history(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        failing = Migration(
            1,
            "failing migration",
            ("CREATE TABLE partial (value TEXT)", "NOT VALID SQL"),
        )

        with (
            patch("kantrip.migrations.MIGRATIONS", (failing,)),
            self.assertRaises(sqlite3.Error),
        ):
            apply_migrations(connection, applied_by="test")

        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        connection.close()
        self.assertEqual([], tables)
        self.assertEqual(0, version)


if __name__ == "__main__":
    unittest.main()
