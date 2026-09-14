import sqlite3
import unittest
from unittest.mock import patch

from kantrip.migrations import (
    MIGRATIONS,
    MigrationChain,
    MigrationError,
    SqlMigration,
    apply_migrations,
)


class FailingMigration(SqlMigration):
    sequence = 1
    name = "failing migration"
    statements = ("CREATE TABLE partial (value TEXT)", "NOT VALID SQL")


class FirstMigration(SqlMigration):
    sequence = 1
    name = "first migration"
    statements = ("SELECT 1",)


class SecondMigration(SqlMigration):
    sequence = 2
    name = "second migration"
    statements = ("SELECT 2",)


class TestMigrations(unittest.TestCase):
    def test_bundled_migration_checksums_are_immutable(self) -> None:
        self.assertEqual(
            [
                "a211043fcbe848180ab783b81fd9a28ecfc4b85780c45c7ce2d55790a4d3f740",
                "60de4ddf3c37d95e114b759b6c21e41cf73f06e9d186e995d7a2a7ba085f6d5f",
            ],
            [migration.checksum for migration in MIGRATIONS],
        )

    def test_failed_migration_rolls_back_schema_and_history(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        failing_chain = MigrationChain(FailingMigration())

        with (
            patch("kantrip.migrations.MIGRATIONS", failing_chain),
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

    def test_chain_exposes_only_migrations_after_a_sequence(self) -> None:
        chain = MigrationChain(FirstMigration(), SecondMigration())

        self.assertEqual((2,), tuple(item.sequence for item in chain.pending_after(1)))
        self.assertEqual(2, chain.latest_sequence)

    def test_chain_rejects_an_empty_or_non_contiguous_registry(self) -> None:
        with self.assertRaisesRegex(MigrationError, "empty"):
            MigrationChain()
        with self.assertRaisesRegex(MigrationError, "not contiguous"):
            MigrationChain(SecondMigration())


if __name__ == "__main__":
    unittest.main()
