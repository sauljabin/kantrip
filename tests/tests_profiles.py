import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from kantrip.migrations import MIGRATIONS, MigrationChain, SqlMigration
from kantrip.profiles import (
    DATABASE_BACKUP_PREFIX,
    DATABASE_MAINTENANCE_SUFFIX,
    DATABASE_SCHEMA_VERSION,
    ProfileStoreError,
    add_profile,
    edit_profile,
    inspect_pending_secret_cleanup,
    load_profiles,
    reconcile_pending_secrets,
    remove_profile,
    resolve_database_path,
)
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.secret_store import SecretStoreError, secret_reference


class TestProfiles(unittest.TestCase):
    def test_resolves_documented_database_precedence(self) -> None:
        environment = {
            "HOME": "/home/example",
            "XDG_DATA_HOME": "/xdg/data",
            "KANTRIP_DATABASE": "/explicit/profiles.db",
        }

        self.assertEqual(Path("/explicit/profiles.db"), resolve_database_path(environment))
        del environment["KANTRIP_DATABASE"]
        self.assertEqual(Path("/xdg/data/kantrip/profiles.db"), resolve_database_path(environment))
        del environment["XDG_DATA_HOME"]
        self.assertEqual(
            Path("/home/example/.local/share/kantrip/profiles.db"),
            resolve_database_path(environment),
        )

    def test_adds_loads_and_removes_a_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "kantrip" / "profiles.db"

            profiles = add_profile("local", path, description="Local development")

            self.assertEqual(
                ["localhost:9092"], profiles.profile("local")["kafka"]["bootstrapServers"]
            )
            self.assertEqual("Local development", profiles.profile("local")["description"])
            self.assertEqual(0o700, path.parent.stat().st_mode & 0o777)
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            self.assertEqual(["local"], list(load_profiles(path).profiles))

            profiles = remove_profile("local", path)

            self.assertEqual({}, profiles.profiles)
            self.assertEqual({}, load_profiles(path).profiles)

    def test_database_uses_wal_and_the_current_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with closing(sqlite3.connect(path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                migrations = connection.execute(
                    "SELECT sequence, name, checksum, applied_at, applied_by "
                    "FROM schema_migrations ORDER BY sequence"
                ).fetchall()

            self.assertEqual("wal", journal_mode)
            self.assertEqual(DATABASE_SCHEMA_VERSION, version)
            self.assertEqual(
                [migration.sequence for migration in MIGRATIONS], [row[0] for row in migrations]
            )
            for expected, row in zip(MIGRATIONS, migrations, strict=True):
                self.assertEqual(expected.name, row[1])
                self.assertEqual(expected.checksum, row[2])
                self.assertTrue(row[3])
                self.assertTrue(row[4])
            self.assertEqual([], list(path.parent.glob(f"{path.name}{DATABASE_BACKUP_PREFIX}*")))
            self.assertEqual(
                0o600,
                Path(f"{path}{DATABASE_MAINTENANCE_SUFFIX}").stat().st_mode & 0o777,
            )

    def test_rejects_a_nonempty_database_without_migration_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("DROP TABLE schema_migrations")
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "schema is invalid"):
                load_profiles(path)
            self.assertEqual([], list(path.parent.glob(f"{path.name}{DATABASE_BACKUP_PREFIX}*")))

    def test_upgrades_sequence_one_to_the_reconciliation_journal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("DROP TABLE credential_reconciliation")
                connection.execute("DELETE FROM schema_migrations WHERE sequence = 2")
                connection.execute("PRAGMA user_version = 1")
                connection.commit()

            profiles = load_profiles(path)

            self.assertIn("local", profiles.profiles)
            with closing(sqlite3.connect(path)) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                history = connection.execute(
                    "SELECT sequence FROM schema_migrations ORDER BY sequence"
                ).fetchall()
                journal_exists = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'credential_reconciliation'"
                ).fetchone()
            backups = list(path.parent.glob(f"{path.name}{DATABASE_BACKUP_PREFIX}*"))
            self.assertEqual(2, version)
            self.assertEqual([(1,), (2,)], history)
            self.assertIsNotNone(journal_exists)
            self.assertEqual(1, len(backups))
            self.assertEqual(0o600, backups[0].stat().st_mode & 0o777)

    def test_each_migration_pass_keeps_a_uniquely_timestamped_private_backup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            next_sequence = len(MIGRATIONS) + 1

            class NextMigration(SqlMigration):
                sequence = next_sequence
                name = "next migration"
                statements = ("UPDATE profiles SET revision = revision",)

            class FollowingMigration(SqlMigration):
                sequence = next_sequence + 1
                name = "following migration"
                statements = ("UPDATE profiles SET revision = revision",)

            second = NextMigration()
            third = FollowingMigration()

            with (
                patch(
                    "kantrip.migrations.MIGRATIONS",
                    MigrationChain(*MIGRATIONS, second),
                ),
                patch(
                    "kantrip.profiles._backup_timestamp",
                    return_value="2026-09-14T01-02-03.000004Z",
                ),
            ):
                load_profiles(path)
            with (
                patch(
                    "kantrip.migrations.MIGRATIONS",
                    MigrationChain(*MIGRATIONS, second, third),
                ),
                patch(
                    "kantrip.profiles._backup_timestamp",
                    return_value="2026-09-15T02-03-04.000005Z",
                ),
            ):
                load_profiles(path)

            backups = sorted(path.parent.glob(f"{path.name}{DATABASE_BACKUP_PREFIX}*"))
            self.assertEqual(2, len(backups))
            self.assertIn("2026-09-14T01-02-03.000004Z", backups[0].name)
            self.assertIn("2026-09-15T02-03-04.000005Z", backups[1].name)
            self.assertTrue(all(backup.stat().st_mode & 0o777 == 0o600 for backup in backups))
            with closing(sqlite3.connect(backups[0])) as connection:
                first_version = connection.execute("PRAGMA user_version").fetchone()[0]
            with closing(sqlite3.connect(backups[1])) as connection:
                second_version = connection.execute("PRAGMA user_version").fetchone()[0]
            self.assertEqual(
                (len(MIGRATIONS), next_sequence),
                (first_version, second_version),
            )

    def test_rejects_modified_migration_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE schema_migrations SET checksum = 'modified' WHERE sequence = 1"
                )
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "migration 1 was modified"):
                load_profiles(path)

    def test_rejects_unknown_applied_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "INSERT INTO schema_migrations VALUES (3, 'unknown', 'checksum', 'now', 'test')"
                )
                connection.execute("PRAGMA user_version = 3")
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "unknown migrations"):
                load_profiles(path)

    def test_rejects_migration_history_that_disagrees_with_user_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA user_version = 0")
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "does not match"):
                load_profiles(path)

    def test_concurrent_writers_do_not_lose_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("initial", path)

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(add_profile, f"profile-{index}", path) for index in range(8)
                ]
                for future in futures:
                    future.result()

            self.assertEqual(
                ["initial", *(f"profile-{index}" for index in range(8))],
                list(load_profiles(path).profiles),
            )

    def test_concurrent_initialization_applies_the_migration_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(add_profile, f"profile-{index}", path) for index in range(4)
                ]
                for future in futures:
                    future.result()

            with closing(sqlite3.connect(path)) as connection:
                migration_count = connection.execute(
                    "SELECT COUNT(*) FROM schema_migrations"
                ).fetchone()[0]

            self.assertEqual(len(MIGRATIONS), migration_count)
            self.assertEqual(4, len(load_profiles(path).profiles))

    def test_missing_database_can_be_loaded_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing" / "profiles.db"

            profiles = load_profiles(path, missing_ok=True)

            self.assertEqual({}, profiles.profiles)
            self.assertFalse(path.parent.exists())

    def test_unknown_profile_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with self.assertRaisesRegex(ProfileStoreError, "profile 'missing' was not found"):
                load_profiles(path).profile("missing")

    def test_adds_a_confluent_registry_connection_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profiles = add_profile(
                "local",
                Path(directory) / "profiles.db",
                registry_url="http://localhost:8081",
            )

        self.assertEqual(
            {
                "provider": "confluent",
                "schema.registry.url": "http://localhost:8081",
            },
            profiles.profile("local")["registry"],
        )

    def test_adds_an_apicurio_registry_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profiles = add_profile(
                "local",
                Path(directory) / "profiles.db",
                registry_provider="apicurio",
                registry_url="http://localhost:8082/apis/registry/v3",
            )

        self.assertEqual(
            {
                "provider": "apicurio",
                "apicurio.registry.url": "http://localhost:8082/apis/registry/v3",
            },
            profiles.profile("local")["registry"],
        )

    def test_edits_plain_profile_fields_and_advances_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            original = add_profile(
                "local",
                path,
                description="Old description",
                registry_url="http://localhost:8081",
            ).profile("local")

            profiles = edit_profile(
                "local",
                path,
                bootstrap_servers=("broker-1.example.com:9092", "broker-2.example.com:9092"),
                description="New description",
                labels={"environment": "development", "owner": "platform"},
                registry_provider="apicurio",
                registry_url="http://localhost:8082/apis/registry/v3",
            )

            updated = profiles.profile("local")
            self.assertEqual(2, profiles.revision("local"))
            self.assertEqual(original["id"], updated["id"])
            self.assertEqual(
                ["broker-1.example.com:9092", "broker-2.example.com:9092"],
                updated["kafka"]["bootstrapServers"],
            )
            self.assertEqual("New description", updated["description"])
            self.assertEqual({"environment": "development", "owner": "platform"}, updated["labels"])
            self.assertEqual(
                {
                    "provider": "apicurio",
                    "apicurio.registry.url": "http://localhost:8082/apis/registry/v3",
                },
                updated["registry"],
            )
            with closing(sqlite3.connect(path)) as connection:
                revision = connection.execute(
                    "SELECT revision FROM profiles WHERE name = 'local'"
                ).fetchone()[0]
            self.assertEqual(2, revision)

    def test_add_persists_labels_and_exposes_initial_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"

            profiles = add_profile(
                "production",
                path,
                labels={"environment": "production", "owner": "platform"},
            )

        self.assertEqual(
            {"environment": "production", "owner": "platform"},
            profiles.profile("production")["labels"],
        )
        self.assertEqual(1, profiles.revision("production"))

    def test_edit_can_add_and_explicitly_remove_a_default_confluent_registry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            added = edit_profile(
                "local",
                path,
                registry_url="http://localhost:8081",
            ).profile("local")
            removed = edit_profile("local", path, remove_registry=True).profile("local")

        self.assertEqual(
            {
                "provider": "confluent",
                "schema.registry.url": "http://localhost:8081",
            },
            added["registry"],
        )
        self.assertNotIn("registry", removed)

    def test_edit_rejects_conflicts_without_mutating_the_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            before = add_profile("local", path, description="Keep me").profile("local")
            cases = (
                ({}, "no profile changes"),
                (
                    {"description": "Replace", "clear_description": True},
                    "cannot be combined",
                ),
                (
                    {"registry_provider": "apicurio"},
                    "requires --registry-url",
                ),
                (
                    {"labels": {"owner": "team"}, "remove_labels": ("owner",)},
                    "cannot be set and removed",
                ),
            )
            for arguments, message in cases:
                with self.subTest(arguments=arguments):
                    with self.assertRaisesRegex(ProfileStoreError, message):
                        edit_profile("local", path, **arguments)
                    self.assertEqual(before, load_profiles(path).profile("local"))

    def test_reconciles_only_exact_journaled_secret_references(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            profile = add_profile("local", path).profile("local")
            reference = secret_reference(profile["id"], "kafka/password")
            with closing(sqlite3.connect(path)) as connection:
                queue_secret_cleanup(connection, reference)
                connection.commit()
            store = _RecordingSecretStore()

            pending = inspect_pending_secret_cleanup(path)
            result = reconcile_pending_secrets(path, store=store)

            self.assertEqual((reference,), tuple(record.secret_reference for record in pending))
            self.assertEqual((reference,), tuple(store.deleted))
            self.assertEqual((1, 1, 0), (result.pending, result.removed, result.failed))
            self.assertEqual((), inspect_pending_secret_cleanup(path))

    def test_failed_secret_reconciliation_remains_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            profile = add_profile("local", path).profile("local")
            reference = secret_reference(profile["id"], "registry/token")
            with closing(sqlite3.connect(path)) as connection:
                queue_secret_cleanup(connection, reference)
                connection.commit()

            result = reconcile_pending_secrets(path, store=_RecordingSecretStore(fail=True))

            self.assertEqual((1, 0, 1), (result.pending, result.removed, result.failed))
            self.assertEqual(1, len(inspect_pending_secret_cleanup(path)))

    def test_rejects_a_stored_registry_without_a_provider(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            profile = add_profile(
                "local",
                path,
                registry_url="http://localhost:8081",
            ).profile("local")
            del profile["registry"]["provider"]
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE profiles SET document = ? WHERE name = 'local'",
                    (json.dumps(profile, sort_keys=True, separators=(",", ":")),),
                )
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "does not match schema at registry"):
                load_profiles(path)

    def test_add_rejects_invalid_input_without_creating_a_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            cases = (
                ({"registry_url": "https://registry.example.com"}, "supports only an http://"),
                ({"registry_provider": "apicurio"}, "requires --registry-url"),
            )
            for arguments, message in cases:
                with self.subTest(arguments=arguments):
                    with self.assertRaisesRegex(ProfileStoreError, message):
                        add_profile("local", path, **arguments)
                    self.assertFalse(path.exists())

    def test_add_refuses_to_replace_an_existing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with self.assertRaisesRegex(ProfileStoreError, "already exists"):
                add_profile("local", path)

    def test_rejects_unsafe_profile_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            for name in ("", ".local", "../local", "folder/local", "local\x1b[31m", "x" * 129):
                with (
                    self.subTest(name=name),
                    self.assertRaisesRegex(ProfileStoreError, "safe characters"),
                ):
                    add_profile(name, path)
            self.assertFalse(path.exists())

    def test_rejects_an_exposed_database_or_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "private" / "profiles.db"
            add_profile("local", database)
            database.chmod(0o644)

            with self.assertRaisesRegex(ProfileStoreError, "permissions are not private"):
                load_profiles(database)

            database.chmod(0o600)
            database.parent.chmod(0o755)
            with self.assertRaisesRegex(ProfileStoreError, "directory is not private"):
                load_profiles(database)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_rejects_a_symlink_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.db"
            database = root / "profiles.db"
            target.touch(mode=0o600)
            database.symlink_to(target)

            with self.assertRaisesRegex(ProfileStoreError, "not a regular file"):
                load_profiles(database)

    def test_rejects_corrupt_and_foreign_databases_without_echoing_contents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            corrupt = Path(directory) / "corrupt.db"
            corrupt.write_bytes(b"password=synthetic-secret")
            corrupt.chmod(0o600)

            with self.assertRaisesRegex(ProfileStoreError, "could not be read safely") as raised:
                load_profiles(corrupt)
            self.assertNotIn("synthetic-secret", str(raised.exception))

            foreign = Path(directory) / "foreign.db"
            with closing(sqlite3.connect(foreign)) as connection:
                connection.execute("CREATE TABLE foreign_data (value TEXT)")
                connection.commit()
            foreign.chmod(0o600)
            with self.assertRaisesRegex(ProfileStoreError, "schema is invalid"):
                add_profile("local", foreign)

    def test_rejects_an_unsupported_database_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA user_version = 99")
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "version 99 is not supported"):
                load_profiles(path)

    def test_rejects_invalid_stored_json_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with closing(sqlite3.connect(path)) as connection:
                connection.execute("UPDATE profiles SET document = ?", ("not-json",))
                connection.commit()
            with self.assertRaisesRegex(ProfileStoreError, "not valid JSON"):
                load_profiles(path)
            with self.assertRaisesRegex(ProfileStoreError, "not valid JSON"):
                add_profile("not-committed", path)
            with closing(sqlite3.connect(path)) as connection:
                names = connection.execute("SELECT name FROM profiles").fetchall()
            self.assertEqual([("local",)], names)

            valid = add_profile("other", Path(directory) / "other.db").profile("other")
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE profiles SET document = ?",
                    (json.dumps(valid, sort_keys=True, separators=(",", ":")),),
                )
                connection.commit()
            with self.assertRaisesRegex(ProfileStoreError, "inconsistent identity"):
                load_profiles(path)


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
