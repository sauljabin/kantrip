import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Event
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import kantrip.profiles as profiles_module
from kantrip.migrations import MIGRATIONS, MigrationChain, SqlMigration
from kantrip.profiles import (
    DATABASE_BACKUP_PREFIX,
    DATABASE_MAINTENANCE_SUFFIX,
    DATABASE_SCHEMA_VERSION,
    KafkaAuthInput,
    ProfileStoreError,
    RegistryAuthInput,
    add_profile,
    database_maintenance_lock,
    edit_profile,
    inspect_pending_secret_cleanup,
    load_profiles,
    reconcile_pending_secrets,
    remove_profile,
    resolve_database_path,
    resolve_profile_snapshot,
)
from kantrip.reconciliation import ReconciliationResult, queue_secret_cleanup
from kantrip.secret_store import SecretStoreError, secret_reference
from tests.unit.pki import synthetic_pki


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

    def test_add_reports_committed_when_post_commit_reload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"

            with (
                patch(
                    "kantrip.profiles._load_profile_collection",
                    side_effect=sqlite3.OperationalError("synthetic reload failure"),
                ),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                add_profile("local", path)

            self.assertEqual(3, raised.exception.exit_code)
            self.assertIn("committed", str(raised.exception))
            self.assertIn("local", load_profiles(path).profiles)

    def test_edit_reports_committed_when_post_commit_reload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with (
                patch(
                    "kantrip.profiles._load_profile_collection",
                    side_effect=sqlite3.OperationalError("synthetic reload failure"),
                ),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                edit_profile("local", path, description="persisted")

            self.assertEqual(3, raised.exception.exit_code)
            self.assertIn("committed", str(raised.exception))
            persisted = load_profiles(path)
            self.assertEqual("persisted", persisted.profile("local")["description"])
            self.assertEqual(2, persisted.revision("local"))

    def test_remove_reports_committed_when_post_commit_reload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with (
                patch(
                    "kantrip.profiles._load_profile_collection",
                    side_effect=sqlite3.OperationalError("synthetic reload failure"),
                ),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                remove_profile("local", path)

            self.assertEqual(3, raised.exception.exit_code)
            self.assertIn("committed", str(raised.exception))
            self.assertNotIn("local", load_profiles(path).profiles)

    def test_add_reports_committed_when_file_hardening_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"

            with (
                patch(
                    "kantrip.profiles._harden_sqlite_files",
                    side_effect=OSError("synthetic chmod failure"),
                ),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                add_profile("local", path)

            self.assertEqual(3, raised.exception.exit_code)
            self.assertIn("committed", str(raised.exception))
            self.assertIn("local", load_profiles(path).profiles)

    def test_lost_commit_ack_is_classified_from_durable_add_evidence(self) -> None:
        for durable, expected_exit in ((True, 3), (False, 1), (None, 4)):
            with self.subTest(durable=durable), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "profiles.db"
                factory = _commit_fault_factory(durable)

                with (
                    patch("kantrip.profiles._connect", side_effect=factory),
                    self.assertRaises(ProfileStoreError) as raised,
                ):
                    add_profile("local", path)

                self.assertEqual(expected_exit, raised.exception.exit_code)
                persisted = load_profiles(path).profiles
                self.assertEqual(durable is not False, "local" in persisted)

    def test_lost_commit_ack_is_confirmed_for_authenticated_mutation_family(self) -> None:
        auth = KafkaAuthInput("plain", username="alice", password="first-secret")
        replacement = KafkaAuthInput("plain", username="alice", password="second-secret")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "add.db"
            store = _RecordingSecretStore()
            with (
                patch("kantrip.profiles._connect", side_effect=_commit_fault_factory(True)),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                add_profile(
                    "local",
                    path,
                    transport="tls",
                    auth=auth,
                    secret_store=store,
                )
            self.assertEqual(3, raised.exception.exit_code)
            self.assertIn("local", load_profiles(path).profiles)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "edit.db"
            store = _RecordingSecretStore()
            add_profile(
                "local",
                path,
                transport="tls",
                auth=auth,
                secret_store=store,
            )
            with (
                patch("kantrip.profiles._connect", side_effect=_commit_fault_factory(True)),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                edit_profile("local", path, auth=replacement, secret_store=store)
            self.assertEqual(3, raised.exception.exit_code)
            self.assertEqual(2, load_profiles(path).revision("local"))
            self.assertEqual(1, len(inspect_pending_secret_cleanup(path)))

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "remove.db"
            store = _RecordingSecretStore()
            add_profile(
                "local",
                path,
                transport="tls",
                auth=auth,
                secret_store=store,
            )
            with (
                patch("kantrip.profiles._connect", side_effect=_commit_fault_factory(True)),
                self.assertRaises(ProfileStoreError) as raised,
            ):
                remove_profile("local", path, secret_store=store)
            self.assertEqual(3, raised.exception.exit_code)
            self.assertNotIn("local", load_profiles(path).profiles)
            self.assertEqual(1, len(inspect_pending_secret_cleanup(path)))

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

    def test_competing_edits_accept_exactly_one_captured_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            initial = add_profile("local", path)
            profile_id = str(initial.profile("local")["id"])
            revision = initial.revision("local")
            barrier = Barrier(3)

            def update(description: str) -> str:
                barrier.wait()
                return edit_profile(
                    "local",
                    path,
                    description=description,
                    expected_profile_id=profile_id,
                    expected_revision=revision,
                ).profile("local")["description"]

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(update, value) for value in ("first", "second")]
                barrier.wait()
                outcomes = []
                for future in futures:
                    try:
                        outcomes.append(("ok", future.result()))
                    except ProfileStoreError as error:
                        outcomes.append(("error", str(error)))

            self.assertEqual(1, sum(kind == "ok" for kind, _ in outcomes))
            self.assertEqual(1, sum(kind == "error" for kind, _ in outcomes))
            self.assertEqual(2, load_profiles(path).revision("local"))

    def test_competing_edit_and_remove_preserve_one_exact_generation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            initial = add_profile("local", path)
            profile_id = str(initial.profile("local")["id"])
            revision = initial.revision("local")
            barrier = Barrier(3)

            def edit() -> None:
                barrier.wait()
                edit_profile(
                    "local",
                    path,
                    description="edited",
                    expected_profile_id=profile_id,
                    expected_revision=revision,
                )

            def remove() -> None:
                barrier.wait()
                remove_profile(
                    "local",
                    path,
                    expected_profile_id=profile_id,
                    expected_revision=revision,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = (executor.submit(edit), executor.submit(remove))
                barrier.wait()
                successes = 0
                failures = 0
                for future in futures:
                    try:
                        future.result()
                        successes += 1
                    except ProfileStoreError:
                        failures += 1

            self.assertEqual((1, 1), (successes, failures))
            remaining = load_profiles(path).profiles
            if "local" in remaining:
                self.assertEqual("edited", remaining["local"]["description"])

    def test_stale_remove_confirmation_cannot_remove_recreated_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            captured = add_profile("local", path)
            stale_id = str(captured.profile("local")["id"])
            stale_revision = captured.revision("local")
            remove_profile("local", path)
            recreated = add_profile("local", path)

            with self.assertRaisesRegex(ProfileStoreError, "changed after removal"):
                remove_profile(
                    "local",
                    path,
                    expected_profile_id=stale_id,
                    expected_revision=stale_revision,
                )

            self.assertEqual(
                recreated.profile("local")["id"],
                load_profiles(path).profile("local")["id"],
            )

    def test_lock_timeout_is_bounded_and_preserves_the_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            original = add_profile("local", path).profile("local")

            with (
                database_maintenance_lock(path),
                patch("kantrip.profiles.DATABASE_TIMEOUT_SECONDS", 0),
                self.assertRaisesRegex(ProfileStoreError, "maintenance is busy"),
            ):
                edit_profile("local", path, description="must-not-commit")

            self.assertEqual(original, load_profiles(path).profile("local"))

    def test_snapshot_and_credential_edit_never_mix_generations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            add_profile(
                "local",
                path,
                transport="tls",
                auth=KafkaAuthInput("plain", username="alice", password="old-secret"),
                secret_store=store,
            )
            entered = Event()
            release = Event()
            blocking_store = _OneShotBlockingGetStore(store, entered, release)

            with ThreadPoolExecutor(max_workers=2) as executor:
                snapshot_future = executor.submit(
                    resolve_profile_snapshot,
                    "local",
                    path,
                    secret_store=blocking_store,
                )
                self.assertTrue(entered.wait(timeout=5))
                edit_future = executor.submit(
                    edit_profile,
                    "local",
                    path,
                    auth=KafkaAuthInput("plain", username="alice", password="new-secret"),
                    secret_store=store,
                )
                release.set()
                snapshot = snapshot_future.result()
                edited = edit_future.result()

            self.assertEqual(1, snapshot.revision)
            self.assertEqual("old-secret", snapshot.kafka.password)
            self.assertEqual(2, edited.revision("local"))
            active_reference = edited.profile("local")["kafka"]["auth"]["passwordRef"]
            self.assertEqual("new-secret", store.values[active_reference])

    def test_registry_only_snapshot_survives_a_concurrent_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "local",
                path,
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="synthetic-user", password="old-registry-secret"
                ),
                secret_store=store,
            ).profile("local")
            old_reference = added["registry"]["auth"]["passwordRef"]
            entered = Event()
            release = Event()
            blocking_store = _OneShotBlockingGetStore(store, entered, release)

            with ThreadPoolExecutor(max_workers=2) as executor:
                snapshot_future = executor.submit(
                    resolve_profile_snapshot, "local", path, secret_store=blocking_store
                )
                self.assertTrue(entered.wait(timeout=5))
                edit_future = executor.submit(
                    edit_profile,
                    "local",
                    path,
                    registry_auth=RegistryAuthInput(
                        "basic", username="synthetic-user", password="new-registry-secret"
                    ),
                    secret_store=store,
                )
                release.set()
                snapshot = snapshot_future.result()
                edited = edit_future.result()

            self.assertEqual(1, snapshot.revision)
            self.assertIsNotNone(snapshot.registry)
            assert snapshot.registry is not None
            self.assertEqual("old-registry-secret", snapshot.registry.password)
            self.assertEqual(2, edited.revision("local"))
            self.assertIn(old_reference, store.deleted)
            self.assertNotIn(old_reference, store.values)

    def test_combined_snapshot_survives_a_concurrent_removal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "local",
                path,
                transport="tls",
                auth=KafkaAuthInput("plain", username="kafka-user", password="old-kafka-secret"),
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="registry-user", password="old-registry-secret"
                ),
                secret_store=store,
            ).profile("local")
            kafka_reference = added["kafka"]["auth"]["passwordRef"]
            registry_reference = added["registry"]["auth"]["passwordRef"]
            entered = Event()
            release = Event()
            blocking_store = _OneShotBlockingGetStore(store, entered, release)

            with ThreadPoolExecutor(max_workers=2) as executor:
                snapshot_future = executor.submit(
                    resolve_profile_snapshot, "local", path, secret_store=blocking_store
                )
                self.assertTrue(entered.wait(timeout=5))
                remove_future = executor.submit(remove_profile, "local", path, secret_store=store)
                release.set()
                snapshot = snapshot_future.result()
                removed = remove_future.result()

            self.assertEqual(1, snapshot.revision)
            self.assertEqual("old-kafka-secret", snapshot.kafka.password)
            self.assertIsNotNone(snapshot.registry)
            assert snapshot.registry is not None
            self.assertEqual("old-registry-secret", snapshot.registry.password)
            self.assertEqual({}, removed.profiles)
            self.assertNotIn(kafka_reference, store.values)
            self.assertNotIn(registry_reference, store.values)

    def test_repair_and_secret_staging_are_serialized_by_the_maintenance_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            add_profile(
                "local",
                path,
                transport="tls",
                auth=KafkaAuthInput("plain", username="alice", password="old-secret"),
                secret_store=store,
            )
            entered = Event()
            release = Event()
            repair_started = Event()
            blocking_store = _OneShotBlockingSetStore(store, entered, release)

            def repair() -> ReconciliationResult:
                repair_started.set()
                return reconcile_pending_secrets(path, store=store)

            with ThreadPoolExecutor(max_workers=2) as executor:
                edit_future = executor.submit(
                    edit_profile,
                    "local",
                    path,
                    auth=KafkaAuthInput("plain", username="alice", password="new-secret"),
                    secret_store=blocking_store,
                )
                self.assertTrue(entered.wait(timeout=5))
                repair_future = executor.submit(repair)
                self.assertTrue(repair_started.wait(timeout=5))
                release.set()
                edited = edit_future.result()
                repaired = repair_future.result()

            self.assertEqual(2, edited.revision("local"))
            self.assertEqual((0, 0, 0), (repaired.pending, repaired.removed, repaired.failed))
            self.assertEqual((), inspect_pending_secret_cleanup(path))
            active_reference = edited.profile("local")["kafka"]["auth"]["passwordRef"]
            self.assertEqual("new-secret", store.values[active_reference])

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
                "auth": {"type": "none"},
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
                "auth": {"type": "none"},
                "provider": "apicurio",
                "apicurio.registry.url": "http://localhost:8082/apis/registry/v3",
            },
            profiles.profile("local")["registry"],
        )

    def test_add_stages_registry_basic_secret_with_the_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            profiles = add_profile(
                "local",
                path,
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="synthetic-user", password="synthetic-password"
                ),
                secret_store=store,
            )

        auth = profiles.profile("local")["registry"]["auth"]
        self.assertEqual("basic", auth["type"])
        self.assertEqual("synthetic-user", auth["username"])
        self.assertNotIn("synthetic-password", json.dumps(profiles.profiles))
        self.assertEqual("synthetic-password", store.values[auth["passwordRef"]])

    def test_edit_rotates_and_removes_registry_credentials_recoverably(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            add_profile(
                "local",
                path,
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="synthetic-user", password="first-password"
                ),
                secret_store=store,
            )
            rotated = edit_profile(
                "local",
                path,
                registry_auth=RegistryAuthInput(
                    "basic", username="synthetic-user", password="second-password"
                ),
                secret_store=store,
            ).profile("local")
            reference = rotated["registry"]["auth"]["passwordRef"]
            self.assertEqual("second-password", store.values[reference])
            cleared = edit_profile(
                "local",
                path,
                registry_auth=RegistryAuthInput("none"),
                secret_store=store,
            ).profile("local")

        self.assertEqual({"type": "none"}, cleared["registry"]["auth"])
        self.assertIn(reference, store.deleted)

    def test_registry_mtls_edit_retains_omitted_private_key_and_password(self) -> None:
        certificate, private_key = _client_identity("registry-key-password")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "local",
                path,
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "mtls",
                    client_certificate=certificate,
                    private_key=private_key,
                    private_key_password="registry-key-password",
                ),
                secret_store=store,
            ).profile("local")
            original_auth = added["registry"]["auth"]

            edited = edit_profile(
                "local",
                path,
                registry_auth=RegistryAuthInput(
                    "mtls",
                    client_certificate=certificate,
                ),
                secret_store=store,
            ).profile("local")

        self.assertEqual(original_auth, edited["registry"]["auth"])
        self.assertEqual(certificate, edited["registry"]["tls"]["clientCertificate"])
        self.assertEqual(private_key, store.values[original_auth["privateKeyRef"]])

    def test_edit_stages_kafka_and_registry_credentials_in_one_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "local",
                path,
                transport="tls",
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="registry-user", password="old-registry-password"
                ),
                secret_store=store,
            ).profile("local")
            old_registry_reference = added["registry"]["auth"]["passwordRef"]

            edited = edit_profile(
                "local",
                path,
                auth=KafkaAuthInput("plain", username="kafka-user", password="kafka-password"),
                registry_auth=RegistryAuthInput(
                    "basic", username="registry-user", password="new-registry-password"
                ),
                secret_store=store,
            ).profile("local")

        kafka_reference = edited["kafka"]["auth"]["passwordRef"]
        registry_reference = edited["registry"]["auth"]["passwordRef"]
        self.assertEqual("kafka-password", store.values[kafka_reference])
        self.assertEqual("new-registry-password", store.values[registry_reference])
        self.assertNotEqual(old_registry_reference, registry_reference)
        self.assertIn(old_registry_reference, store.deleted)

    def test_remove_registry_retires_its_credentials_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "local",
                path,
                transport="tls",
                auth=KafkaAuthInput("plain", username="kafka-user", password="kafka-password"),
                registry_url="https://registry.example.com",
                registry_auth=RegistryAuthInput(
                    "basic", username="registry-user", password="registry-password"
                ),
                secret_store=store,
            ).profile("local")
            kafka_reference = added["kafka"]["auth"]["passwordRef"]
            registry_reference = added["registry"]["auth"]["passwordRef"]

            edited = edit_profile("local", path, remove_registry=True, secret_store=store).profile(
                "local"
            )

        self.assertNotIn("registry", edited)
        self.assertEqual("kafka-password", store.values[kafka_reference])
        self.assertNotIn(registry_reference, store.values)
        self.assertIn(registry_reference, store.deleted)

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
                    "auth": {"type": "none"},
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

    def test_add_and_edit_persist_validated_tls_transport(self) -> None:
        ca_certificates = synthetic_pki().ca
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"

            added = add_profile(
                "production",
                path,
                bootstrap_servers=("broker.example.com:9093",),
                transport="tls",
                ca_certificates=ca_certificates,
            )
            default_profile = edit_profile("production", path, default_trust=True)
            plaintext = edit_profile("production", path, transport="plaintext")

        kafka = added.profile("production")["kafka"]
        self.assertEqual("tls", kafka["transport"])
        self.assertEqual(ca_certificates, kafka["tls"]["caCertificates"])
        self.assertEqual("tls", default_profile.profile("production")["kafka"]["transport"])
        self.assertNotIn("tls", default_profile.profile("production")["kafka"])
        self.assertEqual("plaintext", plaintext.profile("production")["kafka"]["transport"])
        self.assertNotIn("tls", plaintext.profile("production")["kafka"])

    def test_adds_rotates_and_removes_password_authentication_recoverably(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "production",
                path,
                transport="tls",
                auth=KafkaAuthInput(
                    "scram-sha-512",
                    username="application",
                    password="synthetic-password-one",
                ),
                secret_store=store,
            )
            first_auth = added.profile("production")["kafka"]["auth"]
            first_reference = first_auth["passwordRef"]

            edited = edit_profile(
                "production",
                path,
                auth=KafkaAuthInput("plain", password="synthetic-password-two"),
                expected_revision=added.revision("production"),
                secret_store=store,
            )
            second_auth = edited.profile("production")["kafka"]["auth"]
            second_reference = second_auth["passwordRef"]

            self.assertEqual("scram-sha-512", first_auth["type"])
            self.assertEqual("plain", second_auth["type"])
            self.assertEqual("application", second_auth["username"])
            self.assertNotEqual(first_reference, second_reference)
            self.assertNotIn(first_reference, store.values)
            self.assertEqual("synthetic-password-two", store.values[second_reference])
            self.assertNotIn(b"synthetic-password-two", path.read_bytes())

            remove_profile("production", path, secret_store=store)

            self.assertNotIn(second_reference, store.values)

    def test_adds_and_edits_kafka_oauth_with_independent_identity_and_trust(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            added = add_profile(
                "production",
                path,
                transport="tls",
                auth=KafkaAuthInput(
                    "oauth",
                    oauth_token_url="https://idp.example.com/oauth/token",
                    oauth_client_id="kafka-client",
                    oauth_scopes=("openid", "kafka"),
                    oauth_client_secret="first-client-secret",
                    oauth_ca_certificates=synthetic_pki().ca,
                ),
                secret_store=store,
            ).profile("production")
            reference = added["kafka"]["auth"]["clientSecretRef"]

            edited = edit_profile(
                "production",
                path,
                auth=KafkaAuthInput(
                    "oauth",
                    oauth_scopes=("kafka",),
                    oauth_default_trust=True,
                ),
                secret_store=store,
            ).profile("production")

        self.assertEqual("first-client-secret", store.values[reference])
        self.assertEqual(reference, edited["kafka"]["auth"]["clientSecretRef"])
        self.assertEqual(["kafka"], edited["kafka"]["auth"]["scopes"])
        self.assertNotIn("caCertificates", edited["kafka"]["auth"])

    def test_password_authentication_requires_tls_and_expected_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            with self.assertRaisesRegex(ProfileStoreError, "requires --transport tls"):
                add_profile(
                    "invalid",
                    path,
                    auth=KafkaAuthInput(
                        "plain",
                        username="application",
                        password="synthetic-password",
                    ),
                    secret_store=store,
                )
            self.assertFalse(path.exists())

            profiles = add_profile(
                "production",
                path,
                transport="tls",
                auth=KafkaAuthInput(
                    "plain",
                    username="application",
                    password="synthetic-password",
                ),
                secret_store=store,
            )
            with self.assertRaisesRegex(ProfileStoreError, "changed while credentials"):
                edit_profile(
                    "production",
                    path,
                    auth=KafkaAuthInput("plain", password="replacement"),
                    expected_revision=profiles.revision("production") + 1,
                    secret_store=store,
                )

    def test_adds_and_removes_encrypted_mtls_identity(self) -> None:
        certificate, private_key = _client_identity("synthetic-key-password")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            store = _RecordingSecretStore()
            profiles = add_profile(
                "production",
                path,
                transport="tls",
                auth=KafkaAuthInput(
                    "mtls",
                    client_certificate=certificate,
                    private_key=private_key,
                    private_key_password="synthetic-key-password",
                ),
                secret_store=store,
            )

            auth = profiles.profile("production")["kafka"]["auth"]
            self.assertEqual("mtls", auth["type"])
            self.assertEqual(certificate, auth["clientCertificate"])
            self.assertEqual(private_key, store.values[auth["privateKeyRef"]])
            self.assertEqual(
                "synthetic-key-password",
                store.values[auth["privateKeyPasswordRef"]],
            )
            self.assertNotIn(private_key.encode("utf-8"), path.read_bytes())

            remove_profile("production", path, secret_store=store)

            self.assertEqual({}, store.values)

    def test_custom_ca_requires_tls_and_valid_pem(self) -> None:
        ca_certificates = synthetic_pki().ca
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            for arguments, message in (
                ({"ca_certificates": ca_certificates}, "requires --transport tls"),
                (
                    {"transport": "tls", "ca_certificates": "not a certificate"},
                    "only PEM certificates",
                ),
            ):
                with (
                    self.subTest(arguments=arguments),
                    self.assertRaisesRegex(ProfileStoreError, message),
                ):
                    add_profile("invalid", path, **arguments)
            self.assertFalse(path.exists())

            add_profile("plaintext", path)
            with self.assertRaisesRegex(ProfileStoreError, "requires Kafka TLS transport"):
                edit_profile("plaintext", path, default_trust=True)
            with self.assertRaisesRegex(ProfileStoreError, "cannot be combined"):
                edit_profile(
                    "plaintext",
                    path,
                    transport="tls",
                    ca_certificates=ca_certificates,
                    default_trust=True,
                )

    def test_tampered_stored_ca_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            profile = add_profile("production", path, transport="tls").profile("production")
            profile["kafka"]["tls"] = {"caCertificates": "not a certificate"}
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE profiles SET document = ? WHERE name = 'production'",
                    (json.dumps(profile, sort_keys=True, separators=(",", ":")),),
                )
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "does not match schema"):
                load_profiles(path)

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
                "auth": {"type": "none"},
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
                (
                    {"registry_url": "http://user:secret@registry.example.com"},
                    "must not contain credentials",
                ),
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
        self.values: dict[str, str] = {}

    def get(self, reference: str) -> str:
        try:
            return self.values[reference]
        except KeyError as error:
            raise SecretStoreError("synthetic missing value") from error

    def set(self, reference: str, value: str) -> None:
        self.values[reference] = value

    def delete(self, reference: str) -> None:
        if self.fail:
            raise SecretStoreError("synthetic failure")
        self.deleted.append(reference)
        self.values.pop(reference, None)


class _OneShotBlockingGetStore:
    def __init__(
        self,
        delegate: _RecordingSecretStore,
        entered: Event,
        release: Event,
    ) -> None:
        self.delegate = delegate
        self.entered = entered
        self.release = release
        self.blocked = False

    def get(self, reference: str) -> str:
        value = self.delegate.get(reference)
        if not self.blocked:
            self.blocked = True
            self.entered.set()
            if not self.release.wait(timeout=5):
                raise SecretStoreError("synthetic snapshot barrier timed out")
        return value

    def set(self, reference: str, value: str) -> None:
        self.delegate.set(reference, value)

    def delete(self, reference: str) -> None:
        self.delegate.delete(reference)


class _OneShotBlockingSetStore:
    def __init__(
        self,
        delegate: _RecordingSecretStore,
        entered: Event,
        release: Event,
    ) -> None:
        self.delegate = delegate
        self.entered = entered
        self.release = release
        self.blocked = False

    def get(self, reference: str) -> str:
        return self.delegate.get(reference)

    def set(self, reference: str, value: str) -> None:
        self.delegate.set(reference, value)
        if not self.blocked:
            self.blocked = True
            self.entered.set()
            if not self.release.wait(timeout=5):
                raise SecretStoreError("synthetic staging barrier timed out")

    def delete(self, reference: str) -> None:
        self.delegate.delete(reference)


class _CommitFaultConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        durable: bool,
        state: dict[str, bool],
    ) -> None:
        self._connection = connection
        self._durable = durable
        self._state = state
        self._armed = False

    def execute(self, statement: str, parameters: object = ()) -> sqlite3.Cursor:
        normalized = " ".join(statement.upper().split())
        if normalized.startswith(
            ("INSERT INTO PROFILES", "UPDATE PROFILES", "DELETE FROM PROFILES")
        ):
            self._armed = True
        if normalized == "COMMIT" and self._armed:
            self._state["faulted"] = True
            if self._durable:
                self._connection.execute(statement)
            raise sqlite3.OperationalError("synthetic lost commit acknowledgement")
        return self._connection.execute(statement, parameters)

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)


def _commit_fault_factory(durable: bool | None):
    original = profiles_module._connect
    state = {"faulted": False}

    def connect(path: Path, *, writable: bool) -> sqlite3.Connection:
        if not writable and durable is None and state["faulted"]:
            raise sqlite3.OperationalError("synthetic inspection failure")
        connection = original(path, writable=writable)
        if writable:
            return _CommitFaultConnection(
                connection,
                durable=durable is not False,
                state=state,
            )
        return connection

    return connect


def _client_identity(password: str) -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic-client")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    certificate_pem = certificate.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(password.encode("utf-8")),
    ).decode("utf-8")
    return certificate_pem, key_pem


if __name__ == "__main__":
    unittest.main()
