import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from kantrip.doctor import run_doctor
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.profile_storage import DATABASE_BACKUP_PREFIX, load_profiles
from kantrip.profiles import add_profile
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime
from kantrip.secret_store import (
    SecretNotFoundError,
    SecretStoreError,
    SecretStoreInfo,
    secret_reference,
)
from kantrip.secret_value import Secret

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"


class TestDoctor(unittest.TestCase):
    def setUp(self) -> None:
        store = unittest.mock.Mock(
            info=SecretStoreInfo(
                "keyring.backends.SecretService.Keyring",
                "Secret Service",
            )
        )
        patcher = patch("kantrip.doctor.load_secret_store", return_value=store)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_reports_valid_profile_database_and_installed_tools(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        messages = [check.message for check in report.checks]
        self.assertTrue(report.healthy)
        self.assertIn(
            "Profile database is healthy (1 profile, schema version 1)",
            messages,
        )
        self.assertTrue(any(message.startswith("kcat: ") for message in messages))
        self.assertTrue(any("Apache Kafka CLI: all 7" in message for message in messages))
        self.assertTrue(any("Schema Registry console: all 6" in message for message in messages))
        self.assertTrue(any("Registry profiles: 1 profile" in message for message in messages))
        self.assertTrue(
            any(
                "Profile 'local' compatible installed clients: kcat, Kaskade, "
                "Apache/Confluent Java CLI" in message
                for message in messages
            )
        )
        verbose_messages = [
            check.message
            for _, checks in report.sections(verbose=True)
            for check in checks
            if check.verbose_only
        ]
        self.assertTrue(any(message.startswith("Kafka topics: ") for message in verbose_messages))
        self.assertTrue(
            any(
                message.startswith("kafka-protobuf-console-consumer: ")
                for message in verbose_messages
            )
        )

    def test_invalid_database_is_unhealthy_without_contacting_kafka(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            database_path.write_bytes(b"not a sqlite database")
            database_path.chmod(0o600)

            with patch("kantrip.doctor.shutil.which", return_value=None):
                report = run_doctor(
                    {
                        "KANTRIP_DATABASE": str(database_path),
                        "XDG_RUNTIME_DIR": directory,
                        "PATH": "",
                        "SHELL": "/bin/zsh",
                    }
                )

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                "profile database could not be read safely" in check.message
                for check in report.checks
            )
        )

    def test_historyless_database_is_rejected_without_modification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            with closing(sqlite3.connect(database_path)) as connection:
                connection.execute("DROP TABLE schema_migrations")
                connection.commit()
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)
            with closing(sqlite3.connect(database_path)) as connection:
                history_exists = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'schema_migrations'"
                ).fetchone()

        self.assertFalse(report.healthy)
        self.assertIsNone(history_exists)
        self.assertTrue(
            any("profile database schema is invalid" in check.message for check in report.checks)
        )

    def test_exposed_migration_backup_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            backup = Path(
                f"{database_path}{DATABASE_BACKUP_PREFIX}2026-09-14T01-02-03.000004Z-synthetic"
            )
            backup.write_bytes(b"synthetic backup")
            backup.chmod(0o644)

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(
                    {
                        "KANTRIP_DATABASE": str(database_path),
                        "XDG_RUNTIME_DIR": directory,
                        "PATH": "/tools",
                        "SHELL": "/tools/zsh",
                    }
                )

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                "Migration backup permissions are broader" in check.message
                for check in report.checks
            )
        )

    def test_https_registry_profile_is_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            profile = load_profiles(database_path).profile("local")
            profile["registry"]["schema.registry.url"] = "https://localhost:8081"
            with closing(sqlite3.connect(database_path)) as connection:
                connection.execute(
                    "UPDATE profiles SET document = ? WHERE name = 'local'",
                    (json.dumps(profile, sort_keys=True, separators=(",", ":")),),
                )
                connection.commit()
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertTrue(report.healthy)

    def test_names_a_missing_schema_registry_console_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            def installed_without_protobuf_consumer(
                name: str, path: str | None = None
            ) -> str | None:
                if name == "kafka-protobuf-console-consumer":
                    return None
                return _installed_tool(name, path)

            with patch(
                "kantrip.doctor.shutil.which",
                side_effect=installed_without_protobuf_consumer,
            ):
                report = run_doctor(environment)

        self.assertTrue(
            any(
                "missing: kafka-protobuf-console-consumer" in check.message
                for check in report.checks
            )
        )

    def test_active_session_allows_a_benign_path_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            session_directory = root / "session"
            shim_directory = session_directory / "bin"
            shim_directory.mkdir(parents=True)
            virtual_environment = root / "venv" / "bin"
            virtual_environment.mkdir(parents=True)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "KANTRIP_PROFILE": "local",
                "KANTRIP_SESSION_ID": "synthetic-session",
                "KANTRIP_SESSION_DIR": str(session_directory),
                "PATH": f"{virtual_environment}:{shim_directory}:/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertTrue(report.healthy)
        self.assertTrue(
            any(
                check.message == "No supported commands shadow session adapters on PATH"
                for check in report.checks
            )
        )

    def test_active_session_rejects_an_adapter_shadow_earlier_on_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            session_directory = root / "session"
            shim_directory = session_directory / "bin"
            shim_directory.mkdir(parents=True)
            shadow_directory = root / "shadow"
            shadow_directory.mkdir()
            shadow = shadow_directory / "kcat"
            shadow.write_text("#!/bin/sh\n", encoding="utf-8")
            shadow.chmod(0o700)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "KANTRIP_PROFILE": "local",
                "KANTRIP_SESSION_ID": "synthetic-session",
                "KANTRIP_SESSION_DIR": str(session_directory),
                "PATH": f"{shadow_directory}:{shim_directory}:/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                check.message == "Session adapters are shadowed earlier on PATH: kcat"
                for check in report.checks
            )
        )

    def test_runtime_diagnostics_are_read_only_and_hide_paths_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            with patch("kantrip.runtime.time.time", return_value=1000):
                runtime = create_session_runtime(PROFILE_ID, 1, environment)
            runtime_path = runtime.path
            runtime._closed = True
            os.close(runtime._lock_descriptor)
            os.close(runtime._session_descriptor)
            os.close(runtime._root_descriptor)

            with (
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
                patch("kantrip.runtime.time.time", return_value=1000 + SESSION_STALE_SECONDS),
            ):
                report = run_doctor(environment)
            artifact_preserved = runtime_path.exists()

        visible = [check.message for _, checks in report.sections() for check in checks]
        verbose = [check.message for _, checks in report.sections(verbose=True) for check in checks]
        self.assertTrue(artifact_preserved)
        self.assertIn(
            "Sessions on this machine: 0 active, 0 recent inactive, 1 stale; "
            "run 'kantrip doctor --repair' to remove stale sessions",
            visible,
        )
        self.assertFalse(any(str(runtime_path.parent) in message for message in visible))
        self.assertTrue(any(str(runtime_path.parent) in message for message in verbose))

    def test_invalid_runtime_entry_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            runtime_root = root / "kantrip" / "sessions"
            runtime_root.mkdir(mode=0o700, parents=True)
            runtime_root.parent.chmod(0o700)
            (runtime_root / "unexpected").mkdir()
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertIn(
            f"Invalid session runtime entry: {runtime_root / 'unexpected'}; "
            "inspect it and remove it manually",
            [check.message for check in report.checks],
        )

    def test_pending_credential_cleanup_is_reported_without_modification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            profile = load_profiles(database_path).profile("local")
            reference = secret_reference(profile["id"], "registry/token")
            with closing(sqlite3.connect(database_path)) as connection:
                queue_secret_cleanup(connection, reference)
                connection.commit()
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)
            with closing(sqlite3.connect(database_path)) as connection:
                pending = connection.execute(
                    "SELECT secret_reference FROM credential_reconciliation"
                ).fetchall()

        self.assertTrue(report.healthy)
        self.assertEqual([(reference,)], pending)
        self.assertTrue(
            any(
                check.status == "warning"
                and "Credential reconciliation: 1 pending entry" in check.message
                for check in report.checks
            )
        )

    def test_unapproved_credential_backend_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _create_profile_database(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with (
                patch(
                    "kantrip.doctor.load_secret_store",
                    side_effect=SecretStoreError("unsafe backend"),
                ),
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
            ):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                "Credential store backend is unavailable or unsafe" in check.message
                for check in report.checks
            )
        )


def _installed_tool(name: str, path: str | None = None) -> str | None:
    del path
    installed = {
        "kantrip",
        "zsh",
        "kcat",
        "kaskade",
        "kafka-topics",
        "kafka-console-consumer",
        "kafka-console-producer",
        "kafka-consumer-groups",
        "kafka-configs",
        "kafka-acls",
        "kafka-broker-api-versions",
        "kafka-avro-console-consumer",
        "kafka-avro-console-producer",
        "kafka-json-schema-console-consumer",
        "kafka-json-schema-console-producer",
        "kafka-protobuf-console-consumer",
        "kafka-protobuf-console-producer",
    }
    return f"/tools/{name}" if name in installed else None


class TestDoctorSecretsAndSessions(unittest.TestCase):
    """Every referenced secret is checked; sessions of every profile are listed (#52)."""

    def test_reports_every_referenced_secret_by_field_name(self) -> None:
        store = _MemoryStore()
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            add_profile(
                "secure",
                database_path,
                transport="tls",
                auth=KafkaAuthInput(
                    "oauth",
                    oauth_token_url="https://idp.invalid/token",
                    oauth_client_id="kafka-client",
                    oauth_client_secret=Secret("synthetic-client-secret"),
                ),
                registry_url="https://registry.invalid",
                registry_auth=RegistryAuthInput("token", token=Secret("synthetic-token")),
                secret_store=store,
            )
            registry_token = next(ref for ref in store.values if ref.endswith("/registry/token"))
            del store.values[registry_token]
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            with (
                patch("kantrip.doctor.load_secret_store", return_value=store),
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
            ):
                report = run_doctor(environment)

        messages = [check.message for _, checks in report.sections() for check in checks]
        self.assertIn("Profile 'secure' kafka.auth.oauth.client-secret is stored", messages)
        self.assertIn("Profile 'secure' registry.auth.token is missing", messages)
        self.assertFalse(report.healthy)
        self.assertFalse(any("synthetic" in message for message in messages))

    def test_sessions_without_a_profile_name_each_session_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            profile_id = str(load_profiles(database_path).profile("local")["id"])
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            runtimes = [
                create_session_runtime(profile_id, 1, environment),
                create_session_runtime(PROFILE_ID, 1, environment),
            ]
            try:
                with (
                    patch("kantrip.doctor.load_secret_store", return_value=_MemoryStore()),
                    patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
                ):
                    report = run_doctor(environment)
            finally:
                for runtime in runtimes:
                    runtime.close()

        messages = [check.message for _, checks in report.sections() for check in checks]
        self.assertTrue(
            any(m.startswith("Session active: profile 'local', revision 1") for m in messages),
            messages,
        )
        self.assertTrue(
            any(
                m.startswith("Session active: profile not in this database, revision 1")
                for m in messages
            ),
            messages,
        )

    def test_profile_scope_is_visible_and_keeps_machine_wide_totals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            database_path = root / "profiles.db"
            _create_profile_database(database_path)
            add_profile("other", database_path)
            profiles = load_profiles(database_path)
            environment = {
                "KANTRIP_DATABASE": str(database_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            runtimes = [
                create_session_runtime(str(profiles.profile("other")["id"]), 1, environment),
                create_session_runtime(str(profiles.profile("local")["id"]), 1, environment),
            ]
            invalid = runtimes[0].path.parent / "unexpected"
            invalid.mkdir()
            try:
                with (
                    patch("kantrip.doctor.load_secret_store", return_value=_MemoryStore()),
                    patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
                ):
                    report = run_doctor(environment, profile_name="other")
            finally:
                for runtime in runtimes:
                    runtime.close()

        messages = [check.message for _, checks in report.sections() for check in checks]
        self.assertIn("Profile database is healthy (2 profiles, schema version 1)", messages)
        self.assertIn("Profile 'other' has no Registry", messages)
        self.assertIn("This shell is not inside a Kantrip session", messages)
        self.assertIn(
            "Sessions for profile 'other': 1 active, 0 recent inactive, 0 stale", messages
        )
        self.assertEqual(1, sum(m.startswith("Session active: revision 1") for m in messages))
        self.assertIn(
            f"Invalid session runtime entry: {invalid}; inspect it and remove it manually",
            messages,
        )


class _MemoryStore:
    info = SecretStoreInfo("keyring.backends.SecretService.Keyring", "Secret Service")

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, reference: str) -> str:
        try:
            return self.values[reference]
        except KeyError as error:
            raise SecretNotFoundError("synthetic missing secret") from error

    def set(self, reference: str, value: str) -> None:
        self.values[reference] = value

    def delete(self, reference: str) -> None:
        self.values.pop(reference, None)


def _create_profile_database(path: Path) -> None:
    add_profile("local", path, registry_url="http://localhost:8081")


if __name__ == "__main__":
    unittest.main()
