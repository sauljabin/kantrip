import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from kantrip.doctor import run_doctor
from kantrip.profiles import DATABASE_BACKUP_PREFIX, add_profile, load_profiles
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime
from kantrip.secret_store import SecretStoreError, SecretStoreInfo, secret_reference


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
            "Profile database is healthy (1 profile, schema version 2)",
            messages,
        )
        self.assertTrue(any(message.startswith("kcat: ") for message in messages))
        self.assertTrue(any("Apache Kafka CLI: all 7" in message for message in messages))
        self.assertTrue(any("Schema Registry console: all 6" in message for message in messages))
        self.assertTrue(any("Registry profiles: 1 profile" in message for message in messages))
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

    def test_unsupported_registry_profile_is_unhealthy(self) -> None:
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

        self.assertFalse(report.healthy)
        self.assertTrue(any("does not match schema" in check.message for check in report.checks))

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
                runtime = create_session_runtime(environment)
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
        self.assertTrue(any("Runtime stale: 1 session" in message for message in visible))
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
        self.assertTrue(
            any("Runtime invalid: 1 session" in check.message for check in report.checks)
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


def _create_profile_database(path: Path) -> None:
    add_profile("local", path, registry_url="http://localhost:8081")


if __name__ == "__main__":
    unittest.main()
