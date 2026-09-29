import json
import os
import re
import sqlite3
import tempfile
import unittest
from collections import namedtuple
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from kantrip.adapters import rendered_release
from kantrip.doctor import (
    PYTHON_EXCLUSIVE_MAXIMUM,
    PYTHON_MINIMUM,
    DoctorCheck,
    DoctorReport,
    _check_python,
    run_doctor,
)
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.profile_storage import DATABASE_BACKUP_PREFIX, load_profiles
from kantrip.profiles import add_profile
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime
from kantrip.secret_store import (
    LockPolicy,
    SecretNotFoundError,
    SecretStoreError,
    SecretStoreInfo,
    VaultError,
    VaultState,
    VaultStatus,
    secret_reference,
)
from kantrip.secret_value import Secret
from tests.unit.client_versions import SUPPORTED_VERSION_OUTPUT, use_supported_client_versions

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestDoctor(unittest.TestCase):
    def setUp(self) -> None:
        store = unittest.mock.Mock(
            info=SecretStoreInfo(
                "keyring.backends.SecretService.Keyring",
                "Secret Service",
            ),
            **{"vault_status.return_value": None},
        )
        patcher = patch("kantrip.doctor.load_secret_store", return_value=store)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.supported_versions = use_supported_client_versions(self)

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
        self.assertIn("kcat 1.7.0 is supported", verbose_messages)
        self.assertIn("kaskade 5.0.1 is supported", verbose_messages)

    def test_reports_a_client_below_its_floor_and_probes_each_client_once(self) -> None:
        self.supported_versions.stop()
        probes: list[str] = []

        def version_output(resolved: str, option: str, environment: object) -> str:
            del option, environment
            probes.append(resolved)
            if resolved.endswith("/kaskade"):
                return "kaskade, version 5.0.0\n"
            return SUPPORTED_VERSION_OUTPUT

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
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
                patch("kantrip.adapters._version_output", side_effect=version_output),
            ):
                report = run_doctor(environment)

        warnings = [check.message for check in report.checks if check.status == "warning"]
        floor = "kaskade 5.0.0 is not supported; install Kaskade 5.0.1 or newer"
        self.assertIn(floor, warnings)
        self.assertIn(f"Profile 'local' client rejected: Kaskade: {floor}", warnings)
        # The version report and the profile check share one run per client.
        self.assertEqual(sorted(set(probes)), sorted(probes))

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
    # Like shutil.which, an installed absolute path resolves to itself.
    name = name.removeprefix("/tools/")
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


class TestDoctorPythonRange(unittest.TestCase):
    def test_supported_range_matches_requires_python(self) -> None:
        pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        match = re.search(r'^requires-python = "(.+)"$', pyproject, re.MULTILINE)
        minimum = rendered_release(PYTHON_MINIMUM)
        maximum = rendered_release(PYTHON_EXCLUSIVE_MAXIMUM)

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(f">={minimum},<{maximum}", match.group(1))

    def test_supported_range_matches_python_classifiers(self) -> None:
        pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        classified = re.findall(r'"Programming Language :: Python :: (3\.\d+)"', pyproject)
        major = PYTHON_MINIMUM[0]
        supported = [
            rendered_release((major, minor))
            for minor in range(PYTHON_MINIMUM[1], PYTHON_EXCLUSIVE_MAXIMUM[1])
        ]

        self.assertEqual(supported, classified)

    def test_unsupported_python_names_supported_range(self) -> None:
        version_info = namedtuple("version_info", "major minor micro releaselevel serial")
        with patch("kantrip.doctor.sys.version_info", version_info(3, 9, 18, "final", 0)):
            check = _check_python()

        self.assertEqual(
            DoctorCheck(
                "error",
                "Python version is not supported (3.9.18); use Python 3.10 through 3.14",
            ),
            check,
        )


class TestDoctorVault(unittest.TestCase):
    """The vault is reported without unlocking it; one vault error is reported once (#41)."""

    def setUp(self) -> None:
        use_supported_client_versions(self)

    def test_locked_vault_is_reported_without_a_lock_policy(self) -> None:
        messages = self._messages(_vault("locked"))

        self.assertIn(
            "Credential vault: ~/Library/Keychains/kantrip.keychain-db (locked)", messages
        )
        self.assertFalse(any("locks after" in message for message in messages))

    def test_unlocked_vault_reports_its_lock_policy(self) -> None:
        cases = {
            LockPolicy(True, 900): (
                "success",
                "Credential vault locks after 15 minutes idle and on sleep",
            ),
            LockPolicy(False, 60): ("success", "Credential vault locks after 1 minute idle"),
            LockPolicy(True, None): ("success", "Credential vault locks on sleep"),
            LockPolicy(False, None): (
                "warning",
                "Credential vault never locks by itself; set a lock timeout in Keychain Access",
            ),
        }
        for policy, (status, message) in cases.items():
            with self.subTest(policy=policy):
                report = self._report(_vault("unlocked", policy))
                self.assertIn(DoctorCheck(status, message, "Credentials"), report.checks)
                self.assertIn(
                    "Credential vault: ~/Library/Keychains/kantrip.keychain-db (unlocked)",
                    self._text(report),
                )

    def test_missing_vault_and_its_warnings_are_warnings(self) -> None:
        report = self._report(_vault("missing", warnings=("synthetic vault warning",)))

        self.assertIn(
            DoctorCheck(
                "warning",
                "Credential vault: ~/Library/Keychains/kantrip.keychain-db (not found)",
                "Credentials",
            ),
            report.checks,
        )
        self.assertIn(
            DoctorCheck("warning", "synthetic vault warning", "Credentials"), report.checks
        )

    def test_vault_that_cannot_be_inspected_is_an_error(self) -> None:
        store = _MemoryStore()
        store.status = VaultError("synthetic vault problem")

        report = self._report(store)

        self.assertFalse(report.healthy)
        self.assertIn(
            "Credential vault could not be inspected: synthetic vault problem", self._text(report)
        )

    def test_vault_error_stops_profile_checks_with_one_message(self) -> None:
        store = _vault("locked")
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            for name in ("first", "second"):
                add_profile(
                    name,
                    database_path,
                    transport="tls",
                    auth=KafkaAuthInput(
                        "scram-sha-512", username="app", password=Secret("synthetic-password")
                    ),
                    secret_store=store,
                )
            store.get_error = VaultError("synthetic vault is locked")
            report = self._report(store, database_path, directory)

        messages = self._text(report)
        self.assertFalse(report.healthy)
        self.assertEqual(
            ["Profile credentials were not checked: synthetic vault is locked"],
            [message for message in messages if "synthetic vault" in message],
        )
        self.assertFalse(any("kafka.auth.password" in message for message in messages))

    def _messages(self, store: "_MemoryStore") -> list[str]:
        return self._text(self._report(store))

    def _report(
        self,
        store: "_MemoryStore",
        database_path: Path | None = None,
        directory: str | None = None,
    ) -> DoctorReport:
        with tempfile.TemporaryDirectory() as fallback:
            root = directory or fallback
            environment = {
                "KANTRIP_DATABASE": str(database_path or Path(root) / "profiles.db"),
                "XDG_RUNTIME_DIR": root,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            with (
                patch("kantrip.doctor.load_secret_store", return_value=store),
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
            ):
                return run_doctor(environment)

    @staticmethod
    def _text(report: DoctorReport) -> list[str]:
        return [check.message for check in report.checks]


def _vault(
    state: VaultState,
    policy: LockPolicy | None = None,
    *,
    warnings: tuple[str, ...] = (),
) -> "_MemoryStore":
    store = _MemoryStore()
    store.status = VaultStatus("~/Library/Keychains/kantrip.keychain-db", state, policy, warnings)
    return store


class _MemoryStore:
    info = SecretStoreInfo("keyring.backends.SecretService.Keyring", "Secret Service")

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.status: VaultStatus | VaultError | None = None
        self.get_error: VaultError | None = None

    def vault_status(self) -> VaultStatus | None:
        if isinstance(self.status, VaultError):
            raise self.status
        return self.status

    def get(self, reference: str) -> str:
        if self.get_error is not None:
            raise self.get_error
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
