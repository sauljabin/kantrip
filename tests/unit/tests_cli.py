import json
import os
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from email.message import Message
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import yaml
from click.testing import CliRunner

from kantrip import APP_VERSION
from kantrip.cli import cli
from kantrip.cli_inputs import SECRET_FIELDS
from kantrip.console import create_console
from kantrip.maintenance import RepairAction, RepairReport
from kantrip.ping import PingError, PingResult, RegistryPingResult, ping_profile
from kantrip.profile_output import _SECRET_FIELDS as DESCRIBED_SECRET_FIELDS
from kantrip.profile_storage import ProfileStoreError, load_profiles
from kantrip.profiles import add_profile
from kantrip.secret_store import SecretNotFoundError
from kantrip.secret_value import Secret
from tests.unit.pki import KEY_PASSWORD, synthetic_pki, temporary_pki_files


class TestCli(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_help_describes_current_cli(self) -> None:
        result = self.runner.invoke(cli, ["--help"])

        self.assertEqual(0, result.exit_code)
        self.assertIn("Kantrip securely manages local Kafka profiles", result.output)
        self.assertIn("--no-color", result.output)

    def test_version_uses_package_metadata(self) -> None:
        result = self.runner.invoke(cli, ["--version"])

        self.assertEqual(0, result.exit_code)
        self.assertIn(APP_VERSION, result.output)

    def test_no_color_configures_consoles_globally(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environment = {"KANTRIP_DATABASE": str(Path(directory) / "missing.db")}
            with patch("kantrip.cli.create_console", wraps=create_console) as create:
                result = self.runner.invoke(cli, ["--no-color", "list"], env=environment)

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual([True, True], [call.kwargs["no_color"] for call in create.call_args_list])

    def test_no_color_configures_consoles_after_the_subcommand(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environment = {"KANTRIP_DATABASE": str(Path(directory) / "missing.db")}
            with patch("kantrip.cli.create_console", wraps=create_console) as create:
                result = self.runner.invoke(cli, ["list", "--no-color"], env=environment)

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            [False, False, True, True],
            [call.kwargs["no_color"] for call in create.call_args_list],
        )

    def test_command_help_documents_local_no_color(self) -> None:
        for command in (
            "add",
            "edit",
            "remove",
            "list",
            "describe",
            "current",
            "doctor",
            "ping",
            "exec",
        ):
            with self.subTest(command=command):
                result = self.runner.invoke(cli, [command, "--help"])

                self.assertEqual(0, result.exit_code, result.output)
                self.assertIn("--no-color", result.output)

    def test_cleanup_command_is_not_exposed(self) -> None:
        result = self.runner.invoke(cli, ["cleanup"])

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("No such command 'cleanup'", result.output)

    def test_local_no_color_preserves_parser_errors(self) -> None:
        result = self.runner.invoke(cli, ["describe", "--no-color"])

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("Missing argument 'PROFILE'", result.output)
        self.assertNotIn("No such option", result.output)

    def test_profile_commands_use_resolved_database(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}

            listed = self.runner.invoke(cli, ["list"], env=environment)
            described = self.runner.invoke(cli, ["describe", "local"], env=environment)

        self.assertIn("Profile", listed.output)
        self.assertIn("Description", listed.output)
        self.assertIn("Kafka", listed.output)
        self.assertIn("Registry", listed.output)
        self.assertIn("local", listed.output)
        self.assertIn("Local development", listed.output)
        self.assertIn("localhost:8081", listed.output)
        self.assertNotIn("\x1b[", listed.output)
        self.assertEqual(0, described.exit_code, described.output)
        self.assertIn("Profile", described.output)
        self.assertIn("Revision", described.output)
        self.assertIn("localhost:9092", described.output)

    def test_add_and_remove_manage_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "kantrip" / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            added = self.runner.invoke(
                cli,
                [
                    "add",
                    "development",
                    "-b",
                    "broker-1.example.com:9092,broker-2.example.com:9092",
                    "-d",
                    "Development cluster",
                    "-l",
                    "environment=development",
                    "--registry-url",
                    "http://registry.example.com:8081",
                ],
                env=environment,
            )
            profile = load_profiles(database_path).profile("development")
            listed = self.runner.invoke(cli, ["list"], env=environment)
            removed = self.runner.invoke(cli, ["remove", "development", "--yes"], env=environment)
            empty = self.runner.invoke(cli, ["list"], env=environment)

        self.assertEqual(0, added.exit_code, added.output)
        self.assertIn("Profile", listed.output)
        self.assertIn("development", listed.output)
        self.assertIn("Development", listed.output)
        self.assertIn("cluster", listed.output)
        self.assertEqual(
            ["broker-1.example.com:9092", "broker-2.example.com:9092"],
            profile["kafka"]["bootstrapServers"],
        )
        self.assertEqual(
            {
                "auth": {"type": "none"},
                "provider": "confluent",
                "schema.registry.url": "http://registry.example.com:8081",
            },
            profile["registry"],
        )
        self.assertEqual({"environment": "development"}, profile["labels"])
        self.assertEqual(0, removed.exit_code, removed.output)
        self.assertEqual("", empty.output)

    def test_add_and_edit_configure_kafka_tls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}

            with temporary_pki_files(ca=synthetic_pki().ca) as paths:
                added = self.runner.invoke(
                    cli,
                    [
                        "add",
                        "production",
                        "--bootstrap-server",
                        "broker.example.com:9093",
                        "--transport",
                        "tls",
                        "--ca-file",
                        str(paths["ca"]),
                    ],
                    env=environment,
                )
            secure = load_profiles(database_path).profile("production")
            default_trust = self.runner.invoke(
                cli,
                ["edit", "production", "--unset", "kafka.tls.ca"],
                env=environment,
            )
            default_profile = load_profiles(database_path).profile("production")
            edited = self.runner.invoke(
                cli,
                ["edit", "production", "--transport", "plaintext"],
                env=environment,
            )
            plaintext = load_profiles(database_path).profile("production")

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual("tls", secure["kafka"]["transport"])
        self.assertIn("BEGIN CERTIFICATE", secure["kafka"]["tls"]["caCertificates"])
        self.assertEqual(0, default_trust.exit_code, default_trust.output)
        self.assertEqual("tls", default_profile["kafka"]["transport"])
        self.assertNotIn("tls", default_profile["kafka"])
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertEqual("plaintext", plaintext["kafka"]["transport"])
        self.assertNotIn("tls", plaintext["kafka"])

    def test_edit_updates_and_removes_explicit_profile_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            _add_test_profile(database_path)

            updated = self.runner.invoke(
                cli,
                [
                    "edit",
                    "local",
                    "--bootstrap-server",
                    "broker-1.example.com:9092,broker-2.example.com:9092",
                    "--description",
                    "Shared development",
                    "--label",
                    "environment=development",
                    "--registry-url",
                    "http://localhost:8081",
                    "--no-color",
                ],
                env=environment,
            )
            profile = load_profiles(database_path).profile("local")
            removed = self.runner.invoke(
                cli,
                [
                    "edit",
                    "local",
                    "--unset",
                    "description",
                    "--unset",
                    "labels.environment",
                    "--unset",
                    "registry",
                ],
                env=environment,
            )
            final_profile = load_profiles(database_path).profile("local")

        self.assertEqual(0, updated.exit_code, updated.output)
        self.assertIn("Updated profile 'local'", updated.output)
        self.assertEqual(
            ["broker-1.example.com:9092", "broker-2.example.com:9092"],
            profile["kafka"]["bootstrapServers"],
        )
        self.assertEqual("Shared development", profile["description"])
        self.assertEqual({"environment": "development"}, profile["labels"])
        self.assertEqual("confluent", profile["registry"]["provider"])
        self.assertEqual(0, removed.exit_code, removed.output)
        self.assertNotIn("description", final_profile)
        self.assertNotIn("labels", final_profile)
        self.assertNotIn("registry", final_profile)

    def test_list_filters_labels_with_and_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            add_profile(
                "production",
                database_path,
                labels={"environment": "production", "owner": "platform"},
            )
            add_profile(
                "analytics",
                database_path,
                labels={"environment": "production", "owner": "data"},
            )

            result = self.runner.invoke(
                cli,
                ["list", "-l", "environment=production", "-l", "owner=platform"],
                env=environment,
            )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("production", result.output)
        self.assertIn("environment=production", result.output)
        self.assertIn("owner=platform", result.output)
        self.assertNotIn("analytics", result.output)

    def test_list_supports_json_yaml_and_empty_structured_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            add_profile("local", database_path, labels={"environment": "development"})

            as_json = self.runner.invoke(cli, ["list", "-o", "json"], env=environment)
            as_yaml = self.runner.invoke(cli, ["list", "--output", "yaml"], env=environment)
            empty = self.runner.invoke(
                cli,
                ["list", "--label", "environment=production", "--output", "json"],
                env=environment,
            )

        self.assertEqual(0, as_json.exit_code, as_json.output)
        self.assertEqual("local", json.loads(as_json.output)[0]["name"])
        self.assertIn("name: local", as_yaml.output)
        self.assertEqual([], json.loads(empty.output))

    def test_describe_supports_safe_json_and_yaml_observations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            profile = add_profile(
                "local",
                database_path,
                labels={"environment": "development"},
            ).profile("local")

            as_json = self.runner.invoke(
                cli, ["describe", "local", "--output", "json"], env=environment
            )
            as_yaml = self.runner.invoke(cli, ["describe", "local", "-o", "yaml"], env=environment)

        self.assertEqual(0, as_json.exit_code, as_json.output)
        observation = json.loads(as_json.output)
        self.assertEqual("local", observation["name"])
        self.assertEqual(profile["id"], observation["id"])
        self.assertEqual(1, observation["revision"])
        self.assertEqual({"environment": "development"}, observation["labels"])
        self.assertIn("revision: 1", as_yaml.output)
        self.assertNotIn("\x1b[", as_json.output)
        self.assertNotIn("\x1b[", as_yaml.output)

    def test_describe_structured_output_accepts_local_no_color(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            add_profile("local", database_path)

            result = self.runner.invoke(
                cli,
                ["describe", "local", "--output", "json", "--no-color"],
                env=environment,
            )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("local", json.loads(result.output)["name"])
        self.assertNotIn("\x1b[", result.output)

    def test_show_is_no_longer_exposed(self) -> None:
        result = self.runner.invoke(cli, ["show", "local"])

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("No such command 'show'", result.output)

    def test_edit_requires_an_explicit_option(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _add_test_profile(database_path)
            result = self.runner.invoke(
                cli,
                ["edit", "local"],
                env={"KANTRIP_DATABASE": str(database_path)},
            )
            profiles = load_profiles(database_path)

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("Usage:", result.output)
        self.assertIn("edit requires at least one option", result.output)
        self.assertEqual(1, profiles.revision("local"))

    def test_add_and_rotate_password_auth_without_echoing_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            store = _MemorySecretStore()
            with (
                patch("kantrip.profiles.load_secret_store", return_value=store),
                patch(
                    "kantrip.cli_inputs.secret_prompt",
                    side_effect=(
                        Secret("synthetic-password-one"),
                        Secret("synthetic-password-two"),
                    ),
                ),
            ):
                added = self.runner.invoke(
                    cli,
                    [
                        "add",
                        "secure",
                        "--transport",
                        "tls",
                        "--auth",
                        "plain",
                        "--username",
                        "application",
                    ],
                    env=environment,
                )
                first = load_profiles(database_path).profile("secure")["kafka"]["auth"]
                edited = self.runner.invoke(
                    cli,
                    ["edit", "secure", "--replace-secret", "kafka.auth.password"],
                    env=environment,
                )
                second = load_profiles(database_path).profile("secure")["kafka"]["auth"]

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertEqual("plain", second["type"])
        self.assertNotEqual(first["passwordRef"], second["passwordRef"])
        self.assertEqual("synthetic-password-two", store.values[second["passwordRef"]])
        self.assertNotIn(first["passwordRef"], store.values)
        self.assertNotIn("synthetic-password-one", added.output + edited.output)
        self.assertNotIn("synthetic-password-two", added.output + edited.output)

    def test_registry_oauth_edit_retains_secret_and_clears_public_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            store = _MemorySecretStore()
            with (
                temporary_pki_files(ca=synthetic_pki().ca) as paths,
                patch("kantrip.profiles.load_secret_store", return_value=store),
                patch(
                    "kantrip.cli_inputs.secret_prompt",
                    return_value=Secret("synthetic-registry-oauth-secret"),
                ),
            ):
                added = self.runner.invoke(
                    cli,
                    [
                        "add",
                        "secure-registry",
                        "--registry-url",
                        "https://registry.invalid",
                        "--registry-auth",
                        "oauth",
                        "--registry-ca-file",
                        str(paths["ca"]),
                        "--registry-oauth-token-url",
                        "https://idp.invalid/token",
                        "--registry-oauth-client-id",
                        "registry-client",
                        "--registry-oauth-scope",
                        "registry.read",
                        "--registry-oauth-ca-file",
                        str(paths["ca"]),
                        "--registry-oauth-logical-cluster",
                        "lsrc-1",
                        "--registry-oauth-identity-pool-id",
                        "pool-1",
                    ],
                    env=environment,
                )
                before = load_profiles(database_path).profile("secure-registry")
                edited = self.runner.invoke(
                    cli,
                    [
                        "edit",
                        "secure-registry",
                        "--unset",
                        "registry.tls.ca",
                        "--unset",
                        "registry.auth.oauth.scopes",
                        "--unset",
                        "registry.auth.oauth.ca",
                        "--unset",
                        "registry.auth.oauth.logical-cluster",
                        "--unset",
                        "registry.auth.oauth.identity-pool-id",
                    ],
                    env=environment,
                )
                after = load_profiles(database_path).profile("secure-registry")

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertEqual(
            before["registry"]["auth"]["clientSecretRef"],
            after["registry"]["auth"]["clientSecretRef"],
        )
        self.assertEqual([], after["registry"]["auth"]["scopes"])
        self.assertNotIn("caCertificates", after["registry"]["auth"])
        self.assertNotIn("logicalCluster", after["registry"]["auth"])
        self.assertNotIn("identityPoolId", after["registry"]["auth"])
        self.assertNotIn("tls", after["registry"])

    def test_required_secret_without_controlling_terminal_fails_with_guidance(self) -> None:
        with patch("kantrip.cli_inputs.os.open", side_effect=OSError("no tty")):
            result = self.runner.invoke(
                cli,
                [
                    "add",
                    "secure",
                    "--transport",
                    "tls",
                    "--auth",
                    "plain",
                    "--username",
                    "application",
                ],
                env={"KANTRIP_DATABASE": "profiles.db"},
            )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("requires a controlling terminal", result.output)

    def test_remove_confirmation_preserves_captured_profile_when_declined(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _add_test_profile(database_path)
            with patch("kantrip.cli._stdin_is_terminal", return_value=True):
                result = self.runner.invoke(
                    cli,
                    ["remove", "local"],
                    input="n\n",
                    env={"KANTRIP_DATABASE": str(database_path)},
                )

            profile = load_profiles(database_path).profile("local")

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("Removal canceled", result.output)
        self.assertEqual(["localhost:9092"], profile["kafka"]["bootstrapServers"])

    def test_add_rejects_empty_comma_separated_bootstrap_server(self) -> None:
        result = self.runner.invoke(
            cli, ["add", "invalid", "-b", "localhost:9092,"], env={"KANTRIP_DATABASE": "x"}
        )

        self.assertEqual(2, result.exit_code)
        self.assertIn("each broker must be a non-empty host:port address", result.output)

    def test_remove_without_a_terminal_requires_yes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path)}
            refused = self.runner.invoke(cli, ["remove", "local"], input="y\n", env=environment)
            retained = tuple(load_profiles(database_path).profiles)
            removed = self.runner.invoke(cli, ["remove", "local", "-y"], env=environment)
            remaining = tuple(load_profiles(database_path).profiles)

        self.assertEqual(2, refused.exit_code, refused.output)
        self.assertIn("confirmation requires a terminal; pass --yes", refused.output)
        self.assertEqual(("local",), retained)
        self.assertEqual(0, removed.exit_code, removed.output)
        self.assertEqual((), remaining)

    def test_mutation_errors_preserve_committed_and_unknown_exit_statuses(self) -> None:
        for exit_code in (3, 4):
            with (
                self.subTest(exit_code=exit_code),
                patch(
                    "kantrip.cli.add_profile",
                    side_effect=ProfileStoreError(
                        "synthetic mutation outcome", exit_code=exit_code
                    ),
                ),
            ):
                result = self.runner.invoke(cli, ["add", "outcome"])

            self.assertEqual(exit_code, result.exit_code, result.output)
            self.assertIn("synthetic mutation outcome", result.output)

    def test_broken_stdout_after_mutation_reports_committed_exit_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            with patch(
                "kantrip.cli.click.echo",
                side_effect=(BrokenPipeError("synthetic broken pipe"), None),
            ):
                result = self.runner.invoke(
                    cli,
                    ["add", "local"],
                    env={"KANTRIP_DATABASE": str(database_path)},
                )

            self.assertEqual(3, result.exit_code)
            self.assertIn("local", load_profiles(database_path).profiles)

    def test_list_is_empty_when_database_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "missing.db"

            result = self.runner.invoke(cli, ["list"], env={"KANTRIP_DATABASE": str(database_path)})

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("", result.output)

    def test_current_reports_active_profile(self) -> None:
        active = self.runner.invoke(cli, ["current"], env={"KANTRIP_PROFILE": "local"})
        inactive = self.runner.invoke(cli, ["current"], env={"KANTRIP_PROFILE": ""})

        self.assertEqual(0, active.exit_code, active.output)
        self.assertEqual("local\n", active.output)
        self.assertNotEqual(0, inactive.exit_code)
        self.assertIn("no profile is active", inactive.output)

    def test_doctor_uses_readable_status_markers_without_color(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _add_test_profile(database_path)

            with patch("kantrip.cli.run_doctor") as run:
                from kantrip.doctor import DoctorCheck, DoctorReport

                run.return_value = DoctorReport(
                    (
                        DoctorCheck("success", "configuration is valid"),
                        DoctorCheck("success", "resolved executable path", verbose_only=True),
                        DoctorCheck("warning", "kcat was not found"),
                    )
                )
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "doctor"],
                    env={"KANTRIP_DATABASE": str(database_path)},
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("Kantrip Doctor", result.output)
        self.assertIn("System", result.output)
        self.assertIn("[passed] configuration is valid", result.output)
        self.assertIn("[warning] kcat was not found", result.output)
        self.assertNotIn("resolved executable path", result.output)
        self.assertIn("[warning] Healthy with 1 warning", result.output)

    def test_doctor_verbose_shows_detailed_checks(self) -> None:
        with patch("kantrip.cli.run_doctor") as run:
            from kantrip.doctor import DoctorCheck, DoctorReport

            run.return_value = DoctorReport(
                (DoctorCheck("success", "resolved executable path", verbose_only=True),)
            )
            result = self.runner.invoke(cli, ["--no-color", "doctor", "--verbose"])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("└─ [passed] resolved executable path", result.output)
        self.assertIn("[passed] Healthy", result.output)

    def test_doctor_repair_runs_maintenance_before_diagnostics(self) -> None:
        with (
            patch("kantrip.cli.run_repair") as repair,
            patch("kantrip.cli.run_doctor") as run,
        ):
            from kantrip.doctor import DoctorCheck, DoctorReport

            repair.return_value = RepairReport(
                (
                    RepairAction("success", "Applied database migrations: 1"),
                    RepairAction("cleanup", "Sessions: removed 2 stale"),
                )
            )
            run.return_value = DoctorReport((DoctorCheck("success", "configuration is valid"),))
            result = self.runner.invoke(cli, ["doctor", "--repair", "--no-color"])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertLess(
            result.output.index("Kantrip Repair"), result.output.index("Kantrip Doctor")
        )
        self.assertIn("[passed] Applied database migrations: 1", result.output)
        self.assertIn("[cleanup] Sessions: removed 2 stale", result.output)
        repair.assert_called_once_with()
        run.assert_called_once_with()

    def test_doctor_repair_exits_nonzero_when_maintenance_fails(self) -> None:
        with (
            patch("kantrip.cli.run_repair") as repair,
            patch("kantrip.cli.run_doctor") as run,
        ):
            from kantrip.doctor import DoctorCheck, DoctorReport

            repair.return_value = RepairReport((RepairAction("error", "migration failed"),))
            run.return_value = DoctorReport((DoctorCheck("success", "configuration is valid"),))
            result = self.runner.invoke(cli, ["doctor", "--repair", "--no-color"])

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("[failed] migration failed", result.output)

    def test_doctor_help_lists_only_long_maintenance_options(self) -> None:
        result = self.runner.invoke(cli, ["doctor", "--help"])

        self.assertEqual(0, result.exit_code, result.output)
        for option in ("--repair", "--verbose"):
            line = next(line for line in result.output.splitlines() if option in line)
            self.assertTrue(line.lstrip().startswith(f"{option} "))

    def test_doctor_exits_nonzero_for_failed_checks(self) -> None:
        with patch("kantrip.cli.run_doctor") as run:
            from kantrip.doctor import DoctorCheck, DoctorReport

            run.return_value = DoctorReport((DoctorCheck("error", "configuration is invalid"),))
            result = self.runner.invoke(cli, ["--no-color", "doctor"])

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("[failed] configuration is invalid", result.output)
        self.assertIn("[failed] Unhealthy with 1 error", result.output)

    def test_ping_reports_kafka_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult("plaintext reachable", "not configured", "reachability"),
            ) as ping:
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn(
            "[passed] Kafka transport: plaintext reachable; authentication: not configured",
            result.output,
        )
        ping.assert_called_once_with(
            unittest.mock.ANY,
            timeout=1.5,
            kafka=unittest.mock.ANY,
            resolved_registry=None,
        )

    def test_ping_reports_confluent_registry_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(
                    "plaintext reachable",
                    "not configured",
                    "reachability",
                    RegistryPingResult(
                        "confluent",
                        "plaintext reachable",
                        "read query validated",
                    ),
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn("Kafka transport: plaintext reachable", result.output)
        self.assertIn(
            "[passed] Confluent Schema Registry transport: plaintext reachable; proof:",
            result.output,
        )
        self.assertIn("read query validated", " ".join(result.output.split()))

    def test_ping_reports_apicurio_registry_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(
                    "plaintext reachable",
                    "not configured",
                    "reachability",
                    RegistryPingResult(
                        "apicurio",
                        "plaintext reachable",
                        "read query validated",
                    ),
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn(
            "[passed] Apicurio Registry transport: plaintext reachable; proof:",
            result.output,
        )
        self.assertIn("read query validated", " ".join(result.output.split()))

    def test_add_apicurio_requires_and_persists_its_provider(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            environment = {"KANTRIP_DATABASE": str(database_path)}
            result = self.runner.invoke(
                cli,
                [
                    "add",
                    "native",
                    "--registry-provider",
                    "apicurio",
                    "--registry-url",
                    "http://registry.example.com/apis/registry/v3",
                ],
                env=environment,
            )

            profile = load_profiles(database_path).profile("native")

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("apicurio", profile["registry"]["provider"])
        self.assertIn("apicurio.registry.url", profile["registry"])

    def test_add_rejects_a_registry_provider_without_a_url(self) -> None:
        result = self.runner.invoke(
            cli,
            ["add", "native", "--registry-provider", "apicurio"],
            env={"KANTRIP_DATABASE": "profiles.db"},
        )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("requires --registry-url", result.output)

    def test_ping_failure_includes_the_sanitized_underlying_exception(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                side_effect=PingError(
                    "the Kafka cluster did not return metadata",
                    detail="_TRANSPORT: password=visible connection refused",
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local"],
                    env=environment,
                )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("[failed] Could not connect for profile 'local'", result.stderr)
        self.assertIn(
            "Cause: _TRANSPORT: password=<redacted> connection refused",
            result.stderr,
        )
        self.assertNotIn("visible", result.stderr)

    def test_ping_quiet_emits_nothing_on_connection_failure(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                side_effect=PingError(
                    "the Kafka cluster did not return metadata",
                    detail="_TRANSPORT: password=visible connection refused",
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["ping", "local", "--quiet"],
                    env=environment,
                )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)

    def test_ping_quiet_handles_exhausted_kafka_to_registry_deadline(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            profile = load_profiles(database_path).profile("local")
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}

            def exhausted_deadline(*_args: object, **_kwargs: object) -> PingResult:
                return ping_profile(profile, timeout=5)

            with (
                patch("kantrip.ping._probe_kafka"),
                patch("kantrip.ping.time.monotonic", side_effect=(0.0, 6.0)),
                patch("kantrip.cli.ping_profile", side_effect=exhausted_deadline),
            ):
                result = self.runner.invoke(
                    cli,
                    ["ping", "local", "--quiet"],
                    env=environment,
                )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)
        self.assertNotIn("Traceback", result.output)

    def test_ping_quiet_emits_nothing_on_success(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult("plaintext reachable", "not configured", "reachability"),
            ):
                result = self.runner.invoke(
                    cli,
                    ["ping", "local", "--quiet"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)

    def test_ping_quiet_emits_nothing_on_profile_store_failure(self) -> None:
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(
                cli,
                ["ping", "missing", "--quiet"],
                env={"KANTRIP_DATABASE": str(Path("missing.db").resolve())},
            )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)

    def test_ping_help_exposes_quiet_without_verbose(self) -> None:
        result = self.runner.invoke(cli, ["ping", "--help"])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("--quiet", result.output)
        self.assertNotIn("--verbose", result.output)

    def test_exec_preserves_command_arguments_and_exit_status(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch("kantrip.cli.run_profile_session", return_value=17) as run:
                result = self.runner.invoke(
                    cli, ["exec", "local", "--", "kcat", "-L"], env=environment
                )

        self.assertEqual(17, result.exit_code, result.output)
        self.assertEqual(("kcat", "-L"), run.call_args.args[2])

    def test_exec_passes_child_no_color_through_after_separator(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch("kantrip.cli.run_profile_session", return_value=0) as run:
                result = self.runner.invoke(
                    cli,
                    ["exec", "local", "--", "child-command", "--no-color"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(("child-command", "--no-color"), run.call_args.args[2])

    def test_exec_rejects_a_nested_session(self) -> None:
        result = self.runner.invoke(
            cli,
            ["exec", "local"],
            env={"KANTRIP_SESSION_ID": "existing-session"},
        )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("session is already active", result.output)

    def test_import_has_no_filesystem_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            environment = os.environ | {
                "HOME": str(temporary_path),
                "XDG_DATA_HOME": str(temporary_path / "data"),
                "XDG_CONFIG_HOME": str(temporary_path / "config"),
                "XDG_STATE_HOME": str(temporary_path / "state"),
                "XDG_RUNTIME_DIR": str(temporary_path / "runtime"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }

            result = subprocess.run(
                [sys.executable, "-c", "import kantrip"],
                capture_output=True,
                text=True,
                check=False,
                env=environment,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual([], list(temporary_path.iterdir()))

    def test_ping_keeps_kafka_success_when_registry_fails(self) -> None:
        failures = (
            ("transport", URLError("connection refused"), "did not return registry metadata"),
            ("authentication", _http_error(401), "rejected Registry authentication"),
            ("authorization", _http_error(403), "denied Registry authorization"),
        )
        for _, failure, _ in failures:
            if isinstance(failure, HTTPError):
                self.addCleanup(failure.close)
        for name, failure, message in failures:
            with self.subTest(name), self.runner.isolated_filesystem():
                database_path = Path("profiles.db")
                _add_test_profile(database_path, registry=True)
                environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
                with (
                    patch("kantrip.ping._probe_kafka"),
                    patch("kantrip.ping._open_request", side_effect=failure),
                ):
                    result = self.runner.invoke(
                        cli, ["--no-color", "ping", "local"], env=environment
                    )

                self.assertEqual(1, result.exit_code, result.output)
                self.assertIn("[passed] Kafka transport: plaintext reachable", result.stdout)
                self.assertNotIn("Schema Registry transport", result.stdout)
                self.assertIn("Registry check failed for profile 'local'", result.stderr)
                self.assertIn(message, " ".join(result.stderr.split()))

    def test_ping_skips_registry_when_kafka_fails(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with (
                patch("kantrip.ping._probe_kafka", side_effect=PingError("broker unreachable")),
                patch("kantrip.ping._open_request") as registry_request,
            ):
                result = self.runner.invoke(cli, ["--no-color", "ping", "local"], env=environment)

        self.assertEqual(1, result.exit_code, result.output)
        registry_request.assert_not_called()
        self.assertEqual(
            "", result.stdout.replace("[running] Checking profile 'local'", "").strip()
        )
        self.assertIn("Could not connect for profile 'local': broker unreachable", result.stderr)
        self.assertIn(
            "Confluent Schema Registry check skipped: Kafka check failed",
            " ".join(result.stderr.split()),
        )

    def test_ping_quiet_is_silent_for_every_service_outcome(self) -> None:
        outcomes = (
            ("kafka failure", PingError("broker unreachable"), None, 1),
            ("registry failure", None, URLError("connection refused"), 1),
        )
        for name, kafka_failure, registry_failure, status in outcomes:
            with self.subTest(name), self.runner.isolated_filesystem():
                database_path = Path("profiles.db")
                _add_test_profile(database_path, registry=True)
                environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
                with (
                    patch("kantrip.ping._probe_kafka", side_effect=kafka_failure),
                    patch("kantrip.ping._open_request", side_effect=registry_failure),
                ):
                    result = self.runner.invoke(cli, ["ping", "local", "-q"], env=environment)

                self.assertEqual(status, result.exit_code, result.output)
                self.assertEqual("", result.stdout)
                self.assertEqual("", result.stderr)


class TestEditRegistryAuthentication(unittest.TestCase):
    """Every Registry authentication transition through the scripted CLI."""

    URLS: ClassVar[dict[str, str]] = {
        "confluent": "https://registry.invalid",
        "apicurio": "https://registry.invalid/apis/registry/v3",
    }
    TYPES: ClassVar[dict[str, tuple[str, ...]]] = {
        "confluent": ("none", "basic", "token", "mtls", "oauth"),
        "apicurio": ("none", "basic", "mtls", "oauth"),
    }

    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_every_registry_auth_transition(self) -> None:
        pki = synthetic_pki()
        with temporary_pki_files(certificate=pki.client_certificate, key=pki.client_key) as paths:
            for provider, types in self.TYPES.items():
                for source in types:
                    for target in types:
                        if source == target:
                            continue
                        with self.subTest(provider=provider, source=source, target=target):
                            self._assert_transition(provider, source, target, paths)

    def test_provider_change_keeps_supported_authentication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store, environment = self._environment(directory)
            with patch("kantrip.profiles.load_secret_store", return_value=store):
                self._invoke(["add", "p", *self._registry("confluent", "basic", {})], environment)
                before = load_profiles(Path(environment["KANTRIP_DATABASE"])).profile("p")
                result = self._invoke(
                    [
                        "edit",
                        "p",
                        "--registry-provider",
                        "apicurio",
                        "--registry-url",
                        self.URLS["apicurio"],
                    ],
                    environment,
                )
                after = load_profiles(Path(environment["KANTRIP_DATABASE"])).profile("p")

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("apicurio", after["registry"]["provider"])
        self.assertEqual(before["registry"]["auth"], after["registry"]["auth"])
        self.assertEqual({before["registry"]["auth"]["passwordRef"]}, set(store.values))

    def test_provider_change_rejects_unsupported_authentication_without_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store, environment = self._environment(directory)
            with patch("kantrip.profiles.load_secret_store", return_value=store):
                self._invoke(["add", "p", *self._registry("confluent", "token", {})], environment)
                before_values = dict(store.values)
                result = self._invoke(
                    [
                        "edit",
                        "p",
                        "--registry-provider",
                        "apicurio",
                        "--registry-url",
                        self.URLS["apicurio"],
                    ],
                    environment,
                )
                profiles = load_profiles(Path(environment["KANTRIP_DATABASE"]))

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("does not support the current 'token' authentication", result.output)
        self.assertIn("--registry-auth", result.output)
        self.assertEqual("confluent", profiles.profile("p")["registry"]["provider"])
        self.assertEqual(1, profiles.revision("p"))
        self.assertEqual(before_values, store.values)

    def test_kafka_oauth_edit_accepts_no_scopes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store, environment = self._environment(directory)
            with patch("kantrip.profiles.load_secret_store", return_value=store):
                self._invoke(
                    [
                        "add",
                        "p",
                        "--transport",
                        "tls",
                        "--auth",
                        "plain",
                        "--username",
                        "user",
                    ],
                    environment,
                )
                result = self._invoke(
                    [
                        "edit",
                        "p",
                        "--auth",
                        "oauth",
                        "--oauth-token-url",
                        "https://idp.invalid/token",
                        "--oauth-client-id",
                        "client",
                    ],
                    environment,
                )
                auth = load_profiles(Path(environment["KANTRIP_DATABASE"])).profile("p")["kafka"][
                    "auth"
                ]

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual("oauth", auth["type"])
        self.assertEqual({auth["clientSecretRef"]}, set(store.values))

    def _assert_transition(
        self, provider: str, source: str, target: str, paths: dict[str, Path]
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store, environment = self._environment(directory)
            database = Path(environment["KANTRIP_DATABASE"])
            with patch("kantrip.profiles.load_secret_store", return_value=store):
                added = self._invoke(
                    ["add", "p", *self._registry(provider, source, paths)], environment
                )
                edited = self._invoke(["edit", "p", *self._auth(target, paths)], environment)
                registry = load_profiles(database).profile("p")["registry"]

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertEqual(provider, registry["provider"])
        self.assertEqual(target, registry["auth"]["type"])
        references = {
            value
            for key, value in registry["auth"].items()
            if key.endswith("Ref") and isinstance(value, str)
        }
        self.assertEqual(references, set(store.values))
        self.assertEqual(target == "mtls", "clientCertificate" in registry.get("tls", {}))

    def _registry(self, provider: str, auth_type: str, paths: dict[str, Path]) -> list[str]:
        return [
            "--registry-provider",
            provider,
            "--registry-url",
            self.URLS[provider],
            *self._auth(auth_type, paths),
        ]

    @staticmethod
    def _auth(auth_type: str, paths: dict[str, Path]) -> list[str]:
        return {
            "none": ["--registry-auth", "none"],
            "basic": ["--registry-auth", "basic", "--registry-username", "registry-user"],
            "token": ["--registry-auth", "token"],
            "mtls": [
                "--registry-auth",
                "mtls",
                "--registry-client-certificate-file",
                str(paths.get("certificate", "")),
                "--registry-client-key-file",
                str(paths.get("key", "")),
            ],
            "oauth": [
                "--registry-auth",
                "oauth",
                "--registry-oauth-token-url",
                "https://idp.invalid/token",
                "--registry-oauth-client-id",
                "registry-client",
                "--registry-oauth-scope",
                "registry.read",
            ],
        }[auth_type]

    @staticmethod
    def _environment(directory: str) -> tuple["_MemorySecretStore", dict[str, str]]:
        return _MemorySecretStore(), {"KANTRIP_DATABASE": str(Path(directory) / "profiles.db")}

    def _invoke(self, arguments: list[str], environment: dict[str, str]) -> Any:
        with patch("kantrip.cli_inputs.secret_prompt", return_value=Secret("synthetic-secret")):
            return self.runner.invoke(cli, arguments, env=environment)


class TestCliOptionContract(unittest.TestCase):
    """The v0.1 option names: -b, --unset, --replace-secret, and remove --yes (#52)."""

    def setUp(self) -> None:
        self.runner = CliRunner()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "profiles.db"
        self.environment = {"KANTRIP_DATABASE": str(self.database)}
        self.store = _MemorySecretStore()
        for target, value in (
            ("kantrip.profiles.load_secret_store", {"return_value": self.store}),
            ("kantrip.cli_inputs.secret_prompt", {"return_value": Secret("synthetic-secret")}),
        ):
            patcher = patch(target, **value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def invoke(self, *arguments: str) -> Any:
        return self.runner.invoke(cli, list(arguments), env=self.environment)

    def test_bootstrap_server_repeats_and_comma_lists_combine_in_order(self) -> None:
        added = self.invoke("add", "p", "-b", "k1:9092", "--bootstrap-server", "k2:9092,k3:9092")
        stored = load_profiles(self.database).profile("p")["kafka"]["bootstrapServers"]
        edited = self.invoke("edit", "p", "-b", "k4:9092", "-b", "k5:9092")
        replaced = load_profiles(self.database).profile("p")["kafka"]["bootstrapServers"]

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual(["k1:9092", "k2:9092", "k3:9092"], stored)
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertEqual(["k4:9092", "k5:9092"], replaced)

    def test_bootstrap_server_rejects_duplicates(self) -> None:
        for arguments in (("-b", "k1:9092", "-b", "k1:9092"), ("-b", "k1:9092,k1:9092")):
            with self.subTest(arguments):
                result = self.invoke("add", "p", *arguments)

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn("broker 'k1:9092' was supplied more than once", result.output)
        self.assertFalse(self.database.exists())

    def test_removed_flags_are_unknown_options(self) -> None:
        self.invoke("add", "p")
        for command in (
            ("add", "q", "--bootstrap-servers", "k1:9092"),
            ("edit", "p", "--clear-description"),
            ("edit", "p", "--remove-label", "owner"),
            ("edit", "p", "--remove-registry"),
            ("edit", "p", "--default-trust"),
            ("edit", "p", "--clear-oauth-scopes"),
            ("edit", "p", "--oauth-default-trust"),
            ("edit", "p", "--registry-default-trust"),
            ("edit", "p", "--clear-registry-oauth-scopes"),
            ("edit", "p", "--registry-oauth-default-trust"),
            ("edit", "p", "--clear-registry-oauth-logical-cluster"),
            ("edit", "p", "--clear-registry-oauth-identity-pool-id"),
            ("remove", "p", "--force"),
        ):
            with self.subTest(command):
                result = self.invoke(*command)

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn("No such option", result.output)

    def test_unset_rejects_unknown_and_repeated_fields(self) -> None:
        self.invoke("add", "p")
        unknown = self.invoke("edit", "p", "--unset", "kafka.bootstrap-servers")
        empty_label = self.invoke("edit", "p", "--unset", "labels.")
        repeated = self.invoke("edit", "p", "--unset", "description", "--unset", "description")

        self.assertEqual(2, unknown.exit_code, unknown.output)
        self.assertIn("unknown field 'kafka.bootstrap-servers'; valid fields:", unknown.output)
        self.assertIn("labels.KEY", unknown.output)
        self.assertEqual(2, empty_label.exit_code, empty_label.output)
        self.assertEqual(2, repeated.exit_code, repeated.output)
        self.assertIn("each field may be named only once", repeated.output)

    def test_unset_cannot_be_combined_with_setting_the_same_field(self) -> None:
        self.invoke("add", "p", "-l", "owner=platform")
        for arguments, message in (
            (
                ("--unset", "description", "-d", "new"),
                "--unset description cannot be combined with --description",
            ),
            (
                ("--unset", "labels.owner", "-l", "owner=data"),
                "--unset labels.owner cannot be combined with --label owner=VALUE",
            ),
        ):
            with self.subTest(arguments):
                result = self.invoke("edit", "p", *arguments)

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn(message, result.output)
        self.assertEqual(1, load_profiles(self.database).revision("p"))

    def test_unset_kafka_oauth_scopes_and_token_trust(self) -> None:
        with temporary_pki_files(ca=synthetic_pki().ca) as paths:
            added = self.invoke(
                "add",
                "p",
                "--transport",
                "tls",
                "--auth",
                "oauth",
                "--oauth-token-url",
                "https://idp.invalid/token",
                "--oauth-client-id",
                "client",
                "--oauth-scope",
                "kafka.read",
                "--oauth-ca-file",
                str(paths["ca"]),
            )
        before = load_profiles(self.database).profile("p")["kafka"]["auth"]
        edited = self.invoke(
            "edit", "p", "--unset", "kafka.auth.oauth.scopes", "--unset", "kafka.auth.oauth.ca"
        )
        after = load_profiles(self.database).profile("p")["kafka"]["auth"]

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual(["kafka.read"], before["scopes"])
        self.assertIn("caCertificates", before)
        self.assertEqual(0, edited.exit_code, edited.output)
        self.assertNotIn("caCertificates", after)
        self.assertFalse(after.get("scopes"))
        self.assertEqual(before["clientSecretRef"], after["clientSecretRef"])

    def test_replace_secret_uses_the_names_describe_prints(self) -> None:
        described = {
            f"{scope}.auth.{field}"
            for scope in ("kafka", "registry")
            for _, field in DESCRIBED_SECRET_FIELDS
        }

        self.assertLessEqual(set(SECRET_FIELDS), described)
        self.invoke("add", "p", "--transport", "tls", "--auth", "scram-sha-512", "--username", "u")
        old_name = self.invoke("edit", "p", "--replace-secret", "kafka/password")
        new_name = self.invoke("edit", "p", "--replace-secret", "kafka.auth.password")

        self.assertEqual(2, old_name.exit_code, old_name.output)
        self.assertIn("unknown field 'kafka/password'; valid fields:", old_name.output)
        self.assertEqual(0, new_name.exit_code, new_name.output)
        self.assertEqual(2, load_profiles(self.database).revision("p"))


class TestCliResults(unittest.TestCase):
    """Grouped help, TLS inference on `add`, success lines, and the `list` hint (#52)."""

    def setUp(self) -> None:
        self.runner = CliRunner()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "profiles.db"
        self.environment = {"KANTRIP_DATABASE": str(self.database)}
        for target, value in (
            ("kantrip.profiles.load_secret_store", {"return_value": _MemorySecretStore()}),
            ("kantrip.cli_inputs.secret_prompt", {"return_value": Secret("synthetic-secret")}),
        ):
            patcher = patch(target, **value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def invoke(self, *arguments: str) -> Any:
        return self.runner.invoke(cli, list(arguments), env=self.environment)

    def test_help_groups_options_by_connection(self) -> None:
        groups = (
            "Kafka connection:",
            "Kafka authentication:",
            "Kafka OAuth:",
            "Registry:",
            "Registry authentication:",
            "Registry OAuth:",
            "Profile metadata:",
        )
        for command, extra in (("add", ()), ("edit", ("Removal and rotation:",))):
            with self.subTest(command):
                output = self.invoke(command, "--help").output
                positions = [output.index(title) for title in (*groups, *extra)]

                self.assertEqual(sorted(positions), positions)

    def test_success_lines_state_the_result_without_the_database_path(self) -> None:
        added = self.invoke(
            "add", "prod", "-b", "k1:9093,k2:9093", "--auth", "scram-sha-512", "--username", "u"
        )
        edited = self.invoke("edit", "prod", "-b", "k3:9093")
        local = self.invoke("add", "local")
        removed = self.invoke("remove", "prod", "--yes")

        self.assertEqual(
            "Added profile 'prod': k1:9093,k2:9093, tls, scram-sha-512\n", added.stdout
        )
        self.assertEqual("Updated profile 'prod': k3:9093, tls, scram-sha-512\n", edited.stdout)
        self.assertEqual(
            "Added profile 'local': localhost:9092, plaintext, no authentication\n", local.stdout
        )
        self.assertEqual("Removed profile 'prod'\n", removed.stdout)
        for result in (added, edited, local, removed):
            self.assertNotIn(str(self.database.parent), result.output)

    def test_add_infers_tls_from_security_options(self) -> None:
        pki = synthetic_pki()
        with temporary_pki_files(
            ca=pki.ca, certificate=pki.client_certificate, key=pki.client_key
        ) as paths:
            cases = {
                "plain": ((), "plaintext"),
                "ca": (("--ca-file", str(paths["ca"])), "tls"),
                "scram": (("--auth", "scram-sha-256", "--username", "u"), "tls"),
                "mtls": (
                    ("--auth", "mtls")
                    + ("--client-certificate-file", str(paths["certificate"]))
                    + ("--client-key-file", str(paths["key"])),
                    "tls",
                ),
                "explicit": (("--transport", "tls"), "tls"),
            }
            results = {
                name: self.invoke("add", name, *arguments) for name, (arguments, _) in cases.items()
            }
        profiles = load_profiles(self.database)

        for name, (_, transport) in cases.items():
            with self.subTest(name):
                self.assertEqual(0, results[name].exit_code, results[name].output)
                self.assertEqual(transport, profiles.profile(name)["kafka"]["transport"])

    def test_explicit_plaintext_with_authentication_still_fails(self) -> None:
        result = self.invoke(
            "add", "p", "--transport", "plaintext", "--auth", "scram-sha-512", "--username", "u"
        )

        self.assertNotEqual(0, result.exit_code, result.output)
        self.assertIn("requires --transport tls", result.output)
        self.assertFalse(self.database.exists() and "p" in load_profiles(self.database).profiles)

    def test_empty_list_hints_only_on_a_terminal(self) -> None:
        with patch("kantrip.cli._stdout_is_terminal", return_value=True):
            empty_terminal = self.invoke("list")
        empty_pipe = self.invoke("list")
        self.invoke("add", "local")
        with patch("kantrip.cli._stdout_is_terminal", return_value=True):
            no_match = self.invoke("list", "-l", "owner=nobody")

        hint = "No profiles yet. Add one with: kantrip add NAME\n"
        self.assertEqual(0, empty_terminal.exit_code)
        self.assertEqual("", empty_terminal.stdout)
        self.assertEqual(hint, empty_terminal.stderr)
        self.assertEqual("", empty_pipe.stdout + empty_pipe.stderr)
        self.assertEqual("", no_match.stdout + no_match.stderr)


class TestDescribeCompleteness(unittest.TestCase):
    """`describe` shows every public field and never touches the secret store."""

    CERTIFICATE: ClassVar[dict[str, str]] = {
        "subject": "CN=kantrip-client",
        "expires": "2035-01-01T00:00:00Z",
    }

    def setUp(self) -> None:
        self.runner = CliRunner()
        pki = synthetic_pki()
        files = temporary_pki_files(
            ca=pki.ca,
            certificate=pki.client_certificate,
            key=pki.client_key,
            encrypted_key=pki.encrypted_client_key,
        )
        self.paths = {name: str(path) for name, path in files.__enter__().items()}
        self.addCleanup(files.__exit__, None, None, None)

    def test_kafka_authentication_fields(self) -> None:
        paths = self.paths
        cases: dict[str, tuple[list[str], dict[str, Any]]] = {
            "none": (
                [],
                {
                    "bootstrapServers": ["localhost:9092"],
                    "transport": "plaintext",
                    "tls": None,
                    "auth": {"type": "none", "credentials": {}},
                },
            ),
            "scram": (
                ["-b", "kafka.invalid:9093", "--transport", "tls", "--ca-file", paths["ca"]]
                + ["--auth", "scram-sha-512", "--username", "app"],
                {
                    "bootstrapServers": ["kafka.invalid:9093"],
                    "transport": "tls",
                    "tls": {"trust": "custom"},
                    "auth": {
                        "type": "scram-sha-512",
                        "username": "app",
                        "credentials": {"kafka.auth.password": "configured"},
                    },
                },
            ),
            "mtls": (
                ["-b", "kafka.invalid:9093", "--transport", "tls", "--auth", "mtls"]
                + ["--client-certificate-file", paths["certificate"]]
                + ["--client-key-file", paths["encrypted_key"]],
                {
                    "bootstrapServers": ["kafka.invalid:9093"],
                    "transport": "tls",
                    "tls": {"trust": "system"},
                    "auth": {
                        "type": "mtls",
                        "clientCertificate": self.CERTIFICATE,
                        "credentials": {
                            "kafka.auth.private-key": "configured",
                            "kafka.auth.private-key-password": "configured",
                        },
                    },
                },
            ),
            "oauth": (
                ["-b", "kafka.invalid:9093", "--transport", "tls", "--auth", "oauth"]
                + ["--oauth-token-url", "https://idp.invalid/token"]
                + ["--oauth-client-id", "kafka-client", "--oauth-scope", "kafka.read"]
                + ["--oauth-ca-file", paths["ca"]],
                {
                    "bootstrapServers": ["kafka.invalid:9093"],
                    "transport": "tls",
                    "tls": {"trust": "system"},
                    "auth": {
                        "type": "oauth",
                        "oauth": {
                            "tokenUrl": "https://idp.invalid/token",
                            "clientId": "kafka-client",
                            "scopes": ["kafka.read"],
                            "trust": "custom",
                        },
                        "credentials": {"kafka.auth.oauth.client-secret": "configured"},
                    },
                },
            ),
        }
        for name, (arguments, expected) in cases.items():
            with self.subTest(name):
                observation = self._describe(arguments)
                self.assertEqual(expected, observation["kafka"])
                self.assertIsNone(observation["registry"])

    def test_registry_authentication_fields(self) -> None:
        paths = self.paths
        https = ["--registry-url", "https://registry.invalid"]
        cases: dict[str, tuple[list[str], dict[str, Any]]] = {
            "none": (
                ["--registry-url", "http://registry.invalid:8081"],
                {
                    "provider": "confluent",
                    "url": "http://registry.invalid:8081",
                    "tls": None,
                    "auth": {"type": "none", "credentials": {}},
                },
            ),
            "basic": (
                [*https, "--registry-auth", "basic", "--registry-username", "registry-user"],
                {
                    "provider": "confluent",
                    "url": "https://registry.invalid",
                    "tls": {"trust": "system"},
                    "auth": {
                        "type": "basic",
                        "username": "registry-user",
                        "credentials": {"registry.auth.password": "configured"},
                    },
                },
            ),
            "token": (
                [*https, "--registry-ca-file", paths["ca"], "--registry-auth", "token"],
                {
                    "provider": "confluent",
                    "url": "https://registry.invalid",
                    "tls": {"trust": "custom"},
                    "auth": {
                        "type": "token",
                        "credentials": {"registry.auth.token": "configured"},
                    },
                },
            ),
            "mtls": (
                ["--registry-provider", "apicurio"]
                + ["--registry-url", "https://registry.invalid/apis/registry/v3"]
                + ["--registry-auth", "mtls"]
                + ["--registry-client-certificate-file", paths["certificate"]]
                + ["--registry-client-key-file", paths["key"]],
                {
                    "provider": "apicurio",
                    "url": "https://registry.invalid/apis/registry/v3",
                    "tls": {"trust": "system"},
                    "auth": {
                        "type": "mtls",
                        "clientCertificate": self.CERTIFICATE,
                        "credentials": {"registry.auth.private-key": "configured"},
                    },
                },
            ),
            "oauth": (
                [*https, "--registry-auth", "oauth"]
                + ["--registry-oauth-token-url", "https://idp.invalid/token"]
                + ["--registry-oauth-client-id", "registry-client"]
                + ["--registry-oauth-scope", "registry.read"]
                + ["--registry-oauth-logical-cluster", "lsrc-123"]
                + ["--registry-oauth-identity-pool-id", "pool-abc"],
                {
                    "provider": "confluent",
                    "url": "https://registry.invalid",
                    "tls": {"trust": "system"},
                    "auth": {
                        "type": "oauth",
                        "oauth": {
                            "tokenUrl": "https://idp.invalid/token",
                            "clientId": "registry-client",
                            "scopes": ["registry.read"],
                            "trust": "system",
                            "logicalCluster": "lsrc-123",
                            "identityPoolId": "pool-abc",
                        },
                        "credentials": {"registry.auth.oauth.client-secret": "configured"},
                    },
                },
            ),
        }
        for name, (arguments, expected) in cases.items():
            with self.subTest(name):
                self.assertEqual(expected, self._describe(arguments)["registry"])

    def test_human_output_lists_public_fields(self) -> None:
        output = self._describe(
            ["-b", "kafka.invalid:9093", "--transport", "tls", "--auth", "mtls"]
            + ["--client-certificate-file", self.paths["certificate"]]
            + ["--client-key-file", self.paths["key"]]
            + ["--registry-url", "https://registry.invalid", "--registry-auth", "oauth"]
            + ["--registry-oauth-token-url", "https://idp.invalid/token"]
            + ["--registry-oauth-client-id", "registry-client"],
            output_format="human",
        )

        words = " ".join(output.split())
        for expected in (
            "Client certificate CN=kantrip-client",
            "Certificate expires 2035-01-01T00:00:00Z",
            "Credentials kafka.auth.private-key: configured",
            "OAuth token URL https://idp.invalid/token",
            "OAuth client ID registry-client",
            "Credentials registry.auth.oauth.client-secret: configured",
        ):
            self.assertIn(expected, words)

    def _describe(self, arguments: list[str], *, output_format: str = "json") -> Any:
        with tempfile.TemporaryDirectory() as directory:
            store = _MemorySecretStore()
            environment = {"KANTRIP_DATABASE": str(Path(directory) / "profiles.db")}
            with (
                patch("kantrip.profiles.load_secret_store", return_value=store),
                patch("kantrip.cli_inputs.secret_prompt", side_effect=_synthetic_prompt),
            ):
                added = self.runner.invoke(cli, ["add", "p", *arguments], env=environment)
            self.assertEqual(0, added.exit_code, added.output)
            with _no_secret_store():
                results = {
                    selected: self.runner.invoke(
                        cli, ["describe", "p", "-o", selected], env=environment
                    )
                    for selected in ("human", "json", "yaml")
                }

        for result in results.values():
            self.assertEqual(0, result.exit_code, result.output)
            for reference, value in store.values.items():
                self.assertNotIn(reference, result.output)
                self.assertNotIn(value, result.output)
            self.assertNotIn("Ref", result.output)
            self.assertNotIn("BEGIN", result.output)
        observation = json.loads(results["json"].output)
        self.assertEqual(observation, yaml.safe_load(results["yaml"].output))
        if output_format == "human":
            return results["human"].output
        return observation


def _synthetic_prompt(label: str) -> Secret:
    if "private-key password" in label:
        return Secret(KEY_PASSWORD)
    return Secret(f"synthetic-{label.lower().replace(' ', '-')}")


@contextmanager
def _no_secret_store() -> Iterator[None]:
    """Fail the test on any OS credential-store access."""

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("describe must not access the secret store")

    with ExitStack() as stack:
        for target in (
            "kantrip.profiles.load_secret_store",
            "kantrip.secret_store.load_secret_store",
            "keyring.get_keyring",
            "keyring.get_password",
        ):
            stack.enter_context(patch(target, side_effect=forbidden))
        yield


def _http_error(status: int) -> HTTPError:
    return HTTPError("http://localhost:8081/subjects?limit=1", status, "rejected", Message(), None)


def _add_test_profile(path: Path, *, registry: bool = False) -> None:
    add_profile(
        "local",
        path,
        description="Local development",
        registry_url="http://localhost:8081" if registry else None,
    )


class _MemorySecretStore:
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


if __name__ == "__main__":
    unittest.main()
