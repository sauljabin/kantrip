import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from kantrip import APP_VERSION
from kantrip.cli import cli
from kantrip.console import create_console
from kantrip.maintenance import RepairAction, RepairReport
from kantrip.ping import PingError, PingResult, RegistryPingResult
from kantrip.profiles import add_profile, load_profiles

CA_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "kafka-ca.pem"


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
            removed = self.runner.invoke(cli, ["remove", "development"], env=environment)
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

            added = self.runner.invoke(
                cli,
                [
                    "add",
                    "production",
                    "--bootstrap-servers",
                    "broker.example.com:9093",
                    "--transport",
                    "tls",
                    "--ca-file",
                    str(CA_FIXTURE),
                ],
                env=environment,
            )
            secure = load_profiles(database_path).profile("production")
            system_ca = self.runner.invoke(
                cli,
                ["edit", "production", "--system-ca"],
                env=environment,
            )
            system_trust = load_profiles(database_path).profile("production")
            edited = self.runner.invoke(
                cli,
                ["edit", "production", "--transport", "plaintext"],
                env=environment,
            )
            plaintext = load_profiles(database_path).profile("production")

        self.assertEqual(0, added.exit_code, added.output)
        self.assertEqual("tls", secure["kafka"]["transport"])
        self.assertIn("BEGIN CERTIFICATE", secure["kafka"]["tls"]["caCertificates"])
        self.assertEqual(0, system_ca.exit_code, system_ca.output)
        self.assertEqual("tls", system_trust["kafka"]["transport"])
        self.assertNotIn("tls", system_trust["kafka"])
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
                    "--bootstrap-servers",
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
                    "--clear-description",
                    "--remove-label",
                    "environment",
                    "--remove-registry",
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

    def test_edit_requires_an_explicit_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "profiles.db"
            _add_test_profile(database_path)
            result = self.runner.invoke(
                cli,
                ["edit", "local"],
                env={"KANTRIP_DATABASE": str(database_path)},
            )

        self.assertEqual(1, result.exit_code, result.output)
        self.assertIn("no profile changes were requested", result.output)

    def test_add_rejects_empty_comma_separated_bootstrap_server(self) -> None:
        result = self.runner.invoke(
            cli, ["add", "invalid", "-b", "localhost:9092,"], env={"KANTRIP_DATABASE": "x"}
        )

        self.assertNotEqual(0, result.exit_code)
        self.assertIn("comma-separated list of host:port addresses", result.output)

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
                return_value=PingResult(broker_count=2),
            ) as ping:
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn("[passed] Connected to Kafka (2 brokers)", result.output)
        ping.assert_called_once_with(unittest.mock.ANY, timeout=1.5)

    def test_ping_reports_confluent_registry_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(
                    broker_count=2,
                    registry=RegistryPingResult(provider="confluent", count=3),
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local", "--timeout", "1.5"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[running] Checking profile 'local'", result.output)
        self.assertIn("[passed] Connected to Kafka (2 brokers)", result.output)
        self.assertIn("[passed] Connected to Confluent Schema Registry (3 subjects)", result.output)

    def test_ping_reports_apicurio_registry_connectivity(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path, registry=True)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(
                    broker_count=1,
                    registry=RegistryPingResult(provider="apicurio", count=2),
                ),
            ):
                result = self.runner.invoke(
                    cli,
                    ["--no-color", "ping", "local"],
                    env=environment,
                )

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn("[passed] Connected to Apicurio Registry (2 artifacts)", result.output)

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

    def test_ping_quiet_emits_nothing_on_success(self) -> None:
        with self.runner.isolated_filesystem():
            database_path = Path("profiles.db")
            _add_test_profile(database_path)
            environment = {"KANTRIP_DATABASE": str(database_path.resolve())}
            with patch(
                "kantrip.cli.ping_profile",
                return_value=PingResult(broker_count=1),
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


def _add_test_profile(path: Path, *, registry: bool = False) -> None:
    add_profile(
        "local",
        path,
        description="Local development",
        registry_url="http://localhost:8081" if registry else None,
    )


if __name__ == "__main__":
    unittest.main()
