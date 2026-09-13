import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner
from rich.console import Console

from sandbox.__main__ import (
    SmokeFailure,
    _failure_details,
    _kantrip_cli,
    _show_section,
    _write_shell_driver,
    main,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SANDBOX_ENV = PROJECT_ROOT / "sandbox" / ".env"
SANDBOX_COMPOSE = PROJECT_ROOT / "sandbox" / "compose.yml"


def sandbox_versions() -> dict[str, str]:
    return dict(
        line.split("=", 1)
        for line in SANDBOX_ENV.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )


class TestSandbox(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = yaml.safe_load(SANDBOX_COMPOSE.read_text(encoding="utf-8"))
        self.services = self.compose["services"]
        self.versions = sandbox_versions()

    def test_uses_current_kafka_image(self) -> None:
        self.assertEqual("8.3.1", self.versions["CONFLUENT_VERSION"])
        self.assertEqual("3.3.2", self.versions["APICURIO_VERSION"])
        self.assertEqual(
            "confluentinc/cp-kafka:${CONFLUENT_VERSION}", self.services["kafka"]["image"]
        )

    def test_contains_only_the_supported_plaintext_kafka_cluster(self) -> None:
        self.assertEqual(
            {"kafka", "schema-registry", "apicurio", "apicurio-topics"},
            set(self.services),
        )
        protocols = self.services["kafka"]["environment"]["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]
        self.assertNotIn("SSL", protocols)
        self.assertNotIn("SASL", protocols)

    def test_exposes_the_broker_on_standard_host_port(self) -> None:
        service = self.services["kafka"]
        self.assertEqual(["9092:19092"], service["ports"])
        self.assertIn(
            "EXTERNAL://localhost:9092",
            service["environment"]["KAFKA_ADVERTISED_LISTENERS"],
        )
        self.assertEqual("1", service["environment"]["KAFKA_DEFAULT_REPLICATION_FACTOR"])

    def test_contains_plain_schema_registry(self) -> None:
        registry = self.services["schema-registry"]
        environment = registry["environment"]

        self.assertEqual("confluentinc/cp-schema-registry:${CONFLUENT_VERSION}", registry["image"])
        self.assertEqual(["8081:8081"], registry["ports"])
        self.assertEqual("http://0.0.0.0:8081", environment["SCHEMA_REGISTRY_LISTENERS"])
        self.assertEqual("_schemas", environment["SCHEMA_REGISTRY_KAFKASTORE_TOPIC"])
        self.assertEqual("1", environment["SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR"])
        self.assertNotIn("HTTPS", environment["SCHEMA_REGISTRY_LISTENERS"])

    def test_schema_registry_healthcheck_uses_its_available_python_runtime(self) -> None:
        healthcheck = self.services["schema-registry"]["healthcheck"]
        command = healthcheck["test"]

        self.assertEqual(["CMD", "python3", "-c"], command[:3])
        self.assertIn("http://localhost:8081/subjects", command[3])
        self.assertIn("timeout=5", command[3])
        self.assertNotIn("curl", command[3])
        self.assertEqual("30s", healthcheck["start_period"])

    def test_contains_apicurio_on_standard_host_port(self) -> None:
        registry = self.services["apicurio"]

        self.assertEqual("apicurio/apicurio-registry:${APICURIO_VERSION}", registry["image"])
        self.assertEqual(["8082:8080"], registry["ports"])
        self.assertEqual(
            "kafka:9092", registry["environment"]["APICURIO_KAFKASQL_BOOTSTRAP_SERVERS"]
        )
        self.assertEqual(
            "service_completed_successfully",
            registry["depends_on"]["apicurio-topics"]["condition"],
        )

    def test_does_not_use_compose_extension_fields(self) -> None:
        self.assertFalse(any(key.startswith("x-") for key in self.compose))

    def test_uses_stable_sandbox_network_name(self) -> None:
        self.assertEqual("sandbox", self.compose["networks"]["default"]["name"])

    def test_writes_one_sourced_driver_for_interactive_shell_commands(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            driver = _write_shell_driver("bash", ("first", "second"), root)

            self.assertEqual(f". {root / 'commands'} < /dev/null; exit $?", driver)
            self.assertEqual("first\nsecond\n", (root / "commands").read_text())
            self.assertEqual(0o600, (root / "commands").stat().st_mode & 0o777)

    def test_uses_fish_source_syntax_for_the_shell_driver(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            driver = _write_shell_driver("fish", (), root)

            self.assertEqual(
                f"source '{root / 'commands'}' < /dev/null; "
                "set -l kantrip_status $status; exit $kantrip_status",
                driver,
            )

    def test_sections_are_separated_from_the_sandbox_title(self) -> None:
        stream = io.StringIO()
        console = Console(file=stream, color_system=None)

        console.print("Kantrip Sandbox")
        _show_section(console, "Setup")
        _show_section(console, "Kafka CLI")

        self.assertEqual(
            "Kantrip Sandbox\n\nSetup\n\nKafka CLI\n",
            stream.getvalue(),
        )

    def test_captured_kantrip_commands_explicitly_disable_color(self) -> None:
        command = _kantrip_cli("ping", "sandbox")

        self.assertEqual(
            [
                "-m",
                "kantrip.cli",
                "--no-color",
                "ping",
                "sandbox",
            ],
            command[1:],
        )

    def test_failure_details_remove_nested_status_presentation(self) -> None:
        result = subprocess.CompletedProcess(
            ["kantrip", "ping", "sandbox"],
            1,
            stdout="[running] Checking profile 'sandbox'\n",
            stderr=(
                "[failed] Could not connect for profile 'sandbox': the Kafka cluster "
                "did not return metadata\n"
            ),
        )

        self.assertEqual(
            "Could not connect for profile 'sandbox': the Kafka cluster did not return metadata",
            _failure_details(result),
        )

    @patch("sandbox.__main__.smoke", side_effect=SmokeFailure("connectivity failed"))
    def test_main_renders_failures_with_its_selected_presentation(self, _smoke: object) -> None:
        result = CliRunner().invoke(main, ["--no-color"])

        self.assertEqual(1, result.exit_code)
        self.assertEqual("[failed] connectivity failed\n", result.output)
        self.assertNotIn("Error:", result.output)


if __name__ == "__main__":
    unittest.main()
