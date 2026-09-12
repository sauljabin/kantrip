import io
import tempfile
import unittest
from pathlib import Path

import yaml
from rich.console import Console

from sandbox.__main__ import _show_section, _write_shell_driver

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

        self.assertEqual("confluentinc/cp-schema-registry:${CONFLUENT_VERSION}", registry["image"])
        self.assertEqual(["8081:8081"], registry["ports"])
        self.assertEqual(
            "http://0.0.0.0:8081", registry["environment"]["SCHEMA_REGISTRY_LISTENERS"]
        )
        self.assertNotIn("HTTPS", registry["environment"]["SCHEMA_REGISTRY_LISTENERS"])

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


if __name__ == "__main__":
    unittest.main()
