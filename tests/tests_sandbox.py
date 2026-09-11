import unittest
from pathlib import Path

import yaml

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

    def test_keeps_a_repository_specific_network(self) -> None:
        self.assertEqual("kantrip-sandbox", self.compose["networks"]["default"]["name"])


if __name__ == "__main__":
    unittest.main()
