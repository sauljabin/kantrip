import unittest
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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

    def test_uses_current_registry_and_kafka_images(self) -> None:
        self.assertEqual("8.3.1", self.versions["CONFLUENT_VERSION"])
        self.assertEqual("3.3.2", self.versions["APICURIO_VERSION"])
        self.assertEqual(
            "confluentinc/cp-kafka:${CONFLUENT_VERSION}", self.services["kafka1"]["image"]
        )
        self.assertEqual(
            "apicurio/apicurio-registry:${APICURIO_VERSION}",
            self.services["apicurio"]["image"],
        )

    def test_apicurio_uses_replication_safe_kafka_storage(self) -> None:
        environment = self.services["apicurio"]["environment"]
        self.assertEqual("kafkasql", environment["APICURIO_STORAGE_KIND"])
        self.assertEqual(
            "kafka1:9092,kafka2:9092,kafka3:9092",
            environment["APICURIO_KAFKASQL_BOOTSTRAP_SERVERS"],
        )
        self.assertEqual("_apicurio-registry-journal", environment["APICURIO_KAFKASQL_TOPIC"])
        self.assertEqual(
            "_apicurio-registry-snapshots",
            environment["APICURIO_KAFKASQL_SNAPSHOTS_TOPIC"],
        )

        initializer = self.services["apicurio-topics"]
        command = "\n".join(initializer["command"])
        self.assertIn("_apicurio-registry-journal", command)
        self.assertIn("_apicurio-registry-snapshots", command)
        self.assertIn("--partitions 3", command)
        self.assertIn("--replication-factor 3", command)
        self.assertEqual(
            {"condition": "service_completed_successfully"},
            self.services["apicurio"]["depends_on"]["apicurio-topics"],
        )

    def test_registries_have_dependency_aware_health_checks(self) -> None:
        apicurio_healthcheck = " ".join(self.services["apicurio"]["healthcheck"]["test"])
        schema_registry_healthcheck = " ".join(
            self.services["schema-registry"]["healthcheck"]["test"]
        )

        self.assertIn("/apis/registry/v3/system/info", apicurio_healthcheck)
        self.assertIn("urllib.request", schema_registry_healthcheck)
        for registry in ("apicurio", "schema-registry"):
            for broker in ("kafka1", "kafka2", "kafka3"):
                self.assertEqual(
                    {"condition": "service_healthy"},
                    self.services[registry]["depends_on"][broker],
                )

    def test_keeps_a_repository_specific_network(self) -> None:
        self.assertEqual("kantrip-sandbox", self.compose["networks"]["default"]["name"])


if __name__ == "__main__":
    unittest.main()
