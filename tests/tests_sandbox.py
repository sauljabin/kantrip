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
        self.assertEqual(
            "confluentinc/cp-kafka:${CONFLUENT_VERSION}", self.services["kafka1"]["image"]
        )

    def test_contains_only_the_supported_plaintext_kafka_cluster(self) -> None:
        self.assertEqual({"kafka1", "kafka2", "kafka3"}, set(self.services))
        for service in self.services.values():
            protocols = service["environment"]["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]
            self.assertNotIn("SSL", protocols)
            self.assertNotIn("SASL", protocols)

    def test_keeps_a_repository_specific_network(self) -> None:
        self.assertEqual("kantrip-sandbox", self.compose["networks"]["default"]["name"])


if __name__ == "__main__":
    unittest.main()
