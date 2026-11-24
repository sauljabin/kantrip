import unittest
from unittest.mock import patch

from kantrip.adapters import KafkaNeeds
from tests.e2e.java_versions import java_refusal, refused


def _pins(apache: str, confluent: str) -> dict[str, str]:
    return {"APACHE_KAFKA_VERSION": apache, "CONFLUENT_VERSION": confluent}


class TestJavaRefusal(unittest.TestCase):
    """E2E expectations follow Kantrip's own Java floors for the pinned releases."""

    def refusal(
        self, apache: str, confluent: str, executable: str, needs: KafkaNeeds
    ) -> str | None:
        with patch(
            "tests.e2e.java_versions.pinned_versions", return_value=_pins(apache, confluent)
        ):
            return java_refusal(executable, needs)

    def test_floor_rows(self) -> None:
        plain, pem, oauth = (
            KafkaNeeds("none"),
            KafkaNeeds("scram-sha-512", custom_pem=True),
            KafkaNeeds("oauth", custom_pem=True),
        )
        cases = (
            ("2.6.3", "6.0.15", "kafka-topics.sh", plain, None),
            ("2.6.3", "6.0.15", "kafka-topics", plain, None),
            (
                "2.6.3",
                "6.0.15",
                "kafka-topics.sh",
                pem,
                "custom CA profiles require Apache Kafka 2.7",
            ),
            (
                "2.6.3",
                "6.0.15",
                "kafka-avro-console-consumer",
                plain,
                "install Confluent Platform 8.2 or newer",
            ),
            ("2.7.2", "6.1.15", "kafka-topics", pem, None),
            (
                "2.7.2",
                "6.1.15",
                "kafka-topics",
                oauth,
                "install Apache Kafka 4.1 or Confluent Platform 8.1",
            ),
            ("4.3.1", "8.2.5", "kafka-avro-console-consumer", plain, None),
            (
                "4.1.2",
                "8.1.7",
                "kafka-avro-console-consumer",
                plain,
                "install Confluent Platform 8.2 or newer",
            ),
            ("4.1.2", "8.1.7", "kafka-topics.sh", oauth, None),
        )
        for apache, confluent, executable, needs, expected in cases:
            with self.subTest(
                apache=apache, confluent=confluent, executable=executable, needs=needs
            ):
                refusal = self.refusal(apache, confluent, executable, needs)
                if expected is None:
                    self.assertIsNone(refusal)
                else:
                    assert refusal is not None
                    self.assertIn(expected, refusal)

    def test_refusal_text_survives_wrapping(self) -> None:
        refusal = "does not support PEM trust stores; custom CA profiles require Apache Kafka 2.7"
        output = "[failed] kafka-topics.sh 2.6.3 does not support PEM trust\nstores; custom CA profiles\nrequire Apache Kafka 2.7 or Confluent Platform 6.1 or newer"
        self.assertTrue(refused(output, refusal))
        self.assertFalse(refused("command 'kafka-topics.sh' was not found", refusal))
