import os
import tempfile
import unittest
from pathlib import Path

from kantrip.kafka import (
    MAX_CA_BUNDLE_BYTES,
    KafkaConnection,
    KafkaProfileError,
    java_properties,
    kafka_connection,
    librdkafka_properties,
    read_ca_bundle,
    validate_ca_bundle,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CA_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "kafka-ca.pem"


class TestKafkaConnection(unittest.TestCase):
    def test_reads_and_validates_a_bounded_pem_ca_bundle(self) -> None:
        contents = read_ca_bundle(CA_FIXTURE)

        self.assertTrue(contents.startswith("-----BEGIN CERTIFICATE-----\n"))
        self.assertTrue(contents.endswith("-----END CERTIFICATE-----\n"))
        self.assertEqual(contents, validate_ca_bundle(contents))

    def test_rejects_invalid_and_oversized_ca_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invalid = Path(directory) / "invalid.pem"
            invalid.write_text("not a certificate\n", encoding="utf-8")
            oversized = Path(directory) / "oversized.pem"
            oversized.write_bytes(b"x" * (MAX_CA_BUNDLE_BYTES + 1))
            fifo = Path(directory) / "ca.fifo"
            os.mkfifo(fifo)

            with self.assertRaisesRegex(KafkaProfileError, "only PEM certificates"):
                read_ca_bundle(invalid)
            with self.assertRaisesRegex(KafkaProfileError, "1 MiB"):
                read_ca_bundle(oversized)
            with self.assertRaisesRegex(KafkaProfileError, "regular file"):
                read_ca_bundle(fifo)

    def test_parses_plaintext_and_tls_profiles_without_passthrough_properties(self) -> None:
        plaintext = kafka_connection(_profile("plaintext"))
        tls = kafka_connection(_profile("tls"))

        self.assertEqual(KafkaConnection(("broker.invalid:9092",), "plaintext"), plaintext)
        self.assertEqual(KafkaConnection(("broker.invalid:9092",), "tls"), tls)

    def test_renders_tls_verification_for_java_and_librdkafka(self) -> None:
        ca = read_ca_bundle(CA_FIXTURE)
        connection = KafkaConnection(("broker.invalid:9093",), "tls", ca)
        ca_path = Path("/private/session/kafka-ca.pem")

        java = java_properties(connection, ca_location=ca_path)
        librdkafka = librdkafka_properties(connection, ca_location=ca_path)
        inline = librdkafka_properties(connection, inline_ca=True)

        self.assertEqual("SSL", java["security.protocol"])
        self.assertEqual("https", java["ssl.endpoint.identification.algorithm"])
        self.assertEqual("PEM", java["ssl.truststore.type"])
        self.assertEqual(str(ca_path), java["ssl.truststore.location"])
        self.assertEqual("SSL", librdkafka["security.protocol"])
        self.assertEqual("true", librdkafka["enable.ssl.certificate.verification"])
        self.assertEqual("https", librdkafka["ssl.endpoint.identification.algorithm"])
        self.assertEqual(str(ca_path), librdkafka["ssl.ca.location"])
        self.assertEqual(ca, inline["ssl.ca.pem"])

    def test_custom_ca_requires_a_private_file_or_explicit_inline_rendering(self) -> None:
        connection = KafkaConnection(
            ("broker.invalid:9093",),
            "tls",
            read_ca_bundle(CA_FIXTURE),
        )

        with self.assertRaisesRegex(KafkaProfileError, "private session file"):
            java_properties(connection)
        with self.assertRaisesRegex(KafkaProfileError, "private session file"):
            librdkafka_properties(connection)


def _profile(transport: str) -> dict:
    return {
        "kafka": {
            "bootstrapServers": ["broker.invalid:9092"],
            "transport": transport,
            "auth": {"type": "none"},
        }
    }


if __name__ == "__main__":
    unittest.main()
