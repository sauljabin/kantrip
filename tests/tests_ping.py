import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
from urllib.error import URLError

from confluent_kafka import KafkaError, KafkaException

from kantrip.ping import (
    PingError,
    PingResult,
    RegistryPingResult,
    _client_configuration,
    ping_profile,
)
from kantrip.secret_store import secret_reference

CA_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "kafka-ca.pem"


class TestPing(unittest.TestCase):
    def test_profile_uses_admin_client_to_retrieve_cluster_metadata(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["broker-1:9092", "broker-2:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            }
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(
            brokers={1: object(), 2: object()},
            topics={"orders": object()},
        )

        with patch("kantrip.ping.AdminClient", return_value=admin) as admin_client:
            result = ping_profile(profile, timeout=2.5)

        self.assertEqual(PingResult(broker_count=2), result)
        configuration = admin_client.call_args.args[0]
        logger = admin_client.call_args.kwargs["logger"]
        self.assertEqual("broker-1:9092,broker-2:9092", configuration["bootstrap.servers"])
        self.assertEqual("PLAINTEXT", configuration["security.protocol"])
        self.assertEqual("kantrip-ping", configuration["client.id"])
        self.assertEqual(2500, configuration["socket.timeout.ms"])
        self.assertTrue(logger.disabled)
        self.assertFalse(logger.propagate)
        admin.list_topics.assert_called_once_with(timeout=2.5)

    def test_profile_does_not_map_arbitrary_client_properties(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
                "properties": {
                    "common": {"metadata.max.age.ms": 1000, "client.id": "ignored"},
                    "librdkafka": {"api.version.request": True},
                },
            }
        }

        configuration = _client_configuration(profile, 1.0)

        self.assertEqual("kantrip-ping", configuration["client.id"])
        self.assertNotIn("metadata.max.age.ms", configuration)
        self.assertNotIn("api.version.request", configuration)

    def test_tls_profile_enables_verification_and_uses_an_inline_ca(self) -> None:
        ca_certificates = CA_FIXTURE.read_text(encoding="utf-8")
        profile = {
            "kafka": {
                "bootstrapServers": ["broker.invalid:9093"],
                "transport": "tls",
                "auth": {"type": "none"},
                "tls": {"caCertificates": ca_certificates},
            }
        }

        configuration = _client_configuration(profile, 1.0)

        self.assertEqual("SSL", configuration["security.protocol"])
        self.assertEqual("true", configuration["enable.ssl.certificate.verification"])
        self.assertEqual("https", configuration["ssl.endpoint.identification.algorithm"])
        self.assertEqual(ca_certificates, configuration["ssl.ca.pem"])

    def test_authenticated_profile_is_rejected_before_client_creation(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        profile = {
            "id": profile_id,
            "kafka": {
                "bootstrapServers": ["broker.invalid:9093"],
                "transport": "tls",
                "auth": {
                    "type": "scram-sha-512",
                    "username": "synthetic-user",
                    "passwordRef": secret_reference(profile_id, "kafka/password"),
                },
            },
        }

        with (
            patch("kantrip.ping.AdminClient") as admin_client,
            self.assertRaisesRegex(PingError, "authenticated Kafka ping is not yet supported"),
        ):
            ping_profile(profile)

        admin_client.assert_not_called()

    def test_profile_wraps_kafka_errors_without_exposing_client_details(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["unavailable:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            }
        }
        admin = Mock()
        admin.list_topics.side_effect = KafkaException(KafkaError(KafkaError._TIMED_OUT))

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            self.assertRaisesRegex(PingError, "did not return metadata") as raised,
        ):
            ping_profile(profile)

        self.assertEqual("_TIMED_OUT: Local: Timed out", raised.exception.detail)

    def test_unreachable_kafka_does_not_write_native_logs_to_stderr(self) -> None:
        script = """
from kantrip.ping import PingError, ping_profile

profile = {
    "kafka": {
        "bootstrapServers": ["127.0.0.1:1"],
        "transport": "plaintext",
        "auth": {"type": "none"},
        "properties": {"librdkafka": {"debug": "broker"}},
    }
}
try:
    ping_profile(profile, timeout=0.1)
except PingError:
    pass
else:
    raise AssertionError("the unavailable broker unexpectedly returned metadata")
"""

        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("", result.stderr)

    def test_profile_checks_configured_confluent_registry_subjects(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": {
                "provider": "confluent",
                "schema.registry.url": "http://registry.invalid:8081/",
            },
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(brokers={1: object()})
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'["orders-value", "users-value"]'

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            patch("kantrip.ping.urlopen", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(PingResult(1, RegistryPingResult("confluent", 2)), result)
        request = open_registry.call_args.args[0]
        self.assertEqual("http://registry.invalid:8081/subjects", request.full_url)
        self.assertEqual(1.25, open_registry.call_args.kwargs["timeout"])

    def test_profile_checks_configured_apicurio_registry_artifacts(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": {
                "provider": "apicurio",
                "apicurio.registry.url": "http://registry.invalid/apis/registry/v3/",
            },
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(brokers={1: object()})
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"artifacts": [], "count": 7}'

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            patch("kantrip.ping.urlopen", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(PingResult(1, RegistryPingResult("apicurio", 7)), result)
        request = open_registry.call_args.args[0]
        self.assertEqual(
            "http://registry.invalid/apis/registry/v3/search/artifacts?limit=1",
            request.full_url,
        )

    def test_profile_reports_registry_connectivity_failure(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": {
                "provider": "confluent",
                "schema.registry.url": "http://registry.invalid:8081",
            },
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(brokers={1: object()})

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            patch("kantrip.ping.urlopen", side_effect=URLError("connection refused")),
            self.assertRaisesRegex(PingError, "Confluent Schema Registry did not return") as raised,
        ):
            ping_profile(profile)

        self.assertEqual("connection refused", raised.exception.detail)

    def test_profile_rejects_invalid_apicurio_artifact_metadata(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": {
                "provider": "apicurio",
                "apicurio.registry.url": "http://registry.invalid/apis/registry/v3",
            },
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(brokers={1: object()})
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"artifacts": [], "count": true}'

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            patch("kantrip.ping.urlopen", return_value=response),
            self.assertRaisesRegex(PingError, "invalid artifact search response"),
        ):
            ping_profile(profile)


if __name__ == "__main__":
    unittest.main()
