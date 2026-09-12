import unittest
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
        self.assertEqual("broker-1:9092,broker-2:9092", configuration["bootstrap.servers"])
        self.assertEqual("PLAINTEXT", configuration["security.protocol"])
        self.assertEqual("kantrip-ping", configuration["client.id"])
        self.assertEqual(2500, configuration["socket.timeout.ms"])
        admin.list_topics.assert_called_once_with(timeout=2.5)

    def test_profile_maps_common_and_librdkafka_properties(self) -> None:
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

        self.assertEqual(1000, configuration["metadata.max.age.ms"])
        self.assertTrue(configuration["api.version.request"])
        self.assertEqual("kantrip-ping", configuration["client.id"])

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
            self.assertRaisesRegex(PingError, "did not return metadata"),
        ):
            ping_profile(profile)

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
                "schema.registry.url": "http://registry.invalid:8081",
            },
        }
        admin = Mock()
        admin.list_topics.return_value = SimpleNamespace(brokers={1: object()})

        with (
            patch("kantrip.ping.AdminClient", return_value=admin),
            patch("kantrip.ping.urlopen", side_effect=URLError("unavailable")),
            self.assertRaisesRegex(PingError, "Confluent Schema Registry did not return"),
        ):
            ping_profile(profile)

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
