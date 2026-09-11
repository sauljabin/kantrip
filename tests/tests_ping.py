import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException

from kantrip.ping import PingError, PingResult, _client_configuration, ping_profile


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


if __name__ == "__main__":
    unittest.main()
