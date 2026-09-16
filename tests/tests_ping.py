import json
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.error import URLError

from confluent_kafka import KafkaError, KafkaException

from kantrip.ping import (
    PingError,
    PingResult,
    RegistryPingResult,
    _client_configuration,
    _has_connected_broker,
    ping_profile,
)
from kantrip.secret_store import secret_reference
from tests.pki import synthetic_pki


def _connected_admin(configuration: dict[str, object], **kwargs: object) -> Mock:
    del kwargs
    admin = Mock()
    admin.poll.side_effect = lambda timeout: configuration["stats_cb"](
        json.dumps(
            {
                "brokers": {
                    "bootstrap": {
                        "source": "configured",
                        "nodename": "broker-1:9092",
                        "nodeid": -1,
                        "state": "UP",
                    }
                }
            }
        )
    )
    return admin


class _Store:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get(self, reference: str) -> str:
        return self.values[reference]


class TestPing(unittest.TestCase):
    def test_profile_uses_polling_connection_state_without_resource_apis(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["broker-1:9092", "broker-2:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            }
        }

        with patch("kantrip.ping.AdminClient", side_effect=_connected_admin) as admin_client:
            result = ping_profile(profile, timeout=2.5)

        self.assertEqual(
            PingResult("plaintext reachable", "not configured", "reachability"),
            result,
        )
        configuration = admin_client.call_args.args[0]
        logger = admin_client.call_args.kwargs["logger"]
        self.assertEqual("broker-1:9092,broker-2:9092", configuration["bootstrap.servers"])
        self.assertEqual("PLAINTEXT", configuration["security.protocol"])
        self.assertEqual(100, configuration["statistics.interval.ms"])
        self.assertFalse(configuration["enable.sparse.connections"])
        self.assertTrue(logger.disabled)
        self.assertFalse(logger.propagate)

    def test_connection_state_accepts_bootstrap_minus_one_and_excludes_pseudo_brokers(self) -> None:
        self.assertTrue(
            _has_connected_broker(
                {
                    "brokers": {
                        "bootstrap": {
                            "source": "configured",
                            "nodename": "broker:9092",
                            "nodeid": -1,
                            "state": "UP",
                        }
                    }
                }
            )
        )
        cases = (
            ("internal", "broker:9092"),
            ("logical", "broker:9092"),
            ("configured", ""),
        )
        for source, nodename in cases:
            with self.subTest(source=source, nodename=nodename):
                self.assertFalse(
                    _has_connected_broker(
                        {
                            "brokers": {
                                "ignored": {
                                    "source": source,
                                    "nodename": nodename,
                                    "state": "UP",
                                }
                            }
                        }
                    )
                )

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
        ca_certificates = synthetic_pki().ca
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

    def test_authenticated_profile_resolves_and_proves_sasl(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "kafka/password")
        profile = {
            "id": profile_id,
            "kafka": {
                "bootstrapServers": ["broker.invalid:9093"],
                "transport": "tls",
                "auth": {
                    "type": "scram-sha-256",
                    "username": "synthetic-user",
                    "passwordRef": reference,
                },
                "tls": {},
            },
        }

        with patch("kantrip.ping.AdminClient", side_effect=_connected_admin) as admin_client:
            result = ping_profile(
                profile,
                timeout=1,
                secret_store=_Store({reference: "synthetic-password"}),
            )

        self.assertEqual("scram-sha-256 authenticated", result.kafka_authentication)
        configuration = admin_client.call_args.args[0]
        self.assertEqual("SASL_SSL", configuration["security.protocol"])
        self.assertEqual("SCRAM-SHA-256", configuration["sasl.mechanism"])
        self.assertEqual("synthetic-password", configuration["sasl.password"])

    def test_probe_classifies_authentication_failure(self) -> None:
        profile = {
            "kafka": {
                "bootstrapServers": ["unavailable:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            }
        }

        def failed_admin(configuration: dict[str, object], **kwargs: object) -> Mock:
            del kwargs
            configuration["error_cb"](
                KafkaError(KafkaError._AUTHENTICATION, "SASL authentication failed")
            )
            admin = Mock()
            admin.poll.side_effect = KafkaException(
                KafkaError(KafkaError._AUTHENTICATION, "SASL authentication failed")
            )
            return admin

        with (
            patch("kantrip.ping.AdminClient", side_effect=failed_admin),
            self.assertRaisesRegex(PingError, "authentication failed") as raised,
        ):
            ping_profile(profile)

        self.assertIn("authentication failed", str(raised.exception.detail).lower())

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
    raise AssertionError("the unavailable broker unexpectedly connected")
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

    def test_profile_checks_configured_confluent_registry(self) -> None:
        profile = self._registry_profile("confluent")
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'["orders-value"]'

        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping.urlopen", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(RegistryPingResult("confluent", "plaintext reachable"), result.registry)
        self.assertEqual(
            "http://registry.invalid:8081/subjects",
            open_registry.call_args.args[0].full_url,
        )
        self.assertLessEqual(open_registry.call_args.kwargs["timeout"], 1.25)

    def test_profile_checks_configured_apicurio_registry(self) -> None:
        profile = self._registry_profile("apicurio")
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"artifacts": [], "count": 7}'

        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping.urlopen", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(RegistryPingResult("apicurio", "plaintext reachable"), result.registry)
        self.assertEqual(
            "http://registry.invalid/apis/registry/v3/search/artifacts?limit=1",
            open_registry.call_args.args[0].full_url,
        )

    def test_profile_reports_registry_connectivity_failure(self) -> None:
        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping.urlopen", side_effect=URLError("connection refused")),
            self.assertRaisesRegex(PingError, "Confluent Schema Registry did not return") as raised,
        ):
            ping_profile(self._registry_profile("confluent"))

        self.assertEqual("connection refused", raised.exception.detail)

    def test_profile_rejects_invalid_apicurio_artifact_metadata(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"artifacts": [], "count": true}'
        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping.urlopen", return_value=response),
            self.assertRaisesRegex(PingError, "invalid artifact search response"),
        ):
            ping_profile(self._registry_profile("apicurio"))

    @staticmethod
    def _registry_profile(provider: str) -> dict[str, object]:
        registry = (
            {
                "provider": "apicurio",
                "apicurio.registry.url": "http://registry.invalid/apis/registry/v3/",
            }
            if provider == "apicurio"
            else {
                "provider": "confluent",
                "schema.registry.url": "http://registry.invalid:8081/",
            }
        )
        return {
            "kafka": {
                "bootstrapServers": ["localhost:9092"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": registry,
        }


if __name__ == "__main__":
    unittest.main()
