import json
import ssl
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.error import HTTPError, URLError

from confluent_kafka import KafkaError, KafkaException

from kantrip.ping import (
    PingError,
    PingResult,
    RegistryPingResult,
    _client_configuration,
    _has_connected_broker,
    _NoRedirect,
    ping_profile,
)
from kantrip.secret_store import secret_reference
from tests.unit.pki import synthetic_pki


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
            patch("kantrip.ping._open_request", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(
            RegistryPingResult(
                "confluent",
                "plaintext reachable",
                "read query validated",
            ),
            result.registry,
        )
        self.assertEqual(
            "http://registry.invalid:8081/subjects?limit=1",
            open_registry.call_args.args[0].full_url,
        )
        self.assertLessEqual(open_registry.call_args.args[1], 1.25)

    def test_confluent_empty_subject_result_is_valid(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"[]"

        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping._open_request", return_value=response),
        ):
            result = ping_profile(self._registry_profile("confluent"), timeout=1)

        self.assertEqual("read query validated", result.registry.proof)

    def test_authenticated_registry_rejects_a_public_probe_endpoint(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "registry/password")
        profile = self._registry_profile("confluent")
        profile["id"] = profile_id
        registry = profile["registry"]
        assert isinstance(registry, dict)
        registry["schema.registry.url"] = "https://registry.invalid"
        registry["auth"] = {
            "type": "basic",
            "username": "synthetic-user",
            "passwordRef": reference,
        }
        authenticated = MagicMock()
        authenticated.__enter__.return_value.read.return_value = b'["orders-value"]'
        public = MagicMock()
        public.__enter__.return_value.read.return_value = b'["orders-value"]'

        with (
            patch("kantrip.ping._probe_kafka"),
            patch(
                "kantrip.ping._open_request",
                side_effect=(authenticated, public),
            ) as open_registry,
            self.assertRaisesRegex(PingError, "endpoint is public"),
        ):
            ping_profile(
                profile,
                timeout=1,
                secret_store=_Store({reference: "synthetic-password"}),
            )

        request = open_registry.call_args_list[0].args[0]
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))
        anonymous_request = open_registry.call_args_list[1].args[0]
        self.assertIsNone(anonymous_request.get_header("Authorization"))

    def test_profile_checks_configured_apicurio_registry(self) -> None:
        profile = self._registry_profile("apicurio")
        response = MagicMock()
        response.__enter__.return_value.read.return_value = (
            b'{"count":1,"versions":[{"groupId":"default","artifactId":"orders"}]}'
        )

        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping._open_request", return_value=response) as open_registry,
        ):
            result = ping_profile(profile, timeout=1.25)

        self.assertEqual(
            RegistryPingResult(
                "apicurio",
                "plaintext reachable",
                "read query validated",
            ),
            result.registry,
        )
        self.assertEqual(
            "http://registry.invalid/apis/registry/v3/search/versions?limit=1",
            open_registry.call_args.args[0].full_url,
        )

    def test_apicurio_empty_version_result_is_valid(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"count":0,"versions":[]}'

        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping._open_request", return_value=response),
        ):
            result = ping_profile(self._registry_profile("apicurio"), timeout=1)

        self.assertEqual("read query validated", result.registry.proof)

    def test_apicurio_basic_requires_read_query_and_anonymous_rejection(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "registry/password")
        profile = self._registry_profile("apicurio")
        profile["id"] = profile_id
        registry = profile["registry"]
        assert isinstance(registry, dict)
        registry["apicurio.registry.url"] = "https://registry.invalid/apis/registry/v3"
        registry["auth"] = {
            "type": "basic",
            "username": "synthetic-user",
            "passwordRef": reference,
        }
        readable = MagicMock()
        readable.__enter__.return_value.read.return_value = b'{"count":0,"versions":[]}'
        anonymous = HTTPError(
            "https://registry.invalid/apis/registry/v3/search/versions?limit=1",
            403,
            "Forbidden",
            None,
            None,
        )

        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping._open_request", side_effect=(readable, anonymous)) as opened,
        ):
            result = ping_profile(
                profile,
                timeout=1,
                secret_store=_Store({reference: "synthetic-password"}),
            )

        self.assertEqual("basic authenticated read query validated", result.registry.proof)
        self.assertTrue(
            opened.call_args_list[0].args[0].get_header("Authorization").startswith("Basic ")
        )
        self.assertEqual(2, opened.call_count)
        self.assertIsNone(opened.call_args_list[1].args[0].get_header("Authorization"))

    def test_synthetic_proxy_allows_only_selected_registry_probe_routes(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "registry/password")
        cases = (
            (
                "confluent",
                "/subjects?limit=1",
                b"[]",
            ),
            (
                "apicurio",
                "/apis/registry/v3/search/versions?limit=1",
                b'{"count":0,"versions":[]}',
            ),
        )
        for provider, allowed_path, payload in cases:
            with self.subTest(provider=provider):
                profile = self._registry_profile(provider)
                profile["id"] = profile_id
                registry = profile["registry"]
                assert isinstance(registry, dict)
                url_field = (
                    "apicurio.registry.url" if provider == "apicurio" else "schema.registry.url"
                )
                registry[url_field] = (
                    "https://registry.invalid/apis/registry/v3"
                    if provider == "apicurio"
                    else "https://registry.invalid"
                )
                registry["auth"] = {
                    "type": "basic",
                    "username": "synthetic-readonly",
                    "passwordRef": reference,
                }
                seen: list[str] = []

                def synthetic_proxy(
                    request: object,
                    *_args: object,
                    _seen: list[str] = seen,
                    _allowed_path: str = allowed_path,
                    _payload: bytes = payload,
                    **_kwargs: object,
                ) -> object:
                    full_url = request.full_url
                    path = full_url.removeprefix("https://registry.invalid")
                    _seen.append(path)
                    if path != _allowed_path:
                        raise HTTPError(full_url, 404, "blocked by synthetic proxy", None, None)
                    if request.get_header("Authorization") is None:
                        raise HTTPError(full_url, 403, "Forbidden", None, None)
                    response = MagicMock()
                    response.__enter__.return_value.read.return_value = _payload
                    return response

                with (
                    patch("kantrip.ping._probe_kafka"),
                    patch("kantrip.ping._open_request", side_effect=synthetic_proxy),
                ):
                    result = ping_profile(
                        profile,
                        timeout=1,
                        secret_store=_Store({reference: "synthetic-password"}),
                    )

                self.assertEqual("basic authenticated read query validated", result.registry.proof)
                self.assertEqual([allowed_path, allowed_path], seen)

    def test_registry_oauth_uses_bounded_token_then_proves_registry_gate(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "registry/oauth/client-secret")
        profile = self._registry_profile("confluent")
        profile["id"] = profile_id
        registry = profile["registry"]
        assert isinstance(registry, dict)
        registry["schema.registry.url"] = "https://registry.invalid"
        registry["auth"] = {
            "type": "oauth",
            "tokenUrl": "https://idp.invalid/oauth/token",
            "clientId": "registry-client",
            "scopes": ["registry.read"],
            "clientSecretRef": reference,
        }
        token = MagicMock()
        token.__enter__.return_value.read.return_value = (
            b'{"access_token":"short-lived-token","token_type":"Bearer","expires_in":60}'
        )
        authenticated = MagicMock()
        authenticated.__enter__.return_value.read.return_value = b'["orders-value"]'
        anonymous = HTTPError(
            "https://registry.invalid/subjects?limit=1", 401, "Unauthorized", None, None
        )

        with (
            patch("kantrip.ping._probe_kafka"),
            patch(
                "kantrip.ping._open_request",
                side_effect=(token, authenticated, anonymous),
            ) as opened,
        ):
            result = ping_profile(
                profile,
                timeout=1,
                secret_store=_Store({reference: "synthetic-client-secret"}),
            )

        token_request = opened.call_args_list[0].args[0]
        self.assertEqual("POST", token_request.method)
        self.assertEqual(b"grant_type=client_credentials&scope=registry.read", token_request.data)
        registry_request = opened.call_args_list[1].args[0]
        self.assertEqual("Bearer short-lived-token", registry_request.get_header("Authorization"))
        self.assertEqual("oauth authenticated read query validated", result.registry.proof)

    def test_registry_oauth_rejects_invalid_token_without_leaking_secret(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        reference = secret_reference(profile_id, "registry/oauth/client-secret")
        profile = self._registry_profile("confluent")
        profile["id"] = profile_id
        registry = profile["registry"]
        assert isinstance(registry, dict)
        registry["schema.registry.url"] = "https://registry.invalid"
        registry["auth"] = {
            "type": "oauth",
            "tokenUrl": "https://idp.invalid/oauth/token",
            "clientId": "registry-client",
            "scopes": [],
            "clientSecretRef": reference,
        }
        response = MagicMock()
        response.__enter__.return_value.read.return_value = (
            b'{"access_token":"bad","token_type":"Bearer","expires_in":0}'
        )

        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping._open_request", return_value=response),
            self.assertRaisesRegex(PingError, "token response is invalid") as raised,
        ):
            ping_profile(
                profile,
                timeout=1,
                secret_store=_Store({reference: "never-print-this-secret"}),
            )

        self.assertNotIn("never-print-this-secret", str(raised.exception))

    def test_registry_mtls_requires_client_exchange_and_anonymous_rejection(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        key_reference = secret_reference(profile_id, "registry/tls/private-key")
        pki = synthetic_pki()
        profile = self._registry_profile("confluent")
        profile["id"] = profile_id
        registry = profile["registry"]
        assert isinstance(registry, dict)
        registry["schema.registry.url"] = "https://registry.invalid"
        registry["tls"] = {
            "caCertificates": pki.ca,
            "clientCertificate": pki.client_certificate,
        }
        registry["auth"] = {
            "type": "mtls",
            "privateKeyRef": key_reference,
        }
        authenticated = MagicMock()
        authenticated.__enter__.return_value.read.return_value = b'["orders-value"]'
        for rejected in (
            URLError(ssl.SSLError("peer did not return a certificate")),
            ssl.SSLError("certificate required"),
        ):
            with (
                self.subTest(rejected=type(rejected).__name__),
                patch("kantrip.ping._probe_kafka"),
                patch("kantrip.ping._open_request", side_effect=(authenticated, rejected)),
            ):
                result = ping_profile(
                    profile,
                    timeout=1,
                    secret_store=_Store({key_reference: pki.client_key}),
                )

            self.assertEqual(
                "mTLS read query and anonymous rejection validated",
                result.registry.proof,
            )

    def test_registry_rejects_html_and_distinguishes_401_from_403(self) -> None:
        profile = self._registry_profile("confluent")
        html = MagicMock()
        html.__enter__.return_value.headers.get_content_type.return_value = "text/html"
        html.__enter__.return_value.read.return_value = b"<html>login</html>"
        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping._open_request", return_value=html),
            self.assertRaisesRegex(PingError, "non-JSON"),
        ):
            ping_profile(profile, timeout=1)

        for status, message in (
            (401, "authentication"),
            (403, "authorization"),
            (404, "did not return registry metadata"),
        ):
            error = HTTPError(
                "http://registry.invalid:8081/subjects?limit=1",
                status,
                "rejected",
                None,
                None,
            )
            with (
                self.subTest(status=status),
                patch("kantrip.ping._probe_kafka"),
                patch("kantrip.ping._open_request", side_effect=error),
                self.assertRaisesRegex(PingError, message),
            ):
                ping_profile(profile, timeout=1)

    def test_registry_redirect_handler_never_forwards_a_request(self) -> None:
        self.assertIsNone(
            _NoRedirect().redirect_request(
                MagicMock(), MagicMock(), 302, "Found", {}, "https://other.invalid"
            )
        )

    def test_profile_reports_registry_connectivity_failure(self) -> None:
        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping._open_request", side_effect=URLError("connection refused")),
            self.assertRaisesRegex(PingError, "Confluent Schema Registry did not return") as raised,
        ):
            ping_profile(self._registry_profile("confluent"))

        self.assertEqual("connection refused", raised.exception.detail)

    def test_profile_converts_exhausted_kafka_to_registry_deadline(self) -> None:
        profile = self._registry_profile("confluent")

        with (
            patch("kantrip.ping._probe_kafka"),
            patch("kantrip.ping.time.monotonic", side_effect=(0.0, 6.0)),
            self.assertRaisesRegex(
                PingError,
                "Confluent Schema Registry did not return registry metadata",
            ) as raised,
        ):
            ping_profile(profile, timeout=5)

        self.assertEqual("network deadline exhausted", raised.exception.detail)

    def test_profile_rejects_invalid_apicurio_version_search_metadata(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"name": "Apicurio"}'
        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping._open_request", return_value=response),
            self.assertRaisesRegex(PingError, "invalid version-search metadata"),
        ):
            ping_profile(self._registry_profile("apicurio"))

    def test_profile_rejects_invalid_confluent_subject_search_metadata(self) -> None:
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"[1]"
        with (
            patch("kantrip.ping.AdminClient", side_effect=_connected_admin),
            patch("kantrip.ping._open_request", return_value=response),
            self.assertRaisesRegex(PingError, "invalid subject-search metadata"),
        ):
            ping_profile(self._registry_profile("confluent"))

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
