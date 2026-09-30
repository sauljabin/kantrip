import unittest
from pathlib import Path

from kantrip.oauth import OAuthConnection
from kantrip.registry import (
    RegistryConnection,
    RegistryProfileError,
    confluent_console_properties,
    display_registry,
    kaskade_registry_properties,
    registry_connection,
    resolve_registry_connection,
)
from kantrip.secret_store import secret_reference
from kantrip.secret_value import Secret
from tests.unit.pki import KEY_PASSWORD, synthetic_pki


class _Store:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get(self, reference: str) -> str:
        return self.values[reference]


_TOKEN_PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
_TOKEN_REFERENCE = secret_reference(_TOKEN_PROFILE_ID, "registry/token")
_SYNTHETIC_TOKEN = "synthetic-registry-token"


def _token_registry(provider: str, property_name: str, ca: str | None = None) -> dict[str, object]:
    registry: dict[str, object] = {
        "provider": provider,
        property_name: "https://registry.invalid",
        "auth": {"type": "token", "tokenRef": _TOKEN_REFERENCE},
    }
    if ca is not None:
        registry["tls"] = {"caCertificates": ca}
    return {"id": _TOKEN_PROFILE_ID, "registry": registry}


def _resolved_confluent_token(ca: str | None = None) -> RegistryConnection:
    parsed = registry_connection(_token_registry("confluent", "schema.registry.url", ca))
    assert parsed is not None
    return resolve_registry_connection(parsed, _Store({_TOKEN_REFERENCE: _SYNTHETIC_TOKEN}))


class TestRegistry(unittest.TestCase):
    def test_renders_a_fixed_token_for_confluent_java_consoles(self) -> None:
        connection = _resolved_confluent_token(synthetic_pki().ca)

        properties = confluent_console_properties(connection, ca_location=Path("registry-ca.pem"))

        self.assertEqual(
            {
                "schema.registry.url": "https://registry.invalid",
                "schema.registry.ssl.truststore.location": "registry-ca.pem",
                "schema.registry.ssl.truststore.type": "PEM",
                "schema.registry.bearer.auth.credentials.source": "STATIC_TOKEN",
                "schema.registry.bearer.auth.token": _SYNTHETIC_TOKEN,
            },
            properties,
        )

    def test_renders_a_fixed_token_for_confluent_librdkafka_registry_files(self) -> None:
        connection = _resolved_confluent_token(synthetic_pki().ca)

        properties = kaskade_registry_properties(connection, ca_location=Path("registry-ca.pem"))

        self.assertEqual(
            {
                "provider": "confluent",
                "url": "https://registry.invalid",
                "ssl.ca.location": "registry-ca.pem",
                "bearer.auth.credentials.source": "STATIC_TOKEN",
                "bearer.auth.token": _SYNTHETIC_TOKEN,
            },
            properties,
        )

    def test_rejects_an_unresolved_fixed_token(self) -> None:
        parsed = registry_connection(_token_registry("confluent", "schema.registry.url"))
        assert parsed is not None

        for render in (confluent_console_properties, kaskade_registry_properties):
            with (
                self.subTest(render=render.__name__),
                self.assertRaisesRegex(RegistryProfileError, "token is not resolved"),
            ):
                render(parsed)

    def test_rejects_apicurio_fixed_tokens(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "only for Confluent-compatible"):
            registry_connection(_token_registry("apicurio", "apicurio.registry.url"))

        connection = RegistryConnection(
            provider="apicurio",
            url="https://registry.invalid/apis/registry/v3",
            property_name="apicurio.registry.url",
            auth_type="token",
            token=Secret(_SYNTHETIC_TOKEN),
        )
        with self.assertRaisesRegex(RegistryProfileError, "Apicurio does not support fixed"):
            kaskade_registry_properties(connection)
        with self.assertRaisesRegex(RegistryProfileError, "require a Confluent Registry"):
            confluent_console_properties(connection)

    def test_renders_official_shared_apicurio_oauth_security(self) -> None:
        connection = RegistryConnection(
            provider="apicurio",
            url="https://registry.invalid/apis/registry/v3",
            property_name="apicurio.registry.url",
            auth_type="oauth",
            ca_certificates="registry-ca",
            oauth=OAuthConnection(
                token_url="https://idp.invalid/token",
                client_id="registry-client",
                scopes=("registry.read", "profile"),
                client_secret_reference="secret-reference",
                ca_certificates="registry-ca",
                client_secret=Secret("client-secret"),
            ),
        )

        properties = kaskade_registry_properties(
            connection,
            ca_location=Path("registry-ca.pem"),
        )

        self.assertEqual("registry-ca.pem", properties["apicurio.registry.tls.certificates"])
        self.assertEqual(
            "registry.read profile",
            properties["apicurio.registry.auth.client.scope"],
        )

    def test_rejects_apicurio_encrypted_client_key(self) -> None:
        connection = RegistryConnection(
            provider="apicurio",
            url="https://registry.invalid/apis/registry/v3",
            property_name="apicurio.registry.url",
            auth_type="mtls",
            client_certificate="certificate",
            private_key=Secret("private-key"),
            private_key_password=Secret("key-password"),
        )

        with self.assertRaisesRegex(RegistryProfileError, "encrypted PEM"):
            kaskade_registry_properties(
                connection,
                client_certificate_location=Path("registry-client.crt"),
                private_key_location=Path("registry-client.key"),
            )

    def test_resolves_confluent(self) -> None:
        connection = registry_connection(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": "http://registry.invalid:8081",
                }
            }
        )

        self.assertIsNotNone(connection)
        assert connection is not None
        self.assertEqual("confluent", connection.provider)
        self.assertEqual("schema.registry.url", connection.property_name)

    def test_rejects_a_missing_provider(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "registry.provider must"):
            registry_connection(
                {"registry": {"schema.registry.url": "http://registry.invalid:8081"}}
            )

    def test_resolves_native_apicurio(self) -> None:
        connection = registry_connection(
            {
                "registry": {
                    "provider": "apicurio",
                    "apicurio.registry.url": "http://registry.invalid/apis/registry/v3",
                }
            }
        )

        self.assertIsNotNone(connection)
        assert connection is not None
        self.assertEqual("apicurio", connection.provider)
        self.assertEqual("apicurio.registry.url", connection.property_name)

    def test_rejects_provider_specific_property_mismatches(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "incompatible with provider apicurio"):
            registry_connection(
                {
                    "registry": {
                        "provider": "apicurio",
                        "schema.registry.url": "http://registry.invalid:8081",
                    }
                }
            )

    def test_accepts_https_without_auth_and_rejects_embedded_metadata(self) -> None:
        secure = registry_connection(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": "https://registry.invalid",
                }
            }
        )
        self.assertIsNotNone(secure)
        for url in (
            "http://user:secret@registry.invalid",
            "http://registry.invalid?token=synthetic",
            "http://registry.invalid#fragment",
            "http://registry.invalid:70000",
        ):
            with self.subTest(url=url), self.assertRaises(RegistryProfileError):
                registry_connection(
                    {"registry": {"provider": "confluent", "schema.registry.url": url}}
                )

    def test_rejects_authenticated_http_registry(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "require an https"):
            registry_connection(
                {
                    "id": "018f8f13-7c21-7cee-8000-000000000010",
                    "registry": {
                        "provider": "confluent",
                        "schema.registry.url": "http://registry.invalid",
                        "auth": {
                            "type": "basic",
                            "username": "synthetic",
                            "passwordRef": (
                                "profile/018f8f13-7c21-7cee-8000-000000000010/018f8f13-7c21-7cee-8000-000000000011/registry/password"
                            ),
                        },
                    },
                }
            )

    def test_display_redacts_credentials_query_and_fragment(self) -> None:
        rendered = display_registry(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": (
                        "http://user:secret@registry.invalid:8081/path?token=synthetic#fragment"
                    ),
                }
            }
        )

        self.assertEqual("Confluent: http://registry.invalid:8081/path", rendered)

    def test_resolves_a_multiline_encrypted_mtls_private_key(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        key_reference = secret_reference(profile_id, "registry/tls/private-key")
        password_reference = secret_reference(profile_id, "registry/tls/private-key-password")
        pki = synthetic_pki()
        parsed = registry_connection(
            {
                "id": profile_id,
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": "https://registry.invalid",
                    "tls": {"clientCertificate": pki.client_certificate},
                    "auth": {
                        "type": "mtls",
                        "privateKeyRef": key_reference,
                        "privateKeyPasswordRef": password_reference,
                    },
                },
            }
        )
        assert parsed is not None

        resolved = resolve_registry_connection(
            parsed,
            _Store(
                {
                    key_reference: pki.encrypted_client_key,
                    password_reference: KEY_PASSWORD,
                }
            ),
        )

        self.assertEqual(Secret(pki.encrypted_client_key), resolved.private_key)
        self.assertEqual(Secret(KEY_PASSWORD), resolved.private_key_password)


if __name__ == "__main__":
    unittest.main()
