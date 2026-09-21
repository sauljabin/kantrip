import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from kantrip.kafka import (
    MAX_CA_BUNDLE_BYTES,
    MAX_CLIENT_PEM_BYTES,
    KafkaConnection,
    KafkaProfileError,
    java_properties,
    kafka_connection,
    librdkafka_properties,
    read_ca_bundle,
    read_client_certificate,
    read_private_key,
    resolve_kafka_connection,
    validate_ca_bundle,
    validate_client_identity,
)
from kantrip.secret_store import SecretNotFoundError, secret_reference
from tests.pki import synthetic_pki, temporary_pki_files

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestKafkaConnection(unittest.TestCase):
    def test_reads_and_validates_a_bounded_pem_ca_bundle(self) -> None:
        with temporary_pki_files(ca=synthetic_pki().ca) as paths:
            contents = read_ca_bundle(paths["ca"])

        self.assertTrue(contents.startswith("-----BEGIN CERTIFICATE-----\n"))
        self.assertTrue(contents.endswith("-----END CERTIFICATE-----\n"))
        self.assertEqual(contents, validate_ca_bundle(contents))

    def test_rejects_invalid_and_oversized_ca_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            invalid = Path(directory) / "invalid.pem"
            invalid.write_text("not a certificate\n", encoding="utf-8")
            oversized = Path(directory) / "oversized.pem"
            oversized.write_bytes(b"x" * (MAX_CLIENT_PEM_BYTES + 1))
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
        ca = synthetic_pki().ca
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
            synthetic_pki().ca,
        )

        with self.assertRaisesRegex(KafkaProfileError, "private session file"):
            java_properties(connection)
        with self.assertRaisesRegex(KafkaProfileError, "private session file"):
            librdkafka_properties(connection)

    def test_resolves_and_renders_plain_and_scram_credentials(self) -> None:
        for auth_type, mechanism in (
            ("plain", "PLAIN"),
            ("scram-sha-256", "SCRAM-SHA-256"),
            ("scram-sha-512", "SCRAM-SHA-512"),
        ):
            with self.subTest(auth_type=auth_type):
                profile = _authenticated_profile(auth_type)
                reference = profile["kafka"]["auth"]["passwordRef"]
                connection = resolve_kafka_connection(
                    kafka_connection(profile),
                    _MemorySecretStore({reference: 'synthetic\\password"'}),
                )

                java = java_properties(connection)
                librdkafka = librdkafka_properties(connection)

                self.assertEqual("SASL_SSL", java["security.protocol"])
                self.assertEqual(mechanism, java["sasl.mechanism"])
                self.assertIn('username="synthetic-user"', java["sasl.jaas.config"])
                self.assertIn('password="synthetic\\\\password\\""', java["sasl.jaas.config"])
                self.assertEqual("SASL_SSL", librdkafka["security.protocol"])
                self.assertEqual(mechanism, librdkafka["sasl.mechanism"])
                self.assertEqual("synthetic-user", librdkafka["sasl.username"])
                self.assertEqual('synthetic\\password"', librdkafka["sasl.password"])

    def test_resolves_and_renders_native_oauth_with_independent_trust(self) -> None:
        profile = _authenticated_profile("oauth")
        reference = profile["kafka"]["auth"]["clientSecretRef"]
        connection = resolve_kafka_connection(
            kafka_connection(profile),
            _MemorySecretStore({reference: "synthetic-oauth-secret"}),
        )
        oauth_ca_path = Path("/private/session/kafka-oauth-ca.pem")

        java = java_properties(connection, oauth_ca_location=oauth_ca_path)
        librdkafka = librdkafka_properties(connection, oauth_ca_location=oauth_ca_path)

        assert connection.oauth is not None
        self.assertEqual(("openid", "profile"), connection.oauth.scopes)
        self.assertEqual("SASL_SSL", java["security.protocol"])
        self.assertEqual("OAUTHBEARER", java["sasl.mechanism"])
        self.assertEqual(
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            java["sasl.login.callback.handler.class"],
        )
        self.assertEqual(
            "synthetic-oauth-secret",
            java["sasl.oauthbearer.client.credentials.client.secret"],
        )
        self.assertIn(str(oauth_ca_path), java["sasl.jaas.config"])
        self.assertEqual("oidc", librdkafka["sasl.oauthbearer.method"])
        self.assertEqual("openid profile", librdkafka["sasl.oauthbearer.scope"])
        self.assertEqual(str(oauth_ca_path), librdkafka["https.ca.location"])
        self.assertNotIn("ssl.ca.location", librdkafka)

    def test_oauth_rejects_unsafe_endpoint_and_foreign_secret_reference(self) -> None:
        profile = _authenticated_profile("oauth")
        profile["kafka"]["auth"]["tokenUrl"] = "https://idp.invalid/token?secret=value"
        with self.assertRaisesRegex(KafkaProfileError, "query"):
            kafka_connection(profile)

        profile = _authenticated_profile("oauth")
        profile["kafka"]["auth"]["clientSecretRef"] = secret_reference(
            "018f8f13-7c21-7cee-8000-000000000099",
            "kafka/oauth/client-secret",
        )
        with self.assertRaisesRegex(KafkaProfileError, "does not match"):
            kafka_connection(profile)

    def test_password_authentication_rejects_plaintext_and_foreign_references(self) -> None:
        profile = _authenticated_profile("plain")
        profile["kafka"]["transport"] = "plaintext"
        with self.assertRaisesRegex(KafkaProfileError, "requires TLS"):
            kafka_connection(profile)

        profile["kafka"]["transport"] = "tls"
        profile["kafka"]["auth"]["passwordRef"] = secret_reference(
            "018f8f13-7c21-7cee-8000-000000000099",
            "kafka/password",
        )
        with self.assertRaisesRegex(KafkaProfileError, "does not match"):
            kafka_connection(profile)

        unsafe = KafkaConnection(
            ("broker.invalid:9092",),
            "plaintext",
            auth_type="plain",
            username="synthetic-user",
            password="synthetic-password",
        )
        for renderer in (java_properties, librdkafka_properties):
            with (
                self.subTest(renderer=renderer.__name__),
                self.assertRaisesRegex(KafkaProfileError, "requires TLS"),
            ):
                renderer(unsafe)

    def test_validates_resolves_and_renders_mtls_identity(self) -> None:
        certificate, private_key = _client_identity()
        profile = _authenticated_profile("mtls")
        reference = profile["kafka"]["auth"]["privateKeyRef"]
        profile["kafka"]["auth"]["clientCertificate"] = certificate
        connection = resolve_kafka_connection(
            kafka_connection(profile),
            _MemorySecretStore({reference: private_key}),
        )

        java = java_properties(connection)
        inline = librdkafka_properties(connection, inline_client=True)
        files = librdkafka_properties(
            connection,
            client_certificate_location=Path("/private/session/client.crt"),
            private_key_location=Path("/private/session/client.key"),
        )

        self.assertEqual("SSL", java["security.protocol"])
        self.assertEqual("PEM", java["ssl.keystore.type"])
        self.assertEqual(certificate, java["ssl.keystore.certificate.chain"])
        self.assertEqual(private_key, java["ssl.keystore.key"])
        self.assertEqual(certificate, inline["ssl.certificate.pem"])
        self.assertEqual(private_key, inline["ssl.key.pem"])
        self.assertEqual("/private/session/client.crt", files["ssl.certificate.location"])
        self.assertEqual("/private/session/client.key", files["ssl.key.location"])

    def test_rejects_mismatched_mtls_identity_and_missing_secret(self) -> None:
        certificate, _ = _client_identity()
        _, other_key = _client_identity()
        with self.assertRaisesRegex(KafkaProfileError, "does not match"):
            validate_client_identity(certificate, other_key)

        profile = _authenticated_profile("plain")
        with self.assertRaisesRegex(KafkaProfileError, "could not be resolved"):
            resolve_kafka_connection(kafka_connection(profile), _MemorySecretStore({}))

    def test_reads_bounded_client_identity_files(self) -> None:
        certificate, private_key = _client_identity()
        with tempfile.TemporaryDirectory() as directory:
            certificate_path = Path(directory) / "client.crt"
            certificate_path.write_text(certificate, encoding="utf-8")
            key_path = Path(directory) / "client.key"
            key_path.write_text(private_key, encoding="utf-8")
            oversized = Path(directory) / "oversized.key"
            oversized.write_bytes(b"x" * (MAX_CA_BUNDLE_BYTES + 1))

            self.assertEqual(certificate, read_client_certificate(certificate_path))
            self.assertEqual(private_key, read_private_key(key_path))
            with self.assertRaisesRegex(KafkaProfileError, "1 MiB"):
                read_private_key(oversized)
            key_path.write_text(private_key + "unexpected\n", encoding="utf-8")
            with self.assertRaisesRegex(KafkaProfileError, "only one PEM key"):
                read_private_key(key_path)


def _profile(transport: str) -> dict:
    return {
        "kafka": {
            "bootstrapServers": ["broker.invalid:9092"],
            "transport": transport,
            "auth": {"type": "none"},
        }
    }


def _authenticated_profile(auth_type: str) -> dict:
    profile_id = "018f8f13-7c21-7cee-8000-000000000010"
    if auth_type == "mtls":
        auth = {
            "type": "mtls",
            "clientCertificate": synthetic_pki().client_certificate,
            "privateKeyRef": secret_reference(profile_id, "kafka/tls/private-key"),
        }
    elif auth_type == "oauth":
        auth = {
            "type": "oauth",
            "tokenUrl": "https://idp.invalid/oauth/token",
            "clientId": "synthetic-client",
            "scopes": ["openid", "profile"],
            "clientSecretRef": secret_reference(profile_id, "kafka/oauth/client-secret"),
            "caCertificates": synthetic_pki().ca,
        }
    else:
        auth = {
            "type": auth_type,
            "username": "synthetic-user",
            "passwordRef": secret_reference(profile_id, "kafka/password"),
        }
    return {
        "id": profile_id,
        "kafka": {
            "bootstrapServers": ["broker.invalid:9093"],
            "transport": "tls",
            "auth": auth,
        },
    }


def _client_identity() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic-client")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    certificate_pem = certificate.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("utf-8")
    return certificate_pem, key_pem


class _MemorySecretStore:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get(self, reference: str) -> str:
        try:
            return self.values[reference]
        except KeyError as error:
            raise SecretNotFoundError("synthetic missing value") from error

    def set(self, reference: str, value: str) -> None:
        self.values[reference] = value

    def delete(self, reference: str) -> None:
        self.values.pop(reference, None)


if __name__ == "__main__":
    unittest.main()
