"""Validate Kafka profile transport and render canonical client properties."""

from __future__ import annotations

import os
import re
import ssl
import stat
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from kantrip.secret_store import SecretStore, SecretStoreError, parse_secret_reference

KafkaTransport = Literal["plaintext", "tls"]
KafkaAuthType = Literal["none", "plain", "scram-sha-256", "scram-sha-512", "mtls"]
MAX_CA_BUNDLE_BYTES = 1024 * 1024
MAX_CLIENT_PEM_BYTES = 1024 * 1024
CA_BUNDLE_FILENAME = "kafka-ca.pem"
CLIENT_CERTIFICATE_FILENAME = "kafka-client.crt"
CLIENT_KEY_FILENAME = "kafka-client.key"
_CERTIFICATE_PATTERN = re.compile(
    r"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+?" r"-----END CERTIFICATE-----"
)
_PRIVATE_KEY_PATTERN = re.compile(
    r"-----BEGIN (?P<label>(?:ENCRYPTED )?PRIVATE KEY|RSA PRIVATE KEY|EC PRIVATE KEY|"
    r"DSA PRIVATE KEY)-----\s+[A-Za-z0-9+/=\s]+?-----END (?P=label)-----\s*\Z"
)


class KafkaProfileError(ValueError):
    """Raised when Kafka connection metadata is unsafe or unsupported."""


@dataclass(frozen=True)
class KafkaConnection:
    """Validated provider-neutral Kafka connection metadata."""

    bootstrap_servers: tuple[str, ...]
    transport: KafkaTransport
    ca_certificates: str | None = None
    auth_type: KafkaAuthType = "none"
    username: str | None = None
    password_reference: str | None = None
    client_certificate: str | None = None
    private_key_reference: str | None = None
    private_key_password_reference: str | None = None
    password: str | None = None
    private_key: str | None = None
    private_key_password: str | None = None

    @property
    def requires_secrets(self) -> bool:
        """Return whether this connection must be resolved through a secret store."""
        return self.auth_type != "none"


def read_ca_bundle(path: Path) -> str:
    """Read and validate one bounded PEM CA bundle from a regular file."""
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise KafkaProfileError("Kafka CA source must be a regular file")
            if metadata.st_size > MAX_CA_BUNDLE_BYTES:
                raise KafkaProfileError("Kafka CA bundle exceeds the 1 MiB limit")
            contents = source.read(MAX_CA_BUNDLE_BYTES + 1)
    except KafkaProfileError:
        raise
    except OSError as error:
        raise KafkaProfileError("Kafka CA bundle could not be read") from error
    if len(contents) > MAX_CA_BUNDLE_BYTES:
        raise KafkaProfileError("Kafka CA bundle exceeds the 1 MiB limit")
    try:
        text = contents.decode("utf-8")
    except UnicodeDecodeError as error:
        raise KafkaProfileError("Kafka CA bundle must be UTF-8 PEM text") from error
    return validate_ca_bundle(text)


def read_client_certificate(path: Path) -> str:
    """Read and validate a bounded public PEM client certificate chain."""
    return validate_client_certificate(
        _read_bounded_pem(path, MAX_CLIENT_PEM_BYTES, "Kafka client certificate")
    )


def read_private_key(path: Path, *, password: str | None = None) -> str:
    """Read and validate a bounded PEM private key without exposing its value."""
    contents = _read_bounded_pem(path, MAX_CLIENT_PEM_BYTES, "Kafka client private key")
    validate_private_key(contents, password=password)
    return contents


def validate_ca_bundle(contents: str) -> str:
    """Return a normalized PEM certificate bundle or reject it safely."""
    if not isinstance(contents, str):
        raise KafkaProfileError("Kafka CA bundle must be PEM text")
    encoded = contents.encode("utf-8")
    if not encoded or len(encoded) > MAX_CA_BUNDLE_BYTES:
        raise KafkaProfileError("Kafka CA bundle must contain at most 1 MiB of PEM text")
    normalized = contents.replace("\r\n", "\n").strip() + "\n"
    matches = tuple(_CERTIFICATE_PATTERN.finditer(normalized))
    if not matches or not _contains_only_certificates(normalized, matches):
        raise KafkaProfileError("Kafka CA bundle must contain only PEM certificates")
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=normalized)
    except ssl.SSLError as error:
        raise KafkaProfileError("Kafka CA bundle contains an invalid certificate") from error
    return normalized


def validate_client_certificate(contents: str) -> str:
    """Return a normalized PEM client chain or reject it safely."""
    normalized = _normalize_pem(contents, "Kafka client certificate")
    try:
        certificates = x509.load_pem_x509_certificates(normalized.encode("utf-8"))
    except ValueError as error:
        raise KafkaProfileError("Kafka client certificate contains invalid PEM") from error
    if not certificates:
        raise KafkaProfileError("Kafka client certificate must contain a PEM certificate")
    return normalized


def validate_private_key(contents: str, *, password: str | None = None) -> None:
    """Reject malformed, mismatched-password, or unsupported PEM private keys."""
    _load_private_key(contents, password=password)


def validate_client_identity(
    certificate: str,
    private_key: str,
    *,
    password: str | None = None,
) -> tuple[str, str]:
    """Validate a client certificate chain and matching private key."""
    normalized_certificate = validate_client_certificate(certificate)
    try:
        certificates = x509.load_pem_x509_certificates(normalized_certificate.encode("utf-8"))
        normalized_key, key = _load_private_key(private_key, password=password)
        certificate_key = (
            certificates[0]
            .public_key()
            .public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
        private_key_public = key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    except (TypeError, ValueError) as error:
        raise KafkaProfileError("Kafka client identity is invalid") from error
    if certificate_key != private_key_public:
        raise KafkaProfileError("Kafka client certificate does not match its private key")
    return normalized_certificate, normalized_key


def _load_private_key(contents: str, *, password: str | None) -> tuple[str, Any]:
    normalized = _normalize_pem(contents, "Kafka client private key")
    if _PRIVATE_KEY_PATTERN.fullmatch(normalized) is None:
        raise KafkaProfileError("Kafka client private key must contain only one PEM key")
    encoded_password = password.encode("utf-8") if password is not None else None
    try:
        key = serialization.load_pem_private_key(
            normalized.encode("utf-8"),
            password=encoded_password,
        )
    except (TypeError, ValueError) as error:
        raise KafkaProfileError("Kafka client private key or password is invalid") from error
    return normalized, key


def kafka_connection(profile: Mapping[str, Any]) -> KafkaConnection:
    """Parse the supported Kafka connection shape without accepting passthroughs."""
    kafka = profile.get("kafka")
    if not isinstance(kafka, Mapping):
        raise KafkaProfileError("profile Kafka configuration is invalid")
    bootstrap_servers = kafka.get("bootstrapServers")
    if not isinstance(bootstrap_servers, list) or not all(
        isinstance(server, str) for server in bootstrap_servers
    ):
        raise KafkaProfileError("profile Kafka broker addresses are invalid")
    transport = kafka.get("transport")
    if transport not in ("plaintext", "tls"):
        raise KafkaProfileError("Kafka transport is not supported")
    tls = kafka.get("tls")
    if transport == "plaintext":
        if tls is not None:
            raise KafkaProfileError("plaintext Kafka transport cannot include TLS configuration")
        connection = KafkaConnection(tuple(bootstrap_servers), "plaintext")
        return _with_authentication(connection, profile, kafka)
    if tls is None:
        connection = KafkaConnection(tuple(bootstrap_servers), "tls")
        return _with_authentication(connection, profile, kafka)
    if not isinstance(tls, Mapping):
        raise KafkaProfileError("Kafka TLS configuration is invalid")
    ca_certificates = tls.get("caCertificates")
    if ca_certificates is None:
        connection = KafkaConnection(tuple(bootstrap_servers), "tls")
        return _with_authentication(connection, profile, kafka)
    connection = KafkaConnection(
        tuple(bootstrap_servers),
        "tls",
        validate_ca_bundle(ca_certificates),
    )
    return _with_authentication(connection, profile, kafka)


def resolve_kafka_connection(
    connection: KafkaConnection,
    store: SecretStore,
) -> KafkaConnection:
    """Resolve only the exact secrets referenced by one validated connection."""
    try:
        if connection.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
            assert connection.password_reference is not None
            password = store.get(connection.password_reference)
            validate_sasl_credential(password)
            return replace(connection, password=password)
        if connection.auth_type == "mtls":
            assert connection.private_key_reference is not None
            key = store.get(connection.private_key_reference)
            key_password = (
                store.get(connection.private_key_password_reference)
                if connection.private_key_password_reference is not None
                else None
            )
            validate_client_identity(
                connection.client_certificate or "",
                key,
                password=key_password,
            )
            return replace(connection, private_key=key, private_key_password=key_password)
    except SecretStoreError as error:
        raise KafkaProfileError("Kafka credentials could not be resolved") from error
    return connection


def java_properties(
    connection: KafkaConnection,
    *,
    ca_location: Path | None = None,
) -> dict[str, str]:
    """Render canonical Java Kafka connection properties."""
    _validate_render_transport(connection)
    properties = {"bootstrap.servers": ",".join(connection.bootstrap_servers)}
    if connection.transport == "plaintext":
        properties["security.protocol"] = "PLAINTEXT"
        return properties
    properties.update(
        {
            "security.protocol": "SSL",
            "ssl.endpoint.identification.algorithm": "https",
        }
    )
    if connection.ca_certificates is not None:
        if ca_location is None:
            raise KafkaProfileError("Kafka CA bundle requires a private session file")
        properties.update(
            {
                "ssl.truststore.location": str(ca_location),
                "ssl.truststore.type": "PEM",
            }
        )
    _add_java_authentication(properties, connection)
    return properties


def librdkafka_properties(
    connection: KafkaConnection,
    *,
    ca_location: Path | None = None,
    client_certificate_location: Path | None = None,
    private_key_location: Path | None = None,
    inline_ca: bool = False,
    inline_client: bool = False,
) -> dict[str, str]:
    """Render canonical librdkafka connection properties."""
    _validate_render_transport(connection)
    properties = {"bootstrap.servers": ",".join(connection.bootstrap_servers)}
    if connection.transport == "plaintext":
        properties["security.protocol"] = "PLAINTEXT"
        return properties
    properties.update(
        {
            "security.protocol": "SSL",
            "enable.ssl.certificate.verification": "true",
            "ssl.endpoint.identification.algorithm": "https",
        }
    )
    if connection.ca_certificates is not None:
        if ca_location is not None:
            properties["ssl.ca.location"] = str(ca_location)
        elif inline_ca:
            properties["ssl.ca.pem"] = connection.ca_certificates
        else:
            raise KafkaProfileError("Kafka CA bundle requires a private session file")
    _add_librdkafka_authentication(
        properties,
        connection,
        client_certificate_location=client_certificate_location,
        private_key_location=private_key_location,
        inline_client=inline_client,
    )
    return properties


def _with_authentication(
    connection: KafkaConnection,
    profile: Mapping[str, Any],
    kafka: Mapping[str, Any],
) -> KafkaConnection:
    auth = kafka.get("auth")
    if not isinstance(auth, Mapping):
        raise KafkaProfileError("Kafka authentication configuration is invalid")
    auth_type = auth.get("type")
    if auth_type == "none":
        return connection
    if connection.transport != "tls":
        raise KafkaProfileError("Kafka authentication requires TLS transport")
    profile_id = profile.get("id")
    if not isinstance(profile_id, str):
        raise KafkaProfileError("authenticated Kafka profile identity is invalid")
    if auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return _with_password_authentication(connection, auth, profile_id, auth_type)
    if auth_type == "mtls":
        return _with_mtls_authentication(connection, auth, profile_id)
    raise KafkaProfileError("Kafka authentication is not supported")


def _with_password_authentication(
    connection: KafkaConnection,
    auth: Mapping[str, Any],
    profile_id: str,
    auth_type: KafkaAuthType,
) -> KafkaConnection:
    username = auth.get("username")
    reference = auth.get("passwordRef")
    if not isinstance(username, str) or not username or not isinstance(reference, str):
        raise KafkaProfileError("Kafka password authentication configuration is invalid")
    _validate_owned_reference(reference, profile_id, "kafka/password")
    return replace(
        connection,
        auth_type=auth_type,
        username=username,
        password_reference=reference,
    )


def _with_mtls_authentication(
    connection: KafkaConnection,
    auth: Mapping[str, Any],
    profile_id: str,
) -> KafkaConnection:
    certificate = auth.get("clientCertificate")
    key_reference = auth.get("privateKeyRef")
    password_reference = auth.get("privateKeyPasswordRef")
    if not isinstance(certificate, str) or not isinstance(key_reference, str):
        raise KafkaProfileError("Kafka mTLS authentication configuration is invalid")
    _validate_owned_reference(key_reference, profile_id, "kafka/tls/private-key")
    if password_reference is not None:
        if not isinstance(password_reference, str):
            raise KafkaProfileError("Kafka mTLS key password reference is invalid")
        _validate_owned_reference(
            password_reference,
            profile_id,
            "kafka/tls/private-key-password",
        )
    return replace(
        connection,
        auth_type="mtls",
        client_certificate=validate_client_certificate(certificate),
        private_key_reference=key_reference,
        private_key_password_reference=password_reference,
    )


def _validate_owned_reference(reference: str, profile_id: str, field: str) -> None:
    try:
        parsed = parse_secret_reference(reference)
    except SecretStoreError as error:
        raise KafkaProfileError("Kafka credential reference is invalid") from error
    if parsed.profile_id != profile_id or parsed.field != field:
        raise KafkaProfileError("Kafka credential reference does not match its profile")


def _validate_render_transport(connection: KafkaConnection) -> None:
    if connection.auth_type != "none" and connection.transport != "tls":
        raise KafkaProfileError("Kafka authentication requires TLS transport")


def _add_java_authentication(
    properties: dict[str, str],
    connection: KafkaConnection,
) -> None:
    if connection.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        if connection.username is None or connection.password is None:
            raise KafkaProfileError("Kafka password credentials are not resolved")
        mechanism = {
            "plain": "PLAIN",
            "scram-sha-256": "SCRAM-SHA-256",
            "scram-sha-512": "SCRAM-SHA-512",
        }[connection.auth_type]
        login_module = (
            "org.apache.kafka.common.security.plain.PlainLoginModule"
            if connection.auth_type == "plain"
            else "org.apache.kafka.common.security.scram.ScramLoginModule"
        )
        properties.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": mechanism,
                "sasl.jaas.config": (
                    f"{login_module} required username={_jaas_value(connection.username)} "
                    f"password={_jaas_value(connection.password)};"
                ),
            }
        )
    elif connection.auth_type == "mtls":
        if connection.client_certificate is None or connection.private_key is None:
            raise KafkaProfileError("Kafka mTLS credentials are not resolved")
        properties.update(
            {
                "ssl.keystore.type": "PEM",
                "ssl.keystore.certificate.chain": connection.client_certificate,
                "ssl.keystore.key": connection.private_key,
            }
        )
        if connection.private_key_password is not None:
            properties["ssl.key.password"] = connection.private_key_password


def _add_librdkafka_authentication(
    properties: dict[str, str],
    connection: KafkaConnection,
    *,
    client_certificate_location: Path | None,
    private_key_location: Path | None,
    inline_client: bool,
) -> None:
    if connection.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        if connection.username is None or connection.password is None:
            raise KafkaProfileError("Kafka password credentials are not resolved")
        properties.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": connection.auth_type.upper().replace("PLAIN", "PLAIN"),
                "sasl.username": connection.username,
                "sasl.password": connection.password,
            }
        )
        return
    if connection.auth_type != "mtls":
        return
    if connection.client_certificate is None or connection.private_key is None:
        raise KafkaProfileError("Kafka mTLS credentials are not resolved")
    if inline_client:
        properties["ssl.certificate.pem"] = connection.client_certificate
        properties["ssl.key.pem"] = connection.private_key
    elif client_certificate_location is not None and private_key_location is not None:
        properties["ssl.certificate.location"] = str(client_certificate_location)
        properties["ssl.key.location"] = str(private_key_location)
    else:
        raise KafkaProfileError("Kafka mTLS credentials require private session files")
    if connection.private_key_password is not None:
        properties["ssl.key.password"] = connection.private_key_password


def _jaas_value(value: str) -> str:
    validate_sasl_credential(value)
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def validate_sasl_credential(value: str) -> None:
    """Reject password values that cannot be represented safely for every renderer."""
    if not value or any(character in value for character in ("\x00", "\r", "\n")):
        raise KafkaProfileError("Kafka credentials are empty or contain unsupported controls")


def _read_bounded_pem(path: Path, limit: int, label: str) -> str:
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise KafkaProfileError(f"{label} source must be a regular file")
            if metadata.st_size > limit:
                raise KafkaProfileError(f"{label} exceeds the 1 MiB limit")
            contents = source.read(limit + 1)
    except KafkaProfileError:
        raise
    except OSError as error:
        raise KafkaProfileError(f"{label} could not be read") from error
    if len(contents) > limit:
        raise KafkaProfileError(f"{label} exceeds the 1 MiB limit")
    try:
        return contents.decode("utf-8")
    except UnicodeDecodeError as error:
        raise KafkaProfileError(f"{label} must be UTF-8 PEM text") from error


def _normalize_pem(contents: str, label: str) -> str:
    if not isinstance(contents, str):
        raise KafkaProfileError(f"{label} must be PEM text")
    encoded = contents.encode("utf-8")
    if not encoded or len(encoded) > MAX_CLIENT_PEM_BYTES:
        raise KafkaProfileError(f"{label} must contain at most 1 MiB of PEM text")
    return contents.replace("\r\n", "\n").strip() + "\n"


def _contains_only_certificates(
    contents: str,
    matches: tuple[re.Match[str], ...],
) -> bool:
    cursor = 0
    for match in matches:
        if contents[cursor : match.start()].strip():
            return False
        cursor = match.end()
    return not contents[cursor:].strip()


__all__ = [
    "CA_BUNDLE_FILENAME",
    "CLIENT_CERTIFICATE_FILENAME",
    "CLIENT_KEY_FILENAME",
    "MAX_CA_BUNDLE_BYTES",
    "MAX_CLIENT_PEM_BYTES",
    "KafkaConnection",
    "KafkaProfileError",
    "java_properties",
    "kafka_connection",
    "librdkafka_properties",
    "read_ca_bundle",
    "read_client_certificate",
    "read_private_key",
    "resolve_kafka_connection",
    "validate_ca_bundle",
    "validate_client_certificate",
    "validate_client_identity",
    "validate_private_key",
    "validate_sasl_credential",
]
