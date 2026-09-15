"""Validate Kafka profile transport and render canonical client properties."""

from __future__ import annotations

import os
import re
import ssl
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

KafkaTransport = Literal["plaintext", "tls"]
MAX_CA_BUNDLE_BYTES = 1024 * 1024
CA_BUNDLE_FILENAME = "kafka-ca.pem"
_CERTIFICATE_PATTERN = re.compile(
    r"-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+?" r"-----END CERTIFICATE-----"
)


class KafkaProfileError(ValueError):
    """Raised when Kafka connection metadata is unsafe or unsupported."""


@dataclass(frozen=True)
class KafkaConnection:
    """Validated provider-neutral Kafka connection metadata."""

    bootstrap_servers: tuple[str, ...]
    transport: KafkaTransport
    ca_certificates: str | None = None


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
    auth = kafka.get("auth")
    if not isinstance(auth, Mapping) or auth.get("type") != "none":
        raise KafkaProfileError("Kafka authentication is not yet supported")
    transport = kafka.get("transport")
    if transport not in ("plaintext", "tls"):
        raise KafkaProfileError("Kafka transport is not supported")
    tls = kafka.get("tls")
    if transport == "plaintext":
        if tls is not None:
            raise KafkaProfileError("plaintext Kafka transport cannot include TLS configuration")
        return KafkaConnection(tuple(bootstrap_servers), "plaintext")
    if tls is None:
        return KafkaConnection(tuple(bootstrap_servers), "tls")
    if not isinstance(tls, Mapping):
        raise KafkaProfileError("Kafka TLS configuration is invalid")
    ca_certificates = tls.get("caCertificates")
    if ca_certificates is None:
        return KafkaConnection(tuple(bootstrap_servers), "tls")
    return KafkaConnection(
        tuple(bootstrap_servers),
        "tls",
        validate_ca_bundle(ca_certificates),
    )


def java_properties(
    connection: KafkaConnection,
    *,
    ca_location: Path | None = None,
) -> dict[str, str]:
    """Render canonical Java Kafka connection properties."""
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
    return properties


def librdkafka_properties(
    connection: KafkaConnection,
    *,
    ca_location: Path | None = None,
    inline_ca: bool = False,
) -> dict[str, str]:
    """Render canonical librdkafka connection properties."""
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
    return properties


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
    "MAX_CA_BUNDLE_BYTES",
    "KafkaConnection",
    "KafkaProfileError",
    "java_properties",
    "kafka_connection",
    "librdkafka_properties",
    "read_ca_bundle",
    "validate_ca_bundle",
]
