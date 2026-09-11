"""Check whether a profile can reach a Kafka cluster."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from kantrip.schema_registry import SchemaRegistryProfileError, plain_schema_registry_url


class PingError(ConnectionError):
    """Raised when a configured profile service cannot be reached."""


@dataclass(frozen=True)
class PingResult:
    """Metadata returned by a successful Kafka connectivity check."""

    broker_count: int
    schema_registry_subject_count: int | None = None


def ping_profile(profile: Mapping[str, Any], *, timeout: float = 5.0) -> PingResult:
    """Check Kafka metadata and an optional plain Schema Registry connection."""
    try:
        registry_url = plain_schema_registry_url(profile)
    except SchemaRegistryProfileError as error:
        raise PingError(str(error)) from error
    try:
        client = AdminClient(_client_configuration(profile, timeout))
        metadata = client.list_topics(timeout=timeout)
    except KafkaException as error:
        raise PingError("the Kafka cluster did not return metadata") from error
    subject_count = (
        _schema_registry_subject_count(registry_url, timeout) if registry_url is not None else None
    )
    return PingResult(
        broker_count=len(metadata.brokers),
        schema_registry_subject_count=subject_count,
    )


def _schema_registry_subject_count(url: str, timeout: float) -> int:
    request = Request(
        f"{url.rstrip('/')}/subjects",
        headers={"Accept": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            subjects = json.loads(response.read())
    except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
        raise PingError("the Schema Registry did not return its subjects") from error
    if not isinstance(subjects, list) or not all(isinstance(subject, str) for subject in subjects):
        raise PingError("the Schema Registry returned an invalid subjects response")
    return len(subjects)


def _client_configuration(profile: Mapping[str, Any], timeout: float) -> dict[str, Any]:
    kafka = profile["kafka"]
    configured = kafka.get("properties", {})
    properties = {
        str(key): value
        for group in (configured.get("common", {}), configured.get("librdkafka", {}))
        for key, value in group.items()
    }
    properties.update(
        {
            "bootstrap.servers": ",".join(kafka["bootstrapServers"]),
            "security.protocol": "PLAINTEXT",
            "client.id": "kantrip-ping",
            "socket.timeout.ms": max(100, round(timeout * 1000)),
        }
    )
    return properties


__all__ = ["PingError", "PingResult", "ping_profile"]
