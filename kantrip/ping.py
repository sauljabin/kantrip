"""Check whether a profile can reach a Kafka cluster."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


class PingError(ConnectionError):
    """Raised when a profile cannot retrieve Kafka cluster metadata."""


@dataclass(frozen=True)
class PingResult:
    """Metadata returned by a successful Kafka connectivity check."""

    broker_count: int


def ping_profile(profile: Mapping[str, Any], *, timeout: float = 5.0) -> PingResult:
    """Use Kafka's Admin API to retrieve cluster metadata for a profile."""
    try:
        client = AdminClient(_client_configuration(profile, timeout))
        metadata = client.list_topics(timeout=timeout)
    except KafkaException as error:
        raise PingError("the Kafka cluster did not return metadata") from error
    return PingResult(broker_count=len(metadata.brokers))


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
