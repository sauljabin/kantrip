"""Check whether a profile can reach its Kafka cluster and registry."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from kantrip.kafka import KafkaProfileError, kafka_connection, librdkafka_properties
from kantrip.redaction import redact_text
from kantrip.registry import (
    RegistryConnection,
    RegistryProfileError,
    RegistryProvider,
    plain_registry_connection,
)

_QUIET_KAFKA_LOGGER = logging.getLogger("kantrip.ping.librdkafka")
_QUIET_KAFKA_LOGGER.addHandler(logging.NullHandler())
_QUIET_KAFKA_LOGGER.propagate = False
_QUIET_KAFKA_LOGGER.disabled = True


class PingError(ConnectionError):
    """Raised when a configured profile service cannot be reached."""

    def __init__(self, message: str, *, detail: object | None = None) -> None:
        super().__init__(message)
        self.detail = redact_text(detail) if detail is not None else None


@dataclass(frozen=True)
class RegistryPingResult:
    """Provider-specific registry metadata returned by a connectivity check."""

    provider: RegistryProvider
    count: int


@dataclass(frozen=True)
class PingResult:
    """Metadata returned by a successful profile connectivity check."""

    broker_count: int
    registry: RegistryPingResult | None = None


def ping_profile(profile: Mapping[str, Any], *, timeout: float = 5.0) -> PingResult:
    """Check Kafka metadata and an optional plaintext registry connection."""
    try:
        registry = plain_registry_connection(profile)
    except RegistryProfileError as error:
        raise PingError(str(error)) from error
    try:
        configuration = _client_configuration(profile, timeout)
    except KafkaProfileError as error:
        raise PingError(str(error)) from error
    try:
        client = AdminClient(configuration, logger=_QUIET_KAFKA_LOGGER)
        metadata = client.list_topics(timeout=timeout)
    except KafkaException as error:
        raise PingError(
            "the Kafka cluster did not return metadata",
            detail=_exception_message(error),
        ) from error
    registry_result = _registry_metadata(registry, timeout) if registry is not None else None
    return PingResult(broker_count=len(metadata.brokers), registry=registry_result)


def _registry_metadata(connection: RegistryConnection, timeout: float) -> RegistryPingResult:
    if connection.provider == "apicurio":
        return RegistryPingResult("apicurio", _apicurio_artifact_count(connection.url, timeout))
    return RegistryPingResult("confluent", _confluent_subject_count(connection.url, timeout))


def _confluent_subject_count(url: str, timeout: float) -> int:
    body = _registry_json(
        f"{url.rstrip('/')}/subjects",
        timeout,
        "Confluent Schema Registry",
        "application/vnd.schemaregistry.v1+json",
    )
    if not isinstance(body, list) or not all(isinstance(subject, str) for subject in body):
        raise PingError("the Confluent Schema Registry returned an invalid subjects response")
    return len(body)


def _apicurio_artifact_count(url: str, timeout: float) -> int:
    body = _registry_json(
        f"{url.rstrip('/')}/search/artifacts?limit=1",
        timeout,
        "Apicurio Registry",
        "application/json",
    )
    if not isinstance(body, dict):
        raise PingError("the Apicurio Registry returned an invalid artifact search response")
    count = body.get("count")
    artifacts = body.get("artifacts")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or not isinstance(artifacts, list)
    ):
        raise PingError("the Apicurio Registry returned an invalid artifact search response")
    return count


def _registry_json(url: str, timeout: float, name: str, accept: str) -> object:
    request = Request(url, headers={"Accept": accept})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
        raise PingError(
            f"the {name} did not return registry metadata",
            detail=_exception_message(error),
        ) from error


def _exception_message(error: Exception) -> str:
    if isinstance(error, KafkaException) and error.args:
        kafka_error = error.args[0]
        if isinstance(kafka_error, KafkaError):
            return f"{kafka_error.name()}: {kafka_error.str()}"
    if isinstance(error, HTTPError):
        return f"HTTP {error.code}: {error.reason}"
    if isinstance(error, URLError):
        return str(error.reason)
    message = str(error)
    return message or type(error).__name__


def _client_configuration(profile: Mapping[str, Any], timeout: float) -> dict[str, Any]:
    properties: dict[str, Any] = librdkafka_properties(
        kafka_connection(profile),
        inline_ca=True,
    )
    properties.update(
        {
            "client.id": "kantrip-ping",
            "socket.timeout.ms": max(100, round(timeout * 1000)),
        }
    )
    return properties


__all__ = ["PingError", "PingResult", "RegistryPingResult", "ping_profile"]
