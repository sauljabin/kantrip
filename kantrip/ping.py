"""Check configured services without depending on Kafka resource ACLs."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from kantrip.kafka import (
    KafkaConnection,
    KafkaProfileError,
    kafka_connection,
    librdkafka_properties,
    resolve_kafka_connection,
)
from kantrip.redaction import redact_text
from kantrip.registry import (
    RegistryConnection,
    RegistryProfileError,
    RegistryProvider,
    plain_registry_connection,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store

_QUIET_KAFKA_LOGGER = logging.getLogger("kantrip.ping.librdkafka")
_QUIET_KAFKA_LOGGER.addHandler(logging.NullHandler())
_QUIET_KAFKA_LOGGER.propagate = False
_QUIET_KAFKA_LOGGER.disabled = True
_STATISTICS_INTERVAL_MS = 100
KafkaProof = Literal["reachability", "server-tls", "sasl", "mtls"]


class PingError(ConnectionError):
    """Raised when a configured profile service cannot be verified."""

    def __init__(self, message: str, *, detail: object | None = None) -> None:
        super().__init__(message)
        self.detail = redact_text(detail) if detail is not None else None


@dataclass(frozen=True)
class RegistryPingResult:
    """Connectivity observation for one current plaintext Registry profile."""

    provider: RegistryProvider
    transport: str


@dataclass(frozen=True)
class PingResult:
    """Transport and authentication evidence from a successful probe."""

    kafka_transport: str
    kafka_authentication: str
    proof: KafkaProof
    registry: RegistryPingResult | None = None


@dataclass
class _ProbeState:
    connected: bool = False
    latest_error: str | None = None


def ping_profile(
    profile: Mapping[str, Any],
    *,
    timeout: float = 5.0,
    kafka: KafkaConnection | None = None,
    secret_store: SecretStore | None = None,
) -> PingResult:
    """Verify one real broker connection and the current Registry endpoint."""
    deadline = time.monotonic() + timeout
    try:
        registry = plain_registry_connection(profile)
        connection = kafka or kafka_connection(profile)
        if connection.requires_secrets and kafka is None:
            connection = resolve_kafka_connection(connection, secret_store or load_secret_store())
    except (KafkaProfileError, RegistryProfileError, SecretStoreError) as error:
        raise PingError(str(error)) from error

    _probe_kafka(connection, deadline)
    registry_result = _registry_connectivity(registry, deadline) if registry is not None else None
    transport, authentication, proof = _kafka_observation(connection)
    return PingResult(transport, authentication, proof, registry_result)


def _probe_kafka(connection: KafkaConnection, deadline: float) -> None:
    state = _ProbeState()

    def statistics_callback(payload: str) -> int:
        try:
            statistics = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return 0
        if _has_connected_broker(statistics):
            state.connected = True
        return 0

    def error_callback(error: KafkaError) -> None:
        state.latest_error = f"{error.name()}: {error.str()}"

    remaining = _remaining(deadline)
    configuration = _client_configuration(
        connection,
        remaining,
        statistics_callback=statistics_callback,
        error_callback=error_callback,
    )
    try:
        client = AdminClient(configuration, logger=_QUIET_KAFKA_LOGGER)
        while not state.connected:
            remaining = _remaining(deadline)
            client.poll(min(remaining, _STATISTICS_INTERVAL_MS / 1000))
    except TimeoutError as error:
        message = _classify_probe_failure(state.latest_error)
        raise PingError(message, detail=state.latest_error or "deadline exhausted") from error
    except (KafkaException, KafkaProfileError) as error:
        detail = _exception_message(error)
        raise PingError(_classify_probe_failure(detail), detail=detail) from error


def _has_connected_broker(statistics: object) -> bool:
    if not isinstance(statistics, dict):
        return False
    brokers = statistics.get("brokers")
    if not isinstance(brokers, dict):
        return False
    for broker in brokers.values():
        if not isinstance(broker, dict):
            continue
        if broker.get("source") not in {"configured", "learned"}:
            continue
        if not isinstance(broker.get("nodename"), str) or not broker["nodename"]:
            continue
        if broker.get("state") == "UP":
            return True
    return False


def _classify_probe_failure(detail: str | None) -> str:
    normalized = (detail or "").lower()
    if any(token in normalized for token in ("authentication", "sasl", "credential")):
        return "Kafka authentication failed"
    if any(token in normalized for token in ("certificate", "ssl", "tls", "hostname")):
        return "Kafka TLS verification failed"
    if any(token in normalized for token in ("resolve", "name or service", "dns")):
        return "Kafka broker DNS resolution failed"
    if any(token in normalized for token in ("connect", "transport", "broker")):
        return "Kafka broker connection failed"
    return "Kafka connection proof was inconclusive before the deadline"


def _kafka_observation(connection: KafkaConnection) -> tuple[str, str, KafkaProof]:
    if connection.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return ("verified TLS", f"{connection.auth_type} authenticated", "sasl")
    if connection.auth_type == "mtls":
        return ("verified TLS", "mTLS client exchange completed", "mtls")
    if connection.transport == "tls":
        return ("verified TLS", "not configured", "server-tls")
    return ("plaintext reachable", "not configured", "reachability")


def _registry_connectivity(
    connection: RegistryConnection,
    deadline: float,
) -> RegistryPingResult:
    timeout = _remaining(deadline)
    if connection.provider == "apicurio":
        _apicurio_artifact_count(connection.url, timeout)
    else:
        _confluent_subject_count(connection.url, timeout)
    return RegistryPingResult(connection.provider, "plaintext reachable")


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


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("network deadline exhausted")
    return remaining


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


def _client_configuration(
    connection_or_profile: KafkaConnection | Mapping[str, Any],
    timeout: float,
    *,
    statistics_callback: object | None = None,
    error_callback: object | None = None,
) -> dict[str, Any]:
    """Create the bounded public librdkafka connection-state probe configuration."""
    connection = (
        connection_or_profile
        if isinstance(connection_or_profile, KafkaConnection)
        else kafka_connection(connection_or_profile)
    )
    properties: dict[str, Any] = librdkafka_properties(
        connection,
        inline_ca=True,
        inline_client=True,
    )
    properties.update(
        {
            "client.id": "kantrip-ping",
            "socket.timeout.ms": max(100, round(timeout * 1000)),
            "socket.connection.setup.timeout.ms": max(1000, round(timeout * 1000)),
            "statistics.interval.ms": _STATISTICS_INTERVAL_MS,
            "enable.sparse.connections": False,
        }
    )
    if statistics_callback is not None:
        properties["stats_cb"] = statistics_callback
    if error_callback is not None:
        properties["error_cb"] = error_callback
    return properties


__all__ = ["PingError", "PingResult", "RegistryPingResult", "ping_profile"]
