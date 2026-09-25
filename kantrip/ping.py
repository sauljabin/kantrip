"""Check configured services without depending on Kafka resource ACLs."""

from __future__ import annotations

import base64
import json
import logging
import ssl
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urlencode
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from kantrip._files import write_exclusive_text
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
    _UnresolvedRegistry,
    registry_connection,
    resolve_registry_connection,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store
from kantrip.secret_value import reveal_optional

_QUIET_KAFKA_LOGGER = logging.getLogger("kantrip.ping.librdkafka")
_QUIET_KAFKA_LOGGER.addHandler(logging.NullHandler())
_QUIET_KAFKA_LOGGER.propagate = False
_QUIET_KAFKA_LOGGER.disabled = True
_STATISTICS_INTERVAL_MS = 100
_MAX_REGISTRY_RESPONSE_BYTES = 1024 * 1024
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
    proof: str


@dataclass(frozen=True)
class PingResult:
    """Kafka evidence plus the outcome of the configured Registry check.

    Kafka always succeeded when a result exists. ``registry`` holds a verified
    Registry observation and ``registry_error`` a Registry failure; both are
    ``None`` when no Registry is configured.
    """

    kafka_transport: str
    kafka_authentication: str
    proof: KafkaProof
    registry: RegistryPingResult | None = None
    registry_error: PingError | None = None

    @property
    def healthy(self) -> bool:
        """Return whether every attempted service check succeeded."""
        return self.registry_error is None


@dataclass
class _ProbeState:
    connected: bool = False
    latest_error: str | None = None


def ping_profile(
    profile: Mapping[str, Any],
    *,
    timeout: float = 5.0,
    kafka: KafkaConnection | None = None,
    resolved_registry: RegistryConnection | None | _UnresolvedRegistry = _UnresolvedRegistry.VALUE,
    secret_store: SecretStore | None = None,
) -> PingResult:
    """Verify one real broker connection, then the configured Registry endpoint.

    Kafka failures raise ``PingError`` and the Registry is not attempted. A
    Registry failure after Kafka success is returned in ``registry_error`` so the
    verified Kafka observation is never discarded.
    """
    deadline = time.monotonic() + timeout
    try:
        registry = (
            registry_connection(profile)
            if isinstance(resolved_registry, _UnresolvedRegistry)
            else resolved_registry
        )
        connection = kafka or kafka_connection(profile)
        selected_store = secret_store
        if connection.requires_secrets and kafka is None:
            selected_store = selected_store or load_secret_store()
            connection = resolve_kafka_connection(connection, selected_store)
        if (
            isinstance(resolved_registry, _UnresolvedRegistry)
            and registry is not None
            and registry.requires_secrets
        ):
            selected_store = selected_store or load_secret_store()
            registry = resolve_registry_connection(registry, selected_store)
    except (KafkaProfileError, RegistryProfileError, SecretStoreError) as error:
        raise PingError(str(error)) from error

    _probe_kafka(connection, deadline)
    transport, authentication, proof = _kafka_observation(connection)
    if registry is None:
        return PingResult(transport, authentication, proof)
    try:
        registry_result = _registry_connectivity(registry, deadline)
    except PingError as error:
        return PingResult(transport, authentication, proof, registry_error=error)
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

    try:
        remaining = _remaining(deadline)
        configuration = _client_configuration(
            connection,
            remaining,
            statistics_callback=statistics_callback,
            error_callback=error_callback,
        )
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
    if connection.auth_type in {"plain", "scram-sha-256", "scram-sha-512", "oauth"}:
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
    registry_name = (
        "Apicurio Registry" if connection.provider == "apicurio" else "Confluent Schema Registry"
    )
    if connection.auth_type not in {"none", "basic", "token", "mtls", "oauth"}:
        raise PingError(f"the {registry_name} authentication mode is unsupported")
    headers = _registry_auth_headers(connection)
    if connection.auth_type == "oauth":
        headers = {"Authorization": f"Bearer {_oauth_access_token(connection, deadline)}"}
    context = _registry_ssl_context(connection)
    try:
        timeout = _remaining(deadline)
    except TimeoutError as error:
        raise PingError(
            f"the {registry_name} did not return registry metadata",
            detail=error,
        ) from error
    if connection.provider == "apicurio":
        probe_url = f"{connection.url.rstrip('/')}/search/versions?limit=1"
        accept = "application/json"
        _apicurio_search_versions(
            probe_url,
            timeout,
            headers=headers,
            context=context,
        )
    else:
        probe_url = f"{connection.url.rstrip('/')}/subjects?limit=1"
        accept = "application/vnd.schemaregistry.v1+json"
        _confluent_subjects(
            probe_url,
            timeout,
            headers=headers,
            context=context,
        )
    if connection.auth_type != "none":
        _require_anonymous_rejection(
            probe_url,
            deadline,
            context=_registry_ssl_context(connection, include_client=False),
            accept=accept,
            accept_mtls_rejection=connection.auth_type == "mtls",
        )
    if connection.auth_type == "none":
        proof = "read query validated"
    elif connection.auth_type == "mtls":
        proof = "mTLS read query and anonymous rejection validated"
    else:
        proof = f"{connection.auth_type} authenticated read query validated"
    return RegistryPingResult(
        connection.provider,
        "verified TLS" if connection.url.startswith("https://") else "plaintext reachable",
        proof,
    )


def _confluent_subjects(
    probe_url: str,
    timeout: float,
    *,
    headers: Mapping[str, str] | None = None,
    context: ssl.SSLContext | None = None,
) -> tuple[str, ...]:
    body = _registry_json(
        probe_url,
        timeout,
        "Confluent Schema Registry",
        "application/vnd.schemaregistry.v1+json",
        headers=headers,
        context=context,
    )
    if not isinstance(body, list) or not all(
        isinstance(subject, str) and subject for subject in body
    ):
        raise PingError("the Confluent Schema Registry returned invalid subject-search metadata")
    return tuple(body)


def _apicurio_search_versions(
    probe_url: str,
    timeout: float,
    *,
    headers: Mapping[str, str] | None = None,
    context: ssl.SSLContext | None = None,
) -> tuple[Mapping[str, object], ...]:
    body = _registry_json(
        probe_url,
        timeout,
        "Apicurio Registry",
        "application/json",
        headers=headers,
        context=context,
    )
    if not isinstance(body, dict):
        raise PingError("the Apicurio Registry returned invalid version-search metadata")
    count = body.get("count")
    versions = body.get("versions")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or not isinstance(versions, list)
        or not all(isinstance(version, dict) for version in versions)
    ):
        raise PingError("the Apicurio Registry returned invalid version-search metadata")
    return tuple(versions)


def _oauth_access_token(connection: RegistryConnection, deadline: float) -> str:
    oauth = connection.oauth
    if oauth is None or oauth.client_secret is None:
        raise PingError("Registry OAuth credentials are unresolved")
    encoded_client_id = quote_plus(oauth.client_id, safe="")
    encoded_client_secret = quote_plus(oauth.client_secret.reveal(), safe="")
    credential = base64.b64encode(f"{encoded_client_id}:{encoded_client_secret}".encode()).decode(
        "ascii"
    )
    form = {"grant_type": "client_credentials"}
    if oauth.scopes:
        form["scope"] = " ".join(oauth.scopes)
    request = Request(
        oauth.token_url,
        data=urlencode(form).encode("ascii"),
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {credential}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    context = ssl.create_default_context()
    if oauth.ca_certificates is not None:
        context.load_verify_locations(cadata=oauth.ca_certificates)
    try:
        with _open_request(request, _remaining(deadline), context=context) as response:
            _require_json_content_type(response, "Registry OAuth token endpoint")
            payload = response.read(_MAX_REGISTRY_RESPONSE_BYTES + 1)
            if len(payload) > _MAX_REGISTRY_RESPONSE_BYTES:
                raise ValueError("OAuth response exceeded the 1 MiB limit")
            body = json.loads(payload)
    except PingError:
        raise
    except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as error:
        raise PingError(
            "Registry OAuth token request failed", detail=_exception_message(error)
        ) from error
    if not isinstance(body, dict):
        raise PingError("Registry OAuth token response is invalid")
    token, token_type, expires_in = (
        body.get("access_token"),
        body.get("token_type"),
        body.get("expires_in"),
    )
    if (
        not isinstance(token, str)
        or not token
        or any(character in token for character in ("\x00", "\r", "\n"))
        or not isinstance(token_type, str)
        or token_type.lower() != "bearer"
        or not isinstance(expires_in, (int, float))
        or isinstance(expires_in, bool)
        or expires_in <= 0
    ):
        raise PingError("Registry OAuth token response is invalid")
    return token


def _registry_json(
    url: str,
    timeout: float,
    name: str,
    accept: str,
    *,
    headers: Mapping[str, str] | None = None,
    context: ssl.SSLContext | None = None,
) -> object:
    request = Request(url, headers={"Accept": accept, **dict(headers or {})})
    try:
        with _open_request(request, timeout, context=context) as response:
            _require_json_content_type(response, name)
            payload = response.read(_MAX_REGISTRY_RESPONSE_BYTES + 1)
            if len(payload) > _MAX_REGISTRY_RESPONSE_BYTES:
                raise ValueError("Registry response exceeded the 1 MiB limit")
            return json.loads(payload)
    except PingError:
        raise
    except HTTPError as error:
        if error.code == 401:
            raise PingError(f"the {name} rejected Registry authentication", detail=error) from error
        if error.code == 403:
            raise PingError(
                f"the {name} denied Registry authorization or returned an ambiguous 403",
                detail=error,
            ) from error
        raise PingError(
            f"the {name} did not return registry metadata",
            detail=_exception_message(error),
        ) from error
    except (URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
        raise PingError(
            f"the {name} did not return registry metadata",
            detail=_exception_message(error),
        ) from error


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


def _open_request(
    request: Request,
    timeout: float,
    *,
    context: ssl.SSLContext | None,
) -> Any:
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context), _NoRedirect())
    return opener.open(request, timeout=timeout)


def _registry_ssl_context(
    connection: RegistryConnection, *, include_client: bool = True
) -> ssl.SSLContext | None:
    if not connection.url.startswith("https://"):
        return None
    context = ssl.create_default_context()
    if connection.ca_certificates is not None:
        context.load_verify_locations(cadata=connection.ca_certificates)
    if include_client and connection.auth_type == "mtls":
        if connection.client_certificate is None or connection.private_key is None:
            raise PingError("Registry mTLS credentials are unresolved")
        try:
            with tempfile.TemporaryDirectory(prefix="kantrip-registry-ping-") as directory:
                certificate_path = Path(directory) / "client.crt"
                key_path = Path(directory) / "client.key"
                write_exclusive_text(certificate_path, connection.client_certificate, mode=0o600)
                write_exclusive_text(key_path, connection.private_key.reveal(), mode=0o600)
                context.load_cert_chain(
                    certificate_path,
                    key_path,
                    password=reveal_optional(connection.private_key_password),
                )
        except (OSError, ssl.SSLError) as error:
            raise PingError(
                "Registry mTLS client identity could not be loaded", detail=error
            ) from error
    return context


def _require_json_content_type(response: object, name: str) -> None:
    headers = getattr(response, "headers", None)
    get_content_type = getattr(headers, "get_content_type", None)
    if not callable(get_content_type):
        return
    content_type = get_content_type()
    if isinstance(content_type, str) and not (
        content_type == "application/json" or content_type.endswith("+json")
    ):
        raise PingError(f"the {name} returned a non-JSON response")


def _registry_auth_headers(connection: RegistryConnection) -> dict[str, str]:
    if connection.auth_type == "basic":
        if connection.username is None or connection.password is None:
            raise PingError("Registry basic credentials are unresolved")
        credential = base64.b64encode(
            f"{connection.username}:{connection.password.reveal()}".encode()
        ).decode("ascii")
        return {"Authorization": f"Basic {credential}"}
    if connection.auth_type == "token":
        if connection.token is None:
            raise PingError("Registry bearer token is unresolved")
        return {"Authorization": f"Bearer {connection.token.reveal()}"}
    return {}


def _require_anonymous_rejection(
    url: str,
    deadline: float,
    *,
    context: ssl.SSLContext | None,
    accept: str,
    accept_mtls_rejection: bool = False,
) -> None:
    request = Request(url, headers={"Accept": accept})
    try:
        with _open_request(request, _remaining(deadline), context=context) as response:
            response.read(1)
    except HTTPError as error:
        if error.code in {401, 403}:
            return
        raise PingError(
            "Registry authentication proof was inconclusive",
            detail=f"anonymous control returned HTTP {error.code}",
        ) from error
    except URLError as error:
        if accept_mtls_rejection and isinstance(error.reason, ssl.SSLError):
            return
        raise PingError("Registry authentication proof was inconclusive", detail=error) from error
    except ssl.SSLError as error:
        if accept_mtls_rejection:
            return
        raise PingError("Registry authentication proof was inconclusive", detail=error) from error
    except (OSError, TimeoutError) as error:
        raise PingError("Registry authentication proof was inconclusive", detail=error) from error
    raise PingError("Registry endpoint is public; configured authentication was not proven")


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
        inline_oauth_ca=True,
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
