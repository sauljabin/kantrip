"""Long-lived Registry OAuth acceptance through released supported clients."""

from __future__ import annotations

import json
import os
import re
import selectors
import shutil
import ssl
import struct
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message, Producer

from sandbox.__main__ import CA_FILE
from tests.e2e.terminal import TerminalProcess, TerminalProcessError

TOKEN_URL = "https://localhost:8443/realms/kantrip/protocol/openid-connect/token"
KEYCLOAK_ADMIN_URL = "https://localhost:8443/admin/realms/kantrip"
CONFLUENT_URL = "https://localhost:8085"
APICURIO_URL = "https://localhost:8084/apis/registry/v3"
MAX_TOKEN_LIFETIME_SECONDS = 60

AdminToken = Callable[[Mapping[str, str], ssl.SSLContext], str]
SetClientEnabled = Callable[..., None]


class RegistryOAuthFailure(RuntimeError):
    """Raised when a long-lived released client misses the OAuth contract."""


@dataclass(frozen=True)
class RegistryResources:
    provider: str
    names: tuple[str, ...]
    schema_ids: tuple[int, ...]
    token_lifetime: int


def exercise_registry_oauth_renewal(
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    *,
    admin_token: AdminToken,
    set_client_enabled: SetClientEnabled,
) -> None:
    """Prove refresh and revoked renewal in three real long-lived clients."""
    suffix = f"{int(time.time())}-{os.getpid()}"
    confluent: RegistryResources | None = None
    apicurio: RegistryResources | None = None
    topics = {
        "confluent": f"kantrip-smoke-e2e-confluent-oauth-{suffix}",
        "apicurio": f"kantrip-smoke-e2e-apicurio-oauth-{suffix}",
        "java": f"kantrip-smoke-e2e-java-oauth-{suffix}",
    }
    clients: dict[str, TerminalProcess] = {}
    java: subprocess.Popen[bytes] | None = None
    java_output = bytearray()
    created_topics: list[str] = []
    try:
        confluent = _register_confluent(credentials, suffix)
        apicurio = _register_apicurio(credentials, suffix, admin_token)
        for topic in topics.values():
            _create_topic(topic)
            created_topics.append(topic)
        _produce(topics["confluent"], confluent.schema_ids[0], "confluent-first")
        _produce(topics["apicurio"], apicurio.schema_ids[0], "apicurio-first")
        _produce(topics["java"], confluent.schema_ids[0], "java-first")
        clients = {
            "confluent": _kaskade("auth-schema-registry-oauth", topics["confluent"], environment),
            "apicurio": _kaskade("auth-apicurio-oauth", topics["apicurio"], environment),
        }
        java = _java_consumer(topics["java"], environment)
        clients["confluent"].wait_for(("confluent-first",), timeout=60)
        clients["apicurio"].wait_for(("apicurio-first",), timeout=60)
        _wait_for_java(java, b"java-first", java_output, credentials)

        first_events = {
            provider: _client_login_ids(credentials, _client_id(provider, credentials), admin_token)
            for provider in ("confluent", "apicurio")
        }
        lifetime = max(confluent.token_lifetime, apicurio.token_lifetime)
        time.sleep(lifetime + 2)
        _produce(topics["confluent"], confluent.schema_ids[1], "confluent-second")
        _produce(topics["apicurio"], apicurio.schema_ids[1], "apicurio-second")
        _produce(topics["java"], confluent.schema_ids[1], "java-second")
        clients["confluent"].write("n")
        clients["apicurio"].write("n")
        clients["confluent"].wait_for(("confluent-second",), timeout=60)
        clients["apicurio"].wait_for(("apicurio-second",), timeout=60)
        _wait_for_java(java, b"java-second", java_output, credentials)
        for provider in ("confluent", "apicurio"):
            current = _client_login_ids(credentials, _client_id(provider, credentials), admin_token)
            if not current.difference(first_events[provider]):
                raise RegistryOAuthFailure(
                    f"{provider} showed no second successful IdP token issuance after expiry"
                )

        disabled: list[str] = []
        try:
            for provider in ("confluent", "apicurio"):
                client_id = _client_id(provider, credentials)
                set_client_enabled(credentials, client_id, enabled=False)
                disabled.append(client_id)
            time.sleep(lifetime + 2)
            _produce(topics["confluent"], confluent.schema_ids[2], "confluent-third")
            clients["confluent"].write("n")
            _expect_kaskade_oauth_failure(clients["confluent"], "confluent-third", credentials)
            _produce(topics["apicurio"], apicurio.schema_ids[2], "apicurio-third")
            clients["apicurio"].write("n")
            _expect_apicurio_oauth_failure(clients["apicurio"], credentials)
            _produce(topics["java"], confluent.schema_ids[2], "java-third")
            _expect_java_oauth_failure(java, java_output, credentials)
        finally:
            for client_id in reversed(disabled):
                set_client_enabled(credentials, client_id, enabled=True)
    finally:
        _cleanup_resources(
            clients, java, created_topics, confluent, apicurio, credentials, admin_token
        )


def _cleanup_resources(
    clients: Mapping[str, TerminalProcess],
    java: subprocess.Popen[bytes] | None,
    topics: list[str],
    confluent: RegistryResources | None,
    apicurio: RegistryResources | None,
    credentials: Mapping[str, str],
    admin_token: AdminToken,
) -> None:
    with ExitStack() as cleanup:
        if apicurio is not None:
            cleanup.callback(_cleanup_apicurio, apicurio, credentials, admin_token)
        if confluent is not None:
            cleanup.callback(_cleanup_confluent, confluent, credentials)
        for topic in topics:
            cleanup.callback(_delete_topic, topic)
        if java is not None:
            cleanup.callback(_terminate, java)
        for client in clients.values():
            cleanup.callback(client.close)


def _kaskade(profile: str, topic: str, environment: Mapping[str, str]) -> TerminalProcess:
    return TerminalProcess(
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kaskade",
            "consumer",
            "--topic",
            topic,
            "--earliest",
            "--value",
            "registry",
            "--kafka",
            f"group.id={topic}-kaskade",
            "--kafka",
            "broker.address.family=v4",
        ),
        environment,
    )


def _java_consumer(topic: str, environment: Mapping[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        (
            *_cli(),
            "exec",
            "auth-schema-registry-oauth",
            "--",
            "kafka-avro-console-consumer",
            "--topic",
            topic,
            "--group",
            f"{topic}-java",
            "--from-beginning",
            "--max-messages",
            "3",
        ),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _expect_kaskade_oauth_failure(
    process: TerminalProcess,
    forbidden_marker: str,
    credentials: Mapping[str, str],
) -> None:
    expected = (
        "OAuth token request failed",
        "Failed to retrieve token",
        "OAuthTokenError",
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            rendered = process.wait_for(expected[:1], timeout=1)
        except TerminalProcessError:
            rendered = process.rendered()
        if any(message in rendered for message in expected):
            _assert_redacted(rendered, credentials)
            if forbidden_marker in rendered:
                raise RegistryOAuthFailure("Kaskade decoded a schema after OAuth revocation")
            return
    _assert_redacted(process.rendered(), credentials)
    raise RegistryOAuthFailure("Kaskade did not surface an OAuth acquisition failure")


def _expect_apicurio_oauth_failure(
    process: TerminalProcess,
    credentials: Mapping[str, str],
) -> None:
    """Read the fallback's actual error in Kaskade's record detail view."""
    process.wait_for(("[3]",), timeout=60)
    process.write("\r")
    process.wait_for(("DESERIALIZER",), timeout=10)
    process.write("n")
    process.wait_for(("[0][1]",), timeout=10)
    process.write("n")
    process.wait_for(("[0][2]",), timeout=10)
    process.write("\x1b[C")
    rendered = process.wait_for(("OAuth token request failed",), timeout=15)
    _assert_redacted(rendered, credentials)
    if "apicurio-third" in rendered:
        raise RegistryOAuthFailure("Kaskade decoded an Apicurio schema after OAuth revocation")


def _wait_for_java(
    process: subprocess.Popen[bytes],
    expected: bytes,
    output: bytearray,
    credentials: Mapping[str, str],
) -> None:
    if process.stdout is None:
        raise RegistryOAuthFailure("Java Registry consumer has no output stream")
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            for key, _ in selector.select(timeout=1):
                output.extend(os.read(key.fd, 4096))
                if expected in output:
                    return
            if process.poll() is not None:
                output.extend(process.stdout.read())
                break
    finally:
        selector.close()
    rendered = output.decode(errors="replace")
    _assert_redacted(rendered, credentials)
    raise RegistryOAuthFailure(f"Java Registry consumer did not decode {expected!r}")


def _expect_java_oauth_failure(
    process: subprocess.Popen[bytes],
    output: bytearray,
    credentials: Mapping[str, str],
) -> None:
    try:
        tail, _ = process.communicate(timeout=60)
    except subprocess.TimeoutExpired as error:
        raise RegistryOAuthFailure(
            "Java Registry consumer did not fail after revocation"
        ) from error
    output.extend(tail)
    rendered = output.decode(errors="replace")
    _assert_redacted(rendered, credentials)
    oauth_failure = (
        "SchemaRegistryOauthTokenRetrieverException" in rendered
        or "Failed to Retrieve OAuth Token for Schema Registry" in rendered
        or (
            TOKEN_URL in rendered
            and "invalid_client" in rendered
            and re.search(r"(?:response code:?|error response code:) 401", rendered) is not None
        )
    )
    third_decoded = "java-third" in rendered
    if process.returncode == 0 or not oauth_failure or third_decoded:
        raise RegistryOAuthFailure(
            "Java Registry consumer did not prove revoked OAuth renewal "
            f"(exit={process.returncode}, oauth_failure={oauth_failure}, "
            f"third_decoded={third_decoded})"
        )


def _register_confluent(credentials: Mapping[str, str], suffix: str) -> RegistryResources:
    token, lifetime = _token(
        _client_id("confluent", credentials),
        _client_secret("confluent", credentials),
    )
    names: list[str] = []
    ids: list[int] = []
    for index in range(1, 4):
        subject = f"kantrip-e2e-oauth-{suffix}-{index}-value"
        response = _request_json(
            f"{CONFLUENT_URL}/subjects/{urllib.parse.quote(subject, safe='')}/versions",
            token,
            method="POST",
            payload={
                "schemaType": "AVRO",
                "schema": _schema(f"ConfluentOAuth{os.getpid()}{index}"),
            },
        )
        schema_id = response.get("id") if isinstance(response, dict) else None
        if not isinstance(schema_id, int):
            raise RegistryOAuthFailure("Confluent registration returned no schema ID")
        names.append(subject)
        ids.append(schema_id)
    return RegistryResources("confluent", tuple(names), tuple(ids), lifetime)


def _register_apicurio(
    credentials: Mapping[str, str], suffix: str, admin_token: AdminToken
) -> RegistryResources:
    with _apicurio_admin_role(credentials, admin_token):
        token, lifetime = _token(
            _client_id("apicurio", credentials),
            _client_secret("apicurio", credentials),
        )
        names: list[str] = []
        ids: list[int] = []
        for index in range(1, 4):
            artifact = f"kantrip-e2e-oauth-{suffix}-{index}"
            response = _request_json(
                f"{APICURIO_URL}/groups/default/artifacts",
                token,
                method="POST",
                payload={
                    "artifactId": artifact,
                    "artifactType": "AVRO",
                    "firstVersion": {
                        "version": "1",
                        "content": {
                            "contentType": "application/json",
                            "content": _schema(f"ApicurioOAuth{os.getpid()}{index}"),
                            "references": [],
                        },
                    },
                },
            )
            version = response.get("version") if isinstance(response, dict) else None
            content_id = version.get("contentId") if isinstance(version, dict) else None
            if not isinstance(content_id, int):
                raise RegistryOAuthFailure("Apicurio registration returned no content ID")
            names.append(artifact)
            ids.append(content_id)
    return RegistryResources("apicurio", tuple(names), tuple(ids), lifetime)


def _token(client_id: str, secret: str) -> tuple[str, int]:
    request = urllib.request.Request(
        TOKEN_URL,
        data=urllib.parse.urlencode(
            {"grant_type": "client_credentials", "scope": "openid"}
        ).encode(),
        headers={
            "Authorization": "Basic " + b64encode(f"{client_id}:{secret}".encode()).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    context = ssl.create_default_context(cafile=str(CA_FILE))
    with urllib.request.urlopen(request, context=context, timeout=15) as response:
        body = json.load(response)
    token = body.get("access_token") if isinstance(body, dict) else None
    lifetime = body.get("expires_in") if isinstance(body, dict) else None
    if (
        not isinstance(token, str)
        or not token
        or not isinstance(lifetime, int)
        or not 1 <= lifetime <= MAX_TOKEN_LIFETIME_SECONDS
    ):
        raise RegistryOAuthFailure("Keycloak returned an invalid Registry token lifetime")
    return token, lifetime


def _request_json(
    url: str,
    token: str,
    *,
    method: str,
    payload: object | None = None,
) -> object:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    context = ssl.create_default_context(cafile=str(CA_FILE))
    with urllib.request.urlopen(request, context=context, timeout=15) as response:
        if response.status == 204:
            return {}
        return json.load(response)


def _schema(name: str) -> str:
    return json.dumps(
        {
            "type": "record",
            "name": name,
            "namespace": "io.kantrip.e2e",
            "fields": [{"name": "value", "type": "string"}],
        }
    )


def _wire_value(schema_id: int, marker: str) -> bytes:
    encoded = marker.encode()
    remaining = len(encoded) << 1
    length = bytearray()
    while remaining & ~0x7F:
        length.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    length.append(remaining)
    return struct.pack(">bI", 0, schema_id) + bytes(length) + encoded


def _produce(topic: str, schema_id: int, marker: str) -> None:
    # Fixture data needs a byte-safe writer: console producers split on newlines,
    # including 0x0A inside a valid schema ID or Avro payload.
    errors: list[KafkaError] = []

    def delivered(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            errors.append(error)

    producer = Producer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "broker.address.family": "v4",
            "acks": "all",
        }
    )
    producer.produce(topic, value=_wire_value(schema_id, marker), callback=delivered)
    if producer.flush(15) or errors:
        raise RegistryOAuthFailure("binary Registry test producer did not deliver its record")


def _create_topic(topic: str) -> None:
    _run_in_cluster(
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    )


def _delete_topic(topic: str) -> None:
    _run_in_cluster(
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--delete",
        "--topic",
        topic,
        accepted=(0, 1),
    )


def _run_in_cluster(*arguments: str, accepted: tuple[int, ...] = (0,)) -> None:
    result = subprocess.run(
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "-n",
            "kantrip-sandbox",
            "exec",
            "kantrip-dual-role-0",
            "-c",
            "kafka",
            "--",
            *arguments,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
    )
    if result.returncode not in accepted:
        raise RegistryOAuthFailure("in-cluster Registry test setup failed")


def _cleanup_confluent(resources: RegistryResources, credentials: Mapping[str, str]) -> None:
    token, _ = _token(
        _client_id("confluent", credentials),
        _client_secret("confluent", credentials),
    )
    for subject in resources.names:
        url = f"{CONFLUENT_URL}/subjects/{urllib.parse.quote(subject, safe='')}"
        _request_json(url, token, method="DELETE")
        _request_json(f"{url}?permanent=true", token, method="DELETE")


def _cleanup_apicurio(
    resources: RegistryResources,
    credentials: Mapping[str, str],
    admin_token: AdminToken,
) -> None:
    with _apicurio_admin_role(credentials, admin_token):
        token, _ = _token(
            _client_id("apicurio", credentials),
            _client_secret("apicurio", credentials),
        )
        for artifact in resources.names:
            _request_json(
                f"{APICURIO_URL}/groups/default/artifacts/{urllib.parse.quote(artifact, safe='')}",
                token,
                method="DELETE",
            )


@contextmanager
def _apicurio_admin_role(credentials: Mapping[str, str], admin_token: AdminToken) -> Iterator[None]:
    context = ssl.create_default_context(cafile=str(CA_FILE))
    token = admin_token(credentials, context)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    client_id = _client_id("apicurio", credentials)
    clients = _admin_json("/clients", headers, params={"clientId": client_id})
    if not isinstance(clients, list) or len(clients) != 1:
        raise RegistryOAuthFailure("Keycloak did not return the Apicurio client")
    internal_id = clients[0].get("id") if isinstance(clients[0], dict) else None
    if not isinstance(internal_id, str):
        raise RegistryOAuthFailure("Keycloak returned an invalid Apicurio client")
    user = _admin_json(f"/clients/{internal_id}/service-account-user", headers)
    user_id = user.get("id") if isinstance(user, dict) else None
    if not isinstance(user_id, str):
        raise RegistryOAuthFailure("Keycloak returned an invalid service account")
    role, role_created = _admin_role(headers)
    mapping = f"/users/{user_id}/role-mappings/realm"
    mappings = _admin_json(mapping, headers)
    already_granted = isinstance(mappings, list) and any(
        isinstance(item, dict) and item.get("name") == "sr-admin" for item in mappings
    )
    if not already_granted:
        _admin_request(mapping, headers, method="POST", payload=[role])
    try:
        yield
    finally:
        if not already_granted:
            _admin_request(mapping, headers, method="DELETE", payload=[role])
        if role_created:
            _admin_request("/roles/sr-admin", headers, method="DELETE")


def _admin_role(headers: Mapping[str, str]) -> tuple[dict[str, object], bool]:
    try:
        role = _admin_json("/roles/sr-admin", headers)
        if not isinstance(role, dict):
            raise RegistryOAuthFailure("Keycloak returned an invalid sr-admin role")
        return role, False
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    _admin_request("/roles", headers, method="POST", payload={"name": "sr-admin"})
    role = _admin_json("/roles/sr-admin", headers)
    if not isinstance(role, dict):
        raise RegistryOAuthFailure("Keycloak did not create sr-admin")
    return role, True


def _client_login_ids(
    credentials: Mapping[str, str], client_id: str, admin_token: AdminToken
) -> frozenset[str]:
    context = ssl.create_default_context(cafile=str(CA_FILE))
    token = admin_token(credentials, context)
    events = _admin_json(
        "/events",
        {"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"client": client_id, "type": "CLIENT_LOGIN", "max": "100"},
    )
    if not isinstance(events, list) or any(
        not isinstance(event, dict) or not isinstance(event.get("id"), str) for event in events
    ):
        raise RegistryOAuthFailure("Keycloak returned invalid client login events")
    # A long-lived sandbox can already have 100 events; count cannot grow at the cap.
    return frozenset(event["id"] for event in events)


def _admin_json(
    path: str,
    headers: Mapping[str, str],
    *,
    params: Mapping[str, str] | None = None,
) -> object:
    suffix = "" if params is None else "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(f"{KEYCLOAK_ADMIN_URL}{path}{suffix}", headers=dict(headers))
    context = ssl.create_default_context(cafile=str(CA_FILE))
    with urllib.request.urlopen(request, context=context, timeout=15) as response:
        return json.load(response)


def _admin_request(
    path: str,
    headers: Mapping[str, str],
    *,
    method: str,
    payload: object | None = None,
) -> None:
    request_headers = dict(headers)
    data = None if payload is None else json.dumps(payload).encode()
    if data is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{KEYCLOAK_ADMIN_URL}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    context = ssl.create_default_context(cafile=str(CA_FILE))
    with urllib.request.urlopen(request, context=context, timeout=15) as response:
        if response.status not in {201, 204}:
            raise RegistryOAuthFailure("Keycloak rejected temporary role management")


def _client_id(provider: str, credentials: Mapping[str, str]) -> str:
    name = (
        "KANTRIP_SANDBOX_APICURIO_CLIENT_ID"
        if provider == "apicurio"
        else "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID"
    )
    return credentials[name]


def _client_secret(provider: str, credentials: Mapping[str, str]) -> str:
    name = (
        "KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET"
        if provider == "apicurio"
        else "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET"
    )
    return credentials[name]


def _cli() -> tuple[str, ...]:
    configured = os.environ.get("KANTRIP_E2E_KANTRIP")
    executable = configured or shutil.which("kantrip")
    if executable is None:
        raise RegistryOAuthFailure("installed kantrip executable is unavailable")
    return executable, "--no-color"


def _assert_redacted(output: str, credentials: Mapping[str, str]) -> None:
    for name, value in credentials.items():
        if name.endswith(("PASSWORD", "SECRET")) and value and value in output:
            raise RegistryOAuthFailure("a credential appeared in Registry OAuth output")
    if re.search(r"eyJ[A-Za-z0-9_.-]+", output):
        raise RegistryOAuthFailure("a bearer token appeared in Registry OAuth output")


def _terminate(process: subprocess.Popen[bytes]) -> None:
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    finally:
        if process.stdout is not None:
            process.stdout.close()


__all__ = ["RegistryOAuthFailure", "exercise_registry_oauth_renewal"]
