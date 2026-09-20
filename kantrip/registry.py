"""Validate profile-owned Schema Registry connections without leaking credentials."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal, cast
from urllib.parse import urlsplit, urlunsplit

from kantrip.kafka import KafkaProfileError, validate_ca_bundle, validate_client_certificate
from kantrip.secret_store import SecretStore, SecretStoreError, parse_secret_reference

RegistryProvider = Literal["apicurio", "confluent"]
RegistryAuthType = Literal["none", "basic", "token", "mtls", "oauth"]
APICURIO_PROVIDER: RegistryProvider = "apicurio"
CONFLUENT_PROVIDER: RegistryProvider = "confluent"
APICURIO_URL_PROPERTY = "apicurio.registry.url"
CONFLUENT_URL_PROPERTY = "schema.registry.url"


class RegistryProfileError(ValueError):
    """Raised when Registry connection metadata is unsafe or unsupported."""


@dataclass(frozen=True)
class OAuthConnection:
    """One client-credentials token endpoint owned by a single service."""

    token_url: str
    client_id: str
    scopes: tuple[str, ...]
    client_secret_reference: str
    ca_certificates: str | None = None
    logical_cluster: str | None = None
    identity_pool_id: str | None = None
    client_secret: str | None = None


@dataclass(frozen=True)
class RegistryConnection:
    """Validated provider-specific Registry connection metadata."""

    provider: RegistryProvider
    url: str
    property_name: str
    auth_type: RegistryAuthType = "none"
    ca_certificates: str | None = None
    username: str | None = None
    password_reference: str | None = None
    token_reference: str | None = None
    client_certificate: str | None = None
    private_key_reference: str | None = None
    private_key_password_reference: str | None = None
    oauth: OAuthConnection | None = None
    password: str | None = None
    token: str | None = None
    private_key: str | None = None
    private_key_password: str | None = None

    @property
    def display_name(self) -> str:
        return (
            "Apicurio Registry"
            if self.provider == APICURIO_PROVIDER
            else "Confluent Schema Registry"
        )

    @property
    def requires_secrets(self) -> bool:
        return self.auth_type != "none"


def registry_connection(profile: Mapping[str, Any]) -> RegistryConnection | None:
    """Parse the strict supported Registry profile shape."""
    registry = profile.get("registry")
    if registry is None:
        return None
    provider, property_name, url, auth, selected_auth = _registry_basics(registry)
    tls = registry.get("tls")
    ca_certificates, client_certificate = _parse_tls(tls)
    if client_certificate is not None and selected_auth != "mtls":
        raise RegistryProfileError("Registry client certificates require mTLS authentication")
    _validate_url(url, secure=selected_auth != "none" or tls is not None)
    if url.startswith("http://"):
        if selected_auth != "none" or tls is not None:
            raise RegistryProfileError(
                "HTTP Registry connections require auth none and no TLS material"
            )
        return RegistryConnection(provider, url, property_name)
    connection = RegistryConnection(
        provider,
        url,
        property_name,
        selected_auth,
        ca_certificates,
        client_certificate=client_certificate,
    )
    return _with_authentication(connection, auth, profile)


def _registry_basics(
    registry: object,
) -> tuple[RegistryProvider, str, str, Mapping[str, Any], RegistryAuthType]:
    if not isinstance(registry, Mapping):
        raise RegistryProfileError("registry must be an object in the selected Kantrip profile")
    provider_value = registry.get("provider")
    if provider_value not in {APICURIO_PROVIDER, CONFLUENT_PROVIDER}:
        raise RegistryProfileError("registry.provider must be confluent or apicurio")
    provider = cast(RegistryProvider, provider_value)
    property_name = (
        APICURIO_URL_PROPERTY if provider == APICURIO_PROVIDER else CONFLUENT_URL_PROPERTY
    )
    allowed = {"provider", property_name, "tls", "auth"}
    unknown = sorted(str(key) for key in registry if key not in allowed)
    if unknown:
        raise RegistryProfileError(
            f"registry contains properties incompatible with provider {provider}: {', '.join(unknown)}"
        )
    url = registry.get(property_name)
    if not isinstance(url, str):
        raise RegistryProfileError(f"registry requires {property_name}")
    auth = registry.get("auth", {"type": "none"})
    if not isinstance(auth, Mapping):
        raise RegistryProfileError("Registry authentication configuration is invalid")
    auth_type = auth.get("type")
    if auth_type not in {"none", "basic", "token", "mtls", "oauth"}:
        raise RegistryProfileError("Registry authentication type is not supported")
    selected_auth = cast(RegistryAuthType, auth_type)
    return provider, property_name, url, auth, selected_auth


def plain_registry_connection(profile: Mapping[str, Any]) -> RegistryConnection | None:
    """Backward-compatible name for parsing a Registry connection."""
    return registry_connection(profile)


def _with_authentication(
    connection: RegistryConnection,
    auth: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> RegistryConnection:
    if connection.auth_type == "none":
        _reject_auth_properties(auth, {"type"})
        return connection
    profile_id = profile.get("id")
    if not isinstance(profile_id, str):
        raise RegistryProfileError("Registry profile identity is invalid")
    return _authenticated_connection(connection, auth, profile_id)


def _authenticated_connection(
    connection: RegistryConnection,
    auth: Mapping[str, Any],
    profile_id: str,
) -> RegistryConnection:
    if connection.auth_type == "basic":
        return _with_basic(connection, auth, profile_id)
    if connection.auth_type == "token":
        _require_confluent_token(connection)
        return _with_token(connection, auth, profile_id)
    if connection.auth_type == "mtls":
        return _with_mtls(connection, auth, profile_id)
    return _with_oauth(connection, auth, profile_id)


def _require_confluent_token(connection: RegistryConnection) -> None:
    if connection.provider != CONFLUENT_PROVIDER:
        raise RegistryProfileError(
            "fixed Registry tokens are supported only for Confluent-compatible Registry"
        )


def resolve_registry_connection(
    connection: RegistryConnection, store: SecretStore
) -> RegistryConnection:
    """Resolve exact Registry credentials only after validation."""
    try:
        if connection.auth_type == "basic":
            assert connection.password_reference is not None
            return replace(connection, password=_secret(store, connection.password_reference))
        if connection.auth_type == "token":
            assert connection.token_reference is not None
            return replace(connection, token=_secret(store, connection.token_reference))
        if connection.auth_type == "mtls":
            assert connection.private_key_reference is not None
            password = (
                _secret(store, connection.private_key_password_reference)
                if connection.private_key_password_reference
                else None
            )
            return replace(
                connection,
                private_key=_secret(store, connection.private_key_reference),
                private_key_password=password,
            )
        if connection.auth_type == "oauth":
            assert connection.oauth is not None
            return replace(
                connection,
                oauth=replace(
                    connection.oauth,
                    client_secret=_secret(store, connection.oauth.client_secret_reference),
                ),
            )
    except SecretStoreError as error:
        raise RegistryProfileError("Registry credentials could not be resolved") from error
    return connection


def display_registry(profile: Mapping[str, Any]) -> str:
    """Return a compact redacted Registry label for profile listings."""
    registry = profile.get("registry")
    if not isinstance(registry, Mapping):
        return "-"
    provider = registry.get("provider")
    property_name = (
        APICURIO_URL_PROPERTY if provider == APICURIO_PROVIDER else CONFLUENT_URL_PROPERTY
    )
    url = registry.get(property_name)
    if not isinstance(url, str):
        return "-"
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "-"
    redacted_url = urlunsplit(
        (parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, "", "")
    )
    name = "Apicurio" if provider == APICURIO_PROVIDER else "Confluent"
    return f"{name}: {redacted_url}"


def _parse_tls(tls: object) -> tuple[str | None, str | None]:
    if tls is None:
        return None, None
    if not isinstance(tls, Mapping) or set(tls) - {"caCertificates", "clientCertificate"}:
        raise RegistryProfileError("Registry TLS configuration is invalid")
    ca, certificate = tls.get("caCertificates"), tls.get("clientCertificate")
    try:
        return (
            validate_ca_bundle(ca) if ca is not None else None,
            validate_client_certificate(certificate) if certificate is not None else None,
        )
    except KafkaProfileError as error:
        raise RegistryProfileError(str(error).replace("Kafka", "Registry")) from error


def _with_basic(
    connection: RegistryConnection, auth: Mapping[str, Any], profile_id: str
) -> RegistryConnection:
    _reject_auth_properties(auth, {"type", "username", "passwordRef"})
    username, reference = auth.get("username"), auth.get("passwordRef")
    if not isinstance(username, str) or not username or not isinstance(reference, str):
        raise RegistryProfileError("Registry basic authentication configuration is invalid")
    _validate_reference(reference, profile_id, "registry/password")
    return replace(connection, username=username, password_reference=reference)


def _with_token(
    connection: RegistryConnection, auth: Mapping[str, Any], profile_id: str
) -> RegistryConnection:
    _reject_auth_properties(auth, {"type", "tokenRef"})
    reference = auth.get("tokenRef")
    if not isinstance(reference, str):
        raise RegistryProfileError("Registry token authentication configuration is invalid")
    _validate_reference(reference, profile_id, "registry/token")
    return replace(connection, token_reference=reference)


def _with_mtls(
    connection: RegistryConnection, auth: Mapping[str, Any], profile_id: str
) -> RegistryConnection:
    _reject_auth_properties(auth, {"type", "privateKeyRef", "privateKeyPasswordRef"})
    if connection.client_certificate is None:
        raise RegistryProfileError("Registry mTLS requires a client certificate")
    reference, password_reference = auth.get("privateKeyRef"), auth.get("privateKeyPasswordRef")
    if not isinstance(reference, str):
        raise RegistryProfileError("Registry mTLS private key reference is invalid")
    _validate_reference(reference, profile_id, "registry/tls/private-key")
    if password_reference is not None:
        if not isinstance(password_reference, str):
            raise RegistryProfileError("Registry mTLS key password reference is invalid")
        _validate_reference(password_reference, profile_id, "registry/tls/private-key-password")
    return replace(
        connection,
        private_key_reference=reference,
        private_key_password_reference=password_reference,
    )


def _with_oauth(
    connection: RegistryConnection, auth: Mapping[str, Any], profile_id: str
) -> RegistryConnection:
    _reject_auth_properties(
        auth,
        {
            "type",
            "tokenUrl",
            "clientId",
            "scopes",
            "clientSecretRef",
            "caCertificates",
            "logicalCluster",
            "identityPoolId",
        },
    )
    token_url, client_id, reference = (
        auth.get("tokenUrl"),
        auth.get("clientId"),
        auth.get("clientSecretRef"),
    )
    scopes = auth.get("scopes", [])
    if (
        not isinstance(token_url, str)
        or not isinstance(client_id, str)
        or not client_id
        or not isinstance(reference, str)
        or not isinstance(scopes, list)
        or not all(isinstance(scope, str) and scope for scope in scopes)
        or len(scopes) != len(set(scopes))
    ):
        raise RegistryProfileError("Registry OAuth configuration is invalid")
    _validate_url(token_url, secure=True)
    _validate_reference(reference, profile_id, "registry/oauth/client-secret")
    ca = auth.get("caCertificates")
    try:
        ca_certificates = validate_ca_bundle(ca) if ca is not None else None
    except KafkaProfileError as error:
        raise RegistryProfileError("Registry OAuth CA bundle is invalid") from error
    logical_cluster, identity_pool_id = auth.get("logicalCluster"), auth.get("identityPoolId")
    if connection.provider != CONFLUENT_PROVIDER and (
        logical_cluster is not None or identity_pool_id is not None
    ):
        raise RegistryProfileError(
            "Registry OAuth routing metadata requires a Confluent-compatible Registry"
        )
    if not all(
        value is None or isinstance(value, str) and value
        for value in (logical_cluster, identity_pool_id)
    ):
        raise RegistryProfileError("Registry OAuth routing metadata is invalid")
    return replace(
        connection,
        oauth=OAuthConnection(
            token_url,
            client_id,
            tuple(scopes),
            reference,
            ca_certificates,
            logical_cluster,
            identity_pool_id,
        ),
    )


def _validate_url(url: str, *, secure: bool) -> None:
    try:
        parsed, port = urlsplit(url), urlsplit(url).port
    except ValueError as error:
        raise RegistryProfileError("Registry URL is invalid") from error
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise RegistryProfileError(
            "Registry URL must not contain credentials, a query, or a fragment"
        )
    if secure and parsed.scheme != "https":
        raise RegistryProfileError(
            "authenticated or TLS Registry connections require an https:// URL"
        )


def _validate_reference(reference: str, profile_id: str, field: str) -> None:
    try:
        parsed = parse_secret_reference(reference)
    except SecretStoreError as error:
        raise RegistryProfileError("Registry credential reference is invalid") from error
    if parsed.profile_id != profile_id or parsed.field != field:
        raise RegistryProfileError("Registry credential reference does not match its profile")


def _reject_auth_properties(auth: Mapping[str, Any], allowed: set[str]) -> None:
    if set(auth) - allowed:
        raise RegistryProfileError(
            "Registry authentication configuration has unsupported properties"
        )


def _secret(store: SecretStore, reference: str) -> str:
    value = store.get(reference)
    if not value or any(character in value for character in ("\x00", "\r", "\n")):
        raise RegistryProfileError("Registry credentials are empty or contain unsupported controls")
    return value


__all__ = [
    "APICURIO_PROVIDER",
    "APICURIO_URL_PROPERTY",
    "CONFLUENT_PROVIDER",
    "CONFLUENT_URL_PROPERTY",
    "OAuthConnection",
    "RegistryAuthType",
    "RegistryConnection",
    "RegistryProfileError",
    "RegistryProvider",
    "display_registry",
    "plain_registry_connection",
    "registry_connection",
    "resolve_registry_connection",
]
