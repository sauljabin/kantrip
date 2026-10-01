"""Map the Registry keys of client properties to an imported Registry connection.

Confluent's serializers read `schema.registry.url`, `basic.auth.*` and
`bearer.auth.*` (each also accepted with a `schema.registry.` prefix), and
`schema.registry.ssl.*` for Registry trust and identity; bare `ssl.*` is
always Kafka's. Apicurio's serdes read `apicurio.registry.url`,
`apicurio.registry.auth.*`, and `apicurio.registry.tls.*`. Both clients use one
trust configuration for the Registry and its OAuth token endpoint, so an
imported OAuth Registry shares its CA with the token endpoint.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from kantrip.kafka import (
    KafkaProfileError,
    read_ca_bundle,
    read_client_certificate,
    read_private_key_text,
    validate_ca_bundle,
    validate_client_certificate,
)
from kantrip.profile_auth import RegistryAuthInput
from kantrip.profile_imports import ImportedRegistry, ProfileImportError
from kantrip.properties_syntax import reportable_key
from kantrip.property_mapping import (
    LABEL,
    PropertyReader,
    check_hostname_verification,
    client_identity,
    java_pem_identity,
    java_pem_trust,
    normalized_pem,
    split_scopes,
)
from kantrip.secret_value import Secret

_CONFLUENT_PREFIXES = ("schema.registry.", "basic.auth.", "bearer.auth.")
_APICURIO_PREFIXES = (
    "apicurio.registry.url",
    "apicurio.registry.auth.",
    "apicurio.registry.tls.",
    "apicurio.registry.proxy.",
)
_CONFLUENT_NAMESPACE = "schema.registry."
_CONFLUENT_SSL_KEYS = (
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.certificates",
    "ssl.keystore.type",
    "ssl.keystore.certificate.chain",
    "ssl.keystore.key",
    "ssl.key.password",
    "ssl.endpoint.identification.algorithm",
)
_APICURIO = "apicurio.registry."
_CERTIFICATE_MARKER = "-----BEGIN CERTIFICATE-----"


def is_registry_key(key: str) -> bool:
    """Return whether a key configures a Confluent or Apicurio Registry connection."""
    return key.startswith(_CONFLUENT_PREFIXES + _APICURIO_PREFIXES)


def parse_registry_properties(
    values: Mapping[str, str], base_directory: Path | None
) -> ImportedRegistry:
    """Return the one Registry connection that Registry keys describe."""
    confluent = sorted(key for key in values if key.startswith(_CONFLUENT_PREFIXES))
    apicurio = sorted(key for key in values if key.startswith(_APICURIO_PREFIXES))
    if confluent and apicurio:
        shown = [
            key if reportable_key(key) else "an unprintable key"
            for key in (confluent[0], apicurio[0])
        ]
        raise ProfileImportError(
            f"{LABEL} mix Confluent and Apicurio Registry keys, such as {shown[0]} and "
            f"{shown[1]}; a profile has one Registry"
        )
    if confluent:
        return _confluent_registry(values, base_directory)
    return _apicurio_registry(values, base_directory)


# Shared rules


def _registry_url(reader: PropertyReader, key: str) -> str:
    """Require one http(s) URL without credentials, a query, or a fragment."""
    url = reader.require(key, "a Registry")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise ProfileImportError(f"{LABEL} {reader.name(key)} is not a valid URL") from error
    if parsed.username is not None or parsed.password is not None:
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} holds credentials; Kantrip does not import "
            "credentials in URLs"
        )
    if (
        "," in url
        or parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} must be one http:// or https:// URL without a "
            "query or fragment"
        )
    return url


def _secured_registry(
    reader: PropertyReader, key: str, provider: str, url: str, auth: RegistryAuthInput
) -> ImportedRegistry:
    """Keep authentication and TLS off plain HTTP, and drop an empty authentication."""
    secured = auth.auth_type != "none" or auth.ca_certificates is not None
    if secured and not url.lower().startswith("https://"):
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} must use https:// for Registry authentication "
            "or TLS settings"
        )
    return ImportedRegistry(provider, url, auth if secured else None)


def _mtls(
    reader: PropertyReader,
    identity: tuple[str, Secret, Secret | None] | None,
    ca: str | None,
    *others: bool,
) -> RegistryAuthInput | None:
    """Return mTLS authentication, which excludes every other Registry authentication."""
    if identity is None:
        return None
    if any(others):
        raise ProfileImportError(
            f"{LABEL} set a Registry client certificate and other Registry credentials; "
            "a profile keeps one Registry authentication"
        )
    certificate, key, password = identity
    return RegistryAuthInput(
        "mtls",
        ca_certificates=ca,
        client_certificate=certificate,
        private_key=key,
        private_key_password=password,
    )


# Confluent


def _confluent_registry(values: Mapping[str, str], base_directory: Path | None) -> ImportedRegistry:
    reader = _confluent_reader(values, base_directory)
    url = _registry_url(reader, "url")
    check_hostname_verification(reader)
    ca = java_pem_trust(reader)
    identity = client_identity(*java_pem_identity(reader))
    basic = reader.take("basic.auth.credentials.source")
    bearer = reader.take("bearer.auth.credentials.source")
    if basic is not None and bearer is not None:
        raise ProfileImportError(
            f"{LABEL} set both {reader.name('basic.auth.credentials.source')} and "
            f"{reader.name('bearer.auth.credentials.source')}; a profile keeps one Registry "
            "authentication"
        )
    auth = _mtls(reader, identity, ca, basic is not None, bearer is not None)
    if auth is None and basic is not None:
        auth = _confluent_basic(reader, basic, ca)
    elif auth is None and bearer is not None:
        auth = _confluent_bearer(reader, bearer, ca)
    if reader.has("basic.auth.user.info") and basic is None:
        raise ProfileImportError(
            f"{LABEL} set {reader.name('basic.auth.user.info')} without "
            "basic.auth.credentials.source USER_INFO, so the client would ignore it"
        )
    reader.reject_unused("a Confluent Schema Registry connection")
    return _secured_registry(
        reader, "url", "confluent", url, auth or RegistryAuthInput("none", ca_certificates=ca)
    )


def _confluent_reader(values: Mapping[str, str], base_directory: Path | None) -> PropertyReader:
    """Strip the `schema.registry.` namespace, rejecting a key given in both spellings."""
    normalized: dict[str, str] = {}
    # Name absent keys by the spelling a file must use for them.
    names = {"url": "schema.registry.url"} | {
        key: f"{_CONFLUENT_NAMESPACE}{key}" for key in _CONFLUENT_SSL_KEYS
    }
    for key, value in values.items():
        name = key.removeprefix(_CONFLUENT_NAMESPACE) if key != "schema.registry.url" else "url"
        if name in normalized:
            raise ProfileImportError(f"{LABEL} set both {names[name]} and {key}; keep one spelling")
        normalized[name] = value
        names[name] = key
    return PropertyReader(normalized, base_directory, names=names)


def _confluent_basic(reader: PropertyReader, source: str, ca: str | None) -> RegistryAuthInput:
    if source == "URL":
        raise ProfileImportError(
            f"{LABEL} take Registry credentials from the URL; Kantrip does not import "
            "credentials in URLs, so use basic.auth.credentials.source USER_INFO"
        )
    if source == "SASL_INHERIT":
        raise ProfileImportError(
            f"{LABEL} reuse the Kafka SASL credentials for the Registry (SASL_INHERIT); "
            "a profile keeps separate Registry credentials"
        )
    if source != "USER_INFO":
        raise ProfileImportError(f"{LABEL} basic.auth.credentials.source must be USER_INFO")
    user_info = reader.require("basic.auth.user.info", "Registry Basic authentication")
    username, separator, password = user_info.partition(":")
    if not separator or not username or not password:
        raise ProfileImportError(
            f"{LABEL} {reader.name('basic.auth.user.info')} must be USER:PASSWORD"
        )
    return RegistryAuthInput(
        "basic", ca_certificates=ca, username=username, password=_secret(reader, password)
    )


def _confluent_bearer(reader: PropertyReader, source: str, ca: str | None) -> RegistryAuthInput:
    if source == "STATIC_TOKEN":
        token = reader.secret("bearer.auth.token", "a fixed Registry token")
        return RegistryAuthInput("token", ca_certificates=ca, token=token)
    if source == "SASL_OAUTHBEARER_INHERIT":
        raise ProfileImportError(
            f"{LABEL} reuse the Kafka OAuth token for the Registry "
            "(SASL_OAUTHBEARER_INHERIT); a profile keeps separate Registry credentials"
        )
    if source != "OAUTHBEARER":
        raise ProfileImportError(
            f"{LABEL} bearer.auth.credentials.source must be STATIC_TOKEN or OAUTHBEARER; "
            "custom credential providers are not supported"
        )
    purpose = "Registry OAuth client credentials"
    return RegistryAuthInput(
        "oauth",
        ca_certificates=ca,
        oauth_token_url=reader.https_url("bearer.auth.issuer.endpoint.url", purpose),
        oauth_client_id=reader.require("bearer.auth.client.id", purpose),
        oauth_client_secret=reader.secret("bearer.auth.client.secret", purpose),
        oauth_scopes=split_scopes(reader.take("bearer.auth.scope"), "bearer.auth.scope"),
        oauth_ca_certificates=ca,
        oauth_logical_cluster=reader.take("bearer.auth.logical.cluster"),
        oauth_identity_pool_id=reader.take("bearer.auth.identity.pool.id"),
    )


def _secret(reader: PropertyReader, value: str) -> Secret:
    secret = Secret(value)
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ProfileImportError(f"{LABEL} {reader.name('basic.auth.user.info')} contains controls")
    return secret


# Apicurio


def _apicurio_registry(values: Mapping[str, str], base_directory: Path | None) -> ImportedRegistry:
    reader = PropertyReader(values, base_directory)
    url = _registry_url(reader, f"{_APICURIO}url")
    if reader.take(f"{_APICURIO}url.version") not in (None, "3"):
        raise ProfileImportError(
            f"{LABEL} {_APICURIO}url.version must be 3; Kantrip uses the Core Registry API v3"
        )
    _apicurio_verification(reader)
    ca = _apicurio_trust(reader)
    identity = client_identity(*_apicurio_identity(reader), None)
    oauth = reader.has(f"{_APICURIO}auth.service.token.endpoint")
    basic = reader.has(f"{_APICURIO}auth.username")
    if oauth and basic:
        raise ProfileImportError(
            f"{LABEL} set both Apicurio OAuth and Basic credentials, and Apicurio would use "
            "only OAuth; keep one"
        )
    auth = _mtls(reader, identity, ca, oauth, basic)
    if auth is None and oauth:
        auth = _apicurio_oauth(reader, ca)
    elif auth is None and basic:
        purpose = "Registry Basic authentication"
        auth = RegistryAuthInput(
            "basic",
            ca_certificates=ca,
            username=reader.require(f"{_APICURIO}auth.username", purpose),
            password=reader.secret(f"{_APICURIO}auth.password", purpose),
        )
    reader.reject_unused("an Apicurio Registry connection")
    return _secured_registry(
        reader,
        f"{_APICURIO}url",
        "apicurio",
        url,
        auth or RegistryAuthInput("none", ca_certificates=ca),
    )


def _apicurio_verification(reader: PropertyReader) -> None:
    trust_all = reader.take(f"{_APICURIO}tls.trust-all")
    if trust_all is not None and trust_all.lower() == "true":
        raise ProfileImportError(
            f"{LABEL} {_APICURIO}tls.trust-all disables certificate verification; "
            "Kantrip does not import it"
        )
    verify_host = reader.take(f"{_APICURIO}tls.verify-host")
    if verify_host is not None and verify_host.lower() != "true":
        raise ProfileImportError(
            f"{LABEL} {_APICURIO}tls.verify-host must be true; "
            "Kantrip does not import disabled hostname verification"
        )


def _apicurio_trust(reader: PropertyReader) -> str | None:
    """Read a PEM trust store file, or PEM certificates given inline or as file paths."""
    store_type = reader.take(f"{_APICURIO}tls.truststore.type")
    location = reader.take(f"{_APICURIO}tls.truststore.location")
    certificates = reader.take(f"{_APICURIO}tls.certificates")
    if location is not None and certificates is not None:
        raise ProfileImportError(
            f"{LABEL} set both {_APICURIO}tls.truststore.location and "
            f"{_APICURIO}tls.certificates, and Apicurio would ignore the certificates"
        )
    # Apicurio defaults the store type to JKS; only a PEM store is importable.
    if (location is not None or store_type is not None) and (store_type or "").upper() != "PEM":
        raise ProfileImportError(
            f"{LABEL} {_APICURIO}tls.truststore.type must be PEM; "
            "JKS and PKCS12 stores are not supported"
        )
    key = f"{_APICURIO}tls.truststore.location" if location else f"{_APICURIO}tls.certificates"
    try:
        if location is not None:
            return read_ca_bundle(reader.path(key, location))
        if certificates is not None:
            return _apicurio_certificates(reader, key, certificates)
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} {key} is invalid: {error}") from error
    return None


def _apicurio_certificates(reader: PropertyReader, key: str, value: str) -> str:
    if _CERTIFICATE_MARKER in value:
        return validate_ca_bundle(normalized_pem(value))
    bundles = [read_ca_bundle(reader.path(key, path.strip())) for path in value.split(",")]
    return validate_ca_bundle("".join(bundles))


def _apicurio_identity(reader: PropertyReader) -> tuple[str | None, Secret | None]:
    """Read the PEM client certificate and key, each given inline or as a file path."""
    certificate_key = f"{_APICURIO}tls.client-certificate"
    private_key_key = f"{_APICURIO}tls.client-key"
    certificate = reader.take(certificate_key)
    key = reader.take(private_key_key)
    try:
        if certificate is not None:
            certificate = (
                validate_client_certificate(normalized_pem(certificate))
                if _CERTIFICATE_MARKER in certificate
                else read_client_certificate(reader.path(certificate_key, certificate))
            )
        if key is None:
            return certificate, None
        if "PRIVATE KEY-----" in key:
            return certificate, Secret(normalized_pem(key))
        return certificate, read_private_key_text(reader.path(private_key_key, key))
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} Apicurio client identity is invalid: {error}") from error


def _apicurio_oauth(reader: PropertyReader, ca: str | None) -> RegistryAuthInput:
    purpose = "Registry OAuth client credentials"
    return RegistryAuthInput(
        "oauth",
        ca_certificates=ca,
        oauth_token_url=reader.https_url(f"{_APICURIO}auth.service.token.endpoint", purpose),
        oauth_client_id=reader.require(f"{_APICURIO}auth.client.id", purpose),
        oauth_client_secret=reader.secret(f"{_APICURIO}auth.client.secret", purpose),
        oauth_scopes=split_scopes(
            reader.take(f"{_APICURIO}auth.client.scope"), f"{_APICURIO}auth.client.scope"
        ),
        oauth_ca_certificates=ca,
    )


__all__ = ["is_registry_key", "parse_registry_properties"]
