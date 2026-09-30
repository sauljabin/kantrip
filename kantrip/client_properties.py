"""Import a Kafka connection from Java or librdkafka client properties.

The dialect comes from the keys, and a closed mapping turns the recognized keys
into an `ImportedConnection`. Security keys outside that mapping fail by name;
keys outside the security namespaces are application settings and are ignored.
Errors name keys and rules, never values.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Protocol, TypeVar

from kantrip.kafka import (
    KafkaProfileError,
    read_ca_bundle,
    read_client_certificate,
    read_private_key_text,
    valid_broker_address,
    validate_ca_bundle,
    validate_client_certificate,
    validate_client_identity,
    validate_sasl_credential,
)
from kantrip.oauth import OAuthProfileError, validate_oauth_endpoint
from kantrip.profile_imports import ImportedConnection, ProfileImportError
from kantrip.properties_syntax import (
    JaasEntry,
    PropertiesSyntaxError,
    key_name,
    parse_jaas_entries,
    parse_java_properties,
    parse_librdkafka_properties,
    reportable_key,
)
from kantrip.secret_value import Secret

LABEL = "Kafka properties"
JAVA = "Java"
LIBRDKAFKA = "librdkafka"

_SHARED_KEYS = frozenset(
    {
        "bootstrap.servers",
        "security.protocol",
        "sasl.mechanism",
        "sasl.oauthbearer.token.endpoint.url",
        "sasl.oauthbearer.scope",
        "ssl.endpoint.identification.algorithm",
        "ssl.key.password",
    }
)
_JAVA_KEYS = _SHARED_KEYS | {
    "sasl.jaas.config",
    "sasl.login.callback.handler.class",
    "sasl.oauthbearer.client.credentials.client.id",
    "sasl.oauthbearer.client.credentials.client.secret",
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.certificates",
    "ssl.keystore.type",
    "ssl.keystore.certificate.chain",
    "ssl.keystore.key",
}
_LIBRDKAFKA_KEYS = _SHARED_KEYS | {
    "sasl.mechanisms",
    "sasl.username",
    "sasl.password",
    "sasl.oauthbearer.method",
    "sasl.oauthbearer.client.id",
    "sasl.oauthbearer.client.secret",
    "ssl.ca.pem",
    "ssl.ca.location",
    "ssl.certificate.pem",
    "ssl.certificate.location",
    "ssl.key.pem",
    "ssl.key.location",
    "enable.ssl.certificate.verification",
    "https.ca.pem",
    "https.ca.location",
}
_JAVA_MARKERS = (
    "sasl.jaas.config",
    "sasl.login.callback.handler.class",
    "ssl.truststore.",
    "ssl.keystore.",
    "sasl.oauthbearer.client.credentials.",
)
_LIBRDKAFKA_MARKERS = (
    "sasl.username",
    "sasl.password",
    "sasl.mechanisms",
    "ssl.ca.",
    "ssl.certificate.",
    "ssl.key.pem",
    "ssl.key.location",
    "enable.ssl.certificate.verification",
    "sasl.oauthbearer.method",
    "sasl.oauthbearer.client.id",
    "sasl.oauthbearer.client.secret",
    "https.ca.",
)
_SECURITY_PREFIXES = ("sasl.", "ssl.", "security.", "https.", "enable.ssl.", "enable.sasl.")
_REGISTRY_PREFIXES = (
    "schema.registry.",
    "basic.auth.",
    "bearer.auth.",
    "apicurio.registry.url",
    "apicurio.registry.auth.",
    "apicurio.registry.tls.",
)
_UNSUPPORTED_REASONS = (
    ("sasl.kerberos.", "Kerberos is not supported"),
    ("sasl.login.class", "custom login classes are not supported"),
    ("sasl.client.callback.handler.class", "custom callback classes are not supported"),
    ("sasl.oauthbearer.config", "unsecured JWTs are not supported"),
    ("enable.sasl.oauthbearer.unsecure.jwt", "unsecured JWTs are not supported"),
    (
        "ssl.keystore.location",
        "use inline PEM in ssl.keystore.certificate.chain and ssl.keystore.key",
    ),
    ("ssl.keystore.password", "JKS and PKCS12 stores are not supported; use PEM"),
    ("ssl.truststore.password", "JKS and PKCS12 stores are not supported; use PEM"),
)
_PLACEHOLDER = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_PASSWORD_MECHANISMS = {
    "PLAIN": "plain",
    "SCRAM-SHA-256": "scram-sha-256",
    "SCRAM-SHA-512": "scram-sha-512",
}
_LOGIN_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "OAUTHBEARER": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
}
_OAUTH_CALLBACK_HANDLER = (
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
)
_SUPPORTED_MECHANISMS = "PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, or OAUTHBEARER"

Parsed = TypeVar("Parsed")


def parse_client_properties(text: str, *, base_directory: Path | None) -> ImportedConnection:
    """Return the Kafka connection Java or librdkafka client properties describe.

    Relative PEM paths resolve against ``base_directory``; ``None`` (stdin)
    requires absolute paths.
    """
    dialect, properties = _dialect_properties(text)
    known = {JAVA: _JAVA_KEYS, LIBRDKAFKA: _LIBRDKAFKA_KEYS}.get(dialect, _SHARED_KEYS)
    connection: dict[str, str] = {}
    ignored: list[str] = []
    for key, value in properties.items():
        if key in known:
            connection[key] = value
        else:
            _reject_security_key(key)
            ignored.append(key)
    _reject_placeholders(connection)
    if not connection:
        raise ProfileImportError(f"{LABEL} set no Kafka connection key")
    reader = _Properties(connection, base_directory)
    mapping = _JavaMapping() if dialect != LIBRDKAFKA else _LibrdkafkaMapping()
    return replace(_connection(reader, mapping), ignored_keys=tuple(ignored))


# Dialect inference


def _dialect_properties(text: str) -> tuple[str, dict[str, str]]:
    """Choose the dialect from marker keys, or accept shared keys both read alike."""
    java, java_error = _attempt(parse_java_properties, text)
    librdkafka, librdkafka_error = _attempt(parse_librdkafka_properties, text)
    for parsed in (java, librdkafka):
        _reject_mixed(parsed)
    java_marked = java is not None and _has_marker(java, _JAVA_MARKERS)
    librdkafka_marked = librdkafka is not None and _has_marker(librdkafka, _LIBRDKAFKA_MARKERS)
    if java_marked and librdkafka_marked:
        assert java is not None and librdkafka is not None
        _raise_mixed(_marker(java, _JAVA_MARKERS), _marker(librdkafka, _LIBRDKAFKA_MARKERS))
    if java is not None and java_marked:
        return JAVA, java
    if librdkafka is not None and librdkafka_marked:
        return LIBRDKAFKA, librdkafka
    _raise_marked_error(java, java_error, librdkafka, librdkafka_error)
    if java is not None and java == librdkafka:
        return "shared", java
    raise ProfileImportError(
        f"{LABEL} set only keys Java and librdkafka share, but the two read the file "
        "differently; add a dialect-specific key or rewrite it as plain key=value lines"
    )


def _attempt(
    parser: Callable[[str], dict[str, str]], text: str
) -> tuple[dict[str, str] | None, PropertiesSyntaxError | None]:
    try:
        return parser(text), None
    except PropertiesSyntaxError as error:
        return None, error


def _raise_marked_error(
    java: dict[str, str] | None,
    java_error: PropertiesSyntaxError | None,
    librdkafka: dict[str, str] | None,
    librdkafka_error: PropertiesSyntaxError | None,
) -> None:
    """Report the syntax error of the dialect whose keys the other reader found."""
    if java_error and librdkafka is not None and _has_marker(librdkafka, _JAVA_MARKERS):
        raise _syntax_error(JAVA, java_error)
    if librdkafka_error and java is not None and _has_marker(java, _LIBRDKAFKA_MARKERS):
        raise _syntax_error(LIBRDKAFKA, librdkafka_error)
    if java_error is None or librdkafka_error is None:
        return
    if str(java_error) == str(librdkafka_error):
        raise ProfileImportError(f"{LABEL} {java_error}")
    raise ProfileImportError(
        f"{LABEL} are neither valid Java properties ({java_error}) "
        f"nor valid librdkafka properties ({librdkafka_error})"
    )


def _syntax_error(dialect: str, error: PropertiesSyntaxError) -> ProfileImportError:
    return ProfileImportError(f"{LABEL} are not valid {dialect} properties: {error}")


def _has_marker(properties: Mapping[str, str], markers: tuple[str, ...]) -> bool:
    return _marker(properties, markers) is not None


def _marker(properties: Mapping[str, str], markers: tuple[str, ...]) -> str | None:
    found = sorted(key for key in properties if _matches(key, markers))
    return found[0] if found else None


def _matches(key: str, markers: tuple[str, ...]) -> bool:
    return any(
        key == marker or (marker.endswith(".") and key.startswith(marker)) for marker in markers
    )


def _reject_mixed(properties: Mapping[str, str] | None) -> None:
    if properties is None:
        return
    java = _marker(properties, _JAVA_MARKERS)
    librdkafka = _marker(properties, _LIBRDKAFKA_MARKERS)
    if java is not None and librdkafka is not None:
        _raise_mixed(java, librdkafka)


def _raise_mixed(java: str | None, librdkafka: str | None) -> None:
    """Fail naming one key of each dialect."""
    shown = [
        key if key and reportable_key(key) else "an unprintable key" for key in (java, librdkafka)
    ]
    raise ProfileImportError(
        f"{LABEL} mix Java and librdkafka keys, such as {shown[0]} and {shown[1]}; "
        "import one dialect"
    )


# Key classification


def _reject_security_key(key: str) -> None:
    """Fail for Registry and unsupported security keys; others are application keys."""
    if key.startswith(_REGISTRY_PREFIXES):
        raise ProfileImportError(
            f"{LABEL} set the Schema Registry {key_name(key)}; remove the Registry keys, "
            "then add the Registry with 'kantrip edit PROFILE --registry-url URL'"
        )
    if key.startswith(_SECURITY_PREFIXES):
        reason = next(
            (reason for prefix, reason in _UNSUPPORTED_REASONS if key.startswith(prefix)),
            "Kantrip does not import it",
        )
        raise ProfileImportError(f"{LABEL} set the unsupported {key_name(key)}; {reason}")


def _reject_placeholders(properties: Mapping[str, str]) -> None:
    for key, value in properties.items():
        if _PLACEHOLDER.search(value):
            raise ProfileImportError(
                f"{LABEL} {key_name(key)} has an unfilled {{{{ … }}}} placeholder"
            )


# Property access


class _Properties:
    """Connection keys that the mapping takes one by one; leftovers fail."""

    def __init__(
        self, values: Mapping[str, str], base_directory: Path | None, *, owner: str = ""
    ) -> None:
        self._values = dict(values)
        self._taken: set[str] = set()
        self._base = base_directory
        self._owner = owner

    def name(self, key: str) -> str:
        return f"{self._owner}{key}"

    def has(self, key: str) -> bool:
        return key in self._values

    def take(self, key: str) -> str | None:
        self._taken.add(key)
        return self._values.get(key)

    def require(self, key: str, purpose: str) -> str:
        value = self.take(key)
        if not value:
            raise ProfileImportError(f"{LABEL} need {self.name(key)} for {purpose}")
        return value

    def child(self, values: Mapping[str, str], owner: str) -> _Properties:
        return _Properties(values, self._base, owner=owner)

    def reject_unused(self, context: str) -> None:
        unused = sorted(set(self._values) - self._taken)
        if unused:
            key = unused[0]
            shown = self.name(key) if reportable_key(key) else "a key with unsupported characters"
            raise ProfileImportError(f"{LABEL} set {shown}, which {context} does not use")

    def path(self, key: str, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        if self._base is None:
            raise ProfileImportError(
                f"{LABEL} read from stdin need an absolute path in {self.name(key)}"
            )
        return self._base / path

    def pem(
        self,
        inline_key: str,
        location_key: str,
        *,
        validate: Callable[[str], Parsed],
        read: Callable[[Path], Parsed],
    ) -> Parsed | None:
        """Read PEM given inline or as a file, never both."""
        inline = self.take(inline_key)
        location = self.take(location_key)
        if inline is not None and location is not None:
            raise ProfileImportError(
                f"{LABEL} set both {self.name(inline_key)} and {self.name(location_key)}"
            )
        key = inline_key if inline is not None else location_key
        try:
            if inline is not None:
                return validate(_normalized_pem(inline))
            if location is not None:
                return read(self.path(location_key, location))
        except KafkaProfileError as error:
            raise ProfileImportError(f"{LABEL} {self.name(key)} is invalid: {error}") from error
        return None

    def secret(self, key: str, purpose: str) -> Secret:
        value = Secret(self.require(key, purpose))
        try:
            validate_sasl_credential(value)
        except KafkaProfileError as error:
            raise ProfileImportError(
                f"{LABEL} {self.name(key)} is empty or contains controls"
            ) from error
        return value


_PEM_BLOCK = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]+)-----(?P<body>[A-Za-z0-9+/=\s]*?)-----END (?P=label)-----"
)


def _normalized_pem(value: str) -> str:
    """Rewrap PEM blocks that a properties file flattened onto one line."""
    blocks = []
    for match in _PEM_BLOCK.finditer(value):
        body = "".join(match.group("body").split())
        lines = [body[index : index + 64] for index in range(0, len(body), 64)]
        label = match.group("label")
        blocks.append("\n".join((f"-----BEGIN {label}-----", *lines, f"-----END {label}-----")))
    if not blocks or _PEM_BLOCK.sub("", value).strip():
        return value
    return "\n".join(blocks) + "\n"


def _bootstrap_servers(reader: _Properties) -> tuple[str, ...] | None:
    value = reader.take("bootstrap.servers")
    if value is None:
        return None
    servers = tuple(server.strip() for server in value.split(","))
    if not all(valid_broker_address(server) for server in servers) or len(set(servers)) != len(
        servers
    ):
        raise ProfileImportError(
            f"{LABEL} bootstrap.servers must list distinct host:port addresses"
        )
    return servers


def _check_hostname_verification(reader: _Properties) -> None:
    """Reject disabled hostname verification."""
    algorithm = reader.take("ssl.endpoint.identification.algorithm")
    if algorithm is not None and algorithm.lower() != "https":
        raise ProfileImportError(
            f"{LABEL} {reader.name('ssl.endpoint.identification.algorithm')} must be https; "
            "Kantrip does not import disabled hostname verification"
        )


def _check_certificate_verification(reader: _Properties) -> None:
    """Reject librdkafka's disabled certificate verification."""
    verification = reader.take("enable.ssl.certificate.verification")
    if verification is not None and verification.lower() != "true":
        raise ProfileImportError(
            f"{LABEL} enable.ssl.certificate.verification must be true; "
            "Kantrip does not import disabled certificate verification"
        )


def _scopes(value: str | None, key: str) -> tuple[str, ...]:
    scopes = tuple(value.split()) if value else ()
    if len(set(scopes)) != len(scopes):
        raise ProfileImportError(f"{LABEL} {key} repeats a scope")
    return scopes


def _token_url(reader: _Properties) -> str:
    url = reader.require("sasl.oauthbearer.token.endpoint.url", "OAuth client credentials")
    try:
        return validate_oauth_endpoint(url)
    except OAuthProfileError as error:
        raise ProfileImportError(
            f"{LABEL} sasl.oauthbearer.token.endpoint.url must use https:// without "
            "credentials, a query, or a fragment"
        ) from error


# Connection mapping


def _connection(reader: _Properties, mapping: _Mapping) -> ImportedConnection:
    servers = _bootstrap_servers(reader)
    _check_hostname_verification(reader)
    _check_certificate_verification(reader)
    protocol = reader.take("security.protocol")
    if protocol is None:
        reader.reject_unused("a connection without security.protocol")
        return ImportedConnection(bootstrap_servers=servers)
    protocol = protocol.upper()
    if protocol == "PLAINTEXT":
        reader.reject_unused("security.protocol PLAINTEXT")
        return ImportedConnection(
            bootstrap_servers=servers, transport="plaintext", auth_type="none"
        )
    if protocol == "SASL_PLAINTEXT":
        raise ProfileImportError(
            f"{LABEL} use security.protocol SASL_PLAINTEXT, which sends credentials "
            "without TLS; Kantrip requires SASL_SSL"
        )
    if protocol not in ("SSL", "SASL_SSL"):
        raise ProfileImportError(f"{LABEL} security.protocol must be PLAINTEXT, SSL, or SASL_SSL")
    ca = mapping.trust(reader)
    if protocol == "SSL":
        authentication = mapping.identity(reader)
        context = "security.protocol SSL"
    else:
        authentication = mapping.sasl(reader)
        context = f"security.protocol SASL_SSL with {authentication.auth_type}"
    reader.reject_unused(context)
    return replace(authentication, bootstrap_servers=servers, transport="tls", ca_certificates=ca)


class _Mapping(Protocol):
    """The dialect-specific keys for trust, client identity, and SASL."""

    def trust(self, reader: _Properties) -> str | None: ...

    def identity(self, reader: _Properties) -> ImportedConnection: ...

    def sasl(self, reader: _Properties) -> ImportedConnection: ...


def _mtls(certificate: str | None, key: Secret | None, password: str | None) -> ImportedConnection:
    if certificate is None and key is None:
        return ImportedConnection(auth_type="none")
    if certificate is None or key is None:
        raise ProfileImportError(f"{LABEL} need both a client certificate and a private key")
    if password == "":
        raise ProfileImportError(f"{LABEL} ssl.key.password is empty")
    key_password = Secret(password) if password is not None else None
    try:
        validated_certificate, validated_key = validate_client_identity(
            certificate, key, password=key_password
        )
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} client identity is invalid: {error}") from error
    return ImportedConnection(
        auth_type="mtls",
        client_certificate=validated_certificate,
        private_key=validated_key,
        private_key_password=key_password,
    )


def _password_authentication(mechanism: str, username: str, password: Secret) -> ImportedConnection:
    if any(character in username for character in ("\x00", "\r", "\n")):
        raise ProfileImportError(f"{LABEL} SASL username contains controls")
    return ImportedConnection(
        auth_type=_PASSWORD_MECHANISMS[mechanism], username=username, password=password
    )


def _unsupported_mechanism(mechanism: str) -> ProfileImportError:
    if mechanism == "GSSAPI":
        return ProfileImportError(
            f"{LABEL} use SASL mechanism GSSAPI (the default without sasl.mechanism); "
            "Kerberos is not supported"
        )
    return ProfileImportError(f"{LABEL} sasl.mechanism must be {_SUPPORTED_MECHANISMS}")


class _JavaMapping(_Mapping):
    def trust(self, reader: _Properties) -> str | None:
        store_type = reader.take("ssl.truststore.type")
        present = reader.has("ssl.truststore.certificates") or reader.has("ssl.truststore.location")
        _require_pem_store(reader, "ssl.truststore.type", store_type, required=present)
        return reader.pem(
            "ssl.truststore.certificates",
            "ssl.truststore.location",
            validate=validate_ca_bundle,
            read=read_ca_bundle,
        )

    def identity(self, reader: _Properties) -> ImportedConnection:
        store_type = reader.take("ssl.keystore.type")
        chain = reader.take("ssl.keystore.certificate.chain")
        key = reader.take("ssl.keystore.key")
        present = chain is not None or key is not None
        _require_pem_store(reader, "ssl.keystore.type", store_type, required=present)
        return _mtls(
            _normalized_pem(chain) if chain is not None else None,
            Secret(_normalized_pem(key)) if key is not None else None,
            reader.take("ssl.key.password") if present else None,
        )

    def sasl(self, reader: _Properties) -> ImportedConnection:
        mechanism = reader.take("sasl.mechanism") or "GSSAPI"
        if mechanism not in _LOGIN_MODULES:
            raise _unsupported_mechanism(mechanism)
        jaas = _jaas(reader, _LOGIN_MODULES[mechanism])
        if mechanism == "OAUTHBEARER":
            return _java_oauth(reader, jaas)
        options = reader.child(jaas.options, "sasl.jaas.config option ")
        username = options.require("username", mechanism)
        password = options.secret("password", mechanism)
        options.reject_unused(f"SASL mechanism {mechanism}")
        return _password_authentication(mechanism, username, password)


def _require_pem_store(
    reader: _Properties, key: str, store_type: str | None, *, required: bool
) -> None:
    if store_type is None and not required:
        return
    if store_type != "PEM":
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} must be PEM; JKS and PKCS12 stores are not supported"
        )


def _jaas(reader: _Properties, login_module: str) -> JaasEntry:
    text = reader.require("sasl.jaas.config", "SASL")
    try:
        entries = parse_jaas_entries(text)
    except PropertiesSyntaxError as error:
        raise ProfileImportError(f"{LABEL} sasl.jaas.config {error}") from error
    if len(entries) != 1:
        raise ProfileImportError(f"{LABEL} sasl.jaas.config must hold exactly one login module")
    entry = entries[0]
    if entry.login_module != login_module:
        raise ProfileImportError(
            f"{LABEL} sasl.jaas.config must use {login_module.rsplit('.', 1)[-1]} for "
            "this sasl.mechanism; other login modules are not supported"
        )
    if entry.control_flag.lower() != "required":
        raise ProfileImportError(f"{LABEL} sasl.jaas.config must use the required control flag")
    return entry


def _java_oauth(reader: _Properties, jaas: JaasEntry) -> ImportedConnection:
    if reader.take("sasl.login.callback.handler.class") != _OAUTH_CALLBACK_HANDLER:
        raise ProfileImportError(
            f"{LABEL} OAUTHBEARER needs sasl.login.callback.handler.class "
            "OAuthBearerLoginCallbackHandler; unsecured JWTs and other callback classes "
            "are not supported"
        )
    options = reader.child(jaas.options, "sasl.jaas.config option ")
    token_url = _token_url(reader)
    client_id, client_secret = _java_client_credentials(reader, options)
    scope = _one_of(reader, "sasl.oauthbearer.scope", options, "scope")
    _check_hostname_verification(options)
    ca = _JavaMapping().trust(options)
    options.reject_unused("OAuth client credentials")
    return ImportedConnection(
        auth_type="oauth",
        oauth_token_url=token_url,
        oauth_client_id=client_id,
        oauth_scopes=_scopes(scope, "OAuth scope"),
        oauth_client_secret=client_secret,
        oauth_ca_certificates=ca,
    )


def _java_client_credentials(reader: _Properties, options: _Properties) -> tuple[str, Secret]:
    """Read the client ID and secret from properties or from JAAS options, not both."""
    prefix = "sasl.oauthbearer.client.credentials."
    in_properties = reader.has(prefix + "client.id") or reader.has(prefix + "client.secret")
    in_jaas = options.has("clientId") or options.has("clientSecret")
    if in_properties and in_jaas:
        raise ProfileImportError(
            f"{LABEL} set OAuth client credentials in both {prefix}* and sasl.jaas.config "
            "options; keep one form"
        )
    source, (id_key, secret_key) = (
        (options, ("clientId", "clientSecret"))
        if in_jaas
        else (reader, (prefix + "client.id", prefix + "client.secret"))
    )
    client_id = source.require(id_key, "OAuth client credentials")
    return client_id, source.secret(secret_key, "OAuth client credentials")


def _one_of(reader: _Properties, key: str, options: _Properties, option: str) -> str | None:
    value = reader.take(key)
    jaas_value = options.take(option)
    if value is not None and jaas_value is not None:
        raise ProfileImportError(
            f"{LABEL} set both {key} and sasl.jaas.config option {option}; keep one"
        )
    return value if value is not None else jaas_value


class _LibrdkafkaMapping(_Mapping):
    def trust(self, reader: _Properties) -> str | None:
        return reader.pem(
            "ssl.ca.pem", "ssl.ca.location", validate=validate_ca_bundle, read=read_ca_bundle
        )

    def identity(self, reader: _Properties) -> ImportedConnection:
        certificate = reader.pem(
            "ssl.certificate.pem",
            "ssl.certificate.location",
            validate=validate_client_certificate,
            read=read_client_certificate,
        )
        key = reader.pem(
            "ssl.key.pem", "ssl.key.location", validate=Secret, read=read_private_key_text
        )
        present = certificate is not None or key is not None
        return _mtls(certificate, key, reader.take("ssl.key.password") if present else None)

    def sasl(self, reader: _Properties) -> ImportedConnection:
        mechanism = reader.take("sasl.mechanism")
        alias = reader.take("sasl.mechanisms")
        if mechanism is not None and alias is not None:
            raise ProfileImportError(f"{LABEL} set both sasl.mechanism and sasl.mechanisms")
        mechanism = mechanism or alias or "GSSAPI"
        if mechanism == "OAUTHBEARER":
            return _librdkafka_oauth(reader)
        if mechanism not in _PASSWORD_MECHANISMS:
            raise _unsupported_mechanism(mechanism)
        username = reader.require("sasl.username", mechanism)
        return _password_authentication(
            mechanism, username, reader.secret("sasl.password", mechanism)
        )


def _librdkafka_oauth(reader: _Properties) -> ImportedConnection:
    if (reader.take("sasl.oauthbearer.method") or "default").lower() != "oidc":
        raise ProfileImportError(
            f"{LABEL} OAUTHBEARER needs sasl.oauthbearer.method oidc; "
            "unsecured JWTs are not supported"
        )
    token_url = _token_url(reader)
    client_id = reader.require("sasl.oauthbearer.client.id", "OAuth client credentials")
    client_secret = reader.secret("sasl.oauthbearer.client.secret", "OAuth client credentials")
    scope = reader.take("sasl.oauthbearer.scope")
    ca = reader.pem(
        "https.ca.pem", "https.ca.location", validate=validate_ca_bundle, read=read_ca_bundle
    )
    return ImportedConnection(
        auth_type="oauth",
        oauth_token_url=token_url,
        oauth_client_id=client_id,
        oauth_scopes=_scopes(scope, "sasl.oauthbearer.scope"),
        oauth_client_secret=client_secret,
        oauth_ca_certificates=ca,
    )


__all__ = ["JAVA", "LABEL", "LIBRDKAFKA", "parse_client_properties"]
