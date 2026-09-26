"""Collect command-line options and no-echo secrets into typed profile inputs."""

from __future__ import annotations

import getpass
import os
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, cast

import click

from kantrip.kafka import KafkaProfileError, read_client_certificate, read_private_key
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.secret_value import Secret

PASSWORD_AUTH_TYPES = frozenset({"plain", "scram-sha-256", "scram-sha-512"})
_KAFKA_SECRET_FIELDS = frozenset(
    {
        "kafka/password",
        "kafka/oauth/client-secret",
        "kafka/tls/private-key",
        "kafka/tls/private-key-password",
    }
)
_REGISTRY_SECRET_FIELDS = frozenset(
    {
        "registry/password",
        "registry/token",
        "registry/tls/private-key",
        "registry/tls/private-key-password",
        "registry/oauth/client-secret",
    }
)
# `--replace-secret FIELD`: the public dotted name → the internal credential field,
# which is also the suffix of the secret's keychain reference.
SECRET_FIELDS = {
    "kafka.auth.password": "kafka/password",
    "kafka.auth.private-key": "kafka/tls/private-key",
    "kafka.auth.private-key-password": "kafka/tls/private-key-password",
    "kafka.auth.oauth.client-secret": "kafka/oauth/client-secret",
    "registry.auth.password": "registry/password",
    "registry.auth.token": "registry/token",
    "registry.auth.private-key": "registry/tls/private-key",
    "registry.auth.private-key-password": "registry/tls/private-key-password",
    "registry.auth.oauth.client-secret": "registry/oauth/client-secret",
}
# `--unset FIELD`: optional fields `edit` can make absent, plus `labels.KEY`.
UNSET_FIELDS = (
    "description",
    "kafka.tls.ca",
    "kafka.auth.oauth.scopes",
    "kafka.auth.oauth.ca",
    "registry",
    "registry.tls.ca",
    "registry.auth.oauth.scopes",
    "registry.auth.oauth.ca",
    "registry.auth.oauth.logical-cluster",
    "registry.auth.oauth.identity-pool-id",
)
LABEL_FIELD_PREFIX = "labels."
# `edit` options (and unset fields) whose presence requests an authentication change.
_KAFKA_AUTH_OPTIONS = (
    "auth_type",
    "username",
    "client_certificate_file",
    "client_key_file",
    "oauth_token_url",
    "oauth_client_id",
    "oauth_scope",
    "oauth_ca_file",
)
_KAFKA_AUTH_UNSETS = ("kafka.auth.oauth.scopes", "kafka.auth.oauth.ca")
_REGISTRY_OAUTH_OPTIONS = (
    "registry_oauth_token_url",
    "registry_oauth_client_id",
    "registry_oauth_scope",
    "registry_oauth_ca_file",
    "registry_oauth_logical_cluster",
    "registry_oauth_identity_pool_id",
)
_REGISTRY_OAUTH_UNSETS = (
    "registry.auth.oauth.scopes",
    "registry.auth.oauth.ca",
    "registry.auth.oauth.logical-cluster",
    "registry.auth.oauth.identity-pool-id",
)
_REGISTRY_AUTH_OPTIONS = (
    "registry_auth",
    "registry_username",
    "registry_client_certificate_file",
    "registry_client_key_file",
    "registry_ca_file",
    *_REGISTRY_OAUTH_OPTIONS,
)
_REGISTRY_AUTH_UNSETS = ("registry.tls.ca", *_REGISTRY_OAUTH_UNSETS)
# An unset field and the `edit` option that sets the same field.
_EDIT_CONFLICTS = (
    ("description", "description"),
    ("kafka.tls.ca", "ca_file"),
    ("kafka.auth.oauth.scopes", "oauth_scope"),
    ("kafka.auth.oauth.ca", "oauth_ca_file"),
    ("registry.tls.ca", "registry_ca_file"),
    ("registry.auth.oauth.ca", "registry_oauth_ca_file"),
    ("registry.auth.oauth.scopes", "registry_oauth_scope"),
    ("registry.auth.oauth.logical-cluster", "registry_oauth_logical_cluster"),
    ("registry.auth.oauth.identity-pool-id", "registry_oauth_identity_pool_id"),
)


@dataclass(frozen=True)
class AddOptions:
    """Every `add` option, named exactly as Click passes it."""

    bootstrap_servers: tuple[str, ...]
    description: str | None
    labels: dict[str, str]
    transport: str
    ca_file: str | None
    auth_type: str
    username: str | None
    client_certificate_file: Path | None
    client_key_file: Path | None
    oauth_token_url: str | None
    oauth_client_id: str | None
    oauth_scope: tuple[str, ...]
    oauth_ca_file: str | None
    registry_provider: str | None
    registry_url: str | None
    registry_auth: str
    registry_username: str | None
    registry_client_certificate_file: Path | None
    registry_client_key_file: Path | None
    registry_ca_file: str | None
    registry_oauth_token_url: str | None
    registry_oauth_client_id: str | None
    registry_oauth_scope: tuple[str, ...]
    registry_oauth_ca_file: str | None
    registry_oauth_logical_cluster: str | None
    registry_oauth_identity_pool_id: str | None


@dataclass(frozen=True)
class EditOptions:
    """Every `edit` option, named exactly as Click passes it."""

    bootstrap_servers: tuple[str, ...] | None
    description: str | None
    labels: dict[str, str]
    unset_fields: tuple[str, ...]
    transport: str | None
    ca_file: str | None
    auth_type: str | None
    username: str | None
    client_certificate_file: Path | None
    client_key_file: Path | None
    oauth_token_url: str | None
    oauth_client_id: str | None
    oauth_scope: tuple[str, ...]
    oauth_ca_file: str | None
    replace_secrets: tuple[str, ...]
    registry_provider: str | None
    registry_url: str | None
    registry_auth: str | None
    registry_username: str | None
    registry_client_certificate_file: Path | None
    registry_client_key_file: Path | None
    registry_ca_file: str | None
    registry_oauth_token_url: str | None
    registry_oauth_client_id: str | None
    registry_oauth_scope: tuple[str, ...]
    registry_oauth_ca_file: str | None
    registry_oauth_logical_cluster: str | None
    registry_oauth_identity_pool_id: str | None

    @property
    def has_changes(self) -> bool:
        """Return whether any option was supplied."""
        return any(_provided(getattr(self, option.name)) for option in fields(self))

    def any_supplied(self, names: tuple[str, ...], unsets: tuple[str, ...] = ()) -> bool:
        """Return whether any of the named options or unset fields was supplied."""
        return any(_provided(getattr(self, name)) for name in names) or any(
            self.unsets(field) for field in unsets
        )

    def unsets(self, field: str) -> bool:
        """Return whether `--unset FIELD` was supplied."""
        return field in self.unset_fields

    @property
    def removed_labels(self) -> tuple[str, ...]:
        """Return the label keys named by `--unset labels.KEY`."""
        return tuple(
            field.removeprefix(LABEL_FIELD_PREFIX)
            for field in self.unset_fields
            if field.startswith(LABEL_FIELD_PREFIX)
        )


def _provided(value: object) -> bool:
    """Return whether an option was supplied; unset flags and empty repeats were not."""
    return value is not None and value is not False and value != () and value != {}


def parse_unset_fields(
    context: click.Context,
    parameter: click.Parameter,
    values: tuple[str, ...],
) -> tuple[str, ...]:
    """Validate `--unset FIELD` names against the closed table."""
    del context, parameter
    if len(set(values)) != len(values):
        raise click.BadParameter("each field may be named only once")
    for value in values:
        label = value.removeprefix(LABEL_FIELD_PREFIX)
        if value not in UNSET_FIELDS and not (value.startswith(LABEL_FIELD_PREFIX) and label):
            valid = ", ".join((*UNSET_FIELDS, f"{LABEL_FIELD_PREFIX}KEY"))
            raise click.BadParameter(f"unknown field '{value}'; valid fields: {valid}")
    return values


def parse_secret_fields(
    context: click.Context,
    parameter: click.Parameter,
    values: tuple[str, ...],
) -> tuple[str, ...]:
    """Map `--replace-secret FIELD` names to internal credential fields."""
    del context, parameter
    if len(set(values)) != len(values):
        raise click.BadParameter("each field may be named only once")
    for value in values:
        if value not in SECRET_FIELDS:
            valid = ", ".join(SECRET_FIELDS)
            raise click.BadParameter(f"unknown field '{value}'; valid fields: {valid}")
    return tuple(SECRET_FIELDS[value] for value in values)


def add_authentication(options: AddOptions) -> tuple[KafkaAuthInput, RegistryAuthInput | None]:
    """Build the Kafka and Registry authentication inputs for `add`."""
    auth = kafka_auth_input(
        options.auth_type,
        options.username,
        options.client_certificate_file,
        options.client_key_file,
        password_required=options.auth_type in PASSWORD_AUTH_TYPES,
        oauth_token_url=options.oauth_token_url,
        oauth_client_id=options.oauth_client_id,
        oauth_scopes=options.oauth_scope if options.auth_type == "oauth" else None,
        oauth_ca_certificates=options.oauth_ca_file,
        oauth_secret_required=options.auth_type == "oauth",
    )
    certificate, key, key_password = _registry_identity(
        options.registry_client_certificate_file, options.registry_client_key_file, None
    )
    registry = registry_auth_input(
        options.registry_auth,
        options.registry_username,
        registry_url=options.registry_url,
        ca_certificates=options.registry_ca_file,
        client_certificate=certificate,
        private_key=key,
        private_key_password=key_password,
        oauth_token_url=options.registry_oauth_token_url,
        oauth_client_id=options.registry_oauth_client_id,
        oauth_scopes=options.registry_oauth_scope,
        oauth_ca_certificates=options.registry_oauth_ca_file,
        oauth_logical_cluster=options.registry_oauth_logical_cluster,
        oauth_identity_pool_id=options.registry_oauth_identity_pool_id,
    )
    return auth, registry


def validate_edit_options(options: EditOptions) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Reject conflicting `edit` options and split secret replacements by service."""
    for field, option in _EDIT_CONFLICTS:
        if options.unsets(field) and _provided(getattr(options, option)):
            raise click.UsageError(
                f"--unset {field} cannot be combined with --{option.replace('_', '-')}"
            )
    removed = set(options.removed_labels).intersection(options.labels)
    if removed:
        raise click.UsageError(
            f"--unset labels.{min(removed)} cannot be combined with --label {min(removed)}=VALUE"
        )
    replace = options.replace_secrets
    kafka = tuple(field for field in replace if field.startswith("kafka/"))
    registry = tuple(field for field in replace if field.startswith("registry/"))
    return kafka, registry


def edit_kafka_authentication(
    options: EditOptions,
    current_auth: Mapping[str, Any],
    replace_fields: tuple[str, ...],
) -> KafkaAuthInput | None:
    """Build a Kafka authentication change for `edit`, or ``None`` when none was requested."""
    if not options.any_supplied(_KAFKA_AUTH_OPTIONS, _KAFKA_AUTH_UNSETS) and not replace_fields:
        return None
    current_type = current_auth.get("type")
    selected = options.auth_type or str(current_auth["type"])
    if options.unsets("kafka.auth.oauth.scopes"):
        scopes: tuple[str, ...] | None = ()
    else:
        scopes = options.oauth_scope or None
    return kafka_auth_input(
        selected,
        options.username,
        options.client_certificate_file,
        options.client_key_file,
        password_required=(
            selected in PASSWORD_AUTH_TYPES and current_type not in PASSWORD_AUTH_TYPES
        ),
        replace_fields=replace_fields,
        oauth_token_url=options.oauth_token_url,
        oauth_client_id=options.oauth_client_id,
        oauth_scopes=scopes,
        oauth_ca_certificates=options.oauth_ca_file,
        oauth_default_trust=options.unsets("kafka.auth.oauth.ca"),
        oauth_secret_required=selected == "oauth" and current_type != "oauth",
    )


def edit_registry_authentication(
    options: EditOptions,
    current_registry: object,
    replace_fields: tuple[str, ...],
) -> RegistryAuthInput | None:
    """Build a Registry authentication change for `edit`, or ``None`` when none was requested."""
    if (
        not options.any_supplied(_REGISTRY_AUTH_OPTIONS, _REGISTRY_AUTH_UNSETS)
        and not replace_fields
    ):
        return None
    stored_url, current_auth, current_tls = _stored_registry(current_registry)
    selected = options.registry_auth or str(current_auth.get("type", "none"))
    # Stored fields of the current type carry over only when the type is unchanged.
    same_type = selected == current_auth.get("type")
    stored_auth: Mapping[str, Any] = current_auth if same_type else {}
    if selected != "oauth" and options.any_supplied(
        _REGISTRY_OAUTH_OPTIONS, _REGISTRY_OAUTH_UNSETS
    ):
        raise click.UsageError("Registry OAuth options require final --registry-auth oauth")
    certificate, key, key_password = _registry_identity(
        options.registry_client_certificate_file,
        options.registry_client_key_file,
        current_tls.get("clientCertificate") if same_type else None,
    )
    registry = registry_auth_input(
        selected,
        options.registry_username or stored_auth.get("username"),
        registry_url=options.registry_url or stored_url,
        ca_certificates=_replaced(
            options.registry_ca_file,
            options.unsets("registry.tls.ca"),
            current_tls.get("caCertificates"),
        ),
        client_certificate=certificate,
        private_key=key,
        private_key_password=key_password,
        oauth_token_url=options.registry_oauth_token_url or stored_auth.get("tokenUrl"),
        oauth_client_id=options.registry_oauth_client_id or stored_auth.get("clientId"),
        oauth_scopes=_edited_scopes(options, stored_auth),
        oauth_ca_certificates=_replaced(
            options.registry_oauth_ca_file,
            options.unsets("registry.auth.oauth.ca"),
            stored_auth.get("caCertificates"),
        ),
        oauth_logical_cluster=_cleared(
            options.unsets("registry.auth.oauth.logical-cluster"),
            options.registry_oauth_logical_cluster,
            stored_auth.get("logicalCluster"),
        ),
        oauth_identity_pool_id=_cleared(
            options.unsets("registry.auth.oauth.identity-pool-id"),
            options.registry_oauth_identity_pool_id,
            stored_auth.get("identityPoolId"),
        ),
        replace_fields=replace_fields,
        secret_required=selected != current_auth.get("type"),
    )
    if registry is None and options.registry_auth == "none":
        return RegistryAuthInput("none")
    return registry


def _stored_registry(registry: object) -> tuple[str | None, dict[str, Any], dict[str, Any]]:
    if not isinstance(registry, dict):
        return None, {"type": "none"}, {}
    url_key = (
        "apicurio.registry.url" if registry.get("provider") == "apicurio" else "schema.registry.url"
    )
    auth = registry.get("auth")
    tls = registry.get("tls")
    return (
        registry.get(url_key),
        auth if isinstance(auth, dict) else {"type": "none"},
        tls if isinstance(tls, dict) else {},
    )


def _replaced(value: str | None, reset: bool, stored: object) -> Any:
    if reset:
        return None
    return value if value is not None else stored


def _cleared(clear: bool, value: str | None, stored: object) -> Any:
    return None if clear else value or stored


def _edited_scopes(options: EditOptions, stored_auth: Mapping[str, Any]) -> tuple[str, ...]:
    if options.unsets("registry.auth.oauth.scopes"):
        return ()
    return options.registry_oauth_scope or tuple(stored_auth.get("scopes", ()))


def _registry_identity(
    certificate_path: Path | None,
    key_path: Path | None,
    stored_certificate: Any,
) -> tuple[Any, Secret | None, Secret | None]:
    if (certificate_path is None) != (key_path is None):
        raise click.UsageError("Registry mTLS requires both client certificate and key files")
    if certificate_path is None or key_path is None:
        return stored_certificate, None, None
    return read_client_identity(certificate_path, key_path, label="Registry")


def kafka_auth_input(
    auth_type: str,
    username: str | None,
    certificate_path: Path | None,
    key_path: Path | None,
    *,
    password_required: bool,
    replace_fields: tuple[str, ...] = (),
    oauth_token_url: str | None = None,
    oauth_client_id: str | None = None,
    oauth_scopes: tuple[str, ...] | None = None,
    oauth_ca_certificates: str | None = None,
    oauth_default_trust: bool = False,
    oauth_secret_required: bool = False,
) -> KafkaAuthInput:
    """Validate Kafka authentication options and collect required secrets."""
    if len(set(replace_fields)) != len(replace_fields):
        raise click.UsageError("--replace-secret fields must be unique")
    _reject_unknown_fields(replace_fields, _KAFKA_SECRET_FIELDS)
    oauth_values = (oauth_token_url, oauth_client_id, oauth_scopes, oauth_ca_certificates)
    has_oauth_options = any(value is not None for value in oauth_values) or oauth_default_trust
    if auth_type != "oauth" and has_oauth_options:
        raise click.UsageError("Kafka OAuth options require --auth oauth")
    if auth_type in PASSWORD_AUTH_TYPES:
        return _password_auth_input(
            auth_type,
            username,
            certificate_path,
            key_path,
            password_required=password_required,
            replace_fields=replace_fields,
        )
    if auth_type == "mtls":
        return _mtls_auth_input(certificate_path, key_path, replace_fields=replace_fields)
    if auth_type == "oauth":
        if username is not None or certificate_path is not None or key_path is not None:
            raise click.UsageError("Kafka OAuth cannot include password or mTLS options")
        if set(replace_fields) - {"kafka/oauth/client-secret"}:
            raise click.UsageError("Kafka OAuth cannot replace a non-OAuth secret")
        client_secret = None
        if oauth_secret_required or replace_fields:
            client_secret = required_secret_prompt("Kafka OAuth client secret")
        return KafkaAuthInput(
            "oauth",
            oauth_token_url=oauth_token_url,
            oauth_client_id=oauth_client_id,
            oauth_scopes=oauth_scopes,
            oauth_client_secret=client_secret,
            oauth_ca_certificates=oauth_ca_certificates,
            oauth_default_trust=oauth_default_trust,
        )
    # OAuth options were already rejected above for every non-OAuth type.
    if replace_fields or username or certificate_path is not None or key_path is not None:
        raise click.UsageError("Kafka auth none cannot include credential options")
    return KafkaAuthInput("none")


def registry_auth_input(
    auth_type: str,
    username: str | None,
    *,
    registry_url: str | None,
    ca_certificates: str | None = None,
    client_certificate: str | None = None,
    private_key: Secret | None = None,
    private_key_password: Secret | None = None,
    oauth_token_url: str | None = None,
    oauth_client_id: str | None = None,
    oauth_scopes: tuple[str, ...] | None = None,
    oauth_ca_certificates: str | None = None,
    oauth_logical_cluster: str | None = None,
    oauth_identity_pool_id: str | None = None,
    replace_fields: tuple[str, ...] = (),
    secret_required: bool = True,
) -> RegistryAuthInput | None:
    """Collect Registry secrets only from the controlling terminal."""
    values = _RegistryValues(
        auth_type,
        username,
        ca_certificates,
        client_certificate,
        private_key,
        private_key_password,
        oauth_token_url,
        oauth_client_id,
        oauth_scopes,
        oauth_ca_certificates,
        oauth_logical_cluster,
        oauth_identity_pool_id,
        replace_fields,
        secret_required,
    )
    _reject_unknown_fields(replace_fields, _REGISTRY_SECRET_FIELDS)
    values.reject_foreign_options()
    if not values.requested:
        return None
    if registry_url is None:
        raise click.UsageError("Registry authentication options require --registry-url")
    if auth_type == "none":
        return values.none_input()
    if auth_type == "basic":
        return values.basic_input()
    if auth_type == "token":
        return values.token_input()
    if auth_type == "mtls":
        return values.mtls_input()
    if auth_type == "oauth":
        return values.oauth_input()
    raise click.UsageError("unsupported Registry authentication type")


@dataclass(frozen=True)
class _RegistryValues:
    """Resolved Registry authentication values awaiting per-type validation."""

    auth_type: str
    username: str | None
    ca_certificates: str | None
    client_certificate: str | None
    private_key: Secret | None
    private_key_password: Secret | None
    oauth_token_url: str | None
    oauth_client_id: str | None
    oauth_scopes: tuple[str, ...] | None
    oauth_ca_certificates: str | None
    oauth_logical_cluster: str | None
    oauth_identity_pool_id: str | None
    replace_fields: tuple[str, ...]
    secret_required: bool

    @property
    def requested(self) -> bool:
        return any(
            (
                self.auth_type != "none",
                self.username is not None,
                self.ca_certificates is not None,
                self.client_certificate is not None,
                self.private_key is not None,
                self.oauth_token_url is not None,
                self.oauth_client_id is not None,
                bool(self.oauth_scopes),
                self.oauth_ca_certificates is not None,
                bool(self.replace_fields),
            )
        )

    def reject_foreign_options(self) -> None:
        oauth_values = (
            self.oauth_token_url,
            self.oauth_client_id,
            self.oauth_ca_certificates,
            self.oauth_logical_cluster,
            self.oauth_identity_pool_id,
        )
        has_oauth = any(value is not None for value in oauth_values) or bool(self.oauth_scopes)
        if self.auth_type != "oauth" and has_oauth:
            raise click.UsageError("Registry OAuth options require --registry-auth oauth")
        identity = (self.client_certificate, self.private_key, self.private_key_password)
        if self.auth_type != "mtls" and any(value is not None for value in identity):
            raise click.UsageError("Registry client identity requires --registry-auth mtls")

    def _replacing(self, field: str) -> bool:
        return self.secret_required or field in self.replace_fields

    def _only_replaces(self, allowed: set[str], message: str) -> None:
        if set(self.replace_fields) - allowed:
            raise click.UsageError(message)

    def none_input(self) -> RegistryAuthInput:
        credentials = (
            self.username,
            self.client_certificate,
            self.private_key,
            self.oauth_token_url,
            self.oauth_client_id,
            self.oauth_ca_certificates,
            self.oauth_logical_cluster,
            self.oauth_identity_pool_id,
        )
        if any(value is not None for value in credentials) or self.replace_fields:
            raise click.UsageError("Registry auth none cannot include credential options")
        return RegistryAuthInput("none", ca_certificates=self.ca_certificates)

    def basic_input(self) -> RegistryAuthInput:
        self._only_replaces(
            {"registry/password"}, "Registry Basic cannot replace a non-Basic secret"
        )
        if not self.username:
            raise click.UsageError("Registry basic authentication requires --registry-username")
        return RegistryAuthInput(
            "basic",
            ca_certificates=self.ca_certificates,
            username=self.username,
            password=(
                required_secret_prompt("Registry password")
                if self._replacing("registry/password")
                else None
            ),
        )

    def token_input(self) -> RegistryAuthInput:
        if self.username is not None:
            raise click.UsageError("Registry token auth cannot include a username")
        self._only_replaces({"registry/token"}, "Registry token auth cannot replace another secret")
        return RegistryAuthInput(
            "token",
            ca_certificates=self.ca_certificates,
            token=(
                required_secret_prompt("Registry token")
                if self._replacing("registry/token")
                else None
            ),
        )

    def mtls_input(self) -> RegistryAuthInput:
        if self.username is not None:
            raise click.UsageError("Registry mTLS cannot include a username")
        self._only_replaces(
            {"registry/tls/private-key", "registry/tls/private-key-password"},
            "Registry mTLS cannot replace a non-mTLS secret",
        )
        if self.replace_fields and self.private_key is None:
            raise click.UsageError(
                "Registry mTLS secret replacement requires client certificate and key files"
            )
        return RegistryAuthInput(
            "mtls",
            ca_certificates=self.ca_certificates,
            client_certificate=self.client_certificate,
            private_key=self.private_key,
            private_key_password=self.private_key_password,
        )

    def oauth_input(self) -> RegistryAuthInput:
        if self.username is not None:
            raise click.UsageError("Registry OAuth cannot include a Basic username")
        self._only_replaces(
            {"registry/oauth/client-secret"}, "Registry OAuth cannot replace a non-OAuth secret"
        )
        if not self.oauth_token_url or not self.oauth_client_id:
            raise click.UsageError("Registry OAuth requires token URL and client ID")
        return RegistryAuthInput(
            "oauth",
            ca_certificates=self.ca_certificates,
            oauth_token_url=self.oauth_token_url,
            oauth_client_id=self.oauth_client_id,
            oauth_scopes=self.oauth_scopes,
            oauth_client_secret=(
                required_secret_prompt("Registry OAuth client secret")
                if self._replacing("registry/oauth/client-secret")
                else None
            ),
            oauth_ca_certificates=self.oauth_ca_certificates,
            oauth_logical_cluster=self.oauth_logical_cluster,
            oauth_identity_pool_id=self.oauth_identity_pool_id,
        )


def _reject_unknown_fields(replace_fields: tuple[str, ...], allowed: frozenset[str]) -> None:
    unknown = sorted(set(replace_fields) - allowed)
    if unknown:
        raise click.UsageError(f"unsupported credential field: {unknown[0]}")


def _password_auth_input(
    auth_type: str,
    username: str | None,
    certificate_path: Path | None,
    key_path: Path | None,
    *,
    password_required: bool,
    replace_fields: tuple[str, ...],
) -> KafkaAuthInput:
    invalid = set(replace_fields) - {"kafka/password"}
    if invalid or certificate_path is not None or key_path is not None:
        raise click.UsageError("Kafka password authentication cannot replace an mTLS field")
    password = _password_prompt() if password_required or replace_fields else None
    return KafkaAuthInput(auth_type, username=username, password=password)


def _mtls_auth_input(
    certificate_path: Path | None,
    key_path: Path | None,
    *,
    replace_fields: tuple[str, ...],
) -> KafkaAuthInput:
    if "kafka/password" in replace_fields:
        raise click.UsageError("Kafka mTLS authentication has no password field")
    replacing_key = "kafka/tls/private-key" in replace_fields
    replacing_key_password = "kafka/tls/private-key-password" in replace_fields
    if replacing_key_password and not replacing_key:
        raise click.UsageError(
            "Kafka private-key password replacement requires replacing the private key"
        )
    if replacing_key and key_path is None:
        key_path = cast(
            Path,
            click.prompt("Kafka private-key file", type=click.Path(path_type=Path)),
        )
    if replacing_key and certificate_path is None:
        certificate_path = cast(
            Path,
            click.prompt("Kafka client-certificate file", type=click.Path(path_type=Path)),
        )
    if (certificate_path is None) != (key_path is None):
        raise click.UsageError("mTLS certificate and key files must be supplied together")
    if certificate_path is None or key_path is None:
        return KafkaAuthInput("mtls")
    certificate, key, password = read_client_identity(certificate_path, key_path)
    return KafkaAuthInput(
        "mtls",
        client_certificate=certificate,
        private_key=key,
        private_key_password=password,
    )


def read_client_identity(
    certificate_path: Path,
    key_path: Path,
    *,
    label: str = "Kafka",
) -> tuple[str, Secret, Secret | None]:
    """Read a certificate and key, prompting for the key password only when needed."""
    try:
        certificate = read_client_certificate(certificate_path)
        key = read_private_key(key_path)
        return certificate, key, None
    except KafkaProfileError as first_error:
        password = secret_prompt(f"{label} private-key password")
        try:
            certificate = read_client_certificate(certificate_path)
            key = read_private_key(key_path, password=password)
        except KafkaProfileError as error:
            raise click.ClickException(str(error)) from error
        if not password:
            raise click.ClickException(str(first_error))
        return certificate, key, password


def _password_prompt() -> Secret:
    value = secret_prompt("Kafka password")
    if not value:
        raise click.ClickException("Kafka password must not be empty")
    return value


def required_secret_prompt(label: str) -> Secret:
    """Prompt without echo and reject an empty value."""
    value = secret_prompt(label)
    if not value:
        raise click.ClickException(f"{label} must not be empty")
    return value


def secret_prompt(label: str) -> Secret:
    """Prompt without echo on the controlling terminal."""
    descriptor: int | None = None
    try:
        descriptor = os.open("/dev/tty", os.O_RDWR | getattr(os, "O_CLOEXEC", 0))
    except OSError as error:
        raise click.ClickException(
            f"{label} requires a controlling terminal; rerun interactively"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    try:
        return Secret(getpass.getpass(f"{label}: "))
    except (EOFError, KeyboardInterrupt) as error:
        raise click.ClickException(f"{label} collection was canceled") from error


__all__ = [
    "LABEL_FIELD_PREFIX",
    "PASSWORD_AUTH_TYPES",
    "SECRET_FIELDS",
    "UNSET_FIELDS",
    "AddOptions",
    "EditOptions",
    "add_authentication",
    "edit_kafka_authentication",
    "edit_registry_authentication",
    "kafka_auth_input",
    "parse_secret_fields",
    "parse_unset_fields",
    "read_client_identity",
    "registry_auth_input",
    "required_secret_prompt",
    "secret_prompt",
    "validate_edit_options",
]
