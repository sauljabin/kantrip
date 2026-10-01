"""Merge an imported Kafka connection with the explicit `add` options.

Options fill what the import leaves out and must match what it sets. The
defaults of `add` apply only afterwards, so they never conflict with an import.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, replace

import click

from kantrip.cli_inputs import AddOptions
from kantrip.client_properties import LABEL as PROPERTIES_LABEL
from kantrip.client_properties import parse_client_properties
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.profile_imports import (
    STDIN_SOURCE,
    ImportedConnection,
    ImportedRegistry,
    read_import_source,
    source_directory,
)
from kantrip.properties_syntax import reportable_key
from kantrip.strimzi import LABEL as STRIMZI_LABEL
from kantrip.strimzi import parse_strimzi_secret

# Options an import may set: each must be absent or equal to the imported value.
_MATCHED_OPTIONS = (
    ("bootstrap_servers", "bootstrap_servers", "--bootstrap-server"),
    ("transport", "transport", "--transport"),
    ("ca_file", "ca_certificates", "--ca-file"),
    ("auth_type", "auth_type", "--auth"),
    ("username", "username", "--username"),
)
# Kafka credential options an import that sets authentication leaves no room for.
_CREDENTIAL_OPTIONS = (
    ("client_certificate_file", "--client-certificate-file"),
    ("client_key_file", "--client-key-file"),
    ("oauth_token_url", "--oauth-token-url"),
    ("oauth_client_id", "--oauth-client-id"),
    ("oauth_scope", "--oauth-scope"),
    ("oauth_ca_file", "--oauth-ca-file"),
)
# The Registry counterparts: options that must match, and credentials that can't join.
_MATCHED_REGISTRY_OPTIONS = (
    ("registry_provider", "--registry-provider"),
    ("registry_url", "--registry-url"),
    ("registry_ca_file", "--registry-ca-file"),
    ("registry_auth", "--registry-auth"),
    ("registry_username", "--registry-username"),
)
_REGISTRY_CREDENTIAL_OPTIONS = (
    ("registry_client_certificate_file", "--registry-client-certificate-file"),
    ("registry_client_key_file", "--registry-client-key-file"),
    ("registry_oauth_token_url", "--registry-oauth-token-url"),
    ("registry_oauth_client_id", "--registry-oauth-client-id"),
    ("registry_oauth_scope", "--registry-oauth-scope"),
    ("registry_oauth_ca_file", "--registry-oauth-ca-file"),
    ("registry_oauth_logical_cluster", "--registry-oauth-logical-cluster"),
    ("registry_oauth_identity_pool_id", "--registry-oauth-identity-pool-id"),
)
_MAX_REPORTED_KEYS = 10


@dataclass(frozen=True)
class _ImportSource:
    """The import option `add` received and the document it names."""

    option: str
    label: str
    source: str


def _import_source(options: AddOptions) -> _ImportSource | None:
    if options.from_properties is not None:
        return _ImportSource("--from-properties", PROPERTIES_LABEL, options.from_properties)
    if options.from_strimzi is not None:
        return _ImportSource("--from-strimzi", STRIMZI_LABEL, options.from_strimzi)
    return None


def imported_connection(options: AddOptions) -> ImportedConnection | None:
    """Read and parse the import `add` names, or return ``None`` without one."""
    source = _import_source(options)
    if source is None:
        return None
    if source.label == STRIMZI_LABEL and options.bootstrap_servers is None:
        raise click.UsageError(
            "--from-strimzi requires --bootstrap-server; a KafkaUser Secret names no brokers"
        )
    text = read_import_source(source.source, stdin=sys.stdin.buffer, label=source.label)
    if source.label == STRIMZI_LABEL:
        return parse_strimzi_secret(text)
    return parse_client_properties(text, base_directory=source_directory(source.source))


def merge_imported_options(options: AddOptions, imported: ImportedConnection) -> AddOptions:
    """Fill absent options from the import, rejecting options that contradict it."""
    source = _import_source(options)
    assert source is not None
    for field, imported_field, flag in _MATCHED_OPTIONS:
        explicit = getattr(options, field)
        value = getattr(imported, imported_field)
        if explicit is not None and value is not None and explicit != value:
            raise click.UsageError(f"{flag} does not match the {source.label}")
    if imported.auth_type is not None:
        flags = [flag for field, flag in _CREDENTIAL_OPTIONS if getattr(options, field)]
        if options.username is not None and imported.username is None:
            flags.append("--username")
        if flags:
            raise click.UsageError(
                f"{flags[0]} cannot be combined with {source.option}, "
                "which sets Kafka authentication"
            )
    merged = replace(
        options,
        bootstrap_servers=options.bootstrap_servers or imported.bootstrap_servers,
        transport=options.transport or imported.transport,
        ca_file=options.ca_file or imported.ca_certificates,
        auth_type=options.auth_type or imported.auth_type,
        username=options.username or imported.username,
    )
    if imported.registry is None:
        return merged
    return _merge_registry_options(merged, imported.registry, source)


def _merge_registry_options(
    options: AddOptions, registry: ImportedRegistry, source: _ImportSource
) -> AddOptions:
    """Fill absent Registry options from an imported Registry, or reject contradictions."""
    auth = registry.auth
    imported = {
        "registry_provider": registry.provider,
        "registry_url": registry.url,
        "registry_ca_file": auth.ca_certificates if auth else None,
        "registry_auth": auth.auth_type if auth else "none",
        "registry_username": auth.username if auth else None,
    }
    for field, flag in _MATCHED_REGISTRY_OPTIONS:
        explicit = getattr(options, field)
        if explicit is not None and imported[field] is not None and explicit != imported[field]:
            raise click.UsageError(f"{flag} does not match the {source.label}")
    flags = [flag for field, flag in _REGISTRY_CREDENTIAL_OPTIONS if getattr(options, field)]
    if options.registry_username is not None and imported["registry_username"] is None:
        flags.append("--registry-username")
    if flags:
        raise click.UsageError(
            f"{flags[0]} cannot be combined with {source.option}, which sets the Registry"
        )
    return replace(
        options,
        registry_provider=options.registry_provider or registry.provider,
        registry_url=options.registry_url or registry.url,
        registry_ca_file=options.registry_ca_file or imported["registry_ca_file"],
        registry_auth=options.registry_auth or imported["registry_auth"],
        registry_username=options.registry_username or imported["registry_username"],
    )


def imported_kafka_authentication(imported: ImportedConnection) -> KafkaAuthInput | None:
    """Build Kafka authentication from imported credentials, or ``None`` without them."""
    if imported.auth_type is None:
        return None
    return KafkaAuthInput(
        imported.auth_type,
        username=imported.username,
        password=imported.password,
        client_certificate=imported.client_certificate,
        private_key=imported.private_key,
        private_key_password=imported.private_key_password,
        oauth_token_url=imported.oauth_token_url,
        oauth_client_id=imported.oauth_client_id,
        oauth_scopes=imported.oauth_scopes,
        oauth_client_secret=imported.oauth_client_secret,
        oauth_ca_certificates=imported.oauth_ca_certificates,
    )


def imported_registry_authentication(
    options: AddOptions, imported: ImportedConnection
) -> RegistryAuthInput | None:
    """Return imported Registry credentials, with a `--registry-ca-file` that filled the CA.

    ``None`` leaves the Registry to the merged options, which then describe no
    authentication and at most a CA.
    """
    registry = imported.registry
    auth = registry.auth if registry is not None else None
    if auth is None or auth.auth_type == "none":
        return None
    ca = options.registry_ca_file
    if auth.ca_certificates is None and ca is not None:
        oauth_ca = ca if auth.auth_type == "oauth" else None
        auth = replace(auth, ca_certificates=ca, oauth_ca_certificates=oauth_ca)
    return auth


def import_notices(options: AddOptions, imported: ImportedConnection) -> tuple[str, ...]:
    """Describe ignored settings and a source file that still holds credentials."""
    source = _import_source(options)
    assert source is not None
    notices: list[str] = []
    if imported.ignored_keys:
        notices.append(_ignored_notice(imported.ignored_keys))
    if imported.has_secrets and source.source != STDIN_SOURCE:
        notices.append(
            f"{source.source} still holds the imported credentials; Kantrip left it "
            "unchanged, so delete it if nothing else needs it"
        )
    return tuple(notices)


def _ignored_notice(keys: tuple[str, ...]) -> str:
    """Name at most ten ignored keys; count the rest and any unprintable ones."""
    names = [key for key in keys if reportable_key(key)][:_MAX_REPORTED_KEYS]
    rest = len(keys) - len(names)
    listed = ", ".join(names) + (f", and {rest} more" if names and rest else "")
    if not names:
        listed = f"{rest} unnamed"
    plural = "setting" if len(keys) == 1 else "settings"
    return f"Ignored {len(keys)} application {plural} a profile doesn't keep: {listed}"


__all__ = [
    "import_notices",
    "imported_connection",
    "imported_kafka_authentication",
    "imported_registry_authentication",
    "merge_imported_options",
]
