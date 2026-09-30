"""Merge an imported Kafka connection with the explicit `add` options.

Options fill what the import leaves out and must match what it sets. The
defaults of `add` apply only afterwards, so they never conflict with an import.
"""

from __future__ import annotations

import sys
from dataclasses import replace

import click

from kantrip.cli_inputs import AddOptions
from kantrip.profile_auth import KafkaAuthInput
from kantrip.profile_imports import ImportedConnection, read_import_source
from kantrip.strimzi import LABEL as STRIMZI_LABEL
from kantrip.strimzi import parse_strimzi_secret

# Options an import may set: each must be absent or equal to the imported value.
_MATCHED_OPTIONS = (
    ("transport", "--transport"),
    ("auth_type", "--auth"),
    ("username", "--username"),
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


def imported_connection(options: AddOptions) -> ImportedConnection | None:
    """Read and parse the import `add` names, or return ``None`` without one."""
    if options.from_strimzi is None:
        return None
    if options.bootstrap_servers is None:
        raise click.UsageError(
            "--from-strimzi requires --bootstrap-server; a KafkaUser Secret names no brokers"
        )
    text = read_import_source(options.from_strimzi, stdin=sys.stdin.buffer, label=STRIMZI_LABEL)
    return parse_strimzi_secret(text)


def merge_imported_options(options: AddOptions, imported: ImportedConnection) -> AddOptions:
    """Fill absent options from the import, rejecting options that contradict it."""
    for field, flag in _MATCHED_OPTIONS:
        explicit = getattr(options, field)
        value = getattr(imported, field)
        if explicit is not None and value is not None and explicit != value:
            raise click.UsageError(f"{flag} does not match the {STRIMZI_LABEL}")
    if imported.auth_type is not None:
        for field, flag in _CREDENTIAL_OPTIONS:
            if getattr(options, field) not in (None, ()):
                raise click.UsageError(
                    f"{flag} cannot be combined with --from-strimzi; "
                    "the Secret sets Kafka authentication"
                )
    return replace(
        options,
        transport=options.transport or imported.transport,
        auth_type=options.auth_type or imported.auth_type,
        username=options.username or imported.username,
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
    )


__all__ = [
    "imported_connection",
    "imported_kafka_authentication",
    "merge_imported_options",
]
