"""Declare the `add` and `edit` options from shared definitions.

Both commands accept the same Kafka and Registry fields. Each field's flags,
type, and parsing are declared once here; a command supplies its own help text
and, for `add`, its default. The `edit`-only options clear, remove, or replace
a stored value.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import click
import cloup

from kantrip.kafka import KafkaProfileError, read_ca_bundle

CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])
OptionDecorator = Callable[[CommandFunction], CommandFunction]

_KAFKA_TRANSPORTS = ("plaintext", "tls")
_KAFKA_AUTH_TYPES = ("none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth")
_REGISTRY_PROVIDERS = ("confluent", "apicurio")
_REGISTRY_AUTH_TYPES = ("none", "basic", "token", "mtls", "oauth")
_PEM_FILE = cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path)


def parse_labels(
    context: click.Context,
    parameter: click.Parameter,
    values: tuple[str, ...],
) -> dict[str, str]:
    """Parse repeated KEY=VALUE labels, rejecting duplicate keys."""
    del context, parameter
    labels: dict[str, str] = {}
    for value in values:
        name, separator, label_value = value.partition("=")
        if not separator or not name:
            raise click.BadParameter("must use KEY=VALUE")
        if name in labels:
            raise click.BadParameter(f"label '{name}' was supplied more than once")
        labels[name] = label_value
    return labels


def _split_bootstrap_servers(
    context: click.Context, parameter: click.Parameter, value: str | None
) -> tuple[str, ...] | None:
    del context, parameter
    if value is None:
        return None
    servers = tuple(server.strip() for server in value.split(","))
    if not servers or any(not server for server in servers):
        raise click.BadParameter("must be a comma-separated list of host:port addresses")
    return servers


def _read_ca_file(
    context: click.Context,
    parameter: click.Parameter,
    value: Path | None,
) -> str | None:
    del context
    if value is None:
        return None
    try:
        return read_ca_bundle(value)
    except KafkaProfileError as error:
        raise click.BadParameter(str(error), param=parameter) from error


def _options(*decorators: OptionDecorator[Any]) -> OptionDecorator[Any]:
    """Apply option decorators in listed order, as if stacked above the command."""

    def apply(function: CommandFunction) -> CommandFunction:
        for decorator in reversed(decorators):
            function = decorator(function)
        return function

    return apply


def _bootstrap_servers(help: str, **settings: Any) -> OptionDecorator[Any]:
    return cloup.option(
        "-b",
        "--bootstrap-servers",
        "bootstrap_servers",
        **settings,
        callback=_split_bootstrap_servers,
        help=help,
    )


def _labels(help: str) -> OptionDecorator[Any]:
    return cloup.option(
        "-l",
        "--label",
        "labels",
        multiple=True,
        callback=parse_labels,
        metavar="KEY=VALUE",
        help=help,
    )


def _choice(
    flag: str, choices: tuple[str, ...], help: str, *declarations: str, **settings: Any
) -> OptionDecorator[Any]:
    return cloup.option(flag, *declarations, type=cloup.Choice(choices), **settings, help=help)


def _pem_file(flag: str, help: str) -> OptionDecorator[Any]:
    """A PEM file the profile layer reads later (certificate chain or private key)."""
    return cloup.option(flag, type=_PEM_FILE, metavar="PATH", help=help)


def _ca_file(flag: str, help: str) -> OptionDecorator[Any]:
    """A CA bundle validated and read while options are parsed."""
    return cloup.option(flag, type=_PEM_FILE, callback=_read_ca_file, metavar="PATH", help=help)


def _flag(flag: str, help: str) -> OptionDecorator[Any]:
    return cloup.option(flag, is_flag=True, help=help)


def _text(flag: str, help: str, *, multiple: bool = False) -> OptionDecorator[Any]:
    return cloup.option(flag, multiple=multiple, help=help)


add_profile_options = _options(
    _bootstrap_servers(
        "Comma-separated Kafka broker addresses.", default="localhost:9092", show_default=True
    ),
    cloup.option("-d", "--description", help="Optional profile description."),
    _labels("Add a label; repeat for multiple labels."),
    _choice(
        "--transport",
        _KAFKA_TRANSPORTS,
        "Kafka transport security.",
        default="plaintext",
        show_default=True,
    ),
    _ca_file("--ca-file", "Copy a PEM CA bundle for Kafka TLS verification."),
    _choice(
        "--auth",
        _KAFKA_AUTH_TYPES,
        "Kafka authentication mechanism.",
        "auth_type",
        default="none",
        show_default=True,
    ),
    _text("--username", "Kafka SASL username."),
    _pem_file("--client-certificate-file", "Copy a public PEM Kafka client certificate chain."),
    _pem_file(
        "--client-key-file", "Read a PEM Kafka client private key into the credential store."
    ),
    _text("--oauth-token-url", "HTTPS Kafka OAuth token endpoint."),
    _text("--oauth-client-id", "Kafka OAuth client identifier."),
    _text("--oauth-scope", "Kafka OAuth scope; repeat as needed.", multiple=True),
    _ca_file("--oauth-ca-file", "Copy a PEM CA bundle for the Kafka OAuth token endpoint."),
    _choice(
        "--registry-provider",
        _REGISTRY_PROVIDERS,
        "Registry provider; defaults to confluent when --registry-url is supplied.",
    ),
    _text("--registry-url", "Optional Registry URL without embedded credentials."),
    _choice(
        "--registry-auth",
        _REGISTRY_AUTH_TYPES,
        "Registry authentication mechanism.",
        default="none",
        show_default=True,
    ),
    _text("--registry-username", "Registry Basic authentication username."),
    _pem_file(
        "--registry-client-certificate-file",
        "Copy a public PEM Registry client certificate chain.",
    ),
    _pem_file(
        "--registry-client-key-file",
        "Read a PEM Registry client private key into the credential store.",
    ),
    _ca_file("--registry-ca-file", "Copy a PEM CA bundle for Registry TLS verification."),
    _text("--registry-oauth-token-url", "HTTPS Registry OAuth token endpoint."),
    _text("--registry-oauth-client-id", "Registry OAuth client identifier."),
    _text("--registry-oauth-scope", "Registry OAuth scope; repeat as needed.", multiple=True),
    _ca_file(
        "--registry-oauth-ca-file", "Copy a PEM CA bundle for the Registry OAuth token endpoint."
    ),
    _text("--registry-oauth-logical-cluster", "Confluent OAuth logical cluster."),
    _text("--registry-oauth-identity-pool-id", "Confluent OAuth identity pool ID."),
)

edit_profile_options = _options(
    _bootstrap_servers("Replace the comma-separated Kafka broker addresses."),
    cloup.option("-d", "--description", help="Replace the profile description."),
    _flag("--clear-description", "Remove the profile description."),
    _labels("Add or replace a label; repeat for multiple labels."),
    cloup.option(
        "--remove-label",
        "remove_labels",
        multiple=True,
        metavar="KEY",
        help="Remove a label; repeat for multiple labels.",
    ),
    _choice("--transport", _KAFKA_TRANSPORTS, "Replace Kafka transport security."),
    _ca_file("--ca-file", "Replace the PEM CA bundle used for Kafka TLS verification."),
    _flag("--default-trust", "Use the client's default trust store for Kafka TLS."),
    _choice(
        "--auth", _KAFKA_AUTH_TYPES, "Replace the Kafka authentication mechanism.", "auth_type"
    ),
    _text("--username", "Replace the Kafka SASL username."),
    _pem_file(
        "--client-certificate-file", "Replace the public PEM Kafka client certificate chain."
    ),
    _pem_file(
        "--client-key-file", "Replace the PEM Kafka client private key in the credential store."
    ),
    _text("--oauth-token-url", "Replace the Kafka OAuth token endpoint."),
    _text("--oauth-client-id", "Replace the Kafka OAuth client identifier."),
    _text("--oauth-scope", "Replace Kafka OAuth scopes.", multiple=True),
    _flag("--clear-oauth-scopes", "Remove all Kafka OAuth scopes."),
    _ca_file("--oauth-ca-file", "Replace Kafka OAuth token-endpoint trust with a PEM CA bundle."),
    _flag("--oauth-default-trust", "Use default trust for the Kafka OAuth token endpoint."),
    cloup.option(
        "--replace-secret",
        "replace_secrets",
        multiple=True,
        metavar="FIELD",
        help="Replace one supported Kafka credential field; repeat as needed.",
    ),
    _choice("--registry-provider", _REGISTRY_PROVIDERS, "Replace the Registry provider."),
    _text("--registry-url", "Add or replace the Registry URL."),
    _choice("--registry-auth", _REGISTRY_AUTH_TYPES, "Replace Registry authentication."),
    _text("--registry-username", "Replace Registry Basic username."),
    _pem_file(
        "--registry-client-certificate-file", "Replace the Registry client certificate chain."
    ),
    _pem_file("--registry-client-key-file", "Replace the Registry client private key."),
    _ca_file("--registry-ca-file", "Replace Registry TLS trust with a PEM CA bundle."),
    _flag("--registry-default-trust", "Use default trust for Registry TLS."),
    _text("--registry-oauth-token-url", "Replace Registry OAuth token endpoint."),
    _text("--registry-oauth-client-id", "Replace Registry OAuth client identifier."),
    _text("--registry-oauth-scope", "Replace Registry OAuth scopes.", multiple=True),
    _flag("--clear-registry-oauth-scopes", "Remove all Registry OAuth scopes."),
    _ca_file("--registry-oauth-ca-file", "Replace Registry OAuth token-endpoint trust."),
    _flag(
        "--registry-oauth-default-trust", "Use default trust for the Registry OAuth token endpoint."
    ),
    _text("--registry-oauth-logical-cluster", "Replace OAuth logical cluster."),
    _text("--registry-oauth-identity-pool-id", "Replace OAuth identity pool ID."),
    _flag("--clear-registry-oauth-logical-cluster", "Remove the Registry OAuth logical cluster."),
    _flag("--clear-registry-oauth-identity-pool-id", "Remove the Registry OAuth identity pool ID."),
    _flag("--remove-registry", "Remove the complete Registry connection."),
)


__all__ = ["add_profile_options", "edit_profile_options", "parse_labels"]
