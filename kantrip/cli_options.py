"""Declare the `add` and `edit` options from shared definitions.

Both commands accept the same Kafka and Registry fields. Each field's flags,
type, and parsing are declared once here; a command supplies its own help text
and, for `add`, its default. The `edit`-only options `--unset` and
`--replace-secret` make an optional field absent or replace a stored secret.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import click
import cloup

from kantrip.cli_inputs import parse_secret_fields, parse_unset_fields
from kantrip.kafka import KafkaProfileError, read_ca_bundle
from kantrip.oauth import OAuthProfileError, validate_oauth_endpoint

CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])
OptionDecorator = Callable[[CommandFunction], CommandFunction]

_KAFKA_TRANSPORTS = ("plaintext", "tls")
_KAFKA_AUTH_TYPES = ("none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth")
_REGISTRY_PROVIDERS = ("confluent", "apicurio")
_REGISTRY_AUTH_TYPES = ("none", "basic", "token", "mtls", "oauth")
_PEM_FILE = cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path)
_BROKER = re.compile(r"(?:\[[0-9A-Fa-f:]+\]|[^\s,:\[\]]+):([0-9]{1,5})")
_LABEL_KEY = re.compile(r"[a-zA-Z0-9._-]+")
_MAX_NAME_LENGTH = 1024


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
        if not _LABEL_KEY.fullmatch(name):
            raise click.BadParameter(
                f"label key '{name}' may contain only letters, digits, dots, "
                "underscores, and hyphens"
            )
        if name in labels:
            raise click.BadParameter(f"label '{name}' was supplied more than once")
        labels[name] = label_value
    return labels


def _split_bootstrap_servers(
    context: click.Context, parameter: click.Parameter, values: tuple[str, ...]
) -> tuple[str, ...] | None:
    """Flatten repeated `-b` values, each of which may be a comma-separated list."""
    del context, parameter
    if not values:
        return None
    servers: list[str] = []
    for value in values:
        for server in (entry.strip() for entry in value.split(",")):
            if not server:
                raise click.BadParameter("each broker must be a non-empty host:port address")
            match = _BROKER.fullmatch(server)
            if match is None:
                raise click.BadParameter(
                    f"broker '{server}' must be host:port, such as kafka.example.com:9092"
                )
            if not 1 <= int(match.group(1)) <= 65535:
                raise click.BadParameter(f"broker '{server}' has a port outside 1-65535")
            if server in servers:
                raise click.BadParameter(f"broker '{server}' was supplied more than once")
            servers.append(server)
    return tuple(servers)


def _token_url(context: click.Context, parameter: click.Parameter, value: str | None) -> str | None:
    del context, parameter
    if value is None:
        return None
    try:
        return validate_oauth_endpoint(value)
    except OAuthProfileError as error:
        raise click.BadParameter(str(error).replace("OAuth token URL", "token URL")) from error


def _single_line(
    context: click.Context, parameter: click.Parameter, value: str | None
) -> str | None:
    del context, parameter
    if value is None:
        return None
    if not value or len(value) > _MAX_NAME_LENGTH:
        raise click.BadParameter(f"must be 1 to {_MAX_NAME_LENGTH} characters")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise click.BadParameter("must be a single line")
    return value


def _scopes(
    context: click.Context, parameter: click.Parameter, values: tuple[str, ...]
) -> tuple[str, ...]:
    del context, parameter
    for index, scope in enumerate(values):
        if not scope or any(character.isspace() or character == "\x00" for character in scope):
            raise click.BadParameter(f"scope '{scope}' must be one token without spaces")
        if scope in values[:index]:
            raise click.BadParameter(f"scope '{scope}' was supplied more than once")
    return values


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
        # The reader names Kafka; this callback also serves Registry and OAuth CA files.
        message = str(error).replace("Kafka CA bundle", "CA bundle")
        raise click.BadParameter(message, param=parameter) from error


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
        "--bootstrap-server",
        "bootstrap_servers",
        multiple=True,
        metavar="HOST:PORT",
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


def _text(flag: str, help: str, *, multiple: bool = False) -> OptionDecorator[Any]:
    return cloup.option(flag, multiple=multiple, help=help)


def _name(flag: str, help: str) -> OptionDecorator[Any]:
    """A single-line identity value such as a username or client ID."""
    return cloup.option(flag, callback=_single_line, help=help)


def _token_url_option(flag: str, help: str) -> OptionDecorator[Any]:
    return cloup.option(flag, callback=_token_url, metavar="URL", help=help)


def _scope_option(flag: str, help: str) -> OptionDecorator[Any]:
    return cloup.option(flag, multiple=True, callback=_scopes, help=help)


add_profile_options = _options(
    cloup.option_group(
        "Kafka connection",
        _bootstrap_servers(
            "Kafka broker; repeat or separate with commas for several.",
            default=("localhost:9092",),
            show_default=True,
        ),
        _choice(
            "--transport",
            _KAFKA_TRANSPORTS,
            "Kafka transport security; defaults to tls when --ca-file, --auth, or a "
            "client certificate is given, otherwise plaintext.",
        ),
        _ca_file("--ca-file", "Copy a PEM CA bundle for Kafka TLS verification."),
    ),
    cloup.option_group(
        "Kafka authentication",
        _choice(
            "--auth",
            _KAFKA_AUTH_TYPES,
            "Kafka authentication mechanism.",
            "auth_type",
            default="none",
            show_default=True,
        ),
        _name("--username", "Kafka SASL username."),
        _pem_file("--client-certificate-file", "Copy a public PEM Kafka client certificate chain."),
        _pem_file(
            "--client-key-file", "Read a PEM Kafka client private key into the credential store."
        ),
    ),
    cloup.option_group(
        "Kafka OAuth",
        _token_url_option("--oauth-token-url", "HTTPS Kafka OAuth token endpoint."),
        _name("--oauth-client-id", "Kafka OAuth client identifier."),
        _scope_option("--oauth-scope", "Kafka OAuth scope; repeat as needed."),
        _ca_file("--oauth-ca-file", "Copy a PEM CA bundle for the Kafka OAuth token endpoint."),
    ),
    cloup.option_group(
        "Registry",
        _choice(
            "--registry-provider",
            _REGISTRY_PROVIDERS,
            "Registry provider; defaults to confluent when --registry-url is supplied.",
        ),
        _text("--registry-url", "Optional Registry URL without embedded credentials."),
        _ca_file("--registry-ca-file", "Copy a PEM CA bundle for Registry TLS verification."),
    ),
    cloup.option_group(
        "Registry authentication",
        _choice(
            "--registry-auth",
            _REGISTRY_AUTH_TYPES,
            "Registry authentication mechanism.",
            default="none",
            show_default=True,
        ),
        _name("--registry-username", "Registry Basic authentication username."),
        _pem_file(
            "--registry-client-certificate-file",
            "Copy a public PEM Registry client certificate chain.",
        ),
        _pem_file(
            "--registry-client-key-file",
            "Read a PEM Registry client private key into the credential store.",
        ),
    ),
    cloup.option_group(
        "Registry OAuth",
        _token_url_option("--registry-oauth-token-url", "HTTPS Registry OAuth token endpoint."),
        _name("--registry-oauth-client-id", "Registry OAuth client identifier."),
        _scope_option("--registry-oauth-scope", "Registry OAuth scope; repeat as needed."),
        _ca_file(
            "--registry-oauth-ca-file",
            "Copy a PEM CA bundle for the Registry OAuth token endpoint.",
        ),
        _text("--registry-oauth-logical-cluster", "Confluent OAuth logical cluster."),
        _text("--registry-oauth-identity-pool-id", "Confluent OAuth identity pool ID."),
    ),
    cloup.option_group(
        "Profile metadata",
        cloup.option("-d", "--description", help="Optional profile description."),
        _labels("Add a label; repeat for multiple labels."),
    ),
)

edit_profile_options = _options(
    cloup.option_group(
        "Kafka connection",
        _bootstrap_servers("Replace the Kafka brokers; repeat or separate with commas."),
        _choice("--transport", _KAFKA_TRANSPORTS, "Replace Kafka transport security."),
        _ca_file("--ca-file", "Replace the PEM CA bundle used for Kafka TLS verification."),
    ),
    cloup.option_group(
        "Kafka authentication",
        _choice(
            "--auth", _KAFKA_AUTH_TYPES, "Replace the Kafka authentication mechanism.", "auth_type"
        ),
        _name("--username", "Replace the Kafka SASL username."),
        _pem_file(
            "--client-certificate-file", "Replace the public PEM Kafka client certificate chain."
        ),
        _pem_file(
            "--client-key-file",
            "Replace the PEM Kafka client private key in the credential store.",
        ),
    ),
    cloup.option_group(
        "Kafka OAuth",
        _token_url_option("--oauth-token-url", "Replace the Kafka OAuth token endpoint."),
        _name("--oauth-client-id", "Replace the Kafka OAuth client identifier."),
        _scope_option("--oauth-scope", "Replace Kafka OAuth scopes."),
        _ca_file(
            "--oauth-ca-file", "Replace Kafka OAuth token-endpoint trust with a PEM CA bundle."
        ),
    ),
    cloup.option_group(
        "Registry",
        _choice("--registry-provider", _REGISTRY_PROVIDERS, "Replace the Registry provider."),
        _text("--registry-url", "Add or replace the Registry URL."),
        _ca_file("--registry-ca-file", "Replace Registry TLS trust with a PEM CA bundle."),
    ),
    cloup.option_group(
        "Registry authentication",
        _choice("--registry-auth", _REGISTRY_AUTH_TYPES, "Replace Registry authentication."),
        _name("--registry-username", "Replace Registry Basic username."),
        _pem_file(
            "--registry-client-certificate-file", "Replace the Registry client certificate chain."
        ),
        _pem_file("--registry-client-key-file", "Replace the Registry client private key."),
    ),
    cloup.option_group(
        "Registry OAuth",
        _token_url_option("--registry-oauth-token-url", "Replace Registry OAuth token endpoint."),
        _name("--registry-oauth-client-id", "Replace Registry OAuth client identifier."),
        _scope_option("--registry-oauth-scope", "Replace Registry OAuth scopes."),
        _ca_file("--registry-oauth-ca-file", "Replace Registry OAuth token-endpoint trust."),
        _text("--registry-oauth-logical-cluster", "Replace OAuth logical cluster."),
        _text("--registry-oauth-identity-pool-id", "Replace OAuth identity pool ID."),
    ),
    cloup.option_group(
        "Profile metadata",
        cloup.option("-d", "--description", help="Replace the profile description."),
        _labels("Add or replace a label; repeat for multiple labels."),
    ),
    cloup.option_group(
        "Removal and rotation",
        cloup.option(
            "--unset",
            "unset_fields",
            multiple=True,
            metavar="FIELD",
            callback=parse_unset_fields,
            help=(
                "Make an optional field absent, such as description, labels.KEY, "
                "kafka.tls.ca or registry; repeat as needed."
            ),
        ),
        cloup.option(
            "--replace-secret",
            "replace_secrets",
            multiple=True,
            metavar="FIELD",
            callback=parse_secret_fields,
            help=(
                "Prompt again for one stored secret, such as kafka.auth.password; "
                "repeat as needed."
            ),
        ),
    ),
)

__all__ = ["add_profile_options", "edit_profile_options", "parse_labels"]
