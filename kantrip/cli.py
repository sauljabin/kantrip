"""Kantrip command-line entry point."""

from __future__ import annotations

import getpass
import os
import sys
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import Any, TypeVar, cast

import click
import cloup
from rich.console import Console
from rich.padding import Padding
from rich.text import Text

from kantrip import APP_VERSION
from kantrip.console import (
    StatusKind,
    create_console,
    create_profile_description,
    create_profile_table,
    create_status_text,
    create_structured_syntax,
    show_progress,
)
from kantrip.doctor import DoctorReport, run_doctor
from kantrip.kafka import (
    KafkaProfileError,
    read_ca_bundle,
    read_client_certificate,
    read_private_key,
)
from kantrip.maintenance import run_repair
from kantrip.ping import PingError, PingResult, ping_profile
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.profile_output import (
    OutputFormat,
    describe_observation,
    dump_observation,
    filter_profiles,
    list_observation,
)
from kantrip.profile_storage import ProfileStoreError, load_profiles
from kantrip.profiles import add_profile, edit_profile, remove_profile, resolve_profile_snapshot
from kantrip.secret_value import Secret
from kantrip.session import SessionError, ensure_session_available, run_profile_session

EPILOG = "More information at https://github.com/sauljabin/kantrip."
CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])
OUTPUT_FORMATS = ("human", "json", "yaml")


class _ProfileClickException(click.ClickException):
    """Render a safe profile error with its public mutation exit status."""


class _CommittedProfileClickException(_ProfileClickException):
    exit_code = 3


class _UnknownProfileClickException(_ProfileClickException):
    exit_code = 4


def _profile_click_exception(error: ProfileStoreError) -> _ProfileClickException:
    exception_type = {
        3: _CommittedProfileClickException,
        4: _UnknownProfileClickException,
    }.get(error.exit_code, _ProfileClickException)
    return exception_type(str(error))


def _echo_committed_mutation(message: str) -> None:
    try:
        click.echo(message)
    except OSError as error:
        raise _CommittedProfileClickException(
            "profile change committed but its result could not be written; "
            "inspect the profile and run 'kantrip doctor --repair'"
        ) from error


def _configure_consoles(context: click.Context, *, no_color: bool) -> None:
    """Configure result and diagnostic consoles on the root context."""
    root_context = context.find_root()
    obj: dict[str, Any] = root_context.ensure_object(dict)
    obj["console"] = create_console(no_color=no_color)
    obj["error_console"] = create_console(
        stream=sys.stderr,
        no_color=no_color,
    )


def _apply_local_no_color(context: click.Context, parameter: click.Parameter, value: bool) -> bool:
    """Disable color before invoking a subcommand when requested locally."""
    del parameter
    if value:
        _configure_consoles(context, no_color=True)
    return value


def local_no_color(function: CommandFunction) -> CommandFunction:
    """Add the command-local form of Kantrip's global color option."""
    decorated = cloup.option(
        "--no-color",
        is_flag=True,
        expose_value=False,
        callback=_apply_local_no_color,
        help="Disable styled terminal output.",
    )(function)
    return decorated


@cloup.group(
    epilog=EPILOG,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@cloup.version_option(APP_VERSION)
@cloup.option(
    "--no-color",
    is_flag=True,
    help="Disable styled terminal output.",
)
@cloup.pass_context
def cli(context: cloup.Context, no_color: bool) -> None:
    """Kantrip securely manages local Kafka profiles for command-line tools and compatible applications."""
    _configure_consoles(context, no_color=no_color)


def console_from_context(context: cloup.Context) -> Console:
    """Return the console initialized for the current invocation."""
    obj: dict[str, Any] = context.ensure_object(dict)
    console = obj.get("console")
    if not isinstance(console, Console):
        raise TypeError("Kantrip console has not been initialized")
    return console


def error_console_from_context(context: cloup.Context) -> Console:
    """Return the diagnostic console initialized for the current invocation."""
    obj: dict[str, Any] = context.ensure_object(dict)
    console = obj.get("error_console")
    if not isinstance(console, Console):
        raise TypeError("Kantrip diagnostic console has not been initialized")
    return console


def _print_structured_observation(
    context: cloup.Context,
    value: Any,
    output_format: str,
) -> None:
    selected_format = cast(OutputFormat, output_format)
    contents = dump_observation(value, selected_format)
    console_from_context(context).print(
        create_structured_syntax(contents, selected_format),
        end="",
    )


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


def _parse_labels(
    context: click.Context,
    parameter: click.Parameter,
    values: tuple[str, ...],
) -> dict[str, str]:
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


def _read_client_identity(
    certificate_path: Path,
    key_path: Path,
    *,
    label: str = "Kafka",
) -> tuple[str, Secret, Secret | None]:
    try:
        certificate = read_client_certificate(certificate_path)
        key = read_private_key(key_path)
        return certificate, key, None
    except KafkaProfileError as first_error:
        password = _secret_prompt(f"{label} private-key password")
        try:
            certificate = read_client_certificate(certificate_path)
            key = read_private_key(key_path, password=password)
        except KafkaProfileError as error:
            raise click.ClickException(str(error)) from error
        if not password:
            raise click.ClickException(str(first_error))
        return certificate, key, password


def _password_prompt() -> Secret:
    value = _secret_prompt("Kafka password")
    if not value:
        raise click.ClickException("Kafka password must not be empty")
    return value


def _secret_prompt(label: str) -> Secret:
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


def _auth_input(  # noqa: C901
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
    if len(set(replace_fields)) != len(replace_fields):
        raise click.UsageError("--replace-secret fields must be unique")
    allowed = {
        "kafka/password",
        "kafka/oauth/client-secret",
        "kafka/tls/private-key",
        "kafka/tls/private-key-password",
    }
    unknown = sorted(set(replace_fields) - allowed)
    if unknown:
        raise click.UsageError(f"unsupported credential field: {unknown[0]}")
    has_oauth_options = (
        any(
            value is not None
            for value in (
                oauth_token_url,
                oauth_client_id,
                oauth_scopes,
                oauth_ca_certificates,
            )
        )
        or oauth_default_trust
    )
    if auth_type != "oauth" and has_oauth_options:
        raise click.UsageError("Kafka OAuth options require --auth oauth")
    if auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return _password_auth_input(
            auth_type,
            username,
            certificate_path,
            key_path,
            password_required=password_required,
            replace_fields=replace_fields,
        )
    if auth_type == "mtls":
        return _mtls_auth_input(
            certificate_path,
            key_path,
            replace_fields=replace_fields,
        )
    if auth_type == "oauth":
        if username is not None or certificate_path is not None or key_path is not None:
            raise click.UsageError("Kafka OAuth cannot include password or mTLS options")
        invalid = set(replace_fields) - {"kafka/oauth/client-secret"}
        if invalid:
            raise click.UsageError("Kafka OAuth cannot replace a non-OAuth secret")
        client_secret = None
        if oauth_secret_required or replace_fields:
            client_secret = _required_secret_prompt("Kafka OAuth client secret")
        return KafkaAuthInput(
            "oauth",
            oauth_token_url=oauth_token_url,
            oauth_client_id=oauth_client_id,
            oauth_scopes=oauth_scopes,
            oauth_client_secret=client_secret,
            oauth_ca_certificates=oauth_ca_certificates,
            oauth_default_trust=oauth_default_trust,
        )
    if (
        replace_fields
        or username
        or certificate_path is not None
        or key_path is not None
        or oauth_token_url is not None
        or oauth_client_id is not None
        or bool(oauth_scopes)
        or oauth_ca_certificates is not None
        or oauth_default_trust
    ):
        raise click.UsageError("Kafka auth none cannot include credential options")
    return KafkaAuthInput("none")


def _registry_auth_input(  # noqa: C901
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
    allowed = {
        "registry/password",
        "registry/token",
        "registry/tls/private-key",
        "registry/tls/private-key-password",
        "registry/oauth/client-secret",
    }
    unknown = sorted(set(replace_fields) - allowed)
    if unknown:
        raise click.UsageError(f"unsupported credential field: {unknown[0]}")
    has_oauth_options = any(
        value is not None
        for value in (
            oauth_token_url,
            oauth_client_id,
            oauth_ca_certificates,
            oauth_logical_cluster,
            oauth_identity_pool_id,
        )
    ) or bool(oauth_scopes)
    if auth_type != "oauth" and has_oauth_options:
        raise click.UsageError("Registry OAuth options require --registry-auth oauth")
    if auth_type != "mtls" and any(
        value is not None for value in (client_certificate, private_key, private_key_password)
    ):
        raise click.UsageError("Registry client identity requires --registry-auth mtls")
    requested = any(
        (
            auth_type != "none",
            username is not None,
            ca_certificates is not None,
            client_certificate is not None,
            private_key is not None,
            oauth_token_url is not None,
            oauth_client_id is not None,
            bool(oauth_scopes),
            oauth_ca_certificates is not None,
            bool(replace_fields),
        )
    )
    if not requested:
        return None
    if registry_url is None:
        raise click.UsageError("Registry authentication options require --registry-url")
    if auth_type == "none":
        if (
            any(
                value is not None
                for value in (
                    username,
                    client_certificate,
                    private_key,
                    oauth_token_url,
                    oauth_client_id,
                    oauth_ca_certificates,
                    oauth_logical_cluster,
                    oauth_identity_pool_id,
                )
            )
            or replace_fields
        ):
            raise click.UsageError("Registry auth none cannot include credential options")
        return RegistryAuthInput("none", ca_certificates=ca_certificates)
    if auth_type == "basic":
        if set(replace_fields) - {"registry/password"}:
            raise click.UsageError("Registry Basic cannot replace a non-Basic secret")
        if not username:
            raise click.UsageError("Registry basic authentication requires --registry-username")
        return RegistryAuthInput(
            "basic",
            ca_certificates=ca_certificates,
            username=username,
            password=(
                _required_secret_prompt("Registry password")
                if secret_required or "registry/password" in replace_fields
                else None
            ),
        )
    if auth_type == "token":
        if username is not None:
            raise click.UsageError("Registry token auth cannot include a username")
        if set(replace_fields) - {"registry/token"}:
            raise click.UsageError("Registry token auth cannot replace another secret")
        return RegistryAuthInput(
            "token",
            ca_certificates=ca_certificates,
            token=(
                _required_secret_prompt("Registry token")
                if secret_required or "registry/token" in replace_fields
                else None
            ),
        )
    if auth_type == "mtls":
        if username is not None:
            raise click.UsageError("Registry mTLS cannot include a username")
        if set(replace_fields) - {
            "registry/tls/private-key",
            "registry/tls/private-key-password",
        }:
            raise click.UsageError("Registry mTLS cannot replace a non-mTLS secret")
        if replace_fields and private_key is None:
            raise click.UsageError(
                "Registry mTLS secret replacement requires client certificate and key files"
            )
        return RegistryAuthInput(
            "mtls",
            ca_certificates=ca_certificates,
            client_certificate=client_certificate,
            private_key=private_key,
            private_key_password=private_key_password,
        )
    if auth_type == "oauth":
        if username is not None:
            raise click.UsageError("Registry OAuth cannot include a Basic username")
        if set(replace_fields) - {"registry/oauth/client-secret"}:
            raise click.UsageError("Registry OAuth cannot replace a non-OAuth secret")
        if not oauth_token_url or not oauth_client_id:
            raise click.UsageError("Registry OAuth requires token URL and client ID")
        return RegistryAuthInput(
            "oauth",
            ca_certificates=ca_certificates,
            oauth_token_url=oauth_token_url,
            oauth_client_id=oauth_client_id,
            oauth_scopes=oauth_scopes,
            oauth_client_secret=(
                _required_secret_prompt("Registry OAuth client secret")
                if secret_required or "registry/oauth/client-secret" in replace_fields
                else None
            ),
            oauth_ca_certificates=oauth_ca_certificates,
            oauth_logical_cluster=oauth_logical_cluster,
            oauth_identity_pool_id=oauth_identity_pool_id,
        )
    raise click.UsageError("unsupported Registry authentication type")


def _required_secret_prompt(label: str) -> Secret:
    value = _secret_prompt(label)
    if not value:
        raise click.ClickException(f"{label} must not be empty")
    return value


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
    certificate, key, password = _read_client_identity(certificate_path, key_path)
    return KafkaAuthInput(
        "mtls",
        client_certificate=certificate,
        private_key=key,
        private_key_password=password,
    )


@cli.command("add")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "-b",
    "--bootstrap-servers",
    "bootstrap_servers",
    default="localhost:9092",
    show_default=True,
    callback=_split_bootstrap_servers,
    help="Comma-separated Kafka broker addresses.",
)
@cloup.option("-d", "--description", help="Optional profile description.")
@cloup.option(
    "-l",
    "--label",
    "labels",
    multiple=True,
    callback=_parse_labels,
    metavar="KEY=VALUE",
    help="Add a label; repeat for multiple labels.",
)
@cloup.option(
    "--transport",
    type=cloup.Choice(("plaintext", "tls")),
    default="plaintext",
    show_default=True,
    help="Kafka transport security.",
)
@cloup.option(
    "--ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Copy a PEM CA bundle for Kafka TLS verification.",
)
@cloup.option(
    "--auth",
    "auth_type",
    type=cloup.Choice(("none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth")),
    default="none",
    show_default=True,
    help="Kafka authentication mechanism.",
)
@cloup.option("--username", help="Kafka SASL username.")
@cloup.option(
    "--client-certificate-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Copy a public PEM Kafka client certificate chain.",
)
@cloup.option(
    "--client-key-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Read a PEM Kafka client private key into the credential store.",
)
@cloup.option("--oauth-token-url", help="HTTPS Kafka OAuth token endpoint.")
@cloup.option("--oauth-client-id", help="Kafka OAuth client identifier.")
@cloup.option("--oauth-scope", multiple=True, help="Kafka OAuth scope; repeat as needed.")
@cloup.option(
    "--oauth-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Copy a PEM CA bundle for the Kafka OAuth token endpoint.",
)
@cloup.option(
    "--registry-provider",
    type=cloup.Choice(("confluent", "apicurio")),
    help="Registry provider; defaults to confluent when --registry-url is supplied.",
)
@cloup.option(
    "--registry-url",
    help="Optional Registry URL without embedded credentials.",
)
@cloup.option(
    "--registry-auth",
    type=cloup.Choice(("none", "basic", "token", "mtls", "oauth")),
    default="none",
    show_default=True,
    help="Registry authentication mechanism.",
)
@cloup.option("--registry-username", help="Registry Basic authentication username.")
@cloup.option(
    "--registry-client-certificate-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Copy a public PEM Registry client certificate chain.",
)
@cloup.option(
    "--registry-client-key-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Read a PEM Registry client private key into the credential store.",
)
@cloup.option(
    "--registry-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Copy a PEM CA bundle for Registry TLS verification.",
)
@cloup.option("--registry-oauth-token-url", help="HTTPS Registry OAuth token endpoint.")
@cloup.option("--registry-oauth-client-id", help="Registry OAuth client identifier.")
@cloup.option(
    "--registry-oauth-scope", multiple=True, help="Registry OAuth scope; repeat as needed."
)
@cloup.option(
    "--registry-oauth-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Copy a PEM CA bundle for the Registry OAuth token endpoint.",
)
@cloup.option("--registry-oauth-logical-cluster", help="Confluent OAuth logical cluster.")
@cloup.option("--registry-oauth-identity-pool-id", help="Confluent OAuth identity pool ID.")
def add_configured_profile(
    profile_name: str,
    bootstrap_servers: tuple[str, ...],
    description: str | None,
    labels: dict[str, str],
    transport: str,
    ca_file: str | None,
    auth_type: str,
    username: str | None,
    client_certificate_file: Path | None,
    client_key_file: Path | None,
    oauth_token_url: str | None,
    oauth_client_id: str | None,
    oauth_scope: tuple[str, ...],
    oauth_ca_file: str | None,
    registry_provider: str | None,
    registry_url: str | None,
    registry_auth: str,
    registry_username: str | None,
    registry_client_certificate_file: Path | None,
    registry_client_key_file: Path | None,
    registry_ca_file: str | None,
    registry_oauth_token_url: str | None,
    registry_oauth_client_id: str | None,
    registry_oauth_scope: tuple[str, ...],
    registry_oauth_ca_file: str | None,
    registry_oauth_logical_cluster: str | None,
    registry_oauth_identity_pool_id: str | None,
) -> None:
    """Add a profile."""
    try:
        auth = _auth_input(
            auth_type,
            username,
            client_certificate_file,
            client_key_file,
            password_required=auth_type in {"plain", "scram-sha-256", "scram-sha-512"},
            oauth_token_url=oauth_token_url,
            oauth_client_id=oauth_client_id,
            oauth_scopes=oauth_scope if auth_type == "oauth" else None,
            oauth_ca_certificates=oauth_ca_file,
            oauth_secret_required=auth_type == "oauth",
        )
        registry_certificate = registry_key = registry_key_password = None
        if (registry_client_certificate_file is None) != (registry_client_key_file is None):
            raise click.UsageError("Registry mTLS requires both client certificate and key files")
        if registry_client_certificate_file is not None:
            assert registry_client_key_file is not None
            registry_certificate, registry_key, registry_key_password = _read_client_identity(
                registry_client_certificate_file,
                registry_client_key_file,
                label="Registry",
            )
        selected_registry_auth = _registry_auth_input(
            registry_auth,
            registry_username,
            registry_url=registry_url,
            ca_certificates=registry_ca_file,
            client_certificate=registry_certificate,
            private_key=registry_key,
            private_key_password=registry_key_password,
            oauth_token_url=registry_oauth_token_url,
            oauth_client_id=registry_oauth_client_id,
            oauth_scopes=registry_oauth_scope,
            oauth_ca_certificates=registry_oauth_ca_file,
            oauth_logical_cluster=registry_oauth_logical_cluster,
            oauth_identity_pool_id=registry_oauth_identity_pool_id,
        )
        profiles = add_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
            labels=labels,
            transport=transport,
            ca_certificates=ca_file,
            auth=auth,
            registry_provider=registry_provider,
            registry_url=registry_url,
            registry_auth=selected_registry_auth,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    _echo_committed_mutation(f"Added profile '{profile_name}' to {profiles.path}")


@cli.command("edit")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "-b",
    "--bootstrap-servers",
    callback=_split_bootstrap_servers,
    help="Replace the comma-separated Kafka broker addresses.",
)
@cloup.option("-d", "--description", help="Replace the profile description.")
@cloup.option("--clear-description", is_flag=True, help="Remove the profile description.")
@cloup.option(
    "-l",
    "--label",
    "labels",
    multiple=True,
    callback=_parse_labels,
    metavar="KEY=VALUE",
    help="Add or replace a label; repeat for multiple labels.",
)
@cloup.option(
    "--remove-label",
    "remove_labels",
    multiple=True,
    metavar="KEY",
    help="Remove a label; repeat for multiple labels.",
)
@cloup.option(
    "--transport",
    type=cloup.Choice(("plaintext", "tls")),
    help="Replace Kafka transport security.",
)
@cloup.option(
    "--ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Replace the PEM CA bundle used for Kafka TLS verification.",
)
@cloup.option(
    "--default-trust",
    is_flag=True,
    help="Use the client's default trust store for Kafka TLS.",
)
@cloup.option(
    "--auth",
    "auth_type",
    type=cloup.Choice(("none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth")),
    help="Replace the Kafka authentication mechanism.",
)
@cloup.option("--username", help="Replace the Kafka SASL username.")
@cloup.option(
    "--client-certificate-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Replace the public PEM Kafka client certificate chain.",
)
@cloup.option(
    "--client-key-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Replace the PEM Kafka client private key in the credential store.",
)
@cloup.option("--oauth-token-url", help="Replace the Kafka OAuth token endpoint.")
@cloup.option("--oauth-client-id", help="Replace the Kafka OAuth client identifier.")
@cloup.option("--oauth-scope", multiple=True, help="Replace Kafka OAuth scopes.")
@cloup.option("--clear-oauth-scopes", is_flag=True, help="Remove all Kafka OAuth scopes.")
@cloup.option(
    "--oauth-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Replace Kafka OAuth token-endpoint trust with a PEM CA bundle.",
)
@cloup.option(
    "--oauth-default-trust",
    is_flag=True,
    help="Use default trust for the Kafka OAuth token endpoint.",
)
@cloup.option(
    "--replace-secret",
    "replace_secrets",
    multiple=True,
    metavar="FIELD",
    help="Replace one supported Kafka credential field; repeat as needed.",
)
@cloup.option(
    "--registry-provider",
    type=cloup.Choice(("confluent", "apicurio")),
    help="Replace the Registry provider.",
)
@cloup.option("--registry-url", help="Add or replace the Registry URL.")
@cloup.option(
    "--registry-auth",
    type=cloup.Choice(("none", "basic", "token", "mtls", "oauth")),
    help="Replace Registry authentication.",
)
@cloup.option("--registry-username", help="Replace Registry Basic username.")
@cloup.option(
    "--registry-client-certificate-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Replace the Registry client certificate chain.",
)
@cloup.option(
    "--registry-client-key-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    metavar="PATH",
    help="Replace the Registry client private key.",
)
@cloup.option(
    "--registry-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Replace Registry TLS trust with a PEM CA bundle.",
)
@cloup.option(
    "--registry-default-trust",
    is_flag=True,
    help="Use default trust for Registry TLS.",
)
@cloup.option("--registry-oauth-token-url", help="Replace Registry OAuth token endpoint.")
@cloup.option("--registry-oauth-client-id", help="Replace Registry OAuth client identifier.")
@cloup.option("--registry-oauth-scope", multiple=True, help="Replace Registry OAuth scopes.")
@cloup.option(
    "--clear-registry-oauth-scopes",
    is_flag=True,
    help="Remove all Registry OAuth scopes.",
)
@cloup.option(
    "--registry-oauth-ca-file",
    type=cloup.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    callback=_read_ca_file,
    metavar="PATH",
    help="Replace Registry OAuth token-endpoint trust.",
)
@cloup.option(
    "--registry-oauth-default-trust",
    is_flag=True,
    help="Use default trust for the Registry OAuth token endpoint.",
)
@cloup.option("--registry-oauth-logical-cluster", help="Replace OAuth logical cluster.")
@cloup.option("--registry-oauth-identity-pool-id", help="Replace OAuth identity pool ID.")
@cloup.option(
    "--clear-registry-oauth-logical-cluster",
    is_flag=True,
    help="Remove the Registry OAuth logical cluster.",
)
@cloup.option(
    "--clear-registry-oauth-identity-pool-id",
    is_flag=True,
    help="Remove the Registry OAuth identity pool ID.",
)
@cloup.option("--remove-registry", is_flag=True, help="Remove the complete Registry connection.")
def edit_configured_profile(  # noqa: C901
    profile_name: str,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: dict[str, str],
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_file: str | None,
    default_trust: bool,
    auth_type: str | None,
    username: str | None,
    client_certificate_file: Path | None,
    client_key_file: Path | None,
    oauth_token_url: str | None,
    oauth_client_id: str | None,
    oauth_scope: tuple[str, ...],
    clear_oauth_scopes: bool,
    oauth_ca_file: str | None,
    oauth_default_trust: bool,
    replace_secrets: tuple[str, ...],
    registry_provider: str | None,
    registry_url: str | None,
    registry_auth: str | None,
    registry_username: str | None,
    registry_client_certificate_file: Path | None,
    registry_client_key_file: Path | None,
    registry_ca_file: str | None,
    registry_default_trust: bool,
    registry_oauth_token_url: str | None,
    registry_oauth_client_id: str | None,
    registry_oauth_scope: tuple[str, ...],
    clear_registry_oauth_scopes: bool,
    registry_oauth_ca_file: str | None,
    registry_oauth_default_trust: bool,
    registry_oauth_logical_cluster: str | None,
    registry_oauth_identity_pool_id: str | None,
    clear_registry_oauth_logical_cluster: bool,
    clear_registry_oauth_identity_pool_id: bool,
    remove_registry: bool,
) -> None:
    """Edit explicit fields of an existing profile."""
    try:
        if clear_oauth_scopes and oauth_scope:
            raise click.UsageError("--clear-oauth-scopes cannot be combined with --oauth-scope")
        if oauth_ca_file is not None and oauth_default_trust:
            raise click.UsageError("--oauth-ca-file cannot be combined with --oauth-default-trust")
        if registry_ca_file is not None and registry_default_trust:
            raise click.UsageError(
                "--registry-ca-file cannot be combined with --registry-default-trust"
            )
        if registry_oauth_ca_file is not None and registry_oauth_default_trust:
            raise click.UsageError(
                "--registry-oauth-ca-file cannot be combined with " "--registry-oauth-default-trust"
            )
        if clear_registry_oauth_scopes and registry_oauth_scope:
            raise click.UsageError(
                "--clear-registry-oauth-scopes cannot be combined with " "--registry-oauth-scope"
            )
        if clear_registry_oauth_logical_cluster and registry_oauth_logical_cluster is not None:
            raise click.UsageError(
                "--clear-registry-oauth-logical-cluster cannot be combined with "
                "--registry-oauth-logical-cluster"
            )
        if clear_registry_oauth_identity_pool_id and registry_oauth_identity_pool_id is not None:
            raise click.UsageError(
                "--clear-registry-oauth-identity-pool-id cannot be combined with "
                "--registry-oauth-identity-pool-id"
            )
        if len(set(replace_secrets)) != len(replace_secrets):
            raise click.UsageError("--replace-secret fields must be unique")
        kafka_replace_secrets = tuple(
            field for field in replace_secrets if field.startswith("kafka/")
        )
        registry_replace_secrets = tuple(
            field for field in replace_secrets if field.startswith("registry/")
        )
        unknown_replace_secrets = (
            set(replace_secrets) - set(kafka_replace_secrets) - set(registry_replace_secrets)
        )
        if unknown_replace_secrets:
            raise click.UsageError(f"unsupported credential field: {min(unknown_replace_secrets)}")
        scripted_edit = any(
            (
                bootstrap_servers is not None,
                description is not None,
                clear_description,
                bool(labels),
                bool(remove_labels),
                transport is not None,
                ca_file is not None,
                default_trust,
                auth_type is not None,
                username is not None,
                client_certificate_file is not None,
                client_key_file is not None,
                oauth_token_url is not None,
                oauth_client_id is not None,
                bool(oauth_scope),
                clear_oauth_scopes,
                oauth_ca_file is not None,
                oauth_default_trust,
                bool(replace_secrets),
                registry_provider is not None,
                registry_url is not None,
                registry_auth is not None,
                registry_username is not None,
                registry_client_certificate_file is not None,
                registry_client_key_file is not None,
                registry_ca_file is not None,
                registry_default_trust,
                registry_oauth_token_url is not None,
                registry_oauth_client_id is not None,
                bool(registry_oauth_scope),
                clear_registry_oauth_scopes,
                registry_oauth_ca_file is not None,
                registry_oauth_default_trust,
                registry_oauth_logical_cluster is not None,
                registry_oauth_identity_pool_id is not None,
                clear_registry_oauth_logical_cluster,
                clear_registry_oauth_identity_pool_id,
                remove_registry,
            )
        )
        if not scripted_edit:
            raise click.UsageError("edit requires at least one option; see 'kantrip edit --help'")
        current = load_profiles(missing_ok=True)
        current_profile = current.profile(profile_name)
        expected_revision = current.revision(profile_name)
        current_auth = current_profile["kafka"]["auth"]
        auth: KafkaAuthInput | None = None
        registry_input: RegistryAuthInput | None = None
        selected_auth = auth_type or str(current_auth["type"])
        auth_requested = any(
            (
                auth_type is not None,
                username is not None,
                client_certificate_file is not None,
                client_key_file is not None,
                oauth_token_url is not None,
                oauth_client_id is not None,
                bool(oauth_scope),
                clear_oauth_scopes,
                oauth_ca_file is not None,
                oauth_default_trust,
                bool(kafka_replace_secrets),
            )
        )
        if auth_requested:
            password_types = {"plain", "scram-sha-256", "scram-sha-512"}
            auth = _auth_input(
                selected_auth,
                username,
                client_certificate_file,
                client_key_file,
                password_required=(
                    selected_auth in password_types
                    and current_auth.get("type") not in password_types
                ),
                replace_fields=kafka_replace_secrets,
                oauth_token_url=oauth_token_url,
                oauth_client_id=oauth_client_id,
                oauth_scopes=(() if clear_oauth_scopes else oauth_scope if oauth_scope else None),
                oauth_ca_certificates=oauth_ca_file,
                oauth_default_trust=oauth_default_trust,
                oauth_secret_required=(
                    selected_auth == "oauth" and current_auth.get("type") != "oauth"
                ),
            )
        registry_requested = any(
            (
                registry_auth is not None,
                registry_username is not None,
                registry_client_certificate_file is not None,
                registry_client_key_file is not None,
                registry_ca_file is not None,
                registry_default_trust,
                registry_oauth_token_url is not None,
                registry_oauth_client_id is not None,
                bool(registry_oauth_scope),
                clear_registry_oauth_scopes,
                registry_oauth_ca_file is not None,
                registry_oauth_default_trust,
                registry_oauth_logical_cluster is not None,
                registry_oauth_identity_pool_id is not None,
                clear_registry_oauth_logical_cluster,
                clear_registry_oauth_identity_pool_id,
                bool(registry_replace_secrets),
            )
        )
        if registry_requested:
            current_registry = current_profile.get("registry")
            stored_url = None
            current_registry_auth: dict[str, Any] = {"type": "none"}
            current_registry_tls: dict[str, Any] = {}
            if isinstance(current_registry, dict):
                url_key = (
                    "apicurio.registry.url"
                    if current_registry.get("provider") == "apicurio"
                    else "schema.registry.url"
                )
                stored_url = current_registry.get(url_key)
                if isinstance(current_registry.get("auth"), dict):
                    current_registry_auth = current_registry["auth"]
                if isinstance(current_registry.get("tls"), dict):
                    current_registry_tls = current_registry["tls"]
            selected_registry_auth = registry_auth or str(current_registry_auth.get("type", "none"))
            # Stored fields of the current type carry over only when the type is unchanged.
            same_registry_auth = selected_registry_auth == current_registry_auth.get("type")
            stored_registry_auth = current_registry_auth if same_registry_auth else {}
            if selected_registry_auth != "oauth" and any(
                (
                    registry_oauth_token_url is not None,
                    registry_oauth_client_id is not None,
                    bool(registry_oauth_scope),
                    clear_registry_oauth_scopes,
                    registry_oauth_ca_file is not None,
                    registry_oauth_default_trust,
                    registry_oauth_logical_cluster is not None,
                    registry_oauth_identity_pool_id is not None,
                    clear_registry_oauth_logical_cluster,
                    clear_registry_oauth_identity_pool_id,
                )
            ):
                raise click.UsageError("Registry OAuth options require final --registry-auth oauth")
            if (registry_client_certificate_file is None) != (registry_client_key_file is None):
                raise click.UsageError(
                    "Registry mTLS requires both client certificate and key files"
                )
            registry_certificate = (
                current_registry_tls.get("clientCertificate") if same_registry_auth else None
            )
            registry_key = registry_key_password = None
            if registry_client_certificate_file is not None:
                assert registry_client_key_file is not None
                registry_certificate, registry_key, registry_key_password = _read_client_identity(
                    registry_client_certificate_file,
                    registry_client_key_file,
                    label="Registry",
                )
            registry_ca = (
                None
                if registry_default_trust
                else (
                    registry_ca_file
                    if registry_ca_file is not None
                    else current_registry_tls.get("caCertificates")
                )
            )
            registry_oauth_ca = (
                None
                if registry_oauth_default_trust
                else (
                    registry_oauth_ca_file
                    if registry_oauth_ca_file is not None
                    else stored_registry_auth.get("caCertificates")
                )
            )
            registry_scopes = (
                ()
                if clear_registry_oauth_scopes
                else (
                    registry_oauth_scope
                    if registry_oauth_scope
                    else tuple(stored_registry_auth.get("scopes", ()))
                )
            )
            registry_input = _registry_auth_input(
                selected_registry_auth,
                registry_username or stored_registry_auth.get("username"),
                registry_url=registry_url or stored_url,
                ca_certificates=registry_ca,
                client_certificate=registry_certificate,
                private_key=registry_key,
                private_key_password=registry_key_password,
                oauth_token_url=registry_oauth_token_url or stored_registry_auth.get("tokenUrl"),
                oauth_client_id=registry_oauth_client_id or stored_registry_auth.get("clientId"),
                oauth_scopes=registry_scopes,
                oauth_ca_certificates=registry_oauth_ca,
                oauth_logical_cluster=(
                    None
                    if clear_registry_oauth_logical_cluster
                    else registry_oauth_logical_cluster
                    or stored_registry_auth.get("logicalCluster")
                ),
                oauth_identity_pool_id=(
                    None
                    if clear_registry_oauth_identity_pool_id
                    else registry_oauth_identity_pool_id
                    or stored_registry_auth.get("identityPoolId")
                ),
                replace_fields=registry_replace_secrets,
                secret_required=(selected_registry_auth != current_registry_auth.get("type")),
            )
            if registry_input is None and registry_auth == "none":
                registry_input = RegistryAuthInput("none")
        profiles = edit_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
            clear_description=clear_description,
            labels=labels,
            remove_labels=remove_labels,
            transport=transport,
            ca_certificates=ca_file,
            default_trust=default_trust,
            auth=auth,
            registry_provider=registry_provider,
            registry_url=registry_url,
            registry_auth=registry_input,
            remove_registry=remove_registry,
            expected_profile_id=str(current_profile["id"]),
            expected_revision=expected_revision,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    _echo_committed_mutation(f"Updated profile '{profile_name}' in {profiles.path}")


@cli.command("remove")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option("--force", is_flag=True, help="Remove without an interactive confirmation.")
def remove_configured_profile(profile_name: str, force: bool) -> None:
    """Remove a profile."""
    try:
        current = load_profiles(missing_ok=True)
        profile = current.profile(profile_name)
        revision = current.revision(profile_name)
        if not force and not click.confirm(f"Remove profile '{profile_name}'?", default=False):
            click.echo("Removal canceled; profile was not changed.")
            return
        profiles = remove_profile(
            profile_name,
            expected_profile_id=str(profile["id"]),
            expected_revision=revision,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    _echo_committed_mutation(f"Removed profile '{profile_name}' from {profiles.path}")


@cli.command("list")
@local_no_color
@cloup.option(
    "-l",
    "--label",
    "labels",
    multiple=True,
    callback=_parse_labels,
    metavar="KEY=VALUE",
    help="Require an exact label; repeat to combine filters with AND.",
)
@cloup.option(
    "-o",
    "--output",
    "output_format",
    type=cloup.Choice(OUTPUT_FORMATS),
    default="human",
    show_default=True,
    help="Output representation.",
)
@cloup.pass_context
def list_profiles(context: cloup.Context, labels: dict[str, str], output_format: str) -> None:
    """List configured profiles."""
    try:
        profiles = load_profiles(missing_ok=True).profiles
    except ProfileStoreError as error:
        raise click.ClickException(str(error)) from error
    selected = filter_profiles(profiles, labels)
    if output_format == "human":
        if selected:
            console_from_context(context).print(create_profile_table(selected))
        return
    _print_structured_observation(context, list_observation(selected), output_format)


@cli.command("describe")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "-o",
    "--output",
    "output_format",
    type=cloup.Choice(OUTPUT_FORMATS),
    default="human",
    show_default=True,
    help="Output representation.",
)
@cloup.pass_context
def describe_profile(context: cloup.Context, profile_name: str, output_format: str) -> None:
    """Describe a profile without exposing secret or internal reference values."""
    try:
        profiles = load_profiles(missing_ok=True)
        profile = profiles.profile(profile_name)
        revision = profiles.revision(profile_name)
    except ProfileStoreError as error:
        raise click.ClickException(str(error)) from error
    observation = describe_observation(profile_name, revision, profile)
    if output_format == "human":
        console_from_context(context).print(create_profile_description(observation))
        return
    _print_structured_observation(context, observation, output_format)


@cli.command("current")
@local_no_color
def current_profile() -> None:
    """Show the profile active in the current Kantrip session."""
    profile_name = os.environ.get("KANTRIP_PROFILE")
    if not profile_name:
        raise click.ClickException(
            "no profile is active; run 'kantrip exec PROFILE' to start a profile session"
        )
    click.echo(profile_name)


@cli.command("doctor")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE", required=False)
@cloup.option(
    "--repair",
    is_flag=True,
    help="Apply migrations, reconcile credentials, and remove stale sessions.",
)
@cloup.option(
    "--verbose",
    is_flag=True,
    help="Show every diagnostic, including resolved paths and profile IDs.",
)
@cloup.option(
    "--sessions",
    is_flag=True,
    help="Show validated sessions captured for PROFILE.",
)
@cloup.pass_context
def doctor(
    context: cloup.Context,
    profile_name: str | None,
    repair: bool,
    verbose: bool,
    sessions: bool,
) -> None:
    """Inspect Kantrip, optionally applying deterministic local repairs."""
    _validate_doctor_options(profile_name, repair=repair, sessions=sessions)
    console = console_from_context(context)
    repair_healthy = _run_and_render_repair(console) if repair else True
    report = (
        run_doctor(profile_name=profile_name, include_sessions=sessions)
        if profile_name is not None or sessions
        else run_doctor()
    )
    _render_doctor_report(console, report, verbose=verbose)
    if not repair_healthy or not report.healthy:
        raise click.exceptions.Exit(1)


def _validate_doctor_options(
    profile_name: str | None,
    *,
    repair: bool,
    sessions: bool,
) -> None:
    if sessions and profile_name is None:
        raise click.UsageError("--sessions requires PROFILE")
    if repair and profile_name is not None:
        raise click.UsageError("PROFILE cannot be combined with --repair")
    if repair and sessions:
        raise click.UsageError("--sessions cannot be combined with --repair")


def _run_and_render_repair(console: Console) -> bool:
    repair_report = run_repair()
    console.print(Text("Kantrip Repair", style="heading"))
    for action in repair_report.actions:
        console.print(
            Padding(
                create_status_text(console, action.status, action.message),
                (0, 0, 0, 2),
                expand=False,
            )
        )
    console.print()
    return repair_report.healthy


def _render_doctor_report(console: Console, report: DoctorReport, *, verbose: bool) -> None:
    console.print(Text("Kantrip Doctor", style="heading"))
    for section, checks in report.sections(verbose=verbose):
        console.print()
        console.print(Text(section, style="heading"))
        for index, check in enumerate(checks):
            status_text = create_status_text(console, check.status, check.message)
            if check.verbose_only:
                has_next_detail = index + 1 < len(checks) and checks[index + 1].verbose_only
                detail_text = Text(
                    "├─ " if has_next_detail else "└─ ",
                    style="muted",
                )
                detail_text.append_text(status_text)
                status_text = detail_text
            console.print(
                Padding(
                    status_text,
                    (0, 0, 0, 2),
                    expand=False,
                )
            )
    console.print()
    if report.error_count:
        summary_status: StatusKind = "error"
        summary = _doctor_summary("Unhealthy", report.error_count, report.warning_count)
    elif report.warning_count:
        summary_status = "warning"
        summary = _doctor_summary("Healthy", 0, report.warning_count)
    else:
        summary_status = "success"
        summary = "Healthy"
    console.print(create_status_text(console, summary_status, summary))


def _doctor_summary(label: str, errors: int, warnings: int) -> str:
    details: list[str] = []
    if errors:
        details.append(f"{errors} error{'s' if errors != 1 else ''}")
    if warnings:
        details.append(f"{warnings} warning{'s' if warnings != 1 else ''}")
    return f"{label} with {', '.join(details)}"


@cli.command("ping")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "--timeout",
    type=click.FloatRange(min=0.1),
    default=5.0,
    show_default=True,
    help="Maximum time in seconds for the connectivity check.",
)
@cloup.option("-q", "--quiet", is_flag=True, help="Return only the connectivity exit status.")
@cloup.pass_context
def ping(context: cloup.Context, profile_name: str, timeout: float, quiet: bool) -> None:
    """Check PROFILE's Kafka and configured registry connections."""
    console = console_from_context(context)
    error_console = error_console_from_context(context)
    try:
        snapshot = resolve_profile_snapshot(profile_name)
    except ProfileStoreError as error:
        if not quiet:
            error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    try:
        progress = (
            nullcontext() if quiet else show_progress(console, f"Checking profile '{profile_name}'")
        )
        with progress:
            result = ping_profile(
                snapshot.document,
                timeout=timeout,
                kafka=snapshot.kafka,
                resolved_registry=snapshot.registry,
            )
    except PingError as error:
        if not quiet:
            _print_ping_failure(
                error_console, f"Could not connect for profile '{profile_name}'", error
            )
            if snapshot.registry is not None:
                error_console.print(
                    create_status_text(
                        error_console,
                        "warning",
                        f"{snapshot.registry.display_name} check skipped: Kafka check failed",
                    )
                )
        raise click.exceptions.Exit(1) from error
    if not quiet:
        _print_ping_result(console, error_console, profile_name, result)
    if not result.healthy:
        raise click.exceptions.Exit(1)


def _print_ping_result(
    console: Console,
    error_console: Console,
    profile_name: str,
    result: PingResult,
) -> None:
    console.print(
        create_status_text(
            console,
            "success",
            f"Kafka transport: {result.kafka_transport}; "
            f"authentication: {result.kafka_authentication}",
        )
    )
    if result.registry is not None:
        product = (
            "Apicurio Registry"
            if result.registry.provider == "apicurio"
            else "Confluent Schema Registry"
        )
        console.print(
            create_status_text(
                console,
                "success",
                f"{product} transport: {result.registry.transport}; "
                f"proof: {result.registry.proof}",
            )
        )
    if result.registry_error is not None:
        _print_ping_failure(
            error_console,
            f"Registry check failed for profile '{profile_name}'",
            result.registry_error,
        )


def _print_ping_failure(error_console: Console, prefix: str, error: PingError) -> None:
    detail = f"\nCause: {error.detail}" if error.detail else ""
    error_console.print(create_status_text(error_console, "error", f"{prefix}: {error}{detail}"))


@cli.command("exec", context_settings={"ignore_unknown_options": True})
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.argument("command", nargs=-1, type=click.UNPROCESSED)
def execute_profile(profile_name: str, command: tuple[str, ...]) -> None:
    """Run a command or interactive subshell with PROFILE."""
    try:
        ensure_session_available()
        snapshot = resolve_profile_snapshot(profile_name)
        exit_code = run_profile_session(
            profile_name,
            snapshot.document,
            command,
            profile_revision=snapshot.revision,
            resolved_kafka=snapshot.kafka,
            resolved_registry=snapshot.registry,
        )
    except (ProfileStoreError, SessionError) as error:
        raise click.ClickException(str(error)) from error
    raise click.exceptions.Exit(exit_code)


def main() -> None:
    """Run the CLI using its installed program name."""
    cli(prog_name="kantrip")


if __name__ == "__main__":
    main()
