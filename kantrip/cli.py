"""Kantrip command-line entry point."""

from __future__ import annotations

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
from kantrip.doctor import run_doctor
from kantrip.kafka import KafkaProfileError, read_ca_bundle
from kantrip.maintenance import run_repair
from kantrip.ping import PingError, ping_profile
from kantrip.profile_output import (
    OutputFormat,
    describe_observation,
    dump_observation,
    filter_profiles,
    list_observation,
)
from kantrip.profiles import (
    ProfileStoreError,
    add_profile,
    edit_profile,
    load_profiles,
    remove_profile,
)
from kantrip.session import SessionError, ensure_session_available, run_profile_session

EPILOG = "More information at https://github.com/sauljabin/kantrip."
CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])
OUTPUT_FORMATS = ("human", "json", "yaml")


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
    "--registry-provider",
    type=cloup.Choice(("confluent", "apicurio")),
    help="Registry provider; defaults to confluent when --registry-url is supplied.",
)
@cloup.option(
    "--registry-url",
    help="Optional plain registry URL.",
)
def add_configured_profile(
    profile_name: str,
    bootstrap_servers: tuple[str, ...],
    description: str | None,
    labels: dict[str, str],
    transport: str,
    ca_file: str | None,
    registry_provider: str | None,
    registry_url: str | None,
) -> None:
    """Add a profile."""
    try:
        profiles = add_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
            labels=labels,
            transport=transport,
            ca_certificates=ca_file,
            registry_provider=registry_provider,
            registry_url=registry_url,
        )
    except ProfileStoreError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Added profile '{profile_name}' to {profiles.path}")


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
    "--registry-provider",
    type=cloup.Choice(("confluent", "apicurio")),
    help="Replace the Registry provider.",
)
@cloup.option("--registry-url", help="Add or replace the plain Registry URL.")
@cloup.option("--remove-registry", is_flag=True, help="Remove the complete Registry connection.")
def edit_configured_profile(
    profile_name: str,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: dict[str, str],
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_file: str | None,
    default_trust: bool,
    registry_provider: str | None,
    registry_url: str | None,
    remove_registry: bool,
) -> None:
    """Edit explicit fields of an existing profile."""
    try:
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
            registry_provider=registry_provider,
            registry_url=registry_url,
            remove_registry=remove_registry,
        )
    except ProfileStoreError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Updated profile '{profile_name}' in {profiles.path}")


@cli.command("remove")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
def remove_configured_profile(profile_name: str) -> None:
    """Remove a profile."""
    try:
        profiles = remove_profile(profile_name)
    except ProfileStoreError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Removed profile '{profile_name}' from {profiles.path}")


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
@cloup.pass_context
def doctor(context: cloup.Context, repair: bool, verbose: bool) -> None:
    """Inspect Kantrip, optionally applying deterministic local repairs."""
    console = console_from_context(context)
    repair_healthy = True
    if repair:
        repair_report = run_repair()
        repair_healthy = repair_report.healthy
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
    report = run_doctor()
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
    if not repair_healthy or not report.healthy:
        raise click.exceptions.Exit(1)


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
@cloup.option("--quiet", is_flag=True, help="Return only the connectivity exit status.")
@cloup.pass_context
def ping(context: cloup.Context, profile_name: str, timeout: float, quiet: bool) -> None:
    """Check PROFILE's Kafka and configured registry connections."""
    console = console_from_context(context)
    try:
        profile = load_profiles(missing_ok=True).profile(profile_name)
        progress = (
            nullcontext() if quiet else show_progress(console, f"Checking profile '{profile_name}'")
        )
        with progress:
            result = ping_profile(profile, timeout=timeout)
    except ProfileStoreError as error:
        if not quiet:
            error_console = error_console_from_context(context)
            error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    except PingError as error:
        if not quiet:
            error_console = error_console_from_context(context)
            detail = f"\nCause: {error.detail}" if error.detail else ""
            error_console.print(
                create_status_text(
                    error_console,
                    "error",
                    f"Could not connect for profile '{profile_name}': {error}{detail}",
                )
            )
        raise click.exceptions.Exit(1) from error
    if quiet:
        return
    console.print(
        create_status_text(
            console,
            "success",
            f"Connected to Kafka ({result.broker_count} broker"
            f"{'s' if result.broker_count != 1 else ''})",
        )
    )
    if result.registry is not None:
        resource = "artifact" if result.registry.provider == "apicurio" else "subject"
        product = (
            "Apicurio Registry"
            if result.registry.provider == "apicurio"
            else "Confluent Schema Registry"
        )
        console.print(
            create_status_text(
                console,
                "success",
                f"Connected to {product} ({result.registry.count} {resource}"
                f"{'s' if result.registry.count != 1 else ''})",
            )
        )


@cli.command("exec", context_settings={"ignore_unknown_options": True})
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.argument("command", nargs=-1, type=click.UNPROCESSED)
def execute_profile(profile_name: str, command: tuple[str, ...]) -> None:
    """Run a command or interactive subshell with PROFILE."""
    try:
        ensure_session_available()
        profile = load_profiles(missing_ok=True).profile(profile_name)
        exit_code = run_profile_session(profile_name, profile, command)
    except (ProfileStoreError, SessionError) as error:
        raise click.ClickException(str(error)) from error
    raise click.exceptions.Exit(exit_code)


def main() -> None:
    """Run the CLI using its installed program name."""
    cli(prog_name="kantrip")


if __name__ == "__main__":
    main()
