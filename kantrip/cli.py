"""Kantrip command-line entry point."""

from __future__ import annotations

import os
import sys
from typing import Any

import click
import cloup
import yaml
from rich.console import Console

from kantrip import APP_VERSION
from kantrip.config import ConfigurationError, add_profile, load_configuration, remove_profile
from kantrip.console import (
    create_console,
    create_profile_table,
    create_status_text,
    create_yaml_syntax,
)
from kantrip.doctor import run_doctor
from kantrip.ping import PingError, ping_profile
from kantrip.redaction import redact_mapping
from kantrip.session import SessionError, ensure_session_available, run_profile_session

EPILOG = "More information at https://github.com/sauljabin/kantrip."


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
    context.ensure_object(dict)
    context.obj["console"] = create_console(no_color=no_color)
    context.obj["error_console"] = create_console(
        stream=sys.stderr,
        no_color=no_color,
    )


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


def _split_bootstrap_servers(
    context: click.Context, parameter: click.Parameter, value: str
) -> tuple[str, ...]:
    del context, parameter
    servers = tuple(server.strip() for server in value.split(","))
    if not servers or any(not server for server in servers):
        raise click.BadParameter("must be a comma-separated list of host:port addresses")
    return servers


@cli.command("add")
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
    "--schema-registry-url",
    help="Optional plain, unauthenticated Schema Registry URL.",
)
def add_configured_profile(
    profile_name: str,
    bootstrap_servers: tuple[str, ...],
    description: str | None,
    schema_registry_url: str | None,
) -> None:
    """Add a plaintext profile."""
    try:
        configuration = add_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
            schema_registry_url=schema_registry_url,
        )
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Added profile '{profile_name}' to {configuration.path}")


@cli.command("remove")
@cloup.argument("profile_name", metavar="PROFILE")
def remove_configured_profile(profile_name: str) -> None:
    """Remove a profile."""
    try:
        configuration = remove_profile(profile_name)
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Removed profile '{profile_name}' from {configuration.path}")


@cli.command("list")
@cloup.pass_context
def list_profiles(context: cloup.Context) -> None:
    """List configured profiles."""
    try:
        configuration = load_configuration(missing_ok=True)
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    if configuration.profiles:
        console_from_context(context).print(create_profile_table(configuration.profiles))


@cli.command("show")
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.pass_context
def show_profile(context: cloup.Context, profile_name: str) -> None:
    """Show a profile with sensitive-looking values redacted."""
    try:
        profile = load_configuration(missing_ok=True).profile(profile_name)
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    contents = yaml.safe_dump(redact_mapping(profile), sort_keys=False)
    console_from_context(context).print(create_yaml_syntax(contents), end="")


@cli.command("current")
def current_profile() -> None:
    """Show the profile active in the current Kantrip session."""
    profile_name = os.environ.get("KANTRIP_PROFILE")
    if not profile_name:
        raise click.ClickException(
            "no profile is active; run 'kantrip exec PROFILE' to start a profile session"
        )
    click.echo(profile_name)


@cli.command("doctor")
@cloup.pass_context
def doctor(context: cloup.Context) -> None:
    """Check Kantrip's local configuration and command environment."""
    console = console_from_context(context)
    report = run_doctor()
    for check in report.checks:
        console.print(create_status_text(console, check.status, check.message))
    if not report.healthy:
        raise click.exceptions.Exit(1)


@cli.command("ping")
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "--timeout",
    type=click.FloatRange(min=0.1),
    default=5.0,
    show_default=True,
    help="Maximum time in seconds for the connectivity check.",
)
@cloup.pass_context
def ping(context: cloup.Context, profile_name: str, timeout: float) -> None:
    """Check PROFILE's Kafka and configured Schema Registry connections."""
    console = console_from_context(context)
    try:
        profile = load_configuration(missing_ok=True).profile(profile_name)
        console.print(create_status_text(console, "progress", f"Checking profile '{profile_name}'"))
        result = ping_profile(profile, timeout=timeout)
    except ConfigurationError as error:
        error_console = error_console_from_context(context)
        error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    except PingError as error:
        error_console = error_console_from_context(context)
        error_console.print(
            create_status_text(
                error_console,
                "error",
                f"Could not connect for profile '{profile_name}': {error}",
            )
        )
        raise click.exceptions.Exit(1) from error
    console.print(
        create_status_text(
            console,
            "success",
            f"Connected to Kafka ({result.broker_count} broker"
            f"{'s' if result.broker_count != 1 else ''})",
        )
    )
    if result.schema_registry_subject_count is not None:
        console.print(
            create_status_text(
                console,
                "success",
                f"Connected to Schema Registry ({result.schema_registry_subject_count} subject"
                f"{'s' if result.schema_registry_subject_count != 1 else ''})",
            )
        )


@cli.command("exec", context_settings={"ignore_unknown_options": True})
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.argument("command", nargs=-1, type=click.UNPROCESSED)
def execute_profile(profile_name: str, command: tuple[str, ...]) -> None:
    """Run a command or interactive subshell with PROFILE."""
    try:
        ensure_session_available()
        profile = load_configuration(missing_ok=True).profile(profile_name)
        exit_code = run_profile_session(profile_name, profile, command)
    except (ConfigurationError, SessionError) as error:
        raise click.ClickException(str(error)) from error
    raise click.exceptions.Exit(exit_code)


def main() -> None:
    """Run the CLI using its installed program name."""
    cli(prog_name="kantrip")


if __name__ == "__main__":
    main()
