"""Kantrip command-line entry point."""

from __future__ import annotations

import os
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


def console_from_context(context: cloup.Context) -> Console:
    """Return the console initialized for the current invocation."""
    obj: dict[str, Any] = context.ensure_object(dict)
    console = obj.get("console")
    if not isinstance(console, Console):
        raise TypeError("Kantrip console has not been initialized")
    return console


@cli.command("add")
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option(
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    default=("localhost:9092",),
    show_default=True,
    help="Kafka broker address; may be repeated.",
)
@cloup.option("--description", help="Optional profile description.")
def add_configured_profile(
    profile_name: str,
    bootstrap_servers: tuple[str, ...],
    description: str | None,
) -> None:
    """Add a plaintext profile."""
    try:
        configuration = add_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
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
