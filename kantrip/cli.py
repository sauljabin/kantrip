"""Kantrip command-line entry point."""

from __future__ import annotations

from typing import Any

import click
import cloup
import yaml

from kantrip import APP_VERSION
from kantrip.config import ConfigurationError, initialize_configuration, load_configuration
from kantrip.console import Consoles, create_consoles
from kantrip.redaction import redact_mapping
from kantrip.session import SessionError, run_profile_session

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
    context.obj["consoles"] = create_consoles(no_color=no_color)


def consoles_from_context(context: cloup.Context) -> Consoles:
    """Return the consoles initialized for the current invocation."""
    obj: dict[str, Any] = context.ensure_object(dict)
    consoles = obj.get("consoles")
    if not isinstance(consoles, Consoles):
        raise TypeError("Kantrip consoles have not been initialized")
    return consoles


@cli.group("config", no_args_is_help=True)
def config_group() -> None:
    """Inspect Kantrip configuration."""


@config_group.command("validate")
def validate_config() -> None:
    """Validate the resolved configuration file."""
    try:
        configuration = load_configuration()
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Configuration is valid: {configuration.path}")


@config_group.command("init")
@cloup.option(
    "--profile",
    "profile_name",
    default="local",
    show_default=True,
    help="Name of the initial profile.",
)
@cloup.option(
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    default=("localhost:9092",),
    show_default=True,
    help="Kafka broker address; may be repeated.",
)
def initialize_config(profile_name: str, bootstrap_servers: tuple[str, ...]) -> None:
    """Create a new plaintext configuration and initial profile."""
    try:
        configuration = initialize_configuration(
            profile_name=profile_name,
            bootstrap_servers=bootstrap_servers,
        )
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Created configuration: {configuration.path}")
    click.echo(f"Created profile: {profile_name}")


@cli.command("list")
def list_profiles() -> None:
    """List configured profiles."""
    try:
        configuration = load_configuration()
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    for name, profile in configuration.profiles.items():
        description = profile.get("description")
        click.echo(f"{name}\t{description}" if description else name)


@cli.command("show")
@cloup.argument("profile_name", metavar="PROFILE")
def show_profile(profile_name: str) -> None:
    """Show a profile without revealing credential references."""
    try:
        profile = load_configuration().profile(profile_name)
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(yaml.safe_dump(redact_mapping(profile), sort_keys=False), nl=False)


@cli.command("exec", context_settings={"ignore_unknown_options": True})
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.argument("command", nargs=-1, type=click.UNPROCESSED)
def execute_profile(profile_name: str, command: tuple[str, ...]) -> None:
    """Run a command or interactive subshell with PROFILE."""
    try:
        profile = load_configuration().profile(profile_name)
        exit_code = run_profile_session(profile_name, profile, command)
    except (ConfigurationError, SessionError) as error:
        raise click.ClickException(str(error)) from error
    raise click.exceptions.Exit(exit_code)


def main() -> None:
    """Run the CLI using its installed program name."""
    cli(prog_name="kantrip")


if __name__ == "__main__":
    main()
