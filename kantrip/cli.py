"""Kantrip command-line entry point."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from typing import Any, TypeVar

import click
import cloup
import yaml
from rich.console import Console
from rich.padding import Padding
from rich.text import Text

from kantrip import APP_VERSION
from kantrip.config import ConfigurationError, add_profile, load_configuration, remove_profile
from kantrip.console import (
    StatusKind,
    create_console,
    create_profile_table,
    create_status_text,
    create_yaml_syntax,
    show_progress,
)
from kantrip.doctor import run_doctor
from kantrip.ping import PingError, ping_profile
from kantrip.redaction import redact_mapping
from kantrip.runtime import SessionRuntimeError, scan_sessions
from kantrip.session import SessionError, ensure_session_available, run_profile_session

EPILOG = "More information at https://github.com/sauljabin/kantrip."
CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])


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


def _split_bootstrap_servers(
    context: click.Context, parameter: click.Parameter, value: str
) -> tuple[str, ...]:
    del context, parameter
    servers = tuple(server.strip() for server in value.split(","))
    if not servers or any(not server for server in servers):
        raise click.BadParameter("must be a comma-separated list of host:port addresses")
    return servers


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
    registry_provider: str | None,
    registry_url: str | None,
) -> None:
    """Add a plaintext profile."""
    try:
        configuration = add_profile(
            profile_name,
            bootstrap_servers=bootstrap_servers,
            description=description,
            registry_provider=registry_provider,
            registry_url=registry_url,
        )
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Added profile '{profile_name}' to {configuration.path}")


@cli.command("remove")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
def remove_configured_profile(profile_name: str) -> None:
    """Remove a profile."""
    try:
        configuration = remove_profile(profile_name)
    except ConfigurationError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Removed profile '{profile_name}' from {configuration.path}")


@cli.command("list")
@local_no_color
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
@local_no_color
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
@local_no_color
def current_profile() -> None:
    """Show the profile active in the current Kantrip session."""
    profile_name = os.environ.get("KANTRIP_PROFILE")
    if not profile_name:
        raise click.ClickException(
            "no profile is active; run 'kantrip exec PROFILE' to start a profile session"
        )
    click.echo(profile_name)


@cli.command("cleanup")
@local_no_color
@cloup.option(
    "--dry-run",
    is_flag=True,
    help="Report stale sessions without removing them.",
)
@cloup.pass_context
def cleanup_sessions(context: cloup.Context, dry_run: bool) -> None:
    """Remove validated session artifacts left by abnormal termination."""
    try:
        report = scan_sessions(remove=not dry_run)
    except SessionRuntimeError as error:
        error_console = error_console_from_context(context)
        error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    console = console_from_context(context)
    action = "Would remove" if dry_run else "Removed"
    affected = report.stale if dry_run else report.removed
    console.print(
        create_status_text(
            console,
            "cleanup",
            f"{action} {affected} stale session{'s' if affected != 1 else ''}; "
            f"active: {report.active}; recent: {report.recent}; invalid: {report.invalid}; "
            f"failed: {report.failed}; truncated: {'yes' if report.truncated else 'no'}",
        )
    )
    if report.has_errors:
        error_console = error_console_from_context(context)
        error_console.print(
            create_status_text(error_console, "error", "Session cleanup was incomplete")
        )
        raise click.exceptions.Exit(1)


@cli.command("doctor")
@local_no_color
@cloup.option(
    "--verbose",
    is_flag=True,
    help="Show every diagnostic, including resolved paths and profile IDs.",
)
@cloup.pass_context
def doctor(context: cloup.Context, verbose: bool) -> None:
    """Check Kantrip's local configuration and command environment."""
    console = console_from_context(context)
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
    if not report.healthy:
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
@cloup.option(
    "--verbose",
    is_flag=True,
    help="Include the sanitized underlying connection error.",
)
@cloup.pass_context
def ping(context: cloup.Context, profile_name: str, timeout: float, verbose: bool) -> None:
    """Check PROFILE's Kafka and configured registry connections."""
    console = console_from_context(context)
    try:
        profile = load_configuration(missing_ok=True).profile(profile_name)
        with show_progress(console, f"Checking profile '{profile_name}'"):
            result = ping_profile(profile, timeout=timeout)
    except ConfigurationError as error:
        error_console = error_console_from_context(context)
        error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    except PingError as error:
        error_console = error_console_from_context(context)
        detail = f"\nCause: {error.detail}" if verbose and error.detail else ""
        error_console.print(
            create_status_text(
                error_console,
                "error",
                f"Could not connect for profile '{profile_name}': {error}{detail}",
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
