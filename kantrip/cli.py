"""Kantrip command-line entry point."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from typing import Any, TypeVar, cast

import click
import cloup
from rich.console import Console
from rich.padding import Padding
from rich.text import Text

from kantrip import APP_VERSION
from kantrip.cli_inputs import (
    AddOptions,
    EditOptions,
    add_authentication,
    add_rule_violation,
    edit_kafka_authentication,
    edit_registry_authentication,
    edit_rule_violation,
    validate_edit_options,
)
from kantrip.cli_options import add_profile_options, edit_profile_options, parse_labels
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
from kantrip.maintenance import run_repair
from kantrip.ping import PingError, PingResult, ping_observation, ping_profile
from kantrip.profile_output import (
    OutputFormat,
    describe_observation,
    dump_observation,
    filter_profiles,
    list_observation,
)
from kantrip.profile_storage import ProfileStoreError, load_profiles
from kantrip.profiles import add_profile, edit_profile, remove_profile, resolve_profile_snapshot
from kantrip.session import SessionError, ensure_session_available, run_profile_session

EPILOG = "More information at https://github.com/sauljabin/kantrip."
CommandFunction = TypeVar("CommandFunction", bound=Callable[..., Any])
OUTPUT_FORMATS = ("human", "json", "yaml")


class _ProfileClickException(click.ClickException):
    """Render a safe profile error with its public mutation exit status."""


class _InvalidProfileClickException(_ProfileClickException):
    exit_code = 2


class _CommittedProfileClickException(_ProfileClickException):
    exit_code = 3


class _UnknownProfileClickException(_ProfileClickException):
    exit_code = 4


def _profile_click_exception(error: ProfileStoreError) -> _ProfileClickException:
    exception_type = {
        2: _InvalidProfileClickException,
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


def _connection_summary(profile: object) -> str:
    """Summarize a committed profile's Kafka connection for the success line."""
    kafka = profile.get("kafka") if isinstance(profile, dict) else None
    if not isinstance(kafka, dict):
        return "connection unknown"
    auth = kafka.get("auth")
    auth_type = auth.get("type") if isinstance(auth, dict) else None
    servers = ",".join(str(server) for server in kafka.get("bootstrapServers", ()))
    authentication = "no authentication" if auth_type in (None, "none") else auth_type
    return f"{servers}, {kafka.get('transport')}, {authentication}"


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
    # Soft wrap: never insert line breaks, which would corrupt long JSON or YAML values.
    console_from_context(context).print(
        create_structured_syntax(contents, selected_format),
        end="",
        soft_wrap=True,
    )


@cli.command("add")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@add_profile_options
def add_configured_profile(profile_name: str, **values: Any) -> None:
    """Add a profile."""
    options = AddOptions(**values)
    violation = add_rule_violation(options)
    if violation is not None:
        raise _InvalidProfileClickException(violation)
    try:
        auth, registry_auth = add_authentication(options)
        profiles = add_profile(
            profile_name,
            bootstrap_servers=options.bootstrap_servers,
            description=options.description,
            labels=options.labels,
            transport=options.resolved_transport,
            ca_certificates=options.ca_file,
            auth=auth,
            registry_provider=options.registry_provider,
            registry_url=options.registry_url,
            registry_auth=registry_auth,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    summary = _connection_summary(profiles.profiles.get(profile_name))
    _echo_committed_mutation(f"Added profile '{profile_name}': {summary}")


@cli.command("edit")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@edit_profile_options
def edit_configured_profile(profile_name: str, **values: Any) -> None:
    """Edit explicit fields of an existing profile."""
    options = EditOptions(**values)
    try:
        kafka_replace, registry_replace = validate_edit_options(options)
        if not options.has_changes:
            raise click.UsageError("edit requires at least one option; see 'kantrip edit --help'")
        current = load_profiles(missing_ok=True)
        current_profile = current.profile(profile_name)
        expected_revision = current.revision(profile_name)
        violation = edit_rule_violation(options, current_profile)
        if violation is not None:
            raise _InvalidProfileClickException(violation)
        auth = edit_kafka_authentication(options, current_profile["kafka"]["auth"], kafka_replace)
        registry_auth = edit_registry_authentication(
            options, current_profile.get("registry"), registry_replace
        )
        profiles = edit_profile(
            profile_name,
            bootstrap_servers=options.bootstrap_servers,
            description=options.description,
            clear_description=options.unsets("description"),
            labels=options.labels,
            remove_labels=options.removed_labels,
            transport=options.transport,
            ca_certificates=options.ca_file,
            default_trust=options.unsets("kafka.tls.ca"),
            auth=auth,
            registry_provider=options.registry_provider,
            registry_url=options.registry_url,
            registry_auth=registry_auth,
            remove_registry=options.unsets("registry"),
            expected_profile_id=str(current_profile["id"]),
            expected_revision=expected_revision,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    summary = _connection_summary(profiles.profiles.get(profile_name))
    _echo_committed_mutation(f"Updated profile '{profile_name}': {summary}")


@cli.command("remove")
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.option("-y", "--yes", is_flag=True, help="Remove without asking for confirmation.")
def remove_configured_profile(profile_name: str, yes: bool) -> None:
    """Remove a profile."""
    try:
        current = load_profiles(missing_ok=True)
        profile = current.profile(profile_name)
        revision = current.revision(profile_name)
        if not yes and not _stdin_is_terminal():
            raise click.UsageError("confirmation requires a terminal; pass --yes")
        if not yes and not click.confirm(f"Remove profile '{profile_name}'?", default=False):
            click.echo("Removal canceled; profile was not changed.")
            return
        remove_profile(
            profile_name,
            expected_profile_id=str(profile["id"]),
            expected_revision=revision,
        )
    except ProfileStoreError as error:
        raise _profile_click_exception(error) from error
    _echo_committed_mutation(f"Removed profile '{profile_name}'")


def _stdin_is_terminal() -> bool:
    return bool(sys.stdin and sys.stdin.isatty())


def _stdout_is_terminal() -> bool:
    return bool(sys.stdout and sys.stdout.isatty())


@cli.command("list")
@local_no_color
@cloup.option(
    "-l",
    "--label",
    "labels",
    multiple=True,
    callback=parse_labels,
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
        elif not profiles and _stdout_is_terminal():
            click.echo("No profiles yet. Add one with: kantrip add NAME", err=True)
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
@cloup.argument("profile_name", metavar="[PROFILE]", required=False)
@cloup.option(
    "--repair",
    is_flag=True,
    help="Apply migrations, reconcile credentials, and remove stale sessions.",
)
@cloup.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show every diagnostic, including resolved paths and profile IDs.",
)
@cloup.option(
    "--sessions",
    is_flag=True,
    help="Show each captured session, for PROFILE or for every profile.",
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
    report = run_doctor(profile_name=profile_name, include_sessions=sessions)
    _render_doctor_report(console, report, verbose=verbose)
    if not repair_healthy or not report.healthy:
        raise click.exceptions.Exit(1)


def _validate_doctor_options(
    profile_name: str | None,
    *,
    repair: bool,
    sessions: bool,
) -> None:
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
def ping(
    context: cloup.Context, profile_name: str, timeout: float, quiet: bool, output_format: str
) -> None:
    """Check PROFILE's Kafka and configured Registry connections.

    Each service is reported as ok, failed, or skipped (not attempted). The exit
    status is 0 only when every attempted check is ok.
    """
    if quiet and output_format != "human":
        raise click.UsageError("--quiet cannot be combined with --output json or yaml")
    console = console_from_context(context)
    error_console = error_console_from_context(context)
    try:
        snapshot = resolve_profile_snapshot(profile_name)
    except ProfileStoreError as error:
        if not quiet:
            error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error
    show_status = output_format == "human" and not quiet
    progress = (
        show_progress(console, f"Checking profile '{profile_name}'")
        if show_status
        else nullcontext()
    )
    result: PingResult | None = None
    kafka_error: PingError | None = None
    try:
        with progress:
            result = ping_profile(
                snapshot.document,
                timeout=timeout,
                kafka=snapshot.kafka,
                resolved_registry=snapshot.registry,
            )
    except PingError as error:
        kafka_error = error
    observation = ping_observation(
        profile_name,
        result,
        kafka_error=kafka_error,
        registry_provider=snapshot.registry.provider if snapshot.registry else None,
    )
    if output_format != "human":
        _print_structured_observation(context, observation, output_format)
    elif not quiet:
        _print_ping_observation(console, error_console, observation)
    if not observation["healthy"]:
        raise click.exceptions.Exit(1)


def _print_ping_observation(
    console: Console, error_console: Console, observation: Mapping[str, Any]
) -> None:
    profile_name = observation["profile"]
    kafka = observation["kafka"]
    if kafka["status"] == "ok":
        console.print(
            create_status_text(
                console,
                "success",
                f"Kafka transport: {kafka['transport']}; "
                f"authentication: {kafka['authentication']}",
            )
        )
    else:
        _print_ping_failure(error_console, f"Could not connect for profile '{profile_name}'", kafka)
    registry = observation["registry"]
    if registry is None:
        return
    product = _registry_product(registry.get("provider"))
    if registry["status"] == "ok":
        console.print(
            create_status_text(
                console,
                "success",
                f"{product} transport: {registry['transport']}; proof: {registry['proof']}",
            )
        )
        if registry["warning"]:
            error_console.print(
                create_status_text(error_console, "warning", f"Warning: {registry['warning']}")
            )
    elif registry["status"] == "failed":
        _print_ping_failure(
            error_console, f"Registry check failed for profile '{profile_name}'", registry
        )
    else:
        error_console.print(
            create_status_text(
                error_console, "warning", f"{product} check skipped: {registry['reason']}"
            )
        )


def _registry_product(provider: object) -> str:
    return "Apicurio Registry" if provider == "apicurio" else "Confluent Schema Registry"


def _print_ping_failure(error_console: Console, prefix: str, failure: Mapping[str, Any]) -> None:
    detail = f"\nCause: {failure['cause']}" if failure.get("cause") else ""
    error_console.print(
        create_status_text(error_console, "error", f"{prefix}: {failure['error']}{detail}")
    )


@cli.command(
    "exec",
    context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False},
)
@local_no_color
@cloup.argument("profile_name", metavar="PROFILE")
@cloup.argument("command", nargs=-1, type=click.UNPROCESSED)
def execute_profile(profile_name: str, command: tuple[str, ...]) -> None:
    """Run COMMAND, or an interactive subshell, with PROFILE.

    Everything after COMMAND is passed to it unchanged, so
    `kantrip exec PROFILE kafka-topics --help` shows the tool's help. Kantrip's
    own options go before PROFILE; `--` before COMMAND is optional.
    """
    if command[:1] == ("--",):
        command = command[1:]
    if command and command[0].startswith("-"):
        raise click.UsageError(
            f"'{command[0]}' is not a command; put Kantrip options before PROFILE: "
            "kantrip exec [OPTIONS] PROFILE [COMMAND]..."
        )
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
