"""Exercise Kantrip's supported clients against the manual sandbox."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

import click
import cloup
from rich.console import Console
from rich.text import Text

from kantrip._files import write_exclusive_text
from kantrip.adapters import (
    KAFKA_ACLS_EXECUTABLES,
    KAFKA_BROKER_API_VERSIONS_EXECUTABLES,
    KAFKA_CONFIGS_EXECUTABLES,
    KAFKA_CONSOLE_CONSUMER_EXECUTABLES,
    KAFKA_CONSOLE_PRODUCER_EXECUTABLES,
    KAFKA_CONSUMER_GROUPS_EXECUTABLES,
    KAFKA_TOPICS_EXECUTABLES,
    KCAT_EXECUTABLES,
    SCHEMA_REGISTRY_EXECUTABLES,
)
from kantrip.console import create_console, create_status_text, show_progress
from kantrip.shells import SUPPORTED_SHELLS, quote_shell_argument
from scripts import TerminalTimeout, run_terminal
from tests.e2e.terminal import TerminalProcess, TerminalProcessError

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_REGISTRY_URL = "http://localhost:8081"
KAFKA_COMMANDS = {
    "topics": KAFKA_TOPICS_EXECUTABLES,
    "producer": KAFKA_CONSOLE_PRODUCER_EXECUTABLES,
    "consumer": KAFKA_CONSOLE_CONSUMER_EXECUTABLES,
    "groups": KAFKA_CONSUMER_GROUPS_EXECUTABLES,
    "configs": KAFKA_CONFIGS_EXECUTABLES,
    "acls": KAFKA_ACLS_EXECUTABLES,
    "broker API versions": KAFKA_BROKER_API_VERSIONS_EXECUTABLES,
    "Schema Registry console": SCHEMA_REGISTRY_EXECUTABLES,
}


class SmokeFailure(RuntimeError):
    """Raised when a sandbox smoke check cannot complete."""


@cloup.command()
@cloup.argument("topic", required=False)
@cloup.option("--profile", default="sandbox", show_default=True)
@cloup.option(
    "-b",
    "--bootstrap-servers",
    "bootstrap_servers",
    default=DEFAULT_BOOTSTRAP_SERVERS,
    show_default=True,
    help="Comma-separated sandbox broker addresses.",
)
@cloup.option("--keep-topic", is_flag=True, help="Leave the smoke topic in the cluster.")
@cloup.option(
    "--registry-provider",
    type=cloup.Choice(("confluent", "apicurio")),
    default="confluent",
    show_default=True,
    help="Sandbox registry provider.",
)
@cloup.option(
    "--registry-url",
    default=DEFAULT_REGISTRY_URL,
    show_default=True,
    help="Sandbox registry URL.",
)
@cloup.option("--no-color", is_flag=True, help="Disable styled terminal output.")
@cloup.option(
    "--shell",
    "shells",
    multiple=True,
    type=cloup.Choice(sorted(SUPPORTED_SHELLS)),
    help="Also exercise an interactive shell; may be repeated.",
)
def main(
    topic: str | None,
    profile: str,
    bootstrap_servers: str,
    keep_topic: bool,
    registry_provider: str,
    registry_url: str,
    no_color: bool,
    shells: tuple[str, ...],
) -> None:
    """Create TOPIC and smoke-test installed adapters against the sandbox."""
    environment = dict(os.environ)
    plain_output = (
        no_color or bool(environment.get("CI")) or bool(environment.get("GITHUB_ACTIONS"))
    )
    console = create_console(no_color=plain_output, environment=environment)
    error_console = create_console(
        stream=sys.stderr,
        no_color=plain_output,
        environment=environment,
    )
    smoke_topic = topic or f"kantrip-smoke-{secrets.token_hex(6)}"
    try:
        smoke(
            console,
            profile=profile,
            bootstrap_servers=tuple(server.strip() for server in bootstrap_servers.split(",")),
            topic=smoke_topic,
            keep_topic=keep_topic,
            registry_provider=registry_provider,
            registry_url=registry_url,
            environment=environment,
            shells=shells,
        )
    except SmokeFailure as error:
        error_console.print(create_status_text(error_console, "error", str(error)))
        raise click.exceptions.Exit(1) from error


def smoke(
    console: Console,
    *,
    profile: str,
    bootstrap_servers: Sequence[str],
    topic: str,
    keep_topic: bool,
    registry_provider: str,
    registry_url: str,
    environment: Mapping[str, str],
    shells: Sequence[str] = (),
) -> None:
    """Run the adapter smoke checks with an isolated Kantrip configuration."""
    installed, kcat_executables = _discover_clients(registry_provider, environment)
    resolved_shells = _resolve_shells(shells, environment)

    with tempfile.TemporaryDirectory(prefix="kantrip-smoke-") as directory:
        smoke_environment = dict(environment)
        smoke_environment["KANTRIP_DATABASE"] = str(Path(directory) / "profiles.db")
        console.print(Text("Kantrip Sandbox", style="heading"))
        _show_section(console, "Setup")
        _add_profile(
            console,
            profile,
            bootstrap_servers,
            registry_provider,
            registry_url,
            smoke_environment,
        )
        _check(
            console,
            f"connect to Kafka and {_registry_name(registry_provider)}",
            _kantrip_cli("ping", profile),
            smoke_environment,
        )
        creator = installed["topics"][0]
        created = False
        try:
            _check(
                console,
                f"{creator}: create smoke topic",
                _kantrip(
                    profile,
                    creator,
                    "--create",
                    "--topic",
                    topic,
                    "--partitions",
                    "1",
                    "--replication-factor",
                    "1",
                ),
                smoke_environment,
            )
            created = True
            _exercise_clients(
                console,
                profile=profile,
                topic=topic,
                registry_provider=registry_provider,
                installed=installed,
                kcat_executables=kcat_executables,
                shells=resolved_shells,
                environment=smoke_environment,
            )
        finally:
            if created and not keep_topic:
                _show_section(console, "Cleanup")
                _delete_topic(console, profile, creator, topic, smoke_environment)
        console.print()
        topic_outcome = "kept" if keep_topic else "deleted"
        console.print(
            create_status_text(
                console,
                "success",
                f"Sandbox smoke checks passed (topic {topic_outcome}: {topic})",
            )
        )


def _discover_clients(
    registry_provider: str,
    environment: Mapping[str, str],
) -> tuple[dict[str, tuple[str, ...]], tuple[str, ...]]:
    installed = {
        adapter: _installed_commands(executables, environment)
        for adapter, executables in KAFKA_COMMANDS.items()
    }
    for adapter, executables in installed.items():
        if adapter == "Schema Registry console" and registry_provider == "apicurio":
            continue
        _require_command(f"Kafka {adapter} CLI", bool(executables))
    kcat_executables = _installed_commands(KCAT_EXECUTABLES, environment)
    _require_command("kcat", "kcat" in kcat_executables)
    _require_command(
        "kaskade",
        shutil.which("kaskade", path=environment.get("PATH")) is not None,
    )
    return installed, kcat_executables


def _exercise_clients(
    console: Console,
    *,
    profile: str,
    topic: str,
    registry_provider: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
    shells: Sequence[tuple[str, str]],
    environment: Mapping[str, str],
) -> None:
    creator = installed["topics"][0]
    if registry_provider == "confluent":
        _show_section(console, "Confluent registry clients")
        _check_schema_registry_clients(console, profile, topic, creator, environment)
    _show_section(console, "Kafka CLI")
    _check_java_operations(console, profile, topic, installed, environment)
    _show_section(console, "Additional clients")
    _check_kcat_operations(console, profile, topic, kcat_executables, environment)
    _check_kaskade_operations(console, profile, topic, environment)
    _check_shells(
        console,
        profile=profile,
        topic=topic,
        installed=installed,
        kcat_executables=kcat_executables,
        shells=shells,
        environment=environment,
    )


def _check_java_operations(
    console: Console,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    environment: Mapping[str, str],
) -> None:
    for executable in installed["topics"]:
        output = _check(
            console,
            f"{executable}: list topics",
            _kantrip(profile, executable, "--list"),
            environment,
        )
        _require_topic(topic, output, executable)
    markers = _produce_java_records(console, profile, topic, installed, environment)
    _consume_java_records(console, profile, topic, installed, markers, environment)
    for executable in installed["groups"]:
        _check(
            console,
            f"{executable}: list consumer groups",
            _kantrip(profile, executable, "--list"),
            environment,
        )
    _check_java_metadata(console, profile, topic, installed, environment)


def _produce_java_records(
    console: Console,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    environment: Mapping[str, str],
) -> tuple[str, ...]:
    markers: list[str] = []
    for index, executable in enumerate(installed["producer"]):
        marker = f"kantrip smoke record {index} from {executable}"
        markers.append(marker)
        _check(
            console,
            f"{executable}: write smoke record",
            _kantrip(profile, executable, "--topic", topic),
            environment,
            input_text=f"{marker}\n",
        )
    return tuple(markers)


def _consume_java_records(
    console: Console,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    markers: Sequence[str],
    environment: Mapping[str, str],
) -> None:
    for index, executable in enumerate(installed["consumer"]):
        output = _check(
            console,
            f"{executable}: read smoke records",
            _kantrip(
                profile,
                executable,
                "--topic",
                topic,
                "--group",
                f"{topic}-consumer-{index}",
                "--from-beginning",
                "--max-messages",
                str(len(markers)),
            ),
            environment,
        )
        for marker in markers:
            _require_topic(marker, output, executable)


def _check_java_metadata(
    console: Console,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    environment: Mapping[str, str],
) -> None:
    for executable in installed["configs"]:
        _check(
            console,
            f"{executable}: describe smoke topic",
            _kantrip(
                profile,
                executable,
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
            ),
            environment,
        )
    for executable in installed["acls"]:
        _check(
            console,
            f"{executable}: list ACLs",
            _kantrip(profile, executable, "--list"),
            environment,
        )
    for executable in installed["broker API versions"]:
        _check(
            console,
            f"{executable}: inspect broker APIs",
            _kantrip(profile, executable),
            environment,
        )


def _check_kcat_operations(
    console: Console,
    profile: str,
    topic: str,
    executables: Sequence[str],
    environment: Mapping[str, str],
) -> None:
    for executable in executables:
        output = _check(
            console,
            f"{executable}: inspect cluster metadata",
            _kantrip(profile, executable, "-X", "broker.address.family=v4", "-L"),
            environment,
        )
        _require_topic(topic, output, executable)
        marker = f"kantrip smoke record from {executable}"
        _check(
            console,
            f"{executable}: produce record",
            _kantrip(profile, executable, "-X", "broker.address.family=v4", "-P", "-t", topic),
            environment,
            input_text=f"{marker}\n",
        )
        output = _check(
            console,
            f"{executable}: consume record",
            _kantrip(
                profile,
                executable,
                "-X",
                "broker.address.family=v4",
                "-C",
                "-t",
                topic,
                "-o",
                "-1",
                "-c",
                "1",
            ),
            environment,
        )
        _require_topic(marker, output, executable)


def _check_shells(
    console: Console,
    *,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
    shells: Sequence[tuple[str, str]],
    environment: Mapping[str, str],
) -> None:
    if shells:
        _show_section(console, "Interactive shells")
    for shell_name, shell in shells:
        _check_shell(
            console,
            shell_name=shell_name,
            shell=shell,
            profile=profile,
            topic=topic,
            consumer_group=f"{topic}-consumer",
            installed=installed,
            kcat_executables=kcat_executables,
            environment=environment,
        )


def _installed_commands(
    executables: Iterable[str], environment: Mapping[str, str]
) -> tuple[str, ...]:
    path = environment.get("PATH")
    return tuple(
        executable
        for executable in sorted(executables)
        if shutil.which(executable, path=path) is not None
    )


def _require_command(name: str, available: bool) -> None:
    if not available:
        raise SmokeFailure(f"required command '{name}' was not found on PATH")


def _resolve_shells(
    shells: Sequence[str], environment: Mapping[str, str]
) -> tuple[tuple[str, str], ...]:
    resolved: list[tuple[str, str]] = []
    for shell_name in dict.fromkeys(shells):
        shell = shutil.which(shell_name, path=environment.get("PATH"))
        _require_command(f"{shell_name} shell", shell is not None)
        assert shell is not None
        resolved.append((shell_name, shell))
    return tuple(resolved)


def _show_section(console: Console, title: str) -> None:
    """Render one readable phase heading in colored and plain terminals."""
    console.print()
    console.print(Text(title, style="heading"))


def _registry_name(provider: str) -> str:
    """Return the user-facing name for a sandbox registry provider."""
    return "Apicurio Registry" if provider == "apicurio" else "Confluent Schema Registry"


def _check_shell(
    console: Console,
    *,
    shell_name: str,
    shell: str,
    profile: str,
    topic: str,
    consumer_group: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
    environment: Mapping[str, str],
) -> None:
    label = f"{shell_name}: verify session adapter shims"
    shell_environment = dict(environment)
    shell_environment["SHELL"] = shell
    commands = _shell_commands(
        shell_name,
        topic=topic,
        consumer_group=consumer_group,
        installed=installed,
        kcat_executables=kcat_executables,
    )
    markers = tuple(f"__KANTRIP_SMOKE_{index}__" for index in range(len(commands)))
    checked = [
        f"{command} && echo {marker} || exit 70"
        for command, marker in zip(commands, markers, strict=True)
    ]
    with tempfile.TemporaryDirectory(prefix=f"kantrip-{shell_name}-smoke-") as directory:
        driver = _write_shell_driver(shell_name, checked, Path(directory))
        try:
            with show_progress(console, label):
                status, output = run_terminal(
                    (*_kantrip_cli("exec", profile),),
                    (driver,),
                    environment=shell_environment,
                    timeout=120,
                )
        except TerminalTimeout as error:
            raise SmokeFailure(f"{label} failed: {error}") from error
    if status or any(marker not in output for marker in markers):
        details = output.strip() or f"shell exited with status {status}"
        raise SmokeFailure(f"{label} failed:\n{details}")
    _require_topic(topic, output, shell_name)
    _require_topic("kantrip smoke record", output, shell_name)
    console.print(create_status_text(console, "success", label))


def _write_shell_driver(shell_name: str, commands: Sequence[str], directory: Path) -> str:
    """Write one sourced command batch so terminal clients cannot consume later commands."""
    script_path = directory / "commands"
    write_exclusive_text(
        script_path,
        "\n".join(commands) + "\n",
        mode=0o600,
    )
    quoted_path = quote_shell_argument(shell_name, str(script_path))
    if shell_name == "fish":
        return (
            f"source {quoted_path} < /dev/null; set -l kantrip_status $status; exit $kantrip_status"
        )
    return f". {quoted_path} < /dev/null; exit $?"


def _shell_commands(
    shell_name: str,
    *,
    topic: str,
    consumer_group: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
) -> list[str]:
    quoted_topic = quote_shell_argument(shell_name, topic)
    quoted_group = quote_shell_argument(shell_name, consumer_group)
    quoted_kantrip = quote_shell_argument(shell_name, _kantrip_executable())
    commands = [f"{quoted_kantrip} current"]
    commands.extend(f"{executable} --list" for executable in installed["topics"])
    commands.extend(
        f"printf 'kantrip smoke record\\n' | {executable} --topic {quoted_topic}"
        for executable in installed["producer"]
    )
    commands.extend(
        f"{executable} --topic {quoted_topic} --group {quoted_group} "
        "--from-beginning --max-messages 1"
        for executable in installed["consumer"]
    )
    commands.extend(f"{executable} --list" for executable in installed["groups"])
    commands.extend(
        f"{executable} --describe --entity-type topics --entity-name {quoted_topic}"
        for executable in installed["configs"]
    )
    commands.extend(f"{executable} --list" for executable in installed["acls"])
    commands.extend(executable for executable in installed["broker API versions"])
    commands.extend(
        f"{executable} -X broker.address.family=v4 -L" for executable in kcat_executables
    )
    return commands


def _check_kaskade_operations(
    console: Console,
    profile: str,
    topic: str,
    environment: Mapping[str, str],
) -> None:
    cases = (
        (
            "kaskade admin: render broker metadata",
            _kantrip(
                profile,
                "kaskade",
                "admin",
                "--refresh-interval",
                "0",
                "--kafka",
                "broker.address.family=v4",
            ),
            (topic,),
        ),
        (
            "kaskade consumer: render a consumed record",
            _kantrip(
                profile,
                "kaskade",
                "consumer",
                "--topic",
                topic,
                "--earliest",
                "--value",
                "string",
                "--kafka",
                f"group.id={topic}-kaskade",
                "--kafka",
                "broker.address.family=v4",
            ),
            ("kantrip smoke record",),
        ),
    )
    for label, command, expected in cases:
        terminal = TerminalProcess(command, environment)
        try:
            with show_progress(console, label):
                terminal.wait_for(expected, timeout=60)
        except TerminalProcessError as error:
            raise SmokeFailure(f"{label} failed: {error}") from error
        finally:
            terminal.close()
        console.print(create_status_text(console, "success", label))


def _add_profile(
    console: Console,
    profile: str,
    bootstrap_servers: Sequence[str],
    registry_provider: str,
    registry_url: str,
    environment: Mapping[str, str],
) -> None:
    command = _kantrip_cli(
        "add",
        profile,
        "--bootstrap-servers",
        ",".join(bootstrap_servers),
    )
    command.extend(("--registry-provider", registry_provider, "--registry-url", registry_url))
    _check(console, "create isolated Kantrip profile", command, environment)


def _kantrip_cli(*arguments: str) -> list[str]:
    """Build a plain-output Kantrip command for capture by the sandbox runner."""
    return [_kantrip_executable(), "--no-color", *arguments]


def _kantrip_executable() -> str:
    configured = os.environ.get("KANTRIP_E2E_KANTRIP")
    resolved = configured or shutil.which("kantrip")
    if resolved is None:
        raise SmokeFailure("required command 'kantrip' was not found on PATH")
    return resolved


def _kantrip(profile: str, executable: str, *arguments: str) -> list[str]:
    return _kantrip_cli(
        "exec",
        profile,
        "--",
        executable,
        *arguments,
    )


def _check_schema_registry_clients(
    console: Console,
    profile: str,
    topic_prefix: str,
    topic_executable: str,
    environment: Mapping[str, str],
) -> None:
    cases = (
        (
            "avro",
            '{"type":"record","name":"KantripE2EAvro","fields":[{"name":"value","type":"string"}]}',
            "kantrip avro record",
        ),
        (
            "json-schema",
            ('{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}'),
            "kantrip json schema record",
        ),
        (
            "protobuf",
            'syntax = "proto3"; message KantripE2EProtobuf { string value = 1; }',
            "kantrip protobuf record",
        ),
    )
    for format_name, schema, marker in cases:
        topic = f"{topic_prefix}-{format_name}"
        producer = f"kafka-{format_name}-console-producer"
        consumer = f"kafka-{format_name}-console-consumer"
        _check(
            console,
            f"{topic_executable}: create {format_name} topic",
            _kantrip(
                profile,
                topic_executable,
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ),
            environment,
        )
        try:
            _check(
                console,
                f"{producer}: produce {format_name}",
                _kantrip(
                    profile,
                    producer,
                    "--topic",
                    topic,
                    "--reader-property",
                    f"value.schema={schema}",
                ),
                environment,
                input_text=json.dumps({"value": marker}) + "\n",
            )
            output = _check(
                console,
                f"{consumer}: consume {format_name}",
                _kantrip(
                    profile,
                    consumer,
                    "--topic",
                    topic,
                    "--group",
                    f"{topic}-schema",
                    "--from-beginning",
                    "--max-messages",
                    "1",
                ),
                environment,
            )
            if marker not in output:
                raise SmokeFailure(
                    f"{consumer} did not decode its {format_name} record: "
                    f"{output.strip()[-500:]}"
                )
        finally:
            _delete_topic(console, profile, topic_executable, topic, environment)


def _check(
    console: Console,
    label: str,
    command: Sequence[str],
    environment: Mapping[str, str],
    *,
    input_text: str | None = None,
) -> str:
    with show_progress(console, label):
        result = subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
            input=input_text,
            check=False,
        )
    output = f"{result.stdout}{result.stderr}"
    if result.returncode:
        details = _failure_details(result)
        raise SmokeFailure(f"{label} failed:\n{details}")
    console.print(create_status_text(console, "success", label))
    return output


def _failure_details(result: subprocess.CompletedProcess[str]) -> str:
    """Remove nested Kantrip presentation while preserving diagnostic content."""
    output = "\n".join(part.rstrip("\n") for part in (result.stdout, result.stderr) if part)
    details: list[str] = []
    for line in output.splitlines():
        if line.startswith("[running] "):
            continue
        for prefix in ("[failed] ", "[warning] ", "[passed] ", "[cleanup] "):
            if line.startswith(prefix):
                line = line.removeprefix(prefix)
                break
        details.append(line)
    return "\n".join(details).strip() or f"command exited with status {result.returncode}"


def _require_topic(topic: str, output: str, executable: str) -> None:
    if topic not in output:
        raise SmokeFailure(f"{executable} did not list the smoke topic '{topic}'")


def _delete_topic(
    console: Console,
    profile: str,
    executable: str,
    topic: str,
    environment: Mapping[str, str],
) -> None:
    console.print(create_status_text(console, "cleanup", f"kafka-topics: delete {topic}"))
    result = subprocess.run(
        _kantrip(profile, executable, "--delete", "--topic", topic),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        console.print(
            create_status_text(console, "warning", f"Could not delete smoke topic {topic}")
        )


if __name__ == "__main__":
    main()
