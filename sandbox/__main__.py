"""Exercise Kantrip's supported clients against the manual sandbox."""

from __future__ import annotations

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
from kantrip.console import create_console, create_status_text
from kantrip.shells import SUPPORTED_SHELLS, quote_shell_argument
from scripts import TerminalTimeout, run_terminal

DEFAULT_BOOTSTRAP_SERVERS = ("localhost:19092",)
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081"
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
    "--bootstrap-server",
    "bootstrap_servers",
    multiple=True,
    default=DEFAULT_BOOTSTRAP_SERVERS,
    show_default=True,
    help="Sandbox broker address; may be repeated.",
)
@cloup.option("--keep-topic", is_flag=True, help="Leave the smoke topic in the cluster.")
@cloup.option(
    "--schema-registry-url",
    default=DEFAULT_SCHEMA_REGISTRY_URL,
    show_default=True,
    help="Sandbox Schema Registry URL.",
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
    bootstrap_servers: tuple[str, ...],
    keep_topic: bool,
    schema_registry_url: str,
    no_color: bool,
    shells: tuple[str, ...],
) -> None:
    """Create TOPIC and smoke-test installed adapters against the sandbox."""
    environment = dict(os.environ)
    plain_output = (
        no_color or bool(environment.get("CI")) or bool(environment.get("GITHUB_ACTIONS"))
    )
    console = create_console(no_color=plain_output, environment=environment)
    smoke_topic = topic or f"kantrip-smoke-{secrets.token_hex(6)}"
    try:
        smoke(
            console,
            profile=profile,
            bootstrap_servers=bootstrap_servers,
            topic=smoke_topic,
            keep_topic=keep_topic,
            schema_registry_url=schema_registry_url,
            environment=environment,
            shells=shells,
        )
    except SmokeFailure as error:
        raise click.ClickException(str(error)) from error


def smoke(
    console: Console,
    *,
    profile: str,
    bootstrap_servers: Sequence[str],
    topic: str,
    keep_topic: bool,
    schema_registry_url: str,
    environment: Mapping[str, str],
    shells: Sequence[str] = (),
) -> None:
    """Run the adapter smoke checks with an isolated Kantrip configuration."""
    installed = {
        adapter: _installed_commands(executables, environment)
        for adapter, executables in KAFKA_COMMANDS.items()
    }
    for adapter, executables in installed.items():
        _require_command(f"Kafka {adapter} CLI", bool(executables))
    kcat_executables = _installed_commands(KCAT_EXECUTABLES, environment)
    _require_command("kcat", "kcat" in kcat_executables)
    _require_command("kaskade", shutil.which("kaskade", path=environment.get("PATH")) is not None)
    resolved_shells = _resolve_shells(shells, environment)

    with tempfile.TemporaryDirectory(prefix="kantrip-smoke-") as directory:
        smoke_environment = dict(environment)
        smoke_environment["KANTRIP_CONFIG"] = str(Path(directory) / "config.yaml")
        _add_profile(
            console,
            profile,
            bootstrap_servers,
            schema_registry_url,
            smoke_environment,
        )
        _check(
            console,
            "check Kafka and Schema Registry connectivity",
            [sys.executable, "-m", "kantrip.cli", "ping", profile],
            smoke_environment,
        )
        creator = installed["topics"][0]
        created = False
        try:
            _check(
                console,
                f"create topic with {creator}",
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
            for executable in installed["Schema Registry console"]:
                _check(
                    console,
                    f"validate the Schema Registry adapter with {executable}",
                    _kantrip(profile, executable, "--help"),
                    smoke_environment,
                )
            for executable in installed["topics"]:
                output = _check(
                    console,
                    f"list topics with {executable}",
                    _kantrip(profile, executable, "--list"),
                    smoke_environment,
                )
                _require_topic(topic, output, executable)
            _check(
                console,
                f"produce a record with {installed['producer'][0]}",
                _kantrip(profile, installed["producer"][0], "--topic", topic),
                smoke_environment,
                input_text="kantrip smoke record\n",
            )
            output = _check(
                console,
                f"consume a record with {installed['consumer'][0]}",
                _kantrip(
                    profile,
                    installed["consumer"][0],
                    "--topic",
                    topic,
                    "--from-beginning",
                    "--max-messages",
                    "1",
                ),
                smoke_environment,
            )
            _require_topic("kantrip smoke record", output, installed["consumer"][0])
            _check(
                console,
                f"list groups with {installed['groups'][0]}",
                _kantrip(profile, installed["groups"][0], "--list"),
                smoke_environment,
            )
            _check(
                console,
                f"describe topic configs with {installed['configs'][0]}",
                _kantrip(
                    profile,
                    installed["configs"][0],
                    "--describe",
                    "--entity-type",
                    "topics",
                    "--entity-name",
                    topic,
                ),
                smoke_environment,
            )
            _check(
                console,
                f"validate the ACL adapter with {installed['acls'][0]}",
                _kantrip(profile, installed["acls"][0], "--version"),
                smoke_environment,
            )
            _check(
                console,
                f"inspect APIs with {installed['broker API versions'][0]}",
                _kantrip(profile, installed["broker API versions"][0]),
                smoke_environment,
            )
            output = _check(
                console,
                "list topics with kcat",
                _kantrip(profile, "kcat", "-L"),
                smoke_environment,
            )
            _require_topic(topic, output, "kcat")
            _check(
                console,
                "validate the Kaskade adapter",
                _kantrip(profile, "kaskade", "admin", "--help"),
                smoke_environment,
            )
            for shell_name, shell in resolved_shells:
                _check_shell(
                    console,
                    shell_name=shell_name,
                    shell=shell,
                    profile=profile,
                    topic=topic,
                    installed=installed,
                    kcat_executables=kcat_executables,
                    environment=smoke_environment,
                )
            console.print(
                create_status_text(
                    console, "success", f"Sandbox adapters passed with topic {topic}"
                )
            )
        finally:
            if created and not keep_topic:
                _delete_topic(console, profile, creator, topic, smoke_environment)


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


def _check_shell(
    console: Console,
    *,
    shell_name: str,
    shell: str,
    profile: str,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
    environment: Mapping[str, str],
) -> None:
    label = f"exercise adapters in {shell_name}"
    console.print(create_status_text(console, "progress", label))
    shell_environment = dict(environment)
    shell_environment["SHELL"] = shell
    commands = _shell_commands(
        shell_name,
        topic=topic,
        installed=installed,
        kcat_executables=kcat_executables,
    )
    markers = tuple(f"__KANTRIP_SMOKE_{index}__" for index in range(len(commands)))
    checked = [
        f"{command} && echo {marker} || exit 70"
        for command, marker in zip(commands, markers, strict=True)
    ]
    checked.append("exit")
    try:
        status, output = run_terminal(
            (sys.executable, "-m", "kantrip.cli", "exec", profile),
            checked,
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


def _shell_commands(
    shell_name: str,
    *,
    topic: str,
    installed: Mapping[str, Sequence[str]],
    kcat_executables: Sequence[str],
) -> list[str]:
    quoted_topic = quote_shell_argument(shell_name, topic)
    quoted_python = quote_shell_argument(shell_name, sys.executable)
    commands = [f"{quoted_python} -m kantrip.cli current"]
    commands.extend(f"{executable} --list" for executable in installed["topics"])
    commands.extend(
        f"printf 'kantrip smoke record\\n' | {executable} --topic {quoted_topic}"
        for executable in installed["producer"]
    )
    commands.extend(
        f"{executable} --topic {quoted_topic} --from-beginning --max-messages 1"
        for executable in installed["consumer"]
    )
    commands.extend(f"{executable} --list" for executable in installed["groups"])
    commands.extend(
        f"{executable} --describe --entity-type topics --entity-name {quoted_topic}"
        for executable in installed["configs"]
    )
    commands.extend(f"{executable} --version" for executable in installed["acls"])
    commands.extend(executable for executable in installed["broker API versions"])
    commands.extend(f"{executable} --help" for executable in installed["Schema Registry console"])
    commands.extend(f"{executable} -L" for executable in kcat_executables)
    commands.extend(("kaskade admin --help", "kaskade consumer --help"))
    return commands


def _add_profile(
    console: Console,
    profile: str,
    bootstrap_servers: Sequence[str],
    schema_registry_url: str,
    environment: Mapping[str, str],
) -> None:
    command = [sys.executable, "-m", "kantrip.cli", "add", profile]
    for server in bootstrap_servers:
        command.extend(("--bootstrap-server", server))
    command.extend(("--schema-registry-url", schema_registry_url))
    _check(console, "prepare an isolated sandbox profile", command, environment)


def _kantrip(profile: str, executable: str, *arguments: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "kantrip.cli",
        "exec",
        profile,
        "--",
        executable,
        *arguments,
    ]


def _check(
    console: Console,
    label: str,
    command: Sequence[str],
    environment: Mapping[str, str],
    *,
    input_text: str | None = None,
) -> str:
    console.print(create_status_text(console, "progress", label))
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
        details = output.strip() or f"command exited with status {result.returncode}"
        raise SmokeFailure(f"{label} failed:\n{details}")
    console.print(create_status_text(console, "success", label))
    return output


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
    console.print(create_status_text(console, "cleanup", f"delete topic {topic}"))
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
