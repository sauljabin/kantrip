"""Exercise Kantrip's supported clients against the manual sandbox."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import click
import cloup
from rich.console import Console

from kantrip.console import create_console

DEFAULT_BOOTSTRAP_SERVERS = ("localhost:19092",)
KAFKA_TOPICS_EXECUTABLES = ("kafka-topics", "kafka-topics.sh")


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
@cloup.option("--no-color", is_flag=True, help="Disable styled terminal output.")
def main(
    topic: str | None,
    profile: str,
    bootstrap_servers: tuple[str, ...],
    keep_topic: bool,
    no_color: bool,
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
            environment=environment,
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
    environment: Mapping[str, str],
) -> None:
    """Run the adapter smoke checks with an isolated Kantrip configuration."""
    installed_topics = _installed_topic_commands(environment)
    _require_command("kafka-topics or kafka-topics.sh", bool(installed_topics))
    _require_command("kcat", shutil.which("kcat", path=environment.get("PATH")) is not None)
    _require_command("kaskade", shutil.which("kaskade", path=environment.get("PATH")) is not None)

    with tempfile.TemporaryDirectory(prefix="kantrip-smoke-") as directory:
        smoke_environment = dict(environment)
        smoke_environment["KANTRIP_CONFIG"] = str(Path(directory) / "config.yaml")
        _add_profile(console, profile, bootstrap_servers, smoke_environment)
        creator = installed_topics[0]
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
            for executable in KAFKA_TOPICS_EXECUTABLES:
                if executable not in installed_topics:
                    console.print(f"[warning]⏭  {executable} is not installed; skipping variant[/]")
                    continue
                output = _check(
                    console,
                    f"list topics with {executable}",
                    _kantrip(profile, executable, "--list"),
                    smoke_environment,
                )
                _require_topic(topic, output, executable)
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
            console.print(f"[success]✅ Sandbox adapters passed with topic {topic}[/]")
        finally:
            if created and not keep_topic:
                _delete_topic(console, profile, creator, topic, smoke_environment)


def _installed_topic_commands(environment: Mapping[str, str]) -> tuple[str, ...]:
    path = environment.get("PATH")
    return tuple(
        executable
        for executable in KAFKA_TOPICS_EXECUTABLES
        if shutil.which(executable, path=path) is not None
    )


def _require_command(name: str, available: bool) -> None:
    if not available:
        raise SmokeFailure(f"required command '{name}' was not found on PATH")


def _add_profile(
    console: Console,
    profile: str,
    bootstrap_servers: Sequence[str],
    environment: Mapping[str, str],
) -> None:
    command = [sys.executable, "-m", "kantrip.cli", "add", profile]
    for server in bootstrap_servers:
        command.extend(("--bootstrap-server", server))
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
) -> str:
    console.print(f"[primary]🧪 {label}[/]")
    result = subprocess.run(
        command,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{result.stdout}{result.stderr}"
    if result.returncode:
        details = output.strip() or f"command exited with status {result.returncode}"
        raise SmokeFailure(f"{label} failed:\n{details}")
    console.print(f"[success]✅ {label}[/]")
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
    console.print(f"[muted]🧹 delete topic {topic}[/]")
    result = subprocess.run(
        _kantrip(profile, executable, "--delete", "--topic", topic),
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        console.print(f"[warning]⚠️  Could not delete smoke topic {topic}[/]")


if __name__ == "__main__":
    main()
