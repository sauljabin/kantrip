"""Check the pinned Java releases against Kantrip's floors through every Kafka mechanism.

A release Kantrip admits must create, produce, consume, and list through the
profile; a release below a floor must be refused before launch, for every
Java command it ships.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path

from kantrip.adapters import SCHEMA_REGISTRY_EXECUTABLES, KafkaNeeds
from sandbox.__main__ import STATE_FILE, load_credentials
from tests.e2e import authentication as auth
from tests.e2e.java_versions import java_refusal, refused
from tests.e2e.shell_environment import isolated_zsh_environment

_FAMILIES = (
    "topics",
    "console-producer",
    "console-consumer",
    "consumer-groups",
    "configs",
    "acls",
    "broker-api-versions",
)
# Apache Kafka archives name the commands with `.sh`; Confluent Platform doesn't.
_DISTRIBUTIONS = (".sh", "")


def main() -> None:
    """Exercise both Java distributions with one profile per authenticated listener."""
    _require_exact_options()
    credentials = load_credentials(STATE_FILE)
    with (
        tempfile.TemporaryDirectory(prefix="kantrip-java-floors-") as directory,
        isolated_zsh_environment(os.environ) as environment,
    ):
        environment["KANTRIP_DATABASE"] = str(Path(directory) / "profiles.db")
        profiles: list[str] = []
        try:
            for case in auth.CASES:
                profile = f"floor-{case.name}"
                auth._add_profile(profile, case, credentials, environment)
                profiles.append(profile)
                needs = KafkaNeeds(case.auth_type, custom_pem=True)
                for suffix in _DISTRIBUTIONS:
                    _exercise(profile, case, suffix, needs, environment)
        finally:
            for profile in reversed(profiles):
                auth._run(
                    (*auth._cli(), "remove", profile, "--yes"), environment, accepted=(0, 1, 3)
                )
    print("Java version floor checks passed")


def _require_exact_options() -> None:
    """Require every Java command to reject an abbreviated long option.

    Kantrip rejects connection options by their full names, so a command that
    accepted `--bootstrap-serve` for `--bootstrap-server` would bypass it.
    """
    commands = [f"kafka-{family}{suffix}" for family in _FAMILIES for suffix in _DISTRIBUTIONS]
    for command in (*commands, *sorted(SCHEMA_REGISTRY_EXECUTABLES)):
        result = subprocess.run(
            (command, "--bootstrap-serve", "127.0.0.1:9"),
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if "bootstrap-serve is not a recognized option" not in result.stdout + result.stderr:
            raise auth.AuthSmokeFailure(f"{command} accepted the abbreviated --bootstrap-serve")
    print(
        f"abbreviated options rejected by {len(commands) + len(SCHEMA_REGISTRY_EXECUTABLES)} commands"
    )


def _exercise(
    profile: str,
    case: auth.AuthCase,
    suffix: str,
    needs: KafkaNeeds,
    environment: Mapping[str, str],
) -> None:
    refusal = java_refusal(f"kafka-topics{suffix}", needs)
    label = f"{case.name}: kafka-*{suffix}"
    if refusal is not None:
        for family in _FAMILIES:
            output = auth._run(
                (*auth._cli(), "exec", profile, "--", f"kafka-{family}{suffix}", "--version"),
                environment,
                accepted=tuple(range(1, 256)),
            )
            if not refused(output, refusal):
                raise auth.AuthSmokeFailure(f"kafka-{family}{suffix} was not refused: {output}")
        print(f"refused before launch: {label}")
        return
    prefix = "kantrip-oauth" if case.auth_type == "oauth" else "kantrip-auth"
    topic = f"{prefix}-floor-{case.name}-{secrets.token_hex(4)}"
    marker = f"kantrip floor record {topic}"

    def kafka(family: str, *arguments: str, input_text: str | None = None) -> str:
        command = (*auth._cli(), "exec", profile, "--", f"kafka-{family}{suffix}", *arguments)
        return auth._run(command, environment, input_text=input_text)

    kafka("topics", "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1")
    try:
        kafka("console-producer", "--topic", topic, input_text=f"{marker}\n")
        output = kafka(
            "console-consumer",
            "--topic",
            topic,
            "--group",
            f"{prefix}-floor-{secrets.token_hex(4)}",
            "--from-beginning",
            "--max-messages",
            "1",
        )
        if marker not in output:
            raise auth.AuthSmokeFailure(f"kafka-console-consumer{suffix} missed {marker!r}")
        kafka("consumer-groups", "--list")
    finally:
        auth._run(
            (
                *auth._cli(),
                "exec",
                profile,
                "--",
                f"kafka-topics{suffix}",
                "--delete",
                "--topic",
                topic,
            ),
            environment,
            accepted=(0, 1),
        )
    print(f"passed: {label}")
