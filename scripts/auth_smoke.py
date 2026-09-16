"""Exercise authenticated Kafka profiles against the disposable sandbox listeners."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from sandbox.__main__ import CA_FILE, STATE_FILE, load_credentials
from scripts import TerminalTimeout, run_terminal


class AuthSmokeFailure(RuntimeError):
    """Raised when authenticated sandbox acceptance cannot complete."""


@dataclass(frozen=True)
class AuthCase:
    """One authenticated listener and its private sandbox inputs."""

    name: str
    port: int
    auth_type: str
    username_field: str | None = None
    password_field: str | None = None
    certificate_field: str | None = None
    key_field: str | None = None


CASES = (
    AuthCase(
        "plain",
        9097,
        "plain",
        "KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD",
    ),
    AuthCase(
        "scram-256",
        9098,
        "scram-sha-256",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_PASSWORD",
    ),
    AuthCase(
        "scram-512",
        9094,
        "scram-sha-512",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD",
    ),
    AuthCase(
        "mtls",
        9095,
        "mtls",
        certificate_field="KANTRIP_SANDBOX_KAFKA_MTLS_CERTIFICATE",
        key_field="KANTRIP_SANDBOX_KAFKA_MTLS_KEY",
    ),
)


def main() -> None:
    """Run authenticated lifecycle, operation, shell, and no-ACL ping checks."""
    credentials = load_credentials(STATE_FILE)
    _require_files((CA_FILE,))
    _require_commands(
        (
            "bash",
            "fish",
            "kafka-console-consumer",
            "kafka-console-producer",
            "kafka-topics",
            "kcat",
            "zsh",
        )
    )
    with tempfile.TemporaryDirectory(prefix="kantrip-auth-smoke-") as directory:
        environment = dict(os.environ)
        environment["KANTRIP_DATABASE"] = str(Path(directory) / "profiles.db")
        profiles: list[str] = []
        try:
            for case in CASES:
                profile = f"auth-{case.name}"
                _add_profile(profile, case, credentials, environment)
                profiles.append(profile)
                _exercise_profile(profile, case, environment)
            no_acl = "auth-no-acl"
            _add_no_acl_profile(no_acl, credentials, environment)
            profiles.append(no_acl)
            _exercise_no_acl_ping(no_acl, environment)
        finally:
            for profile in reversed(profiles):
                _run((*_cli(), "remove", profile, "--force"), environment, accepted=(0, 1, 3))
    print("Authenticated Kafka sandbox smoke checks passed")


def _add_profile(
    profile: str,
    case: AuthCase,
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
) -> None:
    arguments = [
        *_cli(),
        "add",
        profile,
        "--bootstrap-servers",
        f"localhost:{case.port}",
        "--transport",
        "tls",
        "--ca-file",
        str(CA_FILE),
        "--auth",
        case.auth_type,
    ]
    if case.username_field is not None:
        arguments.extend(("--username", credentials[case.username_field]))
        assert case.password_field is not None
        status, output = run_terminal(
            arguments,
            (credentials[case.password_field],),
            environment=environment,
            ready_text="Kafka password",
            timeout=30,
        )
        if status not in (0, 3):
            raise AuthSmokeFailure(_safe_failure(profile, output, credentials))
        return
    assert case.certificate_field is not None and case.key_field is not None
    arguments.extend(
        (
            "--client-certificate-file",
            credentials[case.certificate_field],
            "--client-key-file",
            credentials[case.key_field],
        )
    )
    _run(arguments, environment, accepted=(0, 3))


def _add_no_acl_profile(
    profile: str,
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
) -> None:
    case = AuthCase(
        "no-acl",
        9097,
        "plain",
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD",
    )
    _add_profile(profile, case, credentials, environment)


def _exercise_profile(
    profile: str,
    case: AuthCase,
    environment: Mapping[str, str],
) -> None:
    topic = f"kantrip-auth-{case.name}-{secrets.token_hex(4)}"
    _run((*_cli(), "ping", profile, "--timeout", "10"), environment)
    _run(
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kafka-topics",
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
    _run(
        (*_cli(), "exec", profile, "--", "kafka-console-producer", "--topic", topic),
        environment,
        input_text="kantrip authenticated smoke record\n",
    )
    output = _run(
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kafka-console-consumer",
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            "1",
        ),
        environment,
    )
    if "kantrip authenticated smoke record" not in output:
        raise AuthSmokeFailure(f"{profile} did not consume its smoke record")
    _run((*_cli(), "exec", profile, "--", "kcat", "-L"), environment)
    for shell_name in ("bash", "zsh", "fish"):
        _exercise_shell(profile, shell_name, environment)


def _exercise_shell(
    profile: str,
    shell_name: str,
    environment: Mapping[str, str],
) -> None:
    shell = shutil.which(shell_name, path=environment.get("PATH"))
    assert shell is not None
    shell_environment = dict(environment)
    shell_environment["SHELL"] = shell
    try:
        status, output = run_terminal(
            (*_cli(), "exec", profile),
            ("kafka-topics --list && kcat -L >/dev/null && exit",),
            environment=shell_environment,
            timeout=90,
        )
    except TerminalTimeout as error:
        raise AuthSmokeFailure(f"{profile} {shell_name} session timed out") from error
    if status:
        raise AuthSmokeFailure(f"{profile} {shell_name} session failed: {output.strip()}")


def _exercise_no_acl_ping(profile: str, environment: Mapping[str, str]) -> None:
    _run((*_cli(), "ping", profile, "--timeout", "10"), environment)
    output = _run(
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kafka-topics",
            "--create",
            "--topic",
            f"kantrip-forbidden-{secrets.token_hex(4)}",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ),
        environment,
        accepted=(1,),
    )
    if "authorization" not in output.lower():
        raise AuthSmokeFailure("the no-ACL principal did not receive an authorization failure")


def _run(
    arguments: Sequence[str],
    environment: Mapping[str, str],
    *,
    input_text: str | None = None,
    accepted: tuple[int, ...] = (0,),
) -> str:
    result = subprocess.run(
        arguments,
        env=environment,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    output = f"{result.stdout}{result.stderr}"
    if result.returncode not in accepted:
        raise AuthSmokeFailure(
            f"command failed with status {result.returncode}: {Path(arguments[0]).name}\n{output}"
        )
    return output


def _safe_failure(profile: str, output: str, credentials: Mapping[str, str]) -> str:
    safe = output
    for value in credentials.values():
        if value:
            safe = safe.replace(value, "[REDACTED]")
    return f"could not add {profile}: {safe.strip()}"


def _cli() -> tuple[str, ...]:
    return sys.executable, "-m", "kantrip.cli", "--no-color"


def _require_commands(commands: Sequence[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise AuthSmokeFailure(f"required commands were not found: {', '.join(missing)}")


def _require_files(paths: Sequence[Path]) -> None:
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise AuthSmokeFailure(f"required sandbox files were not found: {', '.join(missing)}")


if __name__ == "__main__":
    try:
        main()
    except AuthSmokeFailure as error:
        print(f"Authenticated Kafka sandbox smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
