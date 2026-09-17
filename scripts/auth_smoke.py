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
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

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

NO_ACL_CASES = (
    AuthCase(
        "plain-no-acl",
        9097,
        "plain",
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD",
    ),
    AuthCase(
        "scram-256-no-acl",
        9098,
        "scram-sha-256",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_PASSWORD",
    ),
    AuthCase(
        "scram-512-no-acl",
        9094,
        "scram-sha-512",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_USERNAME",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_PASSWORD",
    ),
    AuthCase(
        "mtls-no-acl",
        9095,
        "mtls",
        certificate_field="KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_CERTIFICATE",
        key_field="KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEY",
    ),
)


def main() -> None:
    """Run the real listener, authorization, and negative-path matrix."""
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
            for case in NO_ACL_CASES:
                profile = f"auth-{case.name}"
                _add_profile(profile, case, credentials, environment)
                profiles.append(profile)
                _exercise_no_acl_ping(profile, environment)
            _exercise_invalid_credentials(credentials, environment, profiles)
            _exercise_invalid_mtls(directory, environment, profiles)
            _exercise_wrong_ca(credentials, environment, profiles)
            _exercise_wrong_hostname(credentials, environment, profiles)
            _exercise_unavailable_broker(environment, profiles)
            _exercise_unauthenticated_listeners(environment, profiles)
            _exercise_native_oauth(environment)
        finally:
            for profile in reversed(profiles):
                _run((*_cli(), "remove", profile, "--force"), environment, accepted=(0, 1, 3))
    print("Authenticated Kafka sandbox smoke checks passed")


def _add_profile(
    profile: str,
    case: AuthCase,
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    *,
    password_override: str | None = None,
    ca_file: Path = CA_FILE,
    host: str = "localhost",
) -> None:
    arguments = [
        *_cli(),
        "add",
        profile,
        "--bootstrap-servers",
        f"{host}:{case.port}",
        "--transport",
        "tls",
        "--ca-file",
        str(ca_file),
        "--auth",
        case.auth_type,
    ]
    if case.username_field is not None:
        arguments.extend(("--username", credentials[case.username_field]))
        assert case.password_field is not None
        status, output = run_terminal(
            arguments,
            (password_override or credentials[case.password_field],),
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


def _exercise_profile(
    profile: str,
    case: AuthCase,
    environment: Mapping[str, str],
) -> None:
    topic = f"kantrip-auth-{case.name}-{secrets.token_hex(4)}"
    group = f"kantrip-auth-{case.name}-{secrets.token_hex(4)}"
    _run((*_cli(), "ping", profile, "--timeout", "10"), environment)
    _expect_authorization_denied(
        profile,
        f"outside-{case.name}-{secrets.token_hex(4)}",
        environment,
    )
    try:
        _create_topic(profile, topic, environment)
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
                "--group",
                group,
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
    finally:
        _delete_topic(profile, topic, environment)


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
    _expect_authorization_denied(
        profile,
        f"kantrip-auth-forbidden-{secrets.token_hex(4)}",
        environment,
    )


def _expect_authorization_denied(
    profile: str,
    topic: str,
    environment: Mapping[str, str],
) -> None:
    output = _run(
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
        accepted=(1,),
    )
    if "authorization" not in output.lower():
        raise AuthSmokeFailure(f"{profile} did not receive an authorization failure")


def _exercise_invalid_credentials(
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    for case in CASES[:3]:
        profile = f"invalid-{case.name}"
        _add_profile(
            profile,
            case,
            credentials,
            environment,
            password_override=f"invalid-{secrets.token_hex(16)}",
        )
        profiles.append(profile)
        output = _run(
            (*_cli(), "ping", profile, "--timeout", "5"),
            environment,
            accepted=(1,),
        )
        _require_redacted(output, credentials)


def _exercise_invalid_mtls(
    directory: str,
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    certificate, key = _self_signed_client_identity(Path(directory))
    profile = "invalid-mtls"
    _run(
        (
            *_cli(),
            "add",
            profile,
            "--bootstrap-servers",
            "localhost:9095",
            "--transport",
            "tls",
            "--ca-file",
            str(CA_FILE),
            "--auth",
            "mtls",
            "--client-certificate-file",
            str(certificate),
            "--client-key-file",
            str(key),
        ),
        environment,
        accepted=(0, 3),
    )
    profiles.append(profile)
    _run((*_cli(), "ping", profile, "--timeout", "5"), environment, accepted=(1,))


def _exercise_wrong_ca(
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    wrong_ca = Path(credentials["KANTRIP_SANDBOX_WRONG_KAFKA_CA"])
    _require_files((wrong_ca,))
    profile = "invalid-ca"
    _add_profile(profile, CASES[0], credentials, environment, ca_file=wrong_ca)
    profiles.append(profile)
    _run((*_cli(), "ping", profile, "--timeout", "5"), environment, accepted=(1,))


def _exercise_wrong_hostname(
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    profile = "invalid-hostname"
    _add_profile(profile, CASES[0], credentials, environment, host="127.0.0.1")
    profiles.append(profile)
    _run((*_cli(), "ping", profile, "--timeout", "5"), environment, accepted=(1,))


def _exercise_unavailable_broker(
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    profile = "unavailable-broker"
    _run(
        (*_cli(), "add", profile, "--bootstrap-servers", "localhost:9199"),
        environment,
        accepted=(0, 3),
    )
    profiles.append(profile)
    _run((*_cli(), "ping", profile, "--timeout", "1"), environment, accepted=(1,))


def _exercise_unauthenticated_listeners(
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    for name, port, tls in (("plaintext", 9092, False), ("tls", 9093, True)):
        profile = f"anonymous-{name}"
        arguments = [*_cli(), "add", profile, "--bootstrap-servers", f"localhost:{port}"]
        if tls:
            arguments.extend(("--transport", "tls", "--ca-file", str(CA_FILE)))
        _run(arguments, environment, accepted=(0, 3))
        profiles.append(profile)
        topic = f"kantrip-smoke-{name}-{secrets.token_hex(4)}"
        _run((*_cli(), "ping", profile, "--timeout", "10"), environment)
        try:
            _create_topic(profile, topic, environment)
        finally:
            _delete_topic(profile, topic, environment)


def _exercise_native_oauth(environment: Mapping[str, str]) -> None:
    properties = STATE_FILE.parent / "kafka-oauth.properties"
    _require_files((properties,))
    topic = f"kantrip-oauth-{secrets.token_hex(4)}"
    group = f"kantrip-oauth-{secrets.token_hex(4)}"
    oauth_environment = dict(environment)
    oauth_environment["KAFKA_OPTS"] = (
        "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls="
        "https://localhost:8443/realms/kantrip/protocol/openid-connect/token"
    )
    common = ("--bootstrap-server", "localhost:9096")
    try:
        _run(
            (
                "kafka-topics",
                *common,
                "--command-config",
                str(properties),
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ),
            oauth_environment,
        )
        _run(
            (
                "kafka-console-producer",
                *common,
                "--producer.config",
                str(properties),
                "--topic",
                topic,
            ),
            oauth_environment,
            input_text="kantrip oauth smoke record\n",
        )
        output = _run(
            (
                "kafka-console-consumer",
                *common,
                "--consumer.config",
                str(properties),
                "--topic",
                topic,
                "--group",
                group,
                "--from-beginning",
                "--max-messages",
                "1",
            ),
            oauth_environment,
        )
        if "kantrip oauth smoke record" not in output:
            raise AuthSmokeFailure("native OAuth did not consume its smoke record")
    finally:
        _run(
            (
                "kafka-topics",
                *common,
                "--command-config",
                str(properties),
                "--delete",
                "--topic",
                topic,
            ),
            oauth_environment,
            accepted=(0, 1),
        )


def _create_topic(profile: str, topic: str, environment: Mapping[str, str]) -> None:
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


def _delete_topic(profile: str, topic: str, environment: Mapping[str, str]) -> None:
    _run(
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kafka-topics",
            "--delete",
            "--topic",
            topic,
        ),
        environment,
        accepted=(0, 1),
    )


def _require_redacted(output: str, credentials: Mapping[str, str]) -> None:
    for key, value in credentials.items():
        if not key.endswith(("PASSWORD", "SECRET")):
            continue
        if value and value in output:
            raise AuthSmokeFailure("a credential value appeared in command output")


def _self_signed_client_identity(directory: Path) -> tuple[Path, Path]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "untrusted-client")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    certificate_path = directory / "untrusted-client.crt"
    key_path = directory / "untrusted-client.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    certificate_path.chmod(0o600)
    key_path.chmod(0o600)
    return certificate_path, key_path


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
