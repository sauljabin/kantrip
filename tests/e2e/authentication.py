"""Exercise authenticated Kafka profiles against the disposable sandbox listeners."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
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
from tests.e2e.registry_oauth import RegistryOAuthFailure, exercise_registry_oauth_renewal
from tests.e2e.shell_environment import isolated_zsh_environment


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
    oauth_client_id_field: str | None = None
    oauth_client_secret_field: str | None = None


@dataclass(frozen=True)
class RegistryAuthCase:
    """One authenticated Registry endpoint and its private sandbox inputs."""

    name: str
    provider: str
    url: str
    auth_type: str
    username_field: str | None = None
    secret_field: str | None = None
    oauth_client_id_field: str | None = None
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
    AuthCase(
        "oauth",
        9096,
        "oauth",
        oauth_client_id_field="KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID",
        oauth_client_secret_field="KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_SECRET",
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

REGISTRY_CASES = (
    RegistryAuthCase(
        "schema-registry-basic",
        "confluent",
        "https://localhost:8083",
        "basic",
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME",
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_PASSWORD",
    ),
    RegistryAuthCase(
        "schema-registry-oauth",
        "confluent",
        "https://localhost:8085",
        "oauth",
        secret_field="KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET",
        oauth_client_id_field="KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID",
    ),
    RegistryAuthCase(
        "schema-registry-mtls",
        "confluent",
        "https://localhost:8086",
        "mtls",
        certificate_field="KANTRIP_SANDBOX_REGISTRY_MTLS_CERTIFICATE",
        key_field="KANTRIP_SANDBOX_REGISTRY_MTLS_KEY",
    ),
    RegistryAuthCase(
        "apicurio-basic",
        "apicurio",
        "https://localhost:8084/apis/registry/v3",
        "basic",
        "KANTRIP_SANDBOX_APICURIO_CLIENT_ID",
        "KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET",
    ),
    RegistryAuthCase(
        "apicurio-oauth",
        "apicurio",
        "https://localhost:8084/apis/registry/v3",
        "oauth",
        secret_field="KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET",
        oauth_client_id_field="KANTRIP_SANDBOX_APICURIO_CLIENT_ID",
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
            "kafka-avro-console-consumer",
            "kafka-topics",
            "kcat",
            "kubectl",
            "zsh",
        )
    )
    with (
        tempfile.TemporaryDirectory(prefix="kantrip-auth-smoke-") as directory,
        isolated_zsh_environment(os.environ) as environment,
    ):
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
            for registry_case in REGISTRY_CASES:
                profile = f"auth-{registry_case.name}"
                _add_registry_profile(profile, registry_case, credentials, environment)
                profiles.append(profile)
                _run((*_cli(), "ping", profile, "--timeout", "10"), environment)
            _exercise_invalid_credentials(credentials, environment, profiles)
            _exercise_invalid_registry_credentials(directory, credentials, environment, profiles)
            _exercise_invalid_mtls(directory, environment, profiles)
            _exercise_wrong_ca(credentials, environment, profiles)
            _exercise_wrong_hostname(credentials, environment, profiles)
            _exercise_unavailable_broker(environment, profiles)
            _exercise_unauthenticated_listeners(environment, profiles)
            _exercise_oauth_revocation(credentials, environment)
            _run(
                (
                    *_cli(),
                    "exec",
                    "auth-scram-256",
                    "--",
                    "kcat",
                    "-X",
                    "broker.address.family=v4",
                    "-L",
                ),
                environment,
            )
        finally:
            for profile in reversed(profiles):
                _run((*_cli(), "remove", profile, "--force"), environment, accepted=(0, 1, 3))
    print("Authenticated Kafka and Registry sandbox smoke checks passed")


def exercise_registry_oauth() -> None:
    """Exercise long-lived Registry clients with only their two OAuth profiles."""
    credentials = load_credentials(STATE_FILE)
    with tempfile.TemporaryDirectory(prefix="kantrip-registry-oauth-e2e-") as directory:
        environment = dict(os.environ)
        environment["KANTRIP_DATABASE"] = str(Path(directory) / "profiles.db")
        profiles: list[str] = []
        try:
            for case in REGISTRY_CASES:
                if case.auth_type != "oauth":
                    continue
                profile = f"auth-{case.name}"
                _add_registry_profile(profile, case, credentials, environment)
                profiles.append(profile)
            exercise_registry_oauth_renewal(
                credentials,
                environment,
                admin_token=_keycloak_admin_token,
                set_client_enabled=_set_keycloak_client_enabled,
            )
        except RegistryOAuthFailure as error:
            raise AuthSmokeFailure(str(error)) from error
        finally:
            for profile in reversed(profiles):
                _run((*_cli(), "remove", profile, "--force"), environment, accepted=(0, 1, 3))


def _add_registry_profile(
    profile: str,
    case: RegistryAuthCase,
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    *,
    secret_override: str | None = None,
    certificate_override: Path | None = None,
    key_override: Path | None = None,
) -> None:
    arguments = [
        *_cli(),
        "add",
        profile,
        "--bootstrap-servers",
        "localhost:9092",
        "--registry-provider",
        case.provider,
        "--registry-url",
        case.url,
        "--registry-ca-file",
        str(CA_FILE),
        "--registry-auth",
        case.auth_type,
    ]
    if case.auth_type == "mtls":
        assert case.certificate_field is not None and case.key_field is not None
        arguments.extend(
            (
                "--registry-client-certificate-file",
                str(certificate_override or credentials[case.certificate_field]),
                "--registry-client-key-file",
                str(key_override or credentials[case.key_field]),
            )
        )
        _run(arguments, environment, accepted=(0, 3))
        return
    assert case.secret_field is not None
    secret = secret_override or credentials[case.secret_field]
    if case.auth_type == "basic":
        assert case.username_field is not None
        arguments.extend(("--registry-username", credentials[case.username_field]))
        ready_text = "Registry password"
    else:
        assert case.oauth_client_id_field is not None
        arguments.extend(
            (
                "--registry-oauth-token-url",
                "https://localhost:8443/realms/kantrip/protocol/openid-connect/token",
                "--registry-oauth-client-id",
                credentials[case.oauth_client_id_field],
                "--registry-oauth-ca-file",
                str(CA_FILE),
                "--registry-oauth-scope",
                "openid",
            )
        )
        if case.provider == "confluent":
            arguments.extend(("--registry-oauth-logical-cluster", "lsrc-sandbox"))
        ready_text = "Registry OAuth client secret"
    status, output = run_terminal(
        arguments,
        (secret,),
        environment=environment,
        ready_text=ready_text,
        timeout=30,
    )
    if status not in (0, 3):
        raise AuthSmokeFailure(_safe_failure(profile, output, credentials))


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
    if case.oauth_client_id_field is not None:
        assert case.oauth_client_secret_field is not None
        arguments.extend(
            (
                "--oauth-token-url",
                "https://localhost:8443/realms/kantrip/protocol/openid-connect/token",
                "--oauth-client-id",
                credentials[case.oauth_client_id_field],
                "--oauth-ca-file",
                str(CA_FILE),
            )
        )
        status, output = run_terminal(
            arguments,
            (credentials[case.oauth_client_secret_field],),
            environment=environment,
            ready_text="Kafka OAuth client secret",
            timeout=30,
        )
        if status not in (0, 3):
            raise AuthSmokeFailure(_safe_failure(profile, output, credentials))
        return
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
    resource_prefix = "kantrip-oauth" if case.auth_type == "oauth" else "kantrip-auth"
    topic = f"{resource_prefix}-{case.name}-{secrets.token_hex(4)}"
    group = f"{resource_prefix}-{case.name}-{secrets.token_hex(4)}"
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
        _run(
            (
                *_cli(),
                "exec",
                profile,
                "--",
                "kcat",
                "-X",
                "broker.address.family=v4",
                "-L",
            ),
            environment,
        )
        for shell_name in ("bash", "zsh", "fish"):
            _exercise_shell(profile, shell_name, environment)
        if case.auth_type == "oauth":
            _exercise_oauth_refresh(profile, topic, environment)
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
    kcat_command = "kcat -X broker.address.family=v4 -L >/dev/null && exit"
    shell_command = f"kafka-topics --list && {kcat_command}"
    try:
        status, output = run_terminal(
            (*_cli(), "exec", profile),
            (shell_command,),
            environment=shell_environment,
            timeout=90,
        )
    except TerminalTimeout as error:
        raise AuthSmokeFailure(f"{profile} {shell_name} session timed out") from error
    if status:
        raise AuthSmokeFailure(f"{profile} {shell_name} session failed: {output.strip()}")


def _exercise_oauth_refresh(
    profile: str,
    topic: str,
    environment: Mapping[str, str],
) -> None:
    consumers = (
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kafka-console-consumer",
            "--topic",
            topic,
            "--group",
            f"kantrip-oauth-refresh-{secrets.token_hex(4)}",
            "--from-beginning",
            "--max-messages",
            "2",
        ),
        (
            *_cli(),
            "exec",
            profile,
            "--",
            "kcat",
            "-X",
            "broker.address.family=v4",
            "-C",
            "-t",
            topic,
            "-o",
            "beginning",
            "-c",
            "2",
        ),
    )
    processes = [
        subprocess.Popen(
            command,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for command in consumers
    ]
    try:
        time.sleep(22)
        _run(
            (
                *_cli(),
                "exec",
                profile,
                "--",
                "kcat",
                "-X",
                "broker.address.family=v4",
                "-P",
                "-t",
                topic,
            ),
            environment,
            input_text="kantrip oauth refreshed record\n",
        )
        for process in processes:
            output, _ = process.communicate(timeout=45)
            if process.returncode != 0 or "kantrip oauth refreshed record" not in output:
                raise AuthSmokeFailure(
                    "an OAuth client did not survive token expiry and native refresh: "
                    f"{output.strip()}"
                )
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def _exercise_oauth_revocation(
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
) -> None:
    client_id = credentials["KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID"]
    _set_keycloak_client_enabled(credentials, client_id, enabled=False)
    try:
        _run((*_cli(), "ping", "auth-oauth", "--timeout", "10"), environment, accepted=(1,))
    finally:
        _set_keycloak_client_enabled(credentials, client_id, enabled=True)
    _run((*_cli(), "ping", "auth-oauth", "--timeout", "10"), environment)


def _set_keycloak_client_enabled(
    credentials: Mapping[str, str], client_id: str, *, enabled: bool
) -> None:
    context = ssl.create_default_context(cafile=str(CA_FILE))
    token = _keycloak_admin_token(credentials, context)
    request = urllib.request.Request(
        "https://localhost:8443/admin/realms/kantrip/clients?"
        + urllib.parse.urlencode({"clientId": client_id}),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            clients = json.load(response)
        if not isinstance(clients, list) or len(clients) != 1 or not isinstance(clients[0], dict):
            raise AuthSmokeFailure("Keycloak did not return one exact OAuth client")
        representation = clients[0]
        internal_id = representation.get("id")
        if not isinstance(internal_id, str) or not internal_id:
            raise AuthSmokeFailure("Keycloak OAuth client identity is invalid")
        representation["enabled"] = enabled
        update = urllib.request.Request(
            f"https://localhost:8443/admin/realms/kantrip/clients/{internal_id}",
            data=json.dumps(representation).encode(),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="PUT",
        )
        with urllib.request.urlopen(update, context=context, timeout=10) as response:
            if response.status != 204:
                raise AuthSmokeFailure("Keycloak OAuth client update was not acknowledged")
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise AuthSmokeFailure("could not update the sandbox OAuth client") from error


def _keycloak_admin_token(credentials: Mapping[str, str], context: ssl.SSLContext) -> str:
    form = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": credentials["KANTRIP_SANDBOX_KEYCLOAK_ADMIN_USERNAME"],
            "password": credentials["KANTRIP_SANDBOX_KEYCLOAK_ADMIN_PASSWORD"],
        }
    ).encode()
    request = urllib.request.Request(
        "https://localhost:8443/realms/master/protocol/openid-connect/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            body = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise AuthSmokeFailure(
            "could not authenticate to the sandbox Keycloak admin API"
        ) from error
    token = body.get("access_token") if isinstance(body, dict) else None
    if not isinstance(token, str) or not token:
        raise AuthSmokeFailure("Keycloak returned no admin access token")
    return token


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


def _exercise_invalid_registry_credentials(
    directory: str,
    credentials: Mapping[str, str],
    environment: Mapping[str, str],
    profiles: list[str],
) -> None:
    for case in REGISTRY_CASES:
        profile = f"invalid-{case.name}"
        if case.auth_type == "mtls":
            certificate, key = _self_signed_client_identity(Path(directory))
            _add_registry_profile(
                profile,
                case,
                credentials,
                environment,
                certificate_override=certificate,
                key_override=key,
            )
        else:
            _add_registry_profile(
                profile,
                case,
                credentials,
                environment,
                secret_override=f"invalid-{secrets.token_hex(16)}",
            )
        profiles.append(profile)
        output = _run(
            (*_cli(), "ping", profile, "--timeout", "10"),
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
    configured = os.environ.get("KANTRIP_E2E_KANTRIP")
    executable = configured or shutil.which("kantrip")
    if executable is None:
        raise AuthSmokeFailure("required command 'kantrip' was not found")
    return executable, "--no-color"


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
