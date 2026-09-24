"""Create profile-scoped child-process sessions."""

from __future__ import annotations

import os
import shutil
import ssl
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from kantrip._files import write_exclusive_text
from kantrip.adapters import (
    KAFKA_EXECUTABLES,
    KASKADE_EXECUTABLES,
    KCAT_EXECUTABLES,
    SCHEMA_REGISTRY_EXECUTABLES,
    AdapterError,
    create_subshell_shims,
    prepare_command,
    require_adapter_capability,
)
from kantrip.kafka import (
    CA_BUNDLE_FILENAME,
    CLIENT_CERTIFICATE_FILENAME,
    CLIENT_KEY_FILENAME,
    OAUTH_CA_BUNDLE_FILENAME,
    KafkaConnection,
    KafkaProfileError,
    java_properties,
    kafka_connection,
    librdkafka_properties,
    resolve_kafka_connection,
)
from kantrip.registry import (
    APICURIO_PROVIDER,
    CONFLUENT_PROVIDER,
    REGISTRY_CA_BUNDLE_FILENAME,
    REGISTRY_CLIENT_CERTIFICATE_FILENAME,
    REGISTRY_CLIENT_KEY_FILENAME,
    REGISTRY_OAUTH_CA_BUNDLE_FILENAME,
    RegistryConnection,
    RegistryProfileError,
    _UnresolvedRegistry,
    confluent_console_properties,
    kaskade_registry_properties,
    registry_connection,
    resolve_registry_connection,
)
from kantrip.runtime import (
    SessionRuntime,
    SessionRuntimeError,
    cleanup_abandoned_sessions,
    create_session_runtime,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store
from kantrip.shells import ShellError, prepare_interactive_shell, resolve_interactive_shell
from kantrip.supervisor import SupervisorError, run_supervised_process

_SCRUBBED_PREFIXES = ("KAFKA_", "SCHEMA_REGISTRY_", "APICURIO_", "KANTRIP_SANDBOX_")
_SCRUBBED_JAVA_VARIABLES = frozenset(
    {"KAFKA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"}
)
_PLATFORM_CA_BUNDLE_CANDIDATES = (
    Path("/etc/ssl/cert.pem"),
    Path("/etc/ssl/certs/ca-certificates.crt"),
    Path("/etc/pki/tls/certs/ca-bundle.crt"),
    Path("/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"),
    Path("/etc/ssl/ca-bundle.pem"),
)


class SessionError(RuntimeError):
    """Raised when a profile session cannot be prepared or started."""


def ensure_session_available(environment: Mapping[str, str] | None = None) -> None:
    """Reject attempts to create a session below an existing Kantrip session."""
    env = os.environ if environment is None else environment
    if env.get("KANTRIP_SESSION_ID"):
        raise SessionError("a Kantrip session is already active; exit it before starting another")


def run_profile_session(
    profile_name: str,
    profile: Mapping[str, Any],
    command: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    profile_revision: int = 1,
    resolved_kafka: KafkaConnection | None = None,
    resolved_registry: RegistryConnection | None | _UnresolvedRegistry = _UnresolvedRegistry.VALUE,
    secret_store: SecretStore | None = None,
) -> int:
    """Run a command or interactive shell in a temporary profile session."""
    env = dict(os.environ if environment is None else environment)
    ensure_session_available(env)

    try:
        executable = command[0] if command else resolve_interactive_shell(env)
    except ShellError as error:
        raise SessionError(str(error)) from error
    arguments = list(command) if command else [executable]

    if command:
        _validate_executable(executable, env)
    _validate_kcat_arguments(arguments)
    try:
        kafka = resolved_kafka or kafka_connection(profile)
        selected_store = secret_store
        if kafka.requires_secrets and resolved_kafka is None:
            selected_store = selected_store or load_secret_store()
            kafka = resolve_kafka_connection(kafka, selected_store)
    except (KafkaProfileError, SecretStoreError) as error:
        raise SessionError(str(error)) from error
    registry = (
        _profile_registry(profile, selected_store)
        if isinstance(resolved_registry, _UnresolvedRegistry)
        else resolved_registry
    )

    try:
        cleanup_abandoned_sessions(env)
        profile_id = profile.get("id")
        if not isinstance(profile_id, str):
            raise SessionError("profile identity is missing")
        runtime = create_session_runtime(profile_id, profile_revision, env)
    except SessionRuntimeError as error:
        raise SessionError(str(error)) from error
    try:
        return _run_in_runtime(
            runtime,
            profile_name,
            executable,
            arguments,
            bool(command),
            env,
            kafka,
            registry,
        )
    except (SessionRuntimeError, SupervisorError) as error:
        raise SessionError(str(error)) from error
    finally:
        try:
            runtime.close()
        except SessionRuntimeError as error:
            raise SessionError(str(error)) from error


def _run_in_runtime(  # noqa: C901
    runtime: SessionRuntime,
    profile_name: str,
    executable: str,
    arguments: list[str],
    has_command: bool,
    environment: Mapping[str, str],
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> int:
    session_directory = runtime.path
    ca_path: Path | None = None
    if kafka.ca_certificates is not None:
        ca_path = session_directory / CA_BUNDLE_FILENAME
        write_exclusive_text(ca_path, kafka.ca_certificates, mode=0o600)
    certificate_path: Path | None = None
    private_key_path: Path | None = None
    if kafka.auth_type == "mtls":
        if kafka.client_certificate is None or kafka.private_key is None:
            raise SessionError("Kafka mTLS credentials are not resolved")
        certificate_path = session_directory / CLIENT_CERTIFICATE_FILENAME
        private_key_path = session_directory / CLIENT_KEY_FILENAME
        write_exclusive_text(certificate_path, kafka.client_certificate, mode=0o600)
        write_exclusive_text(private_key_path, kafka.private_key, mode=0o600)
    oauth_ca_path: Path | None = None
    if kafka.oauth is not None and kafka.oauth.ca_certificates is not None:
        oauth_ca_path = session_directory / OAUTH_CA_BUNDLE_FILENAME
        write_exclusive_text(oauth_ca_path, kafka.oauth.ca_certificates, mode=0o600)
    registry_ca_path: Path | None = None
    registry_oauth_ca_path: Path | None = None
    registry_certificate_path: Path | None = None
    registry_key_path: Path | None = None
    if registry is not None and registry.ca_certificates is not None:
        registry_ca_path = session_directory / REGISTRY_CA_BUNDLE_FILENAME
        write_exclusive_text(registry_ca_path, registry.ca_certificates, mode=0o600)
    if (
        registry is not None
        and registry.provider == CONFLUENT_PROVIDER
        and registry.oauth is not None
        and registry.oauth.ca_certificates is not None
    ):
        registry_oauth_ca_path = session_directory / REGISTRY_OAUTH_CA_BUNDLE_FILENAME
        write_exclusive_text(
            registry_oauth_ca_path,
            _oauth_trust_bundle(registry.oauth.ca_certificates),
            mode=0o600,
        )
    if registry is not None and registry.auth_type == "mtls":
        if registry.client_certificate is None or registry.private_key is None:
            raise SessionError("Registry mTLS credentials are not resolved")
        registry_certificate_path = session_directory / REGISTRY_CLIENT_CERTIFICATE_FILENAME
        registry_key_path = session_directory / REGISTRY_CLIENT_KEY_FILENAME
        write_exclusive_text(registry_certificate_path, registry.client_certificate, mode=0o600)
        write_exclusive_text(registry_key_path, registry.private_key, mode=0o600)
    try:
        kcat_properties = librdkafka_properties(
            kafka,
            ca_location=ca_path,
            client_certificate_location=certificate_path,
            private_key_location=private_key_path,
            oauth_ca_location=oauth_ca_path,
        )
        java_config = java_properties(
            kafka,
            ca_location=ca_path,
            oauth_ca_location=oauth_ca_path,
        )
    except KafkaProfileError as error:
        raise SessionError(str(error)) from error
    kcat_config_path = session_directory / "kcat.conf"
    java_config_path = session_directory / "kafka.properties"
    kaskade_config_path = session_directory / "kaskade.ini"
    kaskade_registry_config_path = session_directory / "kaskade-registry.ini"
    registry_config_path = session_directory / "registry.properties"
    schema_registry_java_config_path = session_directory / "schema-registry-kafka.properties"
    write_exclusive_text(kcat_config_path, _render_properties(kcat_properties), mode=0o600)
    write_exclusive_text(java_config_path, _render_java_properties(java_config), mode=0o600)
    write_exclusive_text(
        kaskade_config_path,
        f"[kafka]\n{_render_properties(kcat_properties)}",
        mode=0o600,
    )
    if registry is not None:
        try:
            kaskade_registry = kaskade_registry_properties(
                registry,
                ca_location=registry_ca_path,
                client_certificate_location=registry_certificate_path,
                private_key_location=registry_key_path,
            )
        except RegistryProfileError:
            kaskade_registry = {}
        try:
            console_registry = (
                confluent_console_properties(
                    registry,
                    ca_location=registry_ca_path,
                    client_certificate_location=registry_certificate_path,
                    private_key_location=registry_key_path,
                )
                if registry.provider != APICURIO_PROVIDER
                else {}
            )
        except RegistryProfileError:
            console_registry = {}
        write_exclusive_text(
            kaskade_registry_config_path,
            f"[kafka]\n{_render_properties(kcat_properties)}"
            f"\n[registry]\n{_render_properties(kaskade_registry)}",
            mode=0o600,
        )
        write_exclusive_text(
            registry_config_path,
            _render_properties(kaskade_registry),
            mode=0o600,
        )
        write_exclusive_text(
            schema_registry_java_config_path,
            _render_java_properties(java_config | console_registry),
            mode=0o600,
        )

    child_environment = _child_environment(
        runtime,
        profile_name,
        environment,
        kcat_properties,
        java_config_path,
        schema_registry_java_config_path,
        kcat_config_path,
        registry_config_path,
        kafka,
        registry,
    )
    try:
        if has_command:
            arguments = prepare_command(
                arguments,
                bootstrap_servers=kcat_properties["bootstrap.servers"],
                java_config_path=java_config_path,
                schema_registry_java_config_path=schema_registry_java_config_path,
                kaskade_config_path=kaskade_config_path,
                kaskade_registry_config_path=kaskade_registry_config_path,
                registry=registry,
            )
            require_adapter_capability(
                arguments[0],
                auth_type=kafka.auth_type,
                custom_pem=kafka.ca_certificates is not None or kafka.auth_type == "mtls",
                environment=environment,
                registry=registry,
            )
        else:
            arguments = _prepare_subshell(
                executable,
                session_directory,
                environment,
                child_environment,
                kcat_properties,
                java_config_path,
                schema_registry_java_config_path,
                kaskade_config_path,
                kaskade_registry_config_path,
                registry,
                registry_oauth_ca_path,
                kafka.ca_certificates is not None or kafka.auth_type == "mtls",
                kafka.auth_type,
            )
    except (AdapterError, ShellError) as error:
        raise SessionError(str(error)) from error
    runtime.mark_running()
    execution_environment = _registry_client_environment(
        arguments,
        child_environment,
        registry,
        registry_oauth_ca_path,
    )
    result = _run_child(
        arguments,
        env=execution_environment,
        check=False,
        interactive=not has_command,
    )
    return result.returncode


def _profile_registry(
    profile: Mapping[str, Any], secret_store: SecretStore | None
) -> RegistryConnection | None:
    try:
        registry = registry_connection(profile)
    except RegistryProfileError as error:
        raise SessionError(str(error)) from error
    if registry is not None and registry.requires_secrets:
        try:
            registry = resolve_registry_connection(registry, secret_store or load_secret_store())
        except (RegistryProfileError, SecretStoreError) as error:
            raise SessionError(str(error)) from error
    return registry


def _run_child(
    arguments: Sequence[str],
    *,
    env: Mapping[str, str],
    check: bool,
    interactive: bool,
) -> subprocess.CompletedProcess[str]:
    del check
    return_code = run_supervised_process(
        arguments,
        environment=env,
        interactive=interactive,
    )
    return subprocess.CompletedProcess(arguments, return_code)


def _child_environment(
    runtime: SessionRuntime,
    profile_name: str,
    environment: Mapping[str, str],
    kcat_properties: Mapping[str, str],
    java_config_path: Path,
    schema_registry_java_config_path: Path,
    kcat_config_path: Path,
    registry_config_path: Path,
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> dict[str, str]:
    child_environment = {
        name: value
        for name, value in environment.items()
        if not name.startswith(_SCRUBBED_PREFIXES) and name not in _SCRUBBED_JAVA_VARIABLES
    }
    child_environment.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": kcat_properties["bootstrap.servers"],
            "KAFKA_JAVA_CONFIG_FILE": str(java_config_path),
            "SCHEMA_REGISTRY_KAFKA_CONFIG_FILE": str(schema_registry_java_config_path),
            "KAFKA_LIBRDKAFKA_CONFIG_FILE": str(kcat_config_path),
            "KAFKA_SECURITY_PROTOCOL": kcat_properties["security.protocol"],
            "KANTRIP_PROFILE": profile_name,
            "KANTRIP_SESSION_DIR": str(runtime.path),
            "KANTRIP_SESSION_ID": runtime.session_id,
            "KCAT_CONFIG": str(kcat_config_path),
        }
    )
    if registry is not None:
        prefix = "APICURIO" if registry.provider == APICURIO_PROVIDER else "SCHEMA"
        child_environment.update(
            {
                f"{prefix}_REGISTRY_CONFIG_FILE": str(registry_config_path),
                f"{prefix}_REGISTRY_URL": registry.url,
            }
        )
    allowed_oauth_urls: list[str] = []
    if kafka.auth_type == "oauth":
        assert kafka.oauth is not None
        allowed_oauth_urls.append(kafka.oauth.token_url)
    if (
        registry is not None
        and registry.provider == CONFLUENT_PROVIDER
        and registry.auth_type == "oauth"
    ):
        assert registry.oauth is not None
        allowed_oauth_urls.append(registry.oauth.token_url)
    if allowed_oauth_urls:
        allowed_urls_property = "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=" + ",".join(
            dict.fromkeys(allowed_oauth_urls)
        )
        child_environment["KAFKA_OPTS"] = allowed_urls_property
        child_environment["SCHEMA_REGISTRY_OPTS"] = allowed_urls_property
    return child_environment


def _prepare_subshell(
    executable: str,
    session_directory: Path,
    environment: Mapping[str, str],
    child_environment: dict[str, str],
    kcat_properties: Mapping[str, str],
    java_config_path: Path,
    schema_registry_java_config_path: Path,
    kaskade_config_path: Path,
    kaskade_registry_config_path: Path,
    registry: RegistryConnection | None,
    registry_oauth_ca_path: Path | None,
    require_java_pem: bool,
    kafka_auth_type: str,
) -> list[str]:
    shim_directory = create_subshell_shims(
        session_directory / "bin",
        bootstrap_servers=kcat_properties["bootstrap.servers"],
        java_config_path=java_config_path,
        schema_registry_java_config_path=schema_registry_java_config_path,
        kcat_config_path=Path(child_environment["KCAT_CONFIG"]),
        kaskade_config_path=kaskade_config_path,
        kaskade_registry_config_path=kaskade_registry_config_path,
        environment=environment,
        registry=registry,
        registry_oauth_ssl_cert_file=registry_oauth_ca_path,
        require_java_pem=require_java_pem,
        kafka_auth_type=kafka_auth_type,
    )
    child_environment["PATH"] = f"{shim_directory}{os.pathsep}{environment.get('PATH', os.defpath)}"
    plan = prepare_interactive_shell(
        executable,
        session_directory,
        shim_directory,
        environment,
        owned_environment={
            name: value
            for name, value in child_environment.items()
            if name.startswith(("KAFKA_", "SCHEMA_REGISTRY_", "APICURIO_", "KANTRIP_"))
            or name == "KCAT_CONFIG"
        },
    )
    child_environment.update(plan.environment_overrides)
    return list(plan.arguments)


def _oauth_trust_bundle(profile_ca: str) -> str:
    default_roots = _platform_default_ca_bundle()
    return f"{default_roots.rstrip()}\n{profile_ca.strip()}\n"


def _platform_default_ca_bundle() -> str:
    compiled_path = ssl.get_default_verify_paths().openssl_cafile
    candidates = (
        *((Path(compiled_path),) if compiled_path is not None else ()),
        *_PLATFORM_CA_BUNDLE_CANDIDATES,
    )
    visited: set[Path] = set()
    for path in candidates:
        if path in visited:
            continue
        visited.add(path)
        try:
            contents = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if contents.strip():
            return contents
    raise SessionError("the platform default CA bundle could not be located or read")


def _registry_client_environment(
    arguments: Sequence[str],
    environment: Mapping[str, str],
    registry: RegistryConnection | None,
    oauth_ca_path: Path | None,
) -> dict[str, str]:
    result = dict(environment)
    if not _is_kaskade_registry_oauth(arguments, registry):
        return result
    result.pop("SSL_CERT_FILE", None)
    result.pop("SSL_CERT_DIR", None)
    if oauth_ca_path is not None:
        result["SSL_CERT_FILE"] = str(oauth_ca_path)
    return result


def _is_kaskade_registry_oauth(
    arguments: Sequence[str], registry: RegistryConnection | None
) -> bool:
    if (
        registry is None
        or registry.auth_type != "oauth"
        or not arguments
        or Path(arguments[0]).name != "kaskade"
        or len(arguments) < 2
        or arguments[1] != "consumer"
    ):
        return False
    return any(
        value.lower() == "registry"
        or value.lower().endswith("=registry")
        or value.lower() in {"-kregistry", "-vregistry"}
        for value in arguments[2:]
    )


def _validate_executable(executable: str, environment: Mapping[str, str]) -> None:
    path = environment.get("PATH")
    if shutil.which(executable, path=path) is None:
        executable_name = Path(executable).name
        if executable_name in KCAT_EXECUTABLES:
            raise SessionError(
                "command 'kcat' was not found; install it with 'brew install kcat' "
                "on macOS or your Linux package manager"
            )
        if executable_name in KAFKA_EXECUTABLES:
            if executable_name in SCHEMA_REGISTRY_EXECUTABLES:
                raise SessionError(
                    f"command '{executable_name}' was not found; install the Confluent Schema "
                    "Registry package and ensure its bin directory is on PATH"
                )
            raise SessionError(
                f"command '{executable_name}' was not found; install the Apache Kafka CLI "
                "and ensure its bin directory is on PATH"
            )
        if executable_name in KASKADE_EXECUTABLES:
            raise SessionError(
                "command 'kaskade' was not found; install it and ensure its executable is on PATH"
            )
        raise SessionError(f"command '{executable}' was not found")


def _validate_kcat_arguments(arguments: Sequence[str]) -> None:
    if Path(arguments[0]).name not in KCAT_EXECUTABLES:
        return
    if any(argument == "-F" or argument.startswith("-F") for argument in arguments[1:]):
        raise SessionError("kcat's -F option cannot override the selected Kantrip profile")


def _render_properties(properties: Mapping[str, str]) -> str:
    for key, value in properties.items():
        if "=" in key or "\n" in key or "\r" in key:
            raise SessionError("kcat property names cannot contain '=' or line breaks")
        if "\n" in value or "\r" in value:
            raise SessionError("client property values cannot contain line breaks")
    return "".join(f"{key}={value}\n" for key, value in sorted(properties.items()))


def _render_java_properties(properties: Mapping[str, str]) -> str:
    """Serialize Java properties without losing PEM or credential characters."""
    rendered: list[str] = []
    for key, value in sorted(properties.items()):
        if any(character in key for character in ("=", ":", "\n", "\r")):
            raise SessionError("Java property names contain unsupported characters")
        escaped = (
            value.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\f", "\\f")
        )
        if escaped.startswith(" "):
            escaped = f"\\{escaped}"
        rendered.append(f"{key}={escaped}\n")
    return "".join(rendered)


__all__ = ["SessionError", "ensure_session_available", "run_profile_session"]
