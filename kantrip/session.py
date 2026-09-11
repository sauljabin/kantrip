"""Create profile-scoped child-process sessions."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import tempfile
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
)
from kantrip.schema_registry import SchemaRegistryProfileError, plain_schema_registry_url
from kantrip.shells import ShellError, prepare_interactive_shell, resolve_interactive_shell


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
    kcat_properties = _client_properties(profile, "librdkafka")
    java_properties = _client_properties(profile, "java")
    schema_registry_url, schema_registry_error = _schema_registry_connection(profile)

    session_id = secrets.token_hex(16)
    with tempfile.TemporaryDirectory(prefix=f"kantrip-{session_id}-") as directory:
        session_directory = Path(directory)
        kcat_config_path = session_directory / "kcat.conf"
        java_config_path = session_directory / "kafka.properties"
        kaskade_config_path = session_directory / "kaskade.ini"
        kaskade_registry_config_path = session_directory / "kaskade-registry.ini"
        schema_registry_config_path = session_directory / "schema-registry.properties"
        write_exclusive_text(kcat_config_path, _render_properties(kcat_properties), mode=0o600)
        write_exclusive_text(java_config_path, _render_properties(java_properties), mode=0o600)
        write_exclusive_text(
            kaskade_config_path,
            f"[kafka]\n{_render_properties(kcat_properties)}",
            mode=0o600,
        )
        if schema_registry_url is not None:
            write_exclusive_text(
                kaskade_registry_config_path,
                f"[kafka]\n{_render_properties(kcat_properties)}"
                f"\n[registry]\nurl={schema_registry_url}\n",
                mode=0o600,
            )
            write_exclusive_text(
                schema_registry_config_path,
                _render_properties({"schema.registry.url": schema_registry_url}),
                mode=0o600,
            )

        child_environment = env | {
            "KAFKA_BOOTSTRAP_SERVERS": kcat_properties["bootstrap.servers"],
            "KAFKA_JAVA_CONFIG_FILE": str(java_config_path),
            "KAFKA_LIBRDKAFKA_CONFIG_FILE": str(kcat_config_path),
            "KAFKA_SECURITY_PROTOCOL": kcat_properties["security.protocol"],
            "KANTRIP_PROFILE": profile_name,
            "KANTRIP_SESSION_DIR": str(session_directory),
            "KANTRIP_SESSION_ID": session_id,
            "KCAT_CONFIG": str(kcat_config_path),
        }
        child_environment.pop("SCHEMA_REGISTRY_CONFIG_FILE", None)
        child_environment.pop("SCHEMA_REGISTRY_URL", None)
        if schema_registry_url is not None:
            child_environment.update(
                {
                    "SCHEMA_REGISTRY_CONFIG_FILE": str(schema_registry_config_path),
                    "SCHEMA_REGISTRY_URL": schema_registry_url,
                }
            )
        try:
            if command:
                arguments = prepare_command(
                    arguments,
                    bootstrap_servers=kcat_properties["bootstrap.servers"],
                    java_config_path=java_config_path,
                    kaskade_config_path=kaskade_config_path,
                    kaskade_registry_config_path=kaskade_registry_config_path,
                    schema_registry_url=schema_registry_url,
                    schema_registry_error=schema_registry_error,
                )
            else:
                shim_directory = create_subshell_shims(
                    session_directory / "bin",
                    bootstrap_servers=kcat_properties["bootstrap.servers"],
                    java_config_path=java_config_path,
                    kaskade_config_path=kaskade_config_path,
                    kaskade_registry_config_path=kaskade_registry_config_path,
                    environment=env,
                    schema_registry_url=schema_registry_url,
                    schema_registry_error=schema_registry_error,
                )
                child_environment["PATH"] = (
                    f"{shim_directory}{os.pathsep}{env.get('PATH', os.defpath)}"
                )
                plan = prepare_interactive_shell(
                    executable,
                    session_directory,
                    shim_directory,
                    env,
                )
                arguments = list(plan.arguments)
                child_environment.update(plan.environment_overrides)
        except (AdapterError, ShellError) as error:
            raise SessionError(str(error)) from error
        try:
            result = subprocess.run(arguments, env=child_environment, check=False)
        except OSError as error:
            raise SessionError(f"command could not be started: {executable}") from error
        return result.returncode


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


def _client_properties(profile: Mapping[str, Any], client: str) -> dict[str, str]:
    kafka = profile["kafka"]
    if kafka["transport"] != "plaintext" or kafka["auth"]["type"] != "none":
        raise SessionError("only plaintext profiles without authentication are supported")

    configured = kafka.get("properties", {})
    properties = {
        str(key): _property_value(value)
        for group in (configured.get("common", {}), configured.get(client, {}))
        for key, value in group.items()
    }
    properties.update(
        {
            "bootstrap.servers": ",".join(kafka["bootstrapServers"]),
            "security.protocol": "PLAINTEXT",
        }
    )
    return properties


def _schema_registry_connection(profile: Mapping[str, Any]) -> tuple[str | None, str | None]:
    try:
        return plain_schema_registry_url(profile), None
    except SchemaRegistryProfileError as error:
        return None, str(error)


def _property_value(value: object) -> str:
    if isinstance(value, bool):
        rendered = str(value).lower()
    else:
        rendered = str(value)
    if "\n" in rendered or "\r" in rendered:
        raise SessionError("kcat property values cannot contain line breaks")
    return rendered


def _render_properties(properties: Mapping[str, str]) -> str:
    for key in properties:
        if "=" in key or "\n" in key or "\r" in key:
            raise SessionError("kcat property names cannot contain '=' or line breaks")
    return "".join(f"{key}={value}\n" for key, value in sorted(properties.items()))


__all__ = ["SessionError", "ensure_session_available", "run_profile_session"]
