"""Create profile-scoped child-process sessions."""

from __future__ import annotations

import os
import secrets
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO

from kantrip.adapters import (
    KAFKA_TOPICS_EXECUTABLES,
    KASKADE_EXECUTABLES,
    AdapterError,
    create_subshell_shims,
    prepare_command,
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
) -> int:
    """Run a command or interactive shell in a temporary profile session."""
    env = dict(os.environ if environment is None else environment)
    ensure_session_available(env)

    executable = command[0] if command else env.get("SHELL", "/bin/sh")
    arguments = list(command) if command else [executable]

    _validate_executable(executable, env)
    _validate_kcat_arguments(arguments)
    kcat_properties = _client_properties(profile, "librdkafka")
    java_properties = _client_properties(profile, "java")

    session_id = secrets.token_hex(16)
    with tempfile.TemporaryDirectory(prefix=f"kantrip-{session_id}-") as directory:
        session_directory = Path(directory)
        kcat_config_path = session_directory / "kcat.conf"
        java_config_path = session_directory / "kafka.properties"
        kaskade_config_path = session_directory / "kaskade.ini"
        _write_private_file(kcat_config_path, _render_properties(kcat_properties))
        _write_private_file(java_config_path, _render_properties(java_properties))
        _write_private_file(
            kaskade_config_path,
            f"[kafka]\n{_render_properties(kcat_properties)}",
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
        try:
            if command:
                arguments = prepare_command(
                    arguments,
                    bootstrap_servers=kcat_properties["bootstrap.servers"],
                    java_config_path=java_config_path,
                    kaskade_config_path=kaskade_config_path,
                )
            else:
                shim_directory = create_subshell_shims(
                    session_directory / "bin",
                    bootstrap_servers=kcat_properties["bootstrap.servers"],
                    java_config_path=java_config_path,
                    kaskade_config_path=kaskade_config_path,
                    environment=env,
                )
                if shim_directory is not None:
                    child_environment["PATH"] = (
                        f"{shim_directory}{os.pathsep}{env.get('PATH', os.defpath)}"
                    )
                    arguments = _prepare_subshell_startup(
                        arguments,
                        shell=executable,
                        shim_directory=shim_directory,
                        session_directory=session_directory,
                        environment=env,
                        child_environment=child_environment,
                    )
        except AdapterError as error:
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
        if executable_name in {"kcat", "kafkacat"}:
            raise SessionError(
                "command 'kcat' was not found; install it with 'brew install kcat' "
                "on macOS or your Linux package manager"
            )
        if executable_name in KAFKA_TOPICS_EXECUTABLES:
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
    if Path(arguments[0]).name not in {"kcat", "kafkacat"}:
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


def _write_private_file(path: Path, contents: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stream: TextIO
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents)


def _prepare_subshell_startup(
    arguments: list[str],
    *,
    shell: str,
    shim_directory: Path,
    session_directory: Path,
    environment: Mapping[str, str],
    child_environment: dict[str, str],
) -> list[str]:
    """Restore the adapter path after an interactive shell loads user configuration."""
    shell_name = Path(shell).name
    if shell_name == "zsh":
        startup_directory = session_directory / "zsh"
        startup_directory.mkdir(mode=0o700)
        original_directory = environment.get("ZDOTDIR") or environment.get("HOME")
        original_startup = (
            Path(original_directory).expanduser() / ".zshrc" if original_directory else None
        )
        history_path = (
            Path(original_directory).expanduser() / ".zsh_history" if original_directory else None
        )
        contents = _render_zsh_startup(
            original_startup,
            shim_directory,
            history_path=history_path,
            original_zdotdir=environment.get("ZDOTDIR"),
            original_shell_sessions_disable=environment.get("SHELL_SESSIONS_DISABLE"),
        )
        _write_private_file(startup_directory / ".zshrc", contents)
        child_environment["ZDOTDIR"] = str(startup_directory)
        child_environment["SHELL_SESSIONS_DISABLE"] = "1"
    elif shell_name == "bash":
        home = environment.get("HOME")
        original_startup = Path(home).expanduser() / ".bashrc" if home else None
        startup_path = session_directory / "bashrc"
        contents = _render_shell_startup(
            original_startup,
            shim_directory,
            refresh_command="hash -r",
        )
        _write_private_file(startup_path, contents)
        return [*arguments, "--rcfile", str(startup_path)]
    return arguments


def _render_zsh_startup(
    original_startup: Path | None,
    shim_directory: Path,
    *,
    history_path: Path | None,
    original_zdotdir: str | None,
    original_shell_sessions_disable: str | None,
) -> str:
    lines = ["# Generated by Kantrip for this profile session."]
    if original_zdotdir is not None:
        lines.append(f"export ZDOTDIR={shlex.quote(original_zdotdir)}")
    else:
        lines.append("unset ZDOTDIR")
    if original_shell_sessions_disable is not None:
        lines.append(
            "export SHELL_SESSIONS_DISABLE=" f"{shlex.quote(original_shell_sessions_disable)}"
        )
    else:
        lines.append("unset SHELL_SESSIONS_DISABLE")
    if history_path is not None:
        lines.append(f"HISTFILE={shlex.quote(str(history_path))}")
    if original_startup is not None and original_startup.is_file():
        lines.append(f"source {shlex.quote(str(original_startup))}")
    lines.extend(
        (
            f'export PATH={shlex.quote(str(shim_directory))}:"${{PATH:-{os.defpath}}}"',
            "rehash",
        )
    )
    return "\n".join(lines) + "\n"


def _render_shell_startup(
    original_startup: Path | None,
    shim_directory: Path,
    *,
    refresh_command: str,
) -> str:
    lines = ["# Generated by Kantrip for this profile session."]
    if original_startup is not None and original_startup.is_file():
        lines.append(f"source {shlex.quote(str(original_startup))}")
    lines.extend(
        (
            f'export PATH={shlex.quote(str(shim_directory))}:"${{PATH:-{os.defpath}}}"',
            refresh_command,
        )
    )
    return "\n".join(lines) + "\n"


__all__ = ["SessionError", "ensure_session_available", "run_profile_session"]
