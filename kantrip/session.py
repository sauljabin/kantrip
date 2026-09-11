"""Create profile-scoped child-process sessions."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO


class SessionError(RuntimeError):
    """Raised when a profile session cannot be prepared or started."""


def run_profile_session(
    profile_name: str,
    profile: Mapping[str, Any],
    command: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
) -> int:
    """Run a command or interactive shell in a temporary profile session."""
    env = dict(os.environ if environment is None else environment)
    executable = command[0] if command else env.get("SHELL", "/bin/sh")
    arguments = list(command) if command else [executable]

    _validate_executable(executable, env)
    _validate_kcat_arguments(arguments)
    kcat_properties = _kcat_properties(profile)

    session_id = secrets.token_hex(16)
    with tempfile.TemporaryDirectory(prefix=f"kantrip-{session_id}-") as directory:
        session_directory = Path(directory)
        config_path = session_directory / "kcat.conf"
        _write_private_file(config_path, _render_properties(kcat_properties))

        child_environment = env | {
            "KAFKA_BOOTSTRAP_SERVERS": kcat_properties["bootstrap.servers"],
            "KAFKA_LIBRDKAFKA_CONFIG_FILE": str(config_path),
            "KAFKA_SECURITY_PROTOCOL": kcat_properties["security.protocol"],
            "KANTRIP_PROFILE": profile_name,
            "KANTRIP_SESSION_DIR": str(session_directory),
            "KANTRIP_SESSION_ID": session_id,
            "KCAT_CONFIG": str(config_path),
        }
        try:
            result = subprocess.run(arguments, env=child_environment, check=False)
        except OSError as error:
            raise SessionError(f"command could not be started: {executable}") from error
        return result.returncode


def _validate_executable(executable: str, environment: Mapping[str, str]) -> None:
    path = environment.get("PATH")
    if shutil.which(executable, path=path) is None:
        if Path(executable).name in {"kcat", "kafkacat"}:
            raise SessionError(
                "command 'kcat' was not found; install it with 'brew install kcat' "
                "on macOS or your Linux package manager"
            )
        raise SessionError(f"command '{executable}' was not found")


def _validate_kcat_arguments(arguments: Sequence[str]) -> None:
    if Path(arguments[0]).name not in {"kcat", "kafkacat"}:
        return
    if any(argument == "-F" or argument.startswith("-F") for argument in arguments[1:]):
        raise SessionError("kcat's -F option cannot override the selected Kantrip profile")


def _kcat_properties(profile: Mapping[str, Any]) -> dict[str, str]:
    kafka = profile["kafka"]
    if kafka["transport"] != "plaintext" or kafka["auth"]["type"] != "none":
        raise SessionError("only plaintext profiles without authentication are supported")

    configured = kafka.get("properties", {})
    properties = {
        str(key): _property_value(value)
        for group in (configured.get("common", {}), configured.get("librdkafka", {}))
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


__all__ = ["SessionError", "run_profile_session"]
