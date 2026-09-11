"""Prepare supported external Kafka commands for profile sessions."""

from __future__ import annotations

import os
import shlex
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

KAFKA_TOPICS_EXECUTABLES = frozenset({"kafka-topics", "kafka-topics.sh"})
_KAFKA_TOPICS_CONNECTION_OPTIONS = ("--bootstrap-server", "--command-config")


class AdapterError(ValueError):
    """Raised when command arguments conflict with a selected profile."""


def prepare_command(
    arguments: Sequence[str],
    *,
    bootstrap_servers: str,
    java_config_path: Path,
) -> list[str]:
    """Inject profile connection options for a supported explicit command."""
    prepared = list(arguments)
    if not prepared or Path(prepared[0]).name not in KAFKA_TOPICS_EXECUTABLES:
        return prepared

    _reject_kafka_topics_overrides(prepared[1:])
    return [
        prepared[0],
        "--bootstrap-server",
        bootstrap_servers,
        "--command-config",
        str(java_config_path),
        *prepared[1:],
    ]


def create_subshell_shims(
    directory: Path,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    environment: Mapping[str, str],
) -> Path | None:
    """Create session-owned kafka-topics shims for installed executable names."""
    search_path = environment.get("PATH", os.defpath)
    executables = {
        name: resolved
        for name in sorted(KAFKA_TOPICS_EXECUTABLES)
        if (resolved := shutil.which(name, path=search_path)) is not None
    }
    if not executables:
        return None

    directory.mkdir(mode=0o700)
    for name, executable in executables.items():
        contents = _render_kafka_topics_shim(
            executable,
            bootstrap_servers=bootstrap_servers,
            java_config_path=java_config_path,
        )
        _write_executable(directory / name, contents)
    return directory


def _reject_kafka_topics_overrides(arguments: Sequence[str]) -> None:
    for argument in arguments:
        for option in _KAFKA_TOPICS_CONNECTION_OPTIONS:
            if argument == option or argument.startswith(f"{option}="):
                raise AdapterError(
                    f"kafka-topics option '{option}' cannot override the selected Kantrip profile"
                )


def _render_kafka_topics_shim(
    executable: str,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
) -> str:
    return f"""#!/bin/sh
for argument in "$@"; do
  case "$argument" in
    --bootstrap-server|--bootstrap-server=*|--command-config|--command-config=*)
      printf '%s\\n' 'kafka-topics connection options cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
done
exec {shlex.quote(executable)} --bootstrap-server {shlex.quote(bootstrap_servers)} --command-config {shlex.quote(str(java_config_path))} "$@"
"""


def _write_executable(path: Path, contents: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
    stream: TextIO
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents)


__all__ = [
    "KAFKA_TOPICS_EXECUTABLES",
    "AdapterError",
    "create_subshell_shims",
    "prepare_command",
]
