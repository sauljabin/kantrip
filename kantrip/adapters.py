"""Prepare supported external Kafka commands for profile sessions."""

from __future__ import annotations

import os
import shlex
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

KASKADE_EXECUTABLES = frozenset({"kaskade"})
KAFKA_TOPICS_EXECUTABLES = frozenset({"kafka-topics", "kafka-topics.sh"})
_KASKADE_COMMANDS = frozenset({"admin", "consumer"})
_KASKADE_CONNECTION_OPTIONS = (
    "--bootstrap-servers",
    "--config-file",
    "--kafka",
    "-b",
)
_KAFKA_TOPICS_CONNECTION_OPTIONS = ("--bootstrap-server", "--command-config")


class AdapterError(ValueError):
    """Raised when command arguments conflict with a selected profile."""


def prepare_command(
    arguments: Sequence[str],
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    kaskade_config_path: Path,
) -> list[str]:
    """Inject profile connection options for a supported explicit command."""
    prepared = list(arguments)
    if not prepared:
        return prepared

    executable = Path(prepared[0]).name
    if executable in KAFKA_TOPICS_EXECUTABLES:
        _reject_kafka_topics_overrides(prepared[1:])
        return [
            prepared[0],
            "--bootstrap-server",
            bootstrap_servers,
            "--command-config",
            str(java_config_path),
            *prepared[1:],
        ]
    if executable in KASKADE_EXECUTABLES and len(prepared) > 1:
        command = prepared[1]
        if command in _KASKADE_COMMANDS:
            _reject_kaskade_overrides(prepared[2:])
            return [
                prepared[0],
                command,
                "--config-file",
                str(kaskade_config_path),
                *prepared[2:],
            ]
    return prepared


def create_subshell_shims(
    directory: Path,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    kaskade_config_path: Path,
    environment: Mapping[str, str],
) -> Path | None:
    """Create session-owned shims for installed adapter executables."""
    search_path = environment.get("PATH", os.defpath)
    kafka_topics_executables = {
        name: resolved
        for name in sorted(KAFKA_TOPICS_EXECUTABLES)
        if (resolved := shutil.which(name, path=search_path)) is not None
    }
    kaskade_executable = shutil.which("kaskade", path=search_path)
    if not kafka_topics_executables and kaskade_executable is None:
        return None

    directory.mkdir(mode=0o700)
    for name, executable in kafka_topics_executables.items():
        contents = _render_kafka_topics_shim(
            executable,
            bootstrap_servers=bootstrap_servers,
            java_config_path=java_config_path,
        )
        _write_executable(directory / name, contents)
    if kaskade_executable is not None:
        _write_executable(
            directory / "kaskade",
            _render_kaskade_shim(kaskade_executable, kaskade_config_path),
        )
    return directory


def _reject_kaskade_overrides(arguments: Sequence[str]) -> None:
    for argument in arguments:
        for option in _KASKADE_CONNECTION_OPTIONS:
            if (
                argument == option
                or argument.startswith(f"{option}=")
                or option == "-b"
                and argument.startswith(option)
            ):
                raise AdapterError(
                    f"kaskade option '{option}' cannot override the selected Kantrip profile"
                )


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


def _render_kaskade_shim(executable: str, config_path: Path) -> str:
    return f"""#!/bin/sh
case "${{1-}}" in
  admin|consumer)
    command="$1"
    shift
    for argument in "$@"; do
      case "$argument" in
        -b|-b*|--bootstrap-servers|--bootstrap-servers=*|--config-file|--config-file=*|--kafka|--kafka=*)
          printf '%s\\n' 'kaskade connection options cannot override the selected Kantrip profile' >&2
          exit 2
          ;;
      esac
    done
    exec {shlex.quote(executable)} "$command" --config-file {shlex.quote(str(config_path))} "$@"
    ;;
  *)
    exec {shlex.quote(executable)} "$@"
    ;;
esac
"""


def _write_executable(path: Path, contents: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
    stream: TextIO
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents)


__all__ = [
    "KAFKA_TOPICS_EXECUTABLES",
    "KASKADE_EXECUTABLES",
    "AdapterError",
    "create_subshell_shims",
    "prepare_command",
]
