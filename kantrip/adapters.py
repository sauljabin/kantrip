"""Prepare supported external Kafka commands for profile sessions."""

from __future__ import annotations

import os
import shlex
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TextIO

KASKADE_EXECUTABLES = frozenset({"kaskade"})
KAFKA_CONSOLE_CONSUMER_EXECUTABLES = frozenset(
    {"kafka-console-consumer", "kafka-console-consumer.sh"}
)
KAFKA_CONSOLE_PRODUCER_EXECUTABLES = frozenset(
    {"kafka-console-producer", "kafka-console-producer.sh"}
)
KAFKA_TOPICS_EXECUTABLES = frozenset({"kafka-topics", "kafka-topics.sh"})
KAFKA_CONSUMER_GROUPS_EXECUTABLES = frozenset({"kafka-consumer-groups", "kafka-consumer-groups.sh"})
KAFKA_CONFIGS_EXECUTABLES = frozenset({"kafka-configs", "kafka-configs.sh"})
KAFKA_ACLS_EXECUTABLES = frozenset({"kafka-acls", "kafka-acls.sh"})
KAFKA_BROKER_API_VERSIONS_EXECUTABLES = frozenset(
    {"kafka-broker-api-versions", "kafka-broker-api-versions.sh"}
)
KAFKA_EXECUTABLE_OPTIONS = {
    **{
        executable: ("--bootstrap-server", "--consumer.config")
        for executable in KAFKA_CONSOLE_CONSUMER_EXECUTABLES
    },
    **{
        executable: ("--bootstrap-server", "--producer.config")
        for executable in KAFKA_CONSOLE_PRODUCER_EXECUTABLES
    },
    **{
        executable: ("--bootstrap-server", "--command-config")
        for executable in (
            KAFKA_TOPICS_EXECUTABLES
            | KAFKA_CONSUMER_GROUPS_EXECUTABLES
            | KAFKA_CONFIGS_EXECUTABLES
            | KAFKA_ACLS_EXECUTABLES
            | KAFKA_BROKER_API_VERSIONS_EXECUTABLES
        )
    },
}
KAFKA_EXECUTABLES = frozenset(KAFKA_EXECUTABLE_OPTIONS)
_KASKADE_COMMANDS = frozenset({"admin", "consumer"})
_KASKADE_CONNECTION_OPTIONS = (
    "--bootstrap-servers",
    "--config-file",
    "--kafka",
    "-b",
)
_KAFKA_ALTERNATE_CONNECTION_OPTIONS = {
    **{executable: ("--broker-list",) for executable in KAFKA_CONSOLE_PRODUCER_EXECUTABLES},
    **{
        executable: ("--zookeeper",)
        for executable in KAFKA_TOPICS_EXECUTABLES | KAFKA_CONSUMER_GROUPS_EXECUTABLES
    },
    **{
        executable: ("--authorizer", "--authorizer-properties", "--bootstrap-controller")
        for executable in KAFKA_ACLS_EXECUTABLES
    },
    **{
        executable: ("--zookeeper", "--bootstrap-controller")
        for executable in KAFKA_CONFIGS_EXECUTABLES
    },
}


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
    kafka_options = KAFKA_EXECUTABLE_OPTIONS.get(executable)
    if kafka_options is not None:
        bootstrap_option, config_option = kafka_options
        _reject_kafka_overrides(executable, prepared[1:], kafka_options)
        return [
            prepared[0],
            bootstrap_option,
            bootstrap_servers,
            config_option,
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
    kafka_executables = {
        name: resolved
        for name in sorted(KAFKA_EXECUTABLES)
        if (resolved := shutil.which(name, path=search_path)) is not None
    }
    kaskade_executable = shutil.which("kaskade", path=search_path)
    if not kafka_executables and kaskade_executable is None:
        return None

    directory.mkdir(mode=0o700)
    for name, executable in kafka_executables.items():
        bootstrap_option, config_option = KAFKA_EXECUTABLE_OPTIONS[name]
        contents = _render_kafka_shim(
            name,
            executable,
            bootstrap_servers=bootstrap_servers,
            java_config_path=java_config_path,
            bootstrap_option=bootstrap_option,
            config_option=config_option,
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


def _reject_kafka_overrides(
    executable: str,
    arguments: Sequence[str],
    injected_options: Sequence[str],
) -> None:
    connection_options = (
        *injected_options,
        *_KAFKA_ALTERNATE_CONNECTION_OPTIONS.get(executable, ()),
    )
    for argument in arguments:
        for option in connection_options:
            if argument == option or argument.startswith(f"{option}="):
                raise AdapterError(
                    f"{executable} option '{option}' cannot override the selected Kantrip profile"
                )


def _render_kafka_shim(
    name: str,
    executable: str,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    bootstrap_option: str,
    config_option: str,
) -> str:
    rejected_options = (
        bootstrap_option,
        config_option,
        *_KAFKA_ALTERNATE_CONNECTION_OPTIONS.get(name, ()),
    )
    rejected_patterns = "|".join(
        pattern for option in rejected_options for pattern in (option, f"{option}=*")
    )
    return f"""#!/bin/sh
for argument in "$@"; do
  case "$argument" in
    {rejected_patterns})
      printf '%s\\n' '{name} connection options cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
done
exec {shlex.quote(executable)} {bootstrap_option} {shlex.quote(bootstrap_servers)} {config_option} {shlex.quote(str(java_config_path))} "$@"
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
    "KAFKA_ACLS_EXECUTABLES",
    "KAFKA_BROKER_API_VERSIONS_EXECUTABLES",
    "KAFKA_CONFIGS_EXECUTABLES",
    "KAFKA_CONSOLE_CONSUMER_EXECUTABLES",
    "KAFKA_CONSOLE_PRODUCER_EXECUTABLES",
    "KAFKA_CONSUMER_GROUPS_EXECUTABLES",
    "KAFKA_EXECUTABLES",
    "KAFKA_EXECUTABLE_OPTIONS",
    "KAFKA_TOPICS_EXECUTABLES",
    "KASKADE_EXECUTABLES",
    "AdapterError",
    "create_subshell_shims",
    "prepare_command",
]
