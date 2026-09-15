"""Prepare supported external Kafka commands for profile sessions."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path

from kantrip._files import write_exclusive_text
from kantrip.registry import CONFLUENT_PROVIDER, RegistryConnection

KASKADE_EXECUTABLES = frozenset({"kaskade"})
KCAT_EXECUTABLES = frozenset({"kcat", "kafkacat"})
KAFKA_CONSOLE_CONSUMER_EXECUTABLES = frozenset(
    {"kafka-console-consumer", "kafka-console-consumer.sh"}
)
KAFKA_CONSOLE_PRODUCER_EXECUTABLES = frozenset(
    {"kafka-console-producer", "kafka-console-producer.sh"}
)
SCHEMA_REGISTRY_CONSUMER_EXECUTABLES = frozenset(
    {
        "kafka-avro-console-consumer",
        "kafka-json-schema-console-consumer",
        "kafka-protobuf-console-consumer",
    }
)
SCHEMA_REGISTRY_PRODUCER_EXECUTABLES = frozenset(
    {
        "kafka-avro-console-producer",
        "kafka-json-schema-console-producer",
        "kafka-protobuf-console-producer",
    }
)
SCHEMA_REGISTRY_EXECUTABLES = (
    SCHEMA_REGISTRY_CONSUMER_EXECUTABLES | SCHEMA_REGISTRY_PRODUCER_EXECUTABLES
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
        for executable in KAFKA_CONSOLE_PRODUCER_EXECUTABLES | SCHEMA_REGISTRY_PRODUCER_EXECUTABLES
    },
    **{
        executable: ("--bootstrap-server", "--consumer.config")
        for executable in SCHEMA_REGISTRY_CONSUMER_EXECUTABLES
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
ADAPTER_EXECUTABLES = KAFKA_EXECUTABLES | KASKADE_EXECUTABLES | KCAT_EXECUTABLES
_KASKADE_COMMANDS = frozenset({"admin", "consumer"})
_KASKADE_CONNECTION_OPTIONS = (
    "--bootstrap-servers",
    "--config-file",
    "--kafka",
    "--registry",
    "-b",
)
_KAFKA_ALTERNATE_CONNECTION_OPTIONS = {
    **{
        executable: ("--broker-list",)
        for executable in KAFKA_CONSOLE_PRODUCER_EXECUTABLES | SCHEMA_REGISTRY_PRODUCER_EXECUTABLES
    },
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


_CLIENT_VERSION_PATTERN = re.compile(r"(?<!\d)(\d+)\.(\d+)(?:\.\d+)?")
_JAVA_PEM_VERSION_TIMEOUT_SECONDS = 5


def require_java_pem_support(
    executable: str,
    *,
    environment: Mapping[str, str],
) -> None:
    """Reject Java clients whose version cannot safely consume a PEM trust store."""
    resolved = shutil.which(executable, path=environment.get("PATH"))
    if resolved is None:
        raise AdapterError(f"command '{Path(executable).name}' was not found")
    try:
        result = subprocess.run(
            [resolved, "--version"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=_JAVA_PEM_VERSION_TIMEOUT_SECONDS,
            check=False,
            env=dict(environment),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise AdapterError(_java_pem_unknown_version_message(Path(executable).name)) from error

    version = _recognized_java_client_version(f"{result.stdout}\n{result.stderr}")
    if result.returncode != 0 or version is None:
        raise AdapterError(_java_pem_unknown_version_message(Path(executable).name))
    if not _java_client_supports_pem(version):
        rendered_version = ".".join(str(part) for part in version)
        raise AdapterError(
            f"{Path(executable).name} {rendered_version} does not support PEM trust stores; "
            "custom CA profiles require Apache Kafka 2.7+ or Confluent Platform 6.1+"
        )


def _recognized_java_client_version(output: str) -> tuple[int, int] | None:
    recognized: tuple[int, int] | None = None
    for match in _CLIENT_VERSION_PATTERN.finditer(output):
        version = int(match.group(1)), int(match.group(2))
        if version[0] in {2, 3, 4, 5, 6, 7, 8}:
            recognized = version
    return recognized


def _java_client_supports_pem(version: tuple[int, int]) -> bool:
    major, minor = version
    if major == 2:
        return minor >= 7
    if major in {3, 4}:
        return True
    if major == 6:
        return minor >= 1
    return major in {7, 8}


def _java_pem_unknown_version_message(executable: str) -> str:
    return (
        f"could not verify whether {executable} supports PEM trust stores; use system trust "
        "or install Apache Kafka 2.7+ or Confluent Platform 6.1+"
    )


def prepare_command(
    arguments: Sequence[str],
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    kaskade_config_path: Path,
    kaskade_registry_config_path: Path,
    registry: RegistryConnection | None = None,
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
        registry_arguments: list[str] = []
        if executable in SCHEMA_REGISTRY_EXECUTABLES:
            connection = _require_confluent_registry(executable, registry)
            registry_arguments = ["--property", f"schema.registry.url={connection.url}"]
        return [
            prepared[0],
            bootstrap_option,
            bootstrap_servers,
            config_option,
            str(java_config_path),
            *registry_arguments,
            *prepared[1:],
        ]
    if executable in KCAT_EXECUTABLES:
        return _prepare_kcat(prepared, registry)
    if executable in KASKADE_EXECUTABLES:
        return _prepare_kaskade(
            prepared,
            kaskade_config_path,
            kaskade_registry_config_path,
            registry,
        )
    return prepared


def _prepare_kcat(prepared: list[str], registry: RegistryConnection | None) -> list[str]:
    executable = Path(prepared[0]).name
    _reject_kcat_overrides(executable, prepared[1:])
    if _kcat_uses_schema_registry(prepared[1:]):
        connection = _require_confluent_registry(executable, registry)
        return [prepared[0], "-r", connection.url, *prepared[1:]]
    return prepared


def _prepare_kaskade(
    prepared: list[str],
    config_path: Path,
    registry_config_path: Path,
    registry: RegistryConnection | None,
) -> list[str]:
    if len(prepared) <= 1 or prepared[1] not in _KASKADE_COMMANDS:
        return prepared
    command = prepared[1]
    _reject_kaskade_overrides(prepared[2:])
    selected_config_path = config_path
    if command == "consumer" and _kaskade_uses_schema_registry(prepared[2:]):
        _require_registry("kaskade", registry)
        selected_config_path = registry_config_path
    return [
        prepared[0],
        command,
        "--config-file",
        str(selected_config_path),
        *prepared[2:],
    ]


def create_subshell_shims(
    directory: Path,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    kaskade_config_path: Path,
    kaskade_registry_config_path: Path,
    environment: Mapping[str, str],
    registry: RegistryConnection | None = None,
    require_java_pem: bool = False,
) -> Path:
    """Create session-owned shims for installed adapter executables."""
    search_path = environment.get("PATH", os.defpath)
    kafka_executables = {
        name: resolved
        for name in sorted(KAFKA_EXECUTABLES)
        if (resolved := shutil.which(name, path=search_path)) is not None
    }
    kaskade_executable = shutil.which("kaskade", path=search_path)
    kcat_executables = {
        name: resolved
        for name in sorted(KCAT_EXECUTABLES)
        if (resolved := shutil.which(name, path=search_path)) is not None
    }
    java_pem_errors: dict[Path, str | None] = {}
    directory.mkdir(mode=0o700)
    for name, executable in kafka_executables.items():
        executable_directory = Path(executable).parent
        if require_java_pem and executable_directory not in java_pem_errors:
            try:
                require_java_pem_support(executable, environment=environment)
            except AdapterError as error:
                java_pem_errors[executable_directory] = str(error)
            else:
                java_pem_errors[executable_directory] = None
        bootstrap_option, config_option = KAFKA_EXECUTABLE_OPTIONS[name]
        contents = _render_kafka_shim(
            name,
            executable,
            bootstrap_servers=bootstrap_servers,
            java_config_path=java_config_path,
            bootstrap_option=bootstrap_option,
            config_option=config_option,
            registry=registry if name in SCHEMA_REGISTRY_EXECUTABLES else None,
            schema_registry_required=name in SCHEMA_REGISTRY_EXECUTABLES,
            capability_error=java_pem_errors.get(executable_directory),
        )
        _write_executable(directory / name, contents)
    if kaskade_executable is not None:
        _write_executable(
            directory / "kaskade",
            _render_kaskade_shim(
                kaskade_executable,
                kaskade_config_path,
                kaskade_registry_config_path,
                registry,
            ),
        )
    for name, executable in kcat_executables.items():
        _write_executable(
            directory / name,
            _render_kcat_shim(name, executable, registry),
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


def _reject_kcat_overrides(executable: str, arguments: Sequence[str]) -> None:
    for index, argument in enumerate(arguments):
        if argument in {"-F", "-r"} or argument.startswith(("-F", "-r")):
            option = argument[:2]
            raise AdapterError(
                f"{executable} option '{option}' cannot override the selected Kantrip profile"
            )
        property_value: str | None = None
        if argument == "-X" and index + 1 < len(arguments):
            property_value = arguments[index + 1]
        elif argument.startswith("-X"):
            property_value = argument[2:]
        if property_value is not None and property_value.startswith("schema.registry.url="):
            raise AdapterError(
                f"{executable} Schema Registry URL cannot override the selected Kantrip profile"
            )


def _kcat_uses_schema_registry(arguments: Sequence[str]) -> bool:
    return any(
        value.lower() in {"avro", "key=avro", "value=avro"}
        for value in _option_values(arguments, "-s")
    )


def _kaskade_uses_schema_registry(arguments: Sequence[str]) -> bool:
    return any(
        value.lower() == "registry"
        for option in ("-k", "--key", "-v", "--value")
        for value in _option_values(arguments, option)
    )


def _option_values(arguments: Sequence[str], option: str) -> tuple[str, ...]:
    values: list[str] = []
    for index, argument in enumerate(arguments):
        if argument == option and index + 1 < len(arguments):
            values.append(arguments[index + 1])
        elif argument.startswith(f"{option}="):
            values.append(argument[len(option) + 1 :])
        elif len(option) == 2 and argument.startswith(option) and argument != option:
            values.append(argument[len(option) :])
    return tuple(values)


def _require_registry(name: str, registry: RegistryConnection | None) -> RegistryConnection:
    if registry is None:
        raise AdapterError(f"{name} requires a registry section in the selected Kantrip profile")
    return registry


def _require_confluent_registry(
    name: str, registry: RegistryConnection | None
) -> RegistryConnection:
    connection = _require_registry(name, registry)
    if connection.provider != CONFLUENT_PROVIDER:
        raise AdapterError(
            f"{name} supports only Confluent-compatible registry profiles; configure "
            "Apicurio's ccompat endpoint with provider confluent"
        )
    return connection


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
    if executable in SCHEMA_REGISTRY_EXECUTABLES:
        _reject_schema_registry_property_overrides(executable, arguments)


def _reject_schema_registry_property_overrides(executable: str, arguments: Sequence[str]) -> None:
    for index, argument in enumerate(arguments):
        if argument in {"--producer-property", "--consumer-property"} or argument.startswith(
            ("--producer-property=", "--consumer-property=")
        ):
            raise AdapterError(
                f"{executable} Kafka client properties cannot override the selected "
                "Kantrip profile"
            )
        property_value: str | None = None
        if argument.startswith("--property="):
            property_value = argument[len("--property=") :]
        elif argument == "--property" and index + 1 < len(arguments):
            property_value = arguments[index + 1]
        if property_value is not None and property_value.split("=", 1)[0] in {
            "bootstrap.servers",
            "schema.registry.url",
        }:
            raise AdapterError(
                f"{executable} property '{property_value.split('=', 1)[0]}' cannot override "
                "the selected Kantrip profile"
            )


def _render_kafka_shim(
    name: str,
    executable: str,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    bootstrap_option: str,
    config_option: str,
    registry: RegistryConnection | None,
    schema_registry_required: bool,
    capability_error: str | None,
) -> str:
    rejected_options = (
        bootstrap_option,
        config_option,
        *_KAFKA_ALTERNATE_CONNECTION_OPTIONS.get(name, ()),
    )
    rejected_patterns = "|".join(
        pattern for option in rejected_options for pattern in (option, f"{option}=*")
    )
    registry_guard = ""
    property_guard = ""
    registry_arguments = ""
    if schema_registry_required:
        registry_guard = _render_registry_shim_guard(name, registry, confluent_only=True)
        property_guard = f"""previous_argument=
for argument in "$@"; do
  if [ "$previous_argument" = '--property' ]; then
    case "$argument" in
      bootstrap.servers=*|schema.registry.url=*)
        printf '%s\\n' '{name} properties cannot override the selected Kantrip profile' >&2
        exit 2
        ;;
    esac
  fi
  case "$argument" in
    --producer-property|--producer-property=*|--consumer-property|--consumer-property=*|--property=bootstrap.servers=*|--property=schema.registry.url=*)
      printf '%s\\n' '{name} properties cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
  previous_argument="$argument"
done
"""
        if registry is not None and registry.provider == CONFLUENT_PROVIDER:
            registry_arguments = " --property " + shlex.quote(f"schema.registry.url={registry.url}")
    capability_guard = ""
    if capability_error is not None:
        capability_guard = f"printf '%s\\n' {shlex.quote(capability_error)} >&2\n" "exit 2\n"
    return f"""#!/bin/sh
{registry_guard}{capability_guard}{property_guard}for argument in "$@"; do
  case "$argument" in
    {rejected_patterns})
      printf '%s\\n' '{name} connection options cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
done
exec {shlex.quote(executable)} {bootstrap_option} {shlex.quote(bootstrap_servers)} {config_option} {shlex.quote(str(java_config_path))}{registry_arguments} "$@"
"""


def _render_registry_shim_guard(
    name: str, registry: RegistryConnection | None, *, confluent_only: bool = False
) -> str:
    try:
        if confluent_only:
            _require_confluent_registry(name, registry)
        else:
            _require_registry(name, registry)
    except AdapterError as error:
        return f"printf '%s\\n' {shlex.quote(str(error))} >&2\nexit 2\n"
    return ""


def _render_kaskade_shim(
    executable: str,
    config_path: Path,
    registry_config_path: Path,
    registry: RegistryConnection | None,
) -> str:
    registry_guard = _render_registry_shim_guard("kaskade", registry)
    return f"""#!/bin/sh
case "${{1-}}" in
  admin|consumer)
    command="$1"
    shift
    registry_deserializer=
    previous_argument=
    for argument in "$@"; do
      case "$argument" in
        -b|-b*|--bootstrap-servers|--bootstrap-servers=*|--config-file|--config-file=*|--kafka|--kafka=*|--registry|--registry=*)
          printf '%s\\n' 'kaskade connection options cannot override the selected Kantrip profile' >&2
          exit 2
          ;;
      esac
      case "$previous_argument:$argument" in
        -k:[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|--key:[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|-v:[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|--value:[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy])
          registry_deserializer=1
          ;;
      esac
      case "$argument" in
        -k[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|-v[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|--key=[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy]|--value=[Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy])
          registry_deserializer=1
          ;;
      esac
      previous_argument="$argument"
    done
    selected_config={shlex.quote(str(config_path))}
    if [ "$command" = consumer ] && [ -n "$registry_deserializer" ]; then
      {registry_guard}      selected_config={shlex.quote(str(registry_config_path))}
    fi
    exec {shlex.quote(executable)} "$command" --config-file "$selected_config" "$@"
    ;;
  *)
    exec {shlex.quote(executable)} "$@"
    ;;
esac
"""


def _render_kcat_shim(
    name: str,
    executable: str,
    registry: RegistryConnection | None,
) -> str:
    registry_guard = _render_registry_shim_guard(name, registry, confluent_only=True)
    registry_url = shlex.quote(registry.url if registry is not None else "")
    return f"""#!/bin/sh
schema_deserializer=
previous_argument=
for argument in "$@"; do
  case "$argument" in
    -F|-F*|-r|-r*)
      printf '%s\\n' '{name} connection options cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
  case "$previous_argument:$argument" in
    -X:schema.registry.url=*)
      printf '%s\\n' '{name} Schema Registry URL cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
  case "$argument" in
    -Xschema.registry.url=*)
      printf '%s\\n' '{name} Schema Registry URL cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
  case "$previous_argument:$argument" in
    -s:avro|-s:key=avro|-s:value=avro)
      schema_deserializer=1
      ;;
  esac
  case "$argument" in
    -savro|-skey=avro|-svalue=avro)
      schema_deserializer=1
      ;;
  esac
  previous_argument="$argument"
done
if [ -n "$schema_deserializer" ]; then
  {registry_guard}  exec {shlex.quote(executable)} -r {registry_url} "$@"
fi
exec {shlex.quote(executable)} "$@"
"""


def _write_executable(path: Path, contents: str) -> None:
    write_exclusive_text(path, contents, mode=0o700)


__all__ = [
    "ADAPTER_EXECUTABLES",
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
    "KCAT_EXECUTABLES",
    "SCHEMA_REGISTRY_CONSUMER_EXECUTABLES",
    "SCHEMA_REGISTRY_EXECUTABLES",
    "SCHEMA_REGISTRY_PRODUCER_EXECUTABLES",
    "AdapterError",
    "create_subshell_shims",
    "prepare_command",
]
