"""Render the POSIX shims that route interactive-shell clients through their adapters."""

from __future__ import annotations

import shlex
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from kantrip._files import write_exclusive_text
from kantrip.adapter_policy import (
    JAVA_COMMANDS,
    KCAT_SCHEMA_REGISTRY_SIGNAL,
    AdapterError,
    ClientConfiguration,
    require_kaskade_registry,
    require_kcat_registry,
    require_registry_console_registry,
)
from kantrip.registry import CONFLUENT_PROVIDER, RegistryConnection


@dataclass(frozen=True)
class ShimInputs:
    """Session values one shim embeds, plus its client's capability decision."""

    configuration: ClientConfiguration
    registry: RegistryConnection | None
    kafka_auth_type: str
    capability_error: str | None = None


def render_java_shim(name: str, executable: str, inputs: ShimInputs) -> str:
    """Render a shim that injects the bootstrap servers and private Java configuration."""
    command = JAVA_COMMANDS[name]
    configuration = inputs.configuration
    registry = inputs.registry
    java_config_path = configuration.java_config
    if (
        command.registry_console is not None
        and configuration.schema_registry_java_config is not None
    ):
        java_config_path = configuration.schema_registry_java_config
    registry_guard = ""
    registry_arguments = ""
    auxiliary_arguments = ""
    if command.registry_console is not None:
        registry_guard = _registry_guard(lambda: require_registry_console_registry(name, registry))
        if registry is not None and registry.provider == CONFLUENT_PROVIDER:
            registry_arguments = (
                f" {command.registry_console.property_option} "
                f"{shlex.quote(f'schema.registry.url={registry.url}')}"
            )
        auxiliary_arguments = (
            f" {command.registry_console.config_option} {shlex.quote(str(java_config_path))}"
        )
    capability_guard = ""
    if inputs.capability_error is not None:
        capability_guard = _failure(inputs.capability_error)
    java_environment = "unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS"
    if not _preserves_kafka_opts(inputs):
        java_environment = f"unset KAFKA_OPTS\n{java_environment}"
    if not (
        command.registry_console is not None
        and registry is not None
        and registry.auth_type == "oauth"
    ):
        java_environment = f"unset SCHEMA_REGISTRY_OPTS\n{java_environment}"
    return f"""#!/bin/sh
{java_environment}
{registry_guard}{capability_guard}{_adapter_guard_command(name)}
exec {shlex.quote(executable)} {command.bootstrap_option} {shlex.quote(configuration.bootstrap_servers)} {command.config_option} {shlex.quote(str(java_config_path))}{auxiliary_arguments}{registry_arguments} "$@"
"""


def render_kaskade_shim(name: str, executable: str, inputs: ShimInputs) -> str:
    """Render a shim that selects Kaskade's private Kafka or Registry INI file."""
    configuration = inputs.configuration
    registry = inputs.registry
    registry_guard = _registry_guard(lambda: require_kaskade_registry(registry))
    if inputs.capability_error is not None:
        registry_guard += _failure(inputs.capability_error)
    oauth_tls_environment = ""
    if registry is not None and registry.auth_type == "oauth":
        oauth_tls_environment = "unset SSL_CERT_FILE SSL_CERT_DIR\n"
    oauth_ssl_cert_file = configuration.registry_oauth_ssl_cert_file
    if oauth_tls_environment and oauth_ssl_cert_file is not None:
        oauth_tls_environment += f"export SSL_CERT_FILE={shlex.quote(str(oauth_ssl_cert_file))}\n"
    return f"""#!/bin/sh
unset KAFKA_OPTS JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
case "${{1-}}" in
  admin|consumer)
    command="$1"
    shift
    {_adapter_guard_command(name)}
    registry_deserializer=
    previous_argument=
    for argument in "$@"; do
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
    selected_config={shlex.quote(str(configuration.kaskade_config))}
    if [ "$command" = consumer ] && [ -n "$registry_deserializer" ]; then
      {registry_guard}      {oauth_tls_environment}      selected_config={shlex.quote(str(configuration.kaskade_registry_config))}
    fi
    exec {shlex.quote(executable)} "$command" --config-file "$selected_config" "$@"
    ;;
  *)
    exec {shlex.quote(executable)} "$@"
    ;;
esac
"""


def render_kcat_shim(name: str, executable: str, inputs: ShimInputs) -> str:
    """Render a shim that exports `KCAT_CONFIG` and adds `-r` for Avro decoding."""
    registry = inputs.registry
    registry_guard = _registry_guard(lambda: require_kcat_registry(name, registry))
    registry_url = shlex.quote(registry.url if registry is not None else "")
    return f"""#!/bin/sh
export KCAT_CONFIG={shlex.quote(str(inputs.configuration.kcat_config))}
schema_deserializer="$({_adapter_guard_invocation(name)})" || exit $?
if [ "$schema_deserializer" = {KCAT_SCHEMA_REGISTRY_SIGNAL} ]; then
  {registry_guard}  exec {shlex.quote(executable)} -r {registry_url} "$@"
fi
exec {shlex.quote(executable)} "$@"
"""


def write_executable(path: Path, contents: str) -> None:
    """Write one private shim that only the session owner can read or execute."""
    write_exclusive_text(path, contents, mode=0o700)


def _preserves_kafka_opts(inputs: ShimInputs) -> bool:
    """Keep the session-owned OAuth URL allow-list that Kafka or Registry OAuth needs."""
    registry = inputs.registry
    return (
        inputs.kafka_auth_type == "oauth"
        or registry is not None
        and registry.provider == CONFLUENT_PROVIDER
        and registry.auth_type == "oauth"
    )


def _registry_guard(require: Callable[[], RegistryConnection]) -> str:
    try:
        require()
    except AdapterError as error:
        return _failure(str(error))
    return ""


def _failure(message: str) -> str:
    return f"printf '%s\\n' {shlex.quote(message)} >&2\nexit 2\n"


def _adapter_guard_command(name: str) -> str:
    return _adapter_guard_invocation(name) + " || exit $?"


def _adapter_guard_invocation(name: str) -> str:
    return (
        f"{shlex.quote(sys.executable)} -I -m kantrip._adapter_guard " f'{shlex.quote(name)} "$@"'
    )


__all__ = [
    "ShimInputs",
    "render_java_shim",
    "render_kaskade_shim",
    "render_kcat_shim",
    "write_executable",
]
