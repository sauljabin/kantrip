"""Apply each supported client's argument policy and inject its profile configuration.

Direct commands and shell-shim guards share these rules. Every client keeps its
native argument grammar in an explicit function; the descriptors in
`kantrip.adapters` only select which function applies.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

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


class AdapterError(ValueError):
    """Raised when command arguments conflict with a selected profile."""


@dataclass(frozen=True)
class ClientConfiguration:
    """Private session values and files that adapters inject into native clients."""

    bootstrap_servers: str
    java_config: Path
    kcat_config: Path
    kaskade_config: Path
    kaskade_registry_config: Path
    schema_registry_java_config: Path | None = None
    registry_oauth_ssl_cert_file: Path | None = None


@dataclass(frozen=True)
class RegistryConsoleOptions:
    """Options through which a Confluent Registry console receives its Registry."""

    config_option: str
    property_option: str


@dataclass(frozen=True)
class JavaCommand:
    """How one Apache/Confluent Java tool receives the profile and what it rejects."""

    config_option: str
    alternate_connection_options: tuple[str, ...] = ()
    group_id_property_options: frozenset[str] = frozenset()
    registry_console: RegistryConsoleOptions | None = None
    bootstrap_option: str = "--bootstrap-server"

    @property
    def connection_options(self) -> tuple[str, ...]:
        """Profile-owned options, in the order the argument policy reports them."""
        registry_config = (
            (self.registry_console.config_option,) if self.registry_console is not None else ()
        )
        return (
            self.bootstrap_option,
            self.config_option,
            *registry_config,
            *self.alternate_connection_options,
        )


_CONSOLE_CONSUMER = JavaCommand(
    "--consumer.config",
    ("--command-config",),
    group_id_property_options=frozenset({"--command-property", "--consumer-property"}),
)
_CONSOLE_PRODUCER = JavaCommand("--producer.config", ("--command-config", "--broker-list"))
JAVA_COMMANDS: Mapping[str, JavaCommand] = {
    **dict.fromkeys(KAFKA_CONSOLE_CONSUMER_EXECUTABLES, _CONSOLE_CONSUMER),
    **dict.fromkeys(KAFKA_CONSOLE_PRODUCER_EXECUTABLES, _CONSOLE_PRODUCER),
    **dict.fromkeys(
        SCHEMA_REGISTRY_CONSUMER_EXECUTABLES,
        replace(
            _CONSOLE_CONSUMER,
            registry_console=RegistryConsoleOptions("--formatter-config", "--formatter-property"),
        ),
    ),
    **dict.fromkeys(
        SCHEMA_REGISTRY_PRODUCER_EXECUTABLES,
        replace(
            _CONSOLE_PRODUCER,
            registry_console=RegistryConsoleOptions("--reader-config", "--reader-property"),
        ),
    ),
    **dict.fromkeys(
        KAFKA_TOPICS_EXECUTABLES | KAFKA_CONSUMER_GROUPS_EXECUTABLES,
        JavaCommand("--command-config", ("--zookeeper",)),
    ),
    **dict.fromkeys(
        KAFKA_CONFIGS_EXECUTABLES,
        JavaCommand("--command-config", ("--zookeeper", "--bootstrap-controller")),
    ),
    **dict.fromkeys(
        KAFKA_ACLS_EXECUTABLES,
        JavaCommand(
            "--command-config",
            ("--authorizer", "--authorizer-properties", "--bootstrap-controller"),
        ),
    ),
    **dict.fromkeys(KAFKA_BROKER_API_VERSIONS_EXECUTABLES, JavaCommand("--command-config")),
}
KAFKA_EXECUTABLES = frozenset(JAVA_COMMANDS)

KCAT_SCHEMA_REGISTRY_SIGNAL = "schema-registry"
_KCAT_CONNECTION_OPTIONS = frozenset({"-F", "-r", "-b"})
_KCAT_FLAG_OPTIONS = frozenset("CPLQqvEVhlTZeJOuU")
_KCAT_VALUE_OPTIONS = frozenset("GtpbDKcmFXdzkHofsr")
_KASKADE_COMMANDS = frozenset({"admin", "consumer"})
_KASKADE_CONNECTION_OPTIONS = (
    "--bootstrap-servers",
    "--config-file",
    "--registry",
    "-b",
)
_SAFE_RUNTIME_KAFKA_PROPERTIES = frozenset({"group.id", "broker.address.family"})
_KAFKA_CLIENT_PROPERTY_OPTIONS = frozenset(
    {"--command-property", "--consumer-property", "--producer-property"}
)
_KAFKA_FORMAT_PROPERTY_OPTIONS = frozenset(
    {"--property", "--formatter-property", "--reader-property"}
)
_PROFILE_PROPERTY_PREFIXES = (
    "sasl.",
    "ssl.",
    "https.",
    "schema.registry.",
    "basic.auth.",
    "bearer.auth.",
    "apicurio.registry.",
)
_PROFILE_PROPERTY_NAMES = frozenset(
    {"bootstrap.servers", "security.protocol", "broker.list", "metadata.broker.list"}
)


def check_java_arguments(executable: str, arguments: Sequence[str]) -> str | None:
    """Reject Java tool arguments that would replace the profile connection."""
    _reject_java_overrides(executable, arguments, JAVA_COMMANDS[executable])
    return None


def check_kcat_arguments(executable: str, arguments: Sequence[str]) -> str | None:
    """Reject kcat overrides and signal when its Avro deserializer needs the Registry."""
    options = _reject_kcat_overrides(executable, arguments)
    return KCAT_SCHEMA_REGISTRY_SIGNAL if _kcat_uses_schema_registry(options) else None


def check_kaskade_arguments(executable: str, arguments: Sequence[str]) -> str | None:
    """Reject Kaskade overrides in the arguments that follow `admin` or `consumer`."""
    del executable
    _reject_kaskade_overrides(arguments)
    return None


def prepare_java_command(
    prepared: list[str],
    configuration: ClientConfiguration,
    registry: RegistryConnection | None,
) -> list[str]:
    """Inject the bootstrap servers and private Java configuration file."""
    executable = Path(prepared[0]).name
    command = JAVA_COMMANDS[executable]
    _reject_java_overrides(executable, prepared[1:], command)
    selected_config_path = configuration.java_config
    registry_arguments: tuple[str, ...] = ()
    if command.registry_console is not None:
        connection = require_registry_console_registry(executable, registry)
        if configuration.schema_registry_java_config is None:
            raise AdapterError(
                f"{executable} requires a private Schema Registry client configuration"
            )
        selected_config_path = configuration.schema_registry_java_config
        registry_arguments = (
            command.registry_console.config_option,
            str(selected_config_path),
            command.registry_console.property_option,
            f"schema.registry.url={connection.url}",
        )
    return [
        prepared[0],
        command.bootstrap_option,
        configuration.bootstrap_servers,
        command.config_option,
        str(selected_config_path),
        *registry_arguments,
        *prepared[1:],
    ]


def prepare_kcat_command(
    prepared: list[str],
    configuration: ClientConfiguration,
    registry: RegistryConnection | None,
) -> list[str]:
    """Pass the Registry URL to kcat's Avro deserializer; `KCAT_CONFIG` carries the rest."""
    del configuration
    executable = Path(prepared[0]).name
    options = _reject_kcat_overrides(executable, prepared[1:])
    if _kcat_uses_schema_registry(options):
        connection = require_kcat_registry(executable, registry)
        return [prepared[0], "-r", connection.url, *prepared[1:]]
    return prepared


def prepare_kaskade_command(
    prepared: list[str],
    configuration: ClientConfiguration,
    registry: RegistryConnection | None,
) -> list[str]:
    """Inject the private Kaskade INI file after the `admin` or `consumer` command."""
    if len(prepared) <= 1 or prepared[1] not in _KASKADE_COMMANDS:
        return prepared
    command = prepared[1]
    _reject_kaskade_overrides(prepared[2:])
    selected_config_path = configuration.kaskade_config
    if command == "consumer" and _kaskade_uses_schema_registry(prepared[2:]):
        require_kaskade_registry(registry)
        selected_config_path = configuration.kaskade_registry_config
    return [
        prepared[0],
        command,
        "--config-file",
        str(selected_config_path),
        *prepared[2:],
    ]


def require_registry_console_registry(
    name: str, registry: RegistryConnection | None
) -> RegistryConnection:
    """Require a Registry that Confluent's Java console serializers can map safely."""
    connection = _require_confluent_registry(name, registry)
    if connection.auth_type == "oauth" and connection.oauth is not None:
        if connection.oauth_logical_cluster is None:
            raise AdapterError(
                f"{name} Confluent Registry OAuth requires a logical cluster identifier"
            )
        if (
            connection.oauth.ca_certificates is not None
            and connection.oauth.ca_certificates != connection.ca_certificates
        ):
            raise AdapterError(
                f"{name} Confluent Registry OAuth uses one ssl.* trust configuration for "
                "Registry and token endpoint; independent CA bundles are not supported"
            )
    return connection


def require_kcat_registry(name: str, registry: RegistryConnection | None) -> RegistryConnection:
    """Require an unauthenticated Confluent-compatible Registry for kcat's `-r`."""
    connection = _require_confluent_registry(name, registry)
    if connection.auth_type != "none":
        raise AdapterError(
            f"{name} Registry decoding does not expose Kantrip's safe "
            f"'{connection.auth_type}' credential mapping"
        )
    return connection


def require_kaskade_registry(registry: RegistryConnection | None) -> RegistryConnection:
    """Require a Registry whose authentication Kaskade's deserializers can map safely."""
    connection = _require_registry("kaskade", registry)
    if connection.auth_type == "token":
        raise AdapterError("kaskade Registry decoding does not support fixed bearer tokens")
    if connection.auth_type != "oauth" or connection.oauth is None:
        return connection
    if connection.provider == CONFLUENT_PROVIDER and connection.oauth_logical_cluster is None:
        raise AdapterError("kaskade Confluent Registry OAuth requires a logical cluster identifier")
    if (
        connection.provider == "apicurio"
        and connection.oauth.ca_certificates is not None
        and connection.oauth.ca_certificates != connection.ca_certificates
    ):
        raise AdapterError(
            "kaskade Apicurio OAuth uses apicurio.registry.tls.certificates for Registry "
            "and token endpoint; independent CA bundles are not supported"
        )
    return connection


def _reject_kaskade_overrides(arguments: Sequence[str]) -> None:
    for index, argument in enumerate(arguments):
        if argument == "--kafka" or argument.startswith("--kafka="):
            property_value = (
                arguments[index + 1]
                if argument == "--kafka" and index + 1 < len(arguments)
                else argument.removeprefix("--kafka=")
            )
            if not _safe_runtime_kafka_property(property_value):
                raise AdapterError(
                    "kaskade option '--kafka' cannot override the selected Kantrip profile"
                )
            continue
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


def _safe_runtime_kafka_property(value: str) -> bool:
    name, separator, setting = value.partition("=")
    if not separator or not setting or name not in _SAFE_RUNTIME_KAFKA_PROPERTIES:
        return False
    return name != "broker.address.family" or setting in {"v4", "v6", "any"}


def _reject_kcat_overrides(
    executable: str, arguments: Sequence[str]
) -> tuple[tuple[str, str | None], ...]:
    options = _kcat_options(executable, arguments)
    for option, value in options:
        if option in _KCAT_CONNECTION_OPTIONS:
            raise AdapterError(
                f"{executable} option '{option}' cannot override the selected Kantrip profile"
            )
        if option == "-X" and (value is None or not _safe_runtime_kafka_property(value)):
            raise AdapterError(
                f"{executable} option '-X' cannot override the selected Kantrip profile"
            )
    return options


def _kcat_options(executable: str, arguments: Sequence[str]) -> tuple[tuple[str, str | None], ...]:
    """Parse short getopt clusters and consume option values before scanning again."""
    options: list[tuple[str, str | None]] = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == "--":
            break
        if not argument.startswith("-") or argument == "-":
            index += 1
            continue
        if argument.startswith("--"):
            raise AdapterError(f"{executable} option '{argument}' is not supported by Kantrip")
        for position, letter in enumerate(argument[1:], start=2):
            option = f"-{letter}"
            if letter in _KCAT_FLAG_OPTIONS:
                options.append((option, None))
                continue
            if letter not in _KCAT_VALUE_OPTIONS:
                raise AdapterError(f"{executable} option '{option}' is not supported by Kantrip")
            value = argument[position:]
            if not value:
                index += 1
                if index >= len(arguments):
                    raise AdapterError(f"{executable} option '{option}' requires a value")
                value = arguments[index]
            options.append((option, value))
            break
        index += 1
    return tuple(options)


def _kcat_uses_schema_registry(options: Sequence[tuple[str, str | None]]) -> bool:
    return any(
        option == "-s" and value is not None and value.lower() in {"avro", "key=avro", "value=avro"}
        for option, value in options
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


def _reject_java_overrides(
    executable: str,
    arguments: Sequence[str],
    command: JavaCommand,
) -> None:
    connection_options = command.connection_options
    for index, argument in enumerate(arguments):
        for option in connection_options:
            if argument == option or argument.startswith(f"{option}="):
                raise AdapterError(
                    f"{executable} option '{option}' cannot override the selected Kantrip profile"
                )
        matching = next(
            (
                option
                for option in _KAFKA_CLIENT_PROPERTY_OPTIONS | _KAFKA_FORMAT_PROPERTY_OPTIONS
                if argument == option or argument.startswith(f"{option}=")
            ),
            None,
        )
        if matching is None:
            continue
        value = (
            arguments[index + 1]
            if argument == matching and index + 1 < len(arguments)
            else argument[len(matching) + 1 :] if argument.startswith(f"{matching}=") else ""
        )
        name, separator, setting = value.partition("=")
        name = name.strip()
        if not separator or not name or not setting:
            raise AdapterError(f"{executable} option '{matching}' requires name=value")
        if matching in _KAFKA_CLIENT_PROPERTY_OPTIONS:
            safe_group = matching in command.group_id_property_options and name == "group.id"
            if not safe_group:
                raise AdapterError(
                    f"{executable} option '{matching}' cannot override the selected Kantrip profile"
                )
        elif _profile_owned_property(name):
            raise AdapterError(
                f"{executable} property '{name}' cannot override the selected Kantrip profile"
            )


def _profile_owned_property(name: str) -> bool:
    return name in _PROFILE_PROPERTY_NAMES or name.startswith(_PROFILE_PROPERTY_PREFIXES)


__all__ = [
    "JAVA_COMMANDS",
    "KAFKA_ACLS_EXECUTABLES",
    "KAFKA_BROKER_API_VERSIONS_EXECUTABLES",
    "KAFKA_CONFIGS_EXECUTABLES",
    "KAFKA_CONSOLE_CONSUMER_EXECUTABLES",
    "KAFKA_CONSOLE_PRODUCER_EXECUTABLES",
    "KAFKA_CONSUMER_GROUPS_EXECUTABLES",
    "KAFKA_EXECUTABLES",
    "KAFKA_TOPICS_EXECUTABLES",
    "KASKADE_EXECUTABLES",
    "KCAT_EXECUTABLES",
    "KCAT_SCHEMA_REGISTRY_SIGNAL",
    "SCHEMA_REGISTRY_CONSUMER_EXECUTABLES",
    "SCHEMA_REGISTRY_EXECUTABLES",
    "SCHEMA_REGISTRY_PRODUCER_EXECUTABLES",
    "AdapterError",
    "ClientConfiguration",
    "JavaCommand",
    "RegistryConsoleOptions",
    "check_java_arguments",
    "check_kaskade_arguments",
    "check_kcat_arguments",
    "prepare_java_command",
    "prepare_kaskade_command",
    "prepare_kcat_command",
    "require_kaskade_registry",
    "require_kcat_registry",
    "require_registry_console_registry",
]
