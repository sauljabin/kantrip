"""Prepare supported external Kafka commands for profile sessions."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
    "--registry",
    "-b",
)
_KASKADE_SAFE_KAFKA_PROPERTIES = frozenset({"group.id", "broker.address.family"})
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


@dataclass(frozen=True)
class AdapterCapability:
    """One shared execution decision for a supported adapter family."""

    adapter: str
    executables: frozenset[str]
    kafka_authentication: frozenset[str]
    registry_providers: frozenset[str]
    pem_version_gate: bool = False
    oauth_version_gate: bool = False


_KAFKA_AUTHENTICATION = frozenset(
    {"none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth"}
)
ADAPTER_CAPABILITIES = (
    AdapterCapability(
        "Apache/Confluent Java CLI",
        KAFKA_EXECUTABLES,
        _KAFKA_AUTHENTICATION,
        frozenset({CONFLUENT_PROVIDER}),
        pem_version_gate=True,
        oauth_version_gate=True,
    ),
    AdapterCapability(
        "kcat",
        KCAT_EXECUTABLES,
        _KAFKA_AUTHENTICATION,
        frozenset({CONFLUENT_PROVIDER}),
    ),
    AdapterCapability(
        "Kaskade",
        KASKADE_EXECUTABLES,
        _KAFKA_AUTHENTICATION,
        frozenset({"confluent", "apicurio"}),
    ),
)


_CLIENT_VERSION_PATTERN = re.compile(r"(?<!\d)(\d+)\.(\d+)(?:\.\d+)?")
_KASKADE_VERSION_PATTERN = re.compile(
    r"\bkaskade,\s+version\s+(\d+)\.(\d+)\.(\d+)([^\s]*)",
    re.IGNORECASE,
)
_JAVA_PEM_VERSION_TIMEOUT_SECONDS = 5
_KASKADE_APICURIO_SECURITY_MIN_VERSION = (5, 0, 1)


def require_java_pem_support(
    executable: str,
    *,
    environment: Mapping[str, str],
) -> None:
    """Reject Java clients whose version cannot safely consume a PEM trust store."""
    version = _java_client_version(executable, environment, capability="PEM trust-store")
    if not _java_client_supports_pem(version):
        rendered_version = ".".join(str(part) for part in version)
        raise AdapterError(
            f"{Path(executable).name} {rendered_version} does not support PEM trust stores; "
            "custom CA profiles require Apache Kafka 2.7+ or Confluent Platform 6.1+"
        )


def require_java_oauth_support(
    executable: str,
    *,
    environment: Mapping[str, str],
) -> None:
    """Require the verified Apache Kafka 4.x native client-credentials callback."""
    version = _java_client_version(executable, environment, capability="OAuth")
    if version[0] < 4:
        rendered_version = ".".join(str(part) for part in version)
        raise AdapterError(
            f"{Path(executable).name} {rendered_version} does not support Kantrip's native "
            "OAuth mapping; install Apache Kafka 4.0+"
        )


def require_adapter_capability(
    executable: str,
    *,
    auth_type: str,
    custom_pem: bool,
    environment: Mapping[str, str],
    registry: RegistryConnection | None = None,
) -> None:
    """Apply the same mechanism and installed-version decision to direct clients."""
    name = Path(executable).name
    capability = next(
        (item for item in ADAPTER_CAPABILITIES if name in item.executables),
        None,
    )
    if capability is None:
        return
    if auth_type not in capability.kafka_authentication:
        raise AdapterError(
            f"{capability.adapter} does not support Kafka authentication '{auth_type}'"
        )
    if custom_pem and capability.pem_version_gate:
        require_java_pem_support(executable, environment=environment)
    if auth_type == "oauth" and capability.oauth_version_gate:
        require_java_oauth_support(executable, environment=environment)
    if name == "kaskade" and registry is not None:
        require_kaskade_apicurio_security_support(
            executable,
            registry,
            environment=environment,
        )


def require_kaskade_apicurio_security_support(
    executable: str,
    registry: RegistryConnection,
    *,
    environment: Mapping[str, str],
) -> None:
    """Gate native Apicurio OAuth scopes on their first stable Kaskade release."""
    if not _requires_new_kaskade_apicurio_security(registry):
        return
    version, suffix, rendered = _kaskade_client_version(executable, environment)
    if not suffix and version >= _KASKADE_APICURIO_SECURITY_MIN_VERSION:
        return
    raise AdapterError(
        f"kaskade {rendered} cannot map native Apicurio OAuth scopes; "
        "install Kaskade 5.0.1 or newer"
    )


def _requires_new_kaskade_apicurio_security(connection: RegistryConnection) -> bool:
    if connection.provider != "apicurio":
        return False
    if connection.auth_type != "oauth" or connection.oauth is None:
        return False
    return bool(connection.oauth.scopes)


def _kaskade_client_version(
    executable: str,
    environment: Mapping[str, str],
) -> tuple[tuple[int, int, int], str, str]:
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
        raise AdapterError("could not verify Kaskade Apicurio security support") from error
    match = _KASKADE_VERSION_PATTERN.search(f"{result.stdout}\n{result.stderr}")
    if result.returncode != 0 or match is None:
        raise AdapterError("could not verify Kaskade Apicurio security support")
    version = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    suffix = match.group(4)
    rendered = ".".join(str(part) for part in version) + suffix
    return version, suffix, rendered


def _recognized_java_client_version(output: str) -> tuple[int, int] | None:
    recognized: tuple[int, int] | None = None
    for match in _CLIENT_VERSION_PATTERN.finditer(output):
        version = int(match.group(1)), int(match.group(2))
        if version[0] in {2, 3, 4, 5, 6, 7, 8}:
            recognized = version
    return recognized


def _java_client_version(
    executable: str,
    environment: Mapping[str, str],
    *,
    capability: str,
) -> tuple[int, int]:
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
        raise AdapterError(
            f"could not verify {capability} support for {Path(executable).name}"
        ) from error
    version = _recognized_java_client_version(f"{result.stdout}\n{result.stderr}")
    if result.returncode != 0 or version is None:
        raise AdapterError(f"could not verify {capability} support for {Path(executable).name}")
    return version


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
        f"could not verify whether {executable} supports PEM trust stores; use default trust "
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
    schema_registry_java_config_path: Path | None = None,
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
        selected_config_path = java_config_path
        registry_url: str | None = None
        if executable in SCHEMA_REGISTRY_EXECUTABLES:
            connection = _require_confluent_registry(executable, registry)
            _require_registry_authentication(executable, connection)
            if schema_registry_java_config_path is None:
                raise AdapterError(
                    f"{executable} requires a private Schema Registry client configuration"
                )
            selected_config_path = schema_registry_java_config_path
            registry_url = connection.url
        auxiliary_config = _schema_registry_auxiliary_config_option(executable)
        auxiliary_property = _schema_registry_auxiliary_property_option(executable)
        return [
            prepared[0],
            bootstrap_option,
            bootstrap_servers,
            config_option,
            str(selected_config_path),
            *(
                (auxiliary_config, str(selected_config_path))
                if auxiliary_config is not None
                else ()
            ),
            *(
                (auxiliary_property, f"schema.registry.url={registry_url}")
                if auxiliary_property is not None and registry_url is not None
                else ()
            ),
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
        _require_registry_authentication(executable, connection)
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
        connection = _require_registry("kaskade", registry)
        _require_registry_authentication("kaskade", connection)
        selected_config_path = registry_config_path
    return [
        prepared[0],
        command,
        "--config-file",
        str(selected_config_path),
        *prepared[2:],
    ]


def create_subshell_shims(  # noqa: C901
    directory: Path,
    *,
    bootstrap_servers: str,
    java_config_path: Path,
    kcat_config_path: Path,
    kaskade_config_path: Path,
    kaskade_registry_config_path: Path,
    environment: Mapping[str, str],
    registry: RegistryConnection | None = None,
    require_java_pem: bool = False,
    kafka_auth_type: str = "none",
    schema_registry_java_config_path: Path | None = None,
    registry_oauth_ssl_cert_file: Path | None = None,
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
        if kafka_auth_type == "oauth" and executable_directory not in java_pem_errors:
            try:
                require_java_oauth_support(executable, environment=environment)
            except AdapterError as error:
                java_pem_errors[executable_directory] = str(error)
            else:
                java_pem_errors[executable_directory] = None
        bootstrap_option, config_option = KAFKA_EXECUTABLE_OPTIONS[name]
        selected_java_config = java_config_path
        if name in SCHEMA_REGISTRY_EXECUTABLES and schema_registry_java_config_path is not None:
            selected_java_config = schema_registry_java_config_path
        contents = _render_kafka_shim(
            name,
            executable,
            bootstrap_servers=bootstrap_servers,
            java_config_path=selected_java_config,
            bootstrap_option=bootstrap_option,
            config_option=config_option,
            registry=registry if name in SCHEMA_REGISTRY_EXECUTABLES else None,
            schema_registry_required=name in SCHEMA_REGISTRY_EXECUTABLES,
            capability_error=java_pem_errors.get(executable_directory),
            preserve_kafka_opts=(
                kafka_auth_type == "oauth"
                or registry is not None
                and registry.provider == CONFLUENT_PROVIDER
                and registry.auth_type == "oauth"
            ),
            preserve_schema_registry_opts=(
                name in SCHEMA_REGISTRY_EXECUTABLES
                and registry is not None
                and registry.auth_type == "oauth"
            ),
        )
        _write_executable(directory / name, contents)
    if kaskade_executable is not None:
        kaskade_capability_error: str | None = None
        if registry is not None:
            try:
                require_kaskade_apicurio_security_support(
                    kaskade_executable,
                    registry,
                    environment=environment,
                )
            except AdapterError as error:
                kaskade_capability_error = str(error)
        _write_executable(
            directory / "kaskade",
            _render_kaskade_shim(
                kaskade_executable,
                kaskade_config_path,
                kaskade_registry_config_path,
                registry,
                capability_error=kaskade_capability_error,
                oauth_ssl_cert_file=registry_oauth_ssl_cert_file,
            ),
        )
    for name, executable in kcat_executables.items():
        _write_executable(
            directory / name,
            _render_kcat_shim(name, executable, kcat_config_path, registry),
        )
    return directory


def _reject_kaskade_overrides(arguments: Sequence[str]) -> None:
    for index, argument in enumerate(arguments):
        if argument == "--kafka" or argument.startswith("--kafka="):
            property_value = (
                arguments[index + 1]
                if argument == "--kafka" and index + 1 < len(arguments)
                else argument.removeprefix("--kafka=")
            )
            if not _safe_kaskade_kafka_property(property_value):
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


def _safe_kaskade_kafka_property(value: str) -> bool:
    name, separator, setting = value.partition("=")
    if not separator or not setting or name not in _KASKADE_SAFE_KAFKA_PROPERTIES:
        return False
    return name != "broker.address.family" or setting in {"v4", "v6", "any"}


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


def _require_registry_authentication(name: str, connection: RegistryConnection) -> None:
    if name in KCAT_EXECUTABLES and connection.auth_type != "none":
        raise AdapterError(
            f"{name} Registry decoding does not expose Kantrip's safe "
            f"'{connection.auth_type}' credential mapping"
        )
    if name == "kaskade" and connection.auth_type == "token":
        raise AdapterError("kaskade Registry decoding does not support fixed bearer tokens")
    if connection.auth_type == "oauth" and connection.oauth is not None:
        _require_registry_oauth_mapping(name, connection)


def _require_registry_oauth_mapping(name: str, connection: RegistryConnection) -> None:
    assert connection.oauth is not None
    if (
        name in SCHEMA_REGISTRY_EXECUTABLES
        or name == "kaskade"
        and connection.provider == CONFLUENT_PROVIDER
    ) and connection.oauth_logical_cluster is None:
        raise AdapterError(f"{name} Confluent Registry OAuth requires a logical cluster identifier")
    if name in SCHEMA_REGISTRY_EXECUTABLES and (
        connection.oauth.ca_certificates is not None
        and connection.oauth.ca_certificates != connection.ca_certificates
    ):
        raise AdapterError(
            f"{name} Confluent Registry OAuth uses one ssl.* trust configuration for "
            "Registry and token endpoint; independent CA bundles are not supported"
        )
    if (
        name == "kaskade"
        and connection.provider == "apicurio"
        and connection.oauth.ca_certificates is not None
        and connection.oauth.ca_certificates != connection.ca_certificates
    ):
        raise AdapterError(
            "kaskade Apicurio OAuth uses apicurio.registry.tls.certificates for Registry "
            "and token endpoint; independent CA bundles are not supported"
        )


def _reject_kafka_overrides(
    executable: str,
    arguments: Sequence[str],
    injected_options: Sequence[str],
) -> None:
    connection_options = (
        *injected_options,
        *(
            (auxiliary_config,)
            if (auxiliary_config := _schema_registry_auxiliary_config_option(executable))
            is not None
            else ()
        ),
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
        if argument in {
            "--command-property",
            "--producer-property",
            "--consumer-property",
        } or argument.startswith(
            ("--command-property=", "--producer-property=", "--consumer-property=")
        ):
            raise AdapterError(
                f"{executable} Kafka client properties cannot override the selected "
                "Kantrip profile"
            )
        property_value: str | None = None
        property_options = ("--property", "--formatter-property", "--reader-property")
        matching_option = next(
            (option for option in property_options if argument.startswith(f"{option}=")),
            None,
        )
        if matching_option is not None:
            property_value = argument[len(matching_option) + 1 :]
        elif argument in property_options and index + 1 < len(arguments):
            property_value = arguments[index + 1]
        property_name = property_value.split("=", 1)[0] if property_value is not None else None
        if property_name == "bootstrap.servers" or (
            property_name is not None and property_name.startswith("schema.registry.")
        ):
            raise AdapterError(
                f"{executable} property '{property_name}' cannot override "
                "the selected Kantrip profile"
            )


def _schema_registry_auxiliary_config_option(executable: str) -> str | None:
    if executable in SCHEMA_REGISTRY_CONSUMER_EXECUTABLES:
        return "--formatter-config"
    if executable in SCHEMA_REGISTRY_PRODUCER_EXECUTABLES:
        return "--reader-config"
    return None


def _schema_registry_auxiliary_property_option(executable: str) -> str | None:
    if executable in SCHEMA_REGISTRY_CONSUMER_EXECUTABLES:
        return "--formatter-property"
    if executable in SCHEMA_REGISTRY_PRODUCER_EXECUTABLES:
        return "--reader-property"
    return None


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
    preserve_kafka_opts: bool,
    preserve_schema_registry_opts: bool,
) -> str:
    auxiliary_config = _schema_registry_auxiliary_config_option(name)
    rejected_options = (
        bootstrap_option,
        config_option,
        *((auxiliary_config,) if auxiliary_config is not None else ()),
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
  case "$previous_argument" in
    --property|--formatter-property|--reader-property)
    case "$argument" in
      bootstrap.servers=*|schema.registry.*)
        printf '%s\\n' '{name} properties cannot override the selected Kantrip profile' >&2
        exit 2
        ;;
    esac
    ;;
  esac
  case "$argument" in
    --command-property|--command-property=*|--producer-property|--producer-property=*|--consumer-property|--consumer-property=*|--property=bootstrap.servers=*|--property=schema.registry.*|--formatter-property=bootstrap.servers=*|--formatter-property=schema.registry.*|--reader-property=bootstrap.servers=*|--reader-property=schema.registry.*)
      printf '%s\\n' '{name} properties cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
  previous_argument="$argument"
done
"""
        if registry is not None and registry.provider == CONFLUENT_PROVIDER:
            auxiliary_property = _schema_registry_auxiliary_property_option(name)
            assert auxiliary_property is not None
            registry_arguments = (
                f" {auxiliary_property} " f"{shlex.quote(f'schema.registry.url={registry.url}')}"
            )
    capability_guard = ""
    if capability_error is not None:
        capability_guard = f"printf '%s\\n' {shlex.quote(capability_error)} >&2\n" "exit 2\n"
    java_environment = "unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS"
    if not preserve_kafka_opts:
        java_environment = f"unset KAFKA_OPTS\n{java_environment}"
    if not preserve_schema_registry_opts:
        java_environment = f"unset SCHEMA_REGISTRY_OPTS\n{java_environment}"
    auxiliary_arguments = (
        f" {auxiliary_config} {shlex.quote(str(java_config_path))}"
        if auxiliary_config is not None
        else ""
    )
    return f"""#!/bin/sh
{java_environment}
{registry_guard}{capability_guard}{property_guard}for argument in "$@"; do
  case "$argument" in
    {rejected_patterns})
      printf '%s\\n' '{name} connection options cannot override the selected Kantrip profile' >&2
      exit 2
      ;;
  esac
done
exec {shlex.quote(executable)} {bootstrap_option} {shlex.quote(bootstrap_servers)} {config_option} {shlex.quote(str(java_config_path))}{auxiliary_arguments}{registry_arguments} "$@"
"""


def _render_registry_shim_guard(
    name: str, registry: RegistryConnection | None, *, confluent_only: bool = False
) -> str:
    try:
        connection = (
            _require_confluent_registry(name, registry)
            if confluent_only
            else _require_registry(name, registry)
        )
        _require_registry_authentication(name, connection)
    except AdapterError as error:
        return f"printf '%s\\n' {shlex.quote(str(error))} >&2\nexit 2\n"
    return ""


def _render_kaskade_shim(
    executable: str,
    config_path: Path,
    registry_config_path: Path,
    registry: RegistryConnection | None,
    *,
    capability_error: str | None = None,
    oauth_ssl_cert_file: Path | None = None,
) -> str:
    registry_guard = _render_registry_shim_guard("kaskade", registry)
    if capability_error is not None:
        registry_guard += f"printf '%s\\n' {shlex.quote(capability_error)} >&2\n" "exit 2\n"
    oauth_tls_environment = ""
    if registry is not None and registry.auth_type == "oauth":
        oauth_tls_environment = "unset SSL_CERT_FILE SSL_CERT_DIR\n"
    if oauth_tls_environment and oauth_ssl_cert_file is not None:
        oauth_tls_environment += f"export SSL_CERT_FILE={shlex.quote(str(oauth_ssl_cert_file))}\n"
    return f"""#!/bin/sh
unset KAFKA_OPTS JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
case "${{1-}}" in
  admin|consumer)
    command="$1"
    shift
    registry_deserializer=
    previous_argument=
    for argument in "$@"; do
      if [ "$previous_argument" = --kafka ]; then
        case "$argument" in
          group.id=?*|broker.address.family=v4|broker.address.family=v6|broker.address.family=any) ;;
          *)
            printf '%s\\n' 'kaskade connection options cannot override the selected Kantrip profile' >&2
            exit 2
            ;;
        esac
      fi
      case "$argument" in
        --kafka|--kafka=group.id=?*|--kafka=broker.address.family=v4|--kafka=broker.address.family=v6|--kafka=broker.address.family=any)
          ;;
        -b|-b*|--bootstrap-servers|--bootstrap-servers=*|--config-file|--config-file=*|--kafka=*|--registry|--registry=*)
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
      {registry_guard}      {oauth_tls_environment}      selected_config={shlex.quote(str(registry_config_path))}
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
    config_path: Path,
    registry: RegistryConnection | None,
) -> str:
    registry_guard = _render_registry_shim_guard(name, registry, confluent_only=True)
    registry_url = shlex.quote(registry.url if registry is not None else "")
    return f"""#!/bin/sh
export KCAT_CONFIG={shlex.quote(str(config_path))}
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
    "ADAPTER_CAPABILITIES",
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
    "AdapterCapability",
    "AdapterError",
    "create_subshell_shims",
    "prepare_command",
    "require_adapter_capability",
    "require_java_oauth_support",
]
