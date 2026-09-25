"""Describe supported client families and route their commands through adapters.

Each `ClientAdapter` records one family's executables, capability matrix, and
the explicit functions that guard its arguments, inject its configuration, and
render its shell shim. Callers dispatch through the descriptor instead of
branching on client names.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from kantrip.adapter_policy import (
    KAFKA_ACLS_EXECUTABLES,
    KAFKA_BROKER_API_VERSIONS_EXECUTABLES,
    KAFKA_CONFIGS_EXECUTABLES,
    KAFKA_CONSOLE_CONSUMER_EXECUTABLES,
    KAFKA_CONSOLE_PRODUCER_EXECUTABLES,
    KAFKA_CONSUMER_GROUPS_EXECUTABLES,
    KAFKA_EXECUTABLES,
    KAFKA_TOPICS_EXECUTABLES,
    KASKADE_EXECUTABLES,
    KCAT_EXECUTABLES,
    SCHEMA_REGISTRY_CONSUMER_EXECUTABLES,
    SCHEMA_REGISTRY_EXECUTABLES,
    SCHEMA_REGISTRY_PRODUCER_EXECUTABLES,
    AdapterError,
    ClientConfiguration,
    check_java_arguments,
    check_kaskade_arguments,
    check_kcat_arguments,
    prepare_java_command,
    prepare_kaskade_command,
    prepare_kcat_command,
)
from kantrip.adapter_shims import (
    ShimInputs,
    render_java_shim,
    render_kaskade_shim,
    render_kcat_shim,
    write_executable,
)
from kantrip.registry import APICURIO_PROVIDER, CONFLUENT_PROVIDER, RegistryConnection

ArgumentCheck = Callable[[str, Sequence[str]], str | None]
CommandPreparation = Callable[
    [list[str], ClientConfiguration, RegistryConnection | None], list[str]
]
ShimRendering = Callable[[str, str, ShimInputs], str]

_KAFKA_AUTHENTICATION = frozenset(
    {"none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth"}
)


@dataclass(frozen=True)
class ClientAdapter:
    """One supported client family and the explicit functions that serve it.

    `check_arguments` rejects profile-owned connection options for both direct
    commands and the shim guard, and may return a signal the shim acts on.
    `prepare_command` and `render_shim` inject the private configuration:
    options for Java tools and Kaskade, `KCAT_CONFIG` plus `-r` for kcat. The
    remaining fields are the family's capability matrix and version gates.
    """

    name: str
    executables: frozenset[str]
    check_arguments: ArgumentCheck
    prepare_command: CommandPreparation
    render_shim: ShimRendering
    missing_command: Callable[[str], str]
    kafka_authentication: frozenset[str] = _KAFKA_AUTHENTICATION
    registry_providers: frozenset[str] = frozenset({CONFLUENT_PROVIDER})
    pem_version_gate: bool = False
    oauth_version_gate: bool = False
    registry_version_gate: bool = False


def _missing_kcat(name: str) -> str:
    del name
    return (
        "command 'kcat' was not found; install it with 'brew install kcat' "
        "on macOS or your Linux package manager"
    )


def _missing_java_command(name: str) -> str:
    if name in SCHEMA_REGISTRY_EXECUTABLES:
        return (
            f"command '{name}' was not found; install the Confluent Schema "
            "Registry package and ensure its bin directory is on PATH"
        )
    return (
        f"command '{name}' was not found; install the Apache Kafka CLI "
        "and ensure its bin directory is on PATH"
    )


def _missing_kaskade(name: str) -> str:
    del name
    return "command 'kaskade' was not found; install it and ensure its executable is on PATH"


KCAT_ADAPTER = ClientAdapter(
    "kcat",
    KCAT_EXECUTABLES,
    check_arguments=check_kcat_arguments,
    prepare_command=prepare_kcat_command,
    render_shim=render_kcat_shim,
    missing_command=_missing_kcat,
)
KASKADE_ADAPTER = ClientAdapter(
    "Kaskade",
    KASKADE_EXECUTABLES,
    check_arguments=check_kaskade_arguments,
    prepare_command=prepare_kaskade_command,
    render_shim=render_kaskade_shim,
    missing_command=_missing_kaskade,
    registry_providers=frozenset({CONFLUENT_PROVIDER, APICURIO_PROVIDER}),
    registry_version_gate=True,
)
JAVA_CLI_ADAPTER = ClientAdapter(
    "Apache/Confluent Java CLI",
    KAFKA_EXECUTABLES,
    check_arguments=check_java_arguments,
    prepare_command=prepare_java_command,
    render_shim=render_java_shim,
    missing_command=_missing_java_command,
    pem_version_gate=True,
    oauth_version_gate=True,
)
CLIENT_ADAPTERS = (KCAT_ADAPTER, KASKADE_ADAPTER, JAVA_CLI_ADAPTER)
_ADAPTERS_BY_EXECUTABLE = {
    executable: adapter for adapter in CLIENT_ADAPTERS for executable in adapter.executables
}
ADAPTER_EXECUTABLES = frozenset(_ADAPTERS_BY_EXECUTABLE)


def client_adapter(executable_name: str) -> ClientAdapter | None:
    """Return the adapter that owns an executable base name, if any."""
    return _ADAPTERS_BY_EXECUTABLE.get(executable_name)


def prepare_command(
    arguments: Sequence[str],
    configuration: ClientConfiguration,
    *,
    registry: RegistryConnection | None = None,
) -> list[str]:
    """Inject profile connection options for a supported explicit command."""
    prepared = list(arguments)
    if not prepared:
        return prepared
    adapter = client_adapter(Path(prepared[0]).name)
    if adapter is None:
        return prepared
    return adapter.prepare_command(prepared, configuration, registry)


def create_subshell_shims(
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
    configuration = ClientConfiguration(
        bootstrap_servers=bootstrap_servers,
        java_config=java_config_path,
        kcat_config=kcat_config_path,
        kaskade_config=kaskade_config_path,
        kaskade_registry_config=kaskade_registry_config_path,
        schema_registry_java_config=schema_registry_java_config_path,
        registry_oauth_ssl_cert_file=registry_oauth_ssl_cert_file,
    )
    installed = _installed_executables(environment.get("PATH", os.defpath))
    gates = _ShimCapabilityGates(environment, registry, require_java_pem, kafka_auth_type)
    directory.mkdir(mode=0o700)
    for adapter, name, executable in installed:
        inputs = ShimInputs(
            configuration,
            registry,
            kafka_auth_type,
            gates.capability_error(adapter, executable),
        )
        write_executable(directory / name, adapter.render_shim(name, executable, inputs))
    return directory


def _installed_executables(search_path: str) -> list[tuple[ClientAdapter, str, str]]:
    return [
        (adapter, name, resolved)
        for adapter in CLIENT_ADAPTERS
        for name in sorted(adapter.executables)
        if (resolved := shutil.which(name, path=search_path)) is not None
    ]


@dataclass
class _ShimCapabilityGates:
    """Decide each installed shim's version gate, probing a Java install directory once."""

    environment: Mapping[str, str]
    registry: RegistryConnection | None
    require_java_pem: bool
    kafka_auth_type: str
    _java_errors: dict[tuple[str, Path], str | None] = field(default_factory=dict)

    def capability_error(self, adapter: ClientAdapter, executable: str) -> str | None:
        registry = self.registry
        if adapter.registry_version_gate and registry is not None:
            return _gate_error(
                lambda: require_kaskade_apicurio_security_support(
                    executable, registry, environment=self.environment
                )
            )
        gate = self._java_gate(adapter)
        if gate is None:
            return None
        key = (adapter.name, Path(executable).parent)
        if key not in self._java_errors:
            self._java_errors[key] = _gate_error(
                lambda: gate(executable, environment=self.environment)
            )
        return self._java_errors[key]

    def _java_gate(self, adapter: ClientAdapter) -> Callable[..., None] | None:
        # A shim applies one Java gate per install directory: PEM support when
        # the profile needs it, otherwise the OAuth mapping when Kafka uses OAuth.
        if self.require_java_pem and adapter.pem_version_gate:
            return require_java_pem_support
        if self.kafka_auth_type == "oauth" and adapter.oauth_version_gate:
            return require_java_oauth_support
        return None


def _gate_error(check: Callable[[], None]) -> str | None:
    try:
        check()
    except AdapterError as error:
        return str(error)
    return None


def require_adapter_capability(
    executable: str,
    *,
    auth_type: str,
    custom_pem: bool,
    environment: Mapping[str, str],
    registry: RegistryConnection | None = None,
) -> None:
    """Apply the same mechanism and installed-version decision to direct clients."""
    adapter = client_adapter(Path(executable).name)
    if adapter is None:
        return
    if auth_type not in adapter.kafka_authentication:
        raise AdapterError(f"{adapter.name} does not support Kafka authentication '{auth_type}'")
    if custom_pem and adapter.pem_version_gate:
        require_java_pem_support(executable, environment=environment)
    if auth_type == "oauth" and adapter.oauth_version_gate:
        require_java_oauth_support(executable, environment=environment)
    if adapter.registry_version_gate and registry is not None:
        require_kaskade_apicurio_security_support(
            executable,
            registry,
            environment=environment,
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


__all__ = [
    "ADAPTER_EXECUTABLES",
    "CLIENT_ADAPTERS",
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
    "SCHEMA_REGISTRY_CONSUMER_EXECUTABLES",
    "SCHEMA_REGISTRY_EXECUTABLES",
    "SCHEMA_REGISTRY_PRODUCER_EXECUTABLES",
    "AdapterError",
    "ClientAdapter",
    "ClientConfiguration",
    "client_adapter",
    "create_subshell_shims",
    "prepare_command",
    "require_adapter_capability",
    "require_java_oauth_support",
    "require_java_pem_support",
    "require_kaskade_apicurio_security_support",
]
