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
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from kantrip.adapter_policy import (
    KAF_EXECUTABLES,
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
    KCL_EXECUTABLES,
    SCHEMA_REGISTRY_CONSUMER_EXECUTABLES,
    SCHEMA_REGISTRY_EXECUTABLES,
    SCHEMA_REGISTRY_PRODUCER_EXECUTABLES,
    AdapterError,
    ClientConfiguration,
    check_java_arguments,
    check_kaf_arguments,
    check_kaskade_arguments,
    check_kcat_arguments,
    check_kcl_arguments,
    prepare_java_command,
    prepare_kaf_command,
    prepare_kaf_environment,
    prepare_kaskade_command,
    prepare_kcat_command,
    prepare_kcl_command,
    prepare_kcl_environment,
    require_kaf_registry,
    require_kcl_registry,
)
from kantrip.adapter_shims import (
    ShimInputs,
    render_java_shim,
    render_kaf_shim,
    render_kaskade_shim,
    render_kcat_shim,
    render_kcl_shim,
    write_executable,
)
from kantrip.registry import APICURIO_PROVIDER, CONFLUENT_PROVIDER, RegistryConnection

ArgumentCheck = Callable[[str, Sequence[str]], str | None]
CommandPreparation = Callable[
    [list[str], ClientConfiguration, RegistryConnection | None], list[str]
]
ShimRendering = Callable[[str, str, ShimInputs], str]
EnvironmentPreparation = Callable[[MutableMapping[str, str], ClientConfiguration], None]


@dataclass(frozen=True)
class VersionGate:
    """The oldest release whose native contract a client's mapping relies on.

    `pattern` reads `--version` output into major, minor, patch, and a suffix;
    a suffixed build, such as a development or pre-release one, never passes.
    """

    pattern: re.Pattern[str]
    minimum: tuple[int, int, int]

    @property
    def rendered_minimum(self) -> str:
        return ".".join(str(part) for part in self.minimum)


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
    remaining fields are the family's capability matrix and version gates;
    `registry_check` gates a family that reads the profile's Registry on every run,
    `minimum_version` gates every launch on the installed release, and
    `prepare_environment` adjusts a direct child's environment.
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
    registry_check: Callable[[RegistryConnection], object] | None = None
    prepare_environment: EnvironmentPreparation | None = None
    minimum_version: VersionGate | None = None


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


def _missing_kaf(name: str) -> str:
    del name
    return (
        "command 'kaf' was not found; install it with 'brew install kaf' on macOS or "
        "from https://github.com/birdayz/kaf/releases on Linux"
    )


def _missing_kcl(name: str) -> str:
    del name
    return (
        "command 'kcl' was not found; install it from "
        "https://github.com/twmb/kcl/releases and ensure its executable is on PATH"
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
KAF_ADAPTER = ClientAdapter(
    "kaf",
    KAF_EXECUTABLES,
    check_arguments=check_kaf_arguments,
    prepare_command=prepare_kaf_command,
    render_shim=render_kaf_shim,
    missing_command=_missing_kaf,
    # kaf's token client uses Go's default trust store and cannot take the
    # profile's token-endpoint CA, so OAuth has no safe mapping.
    kafka_authentication=_KAFKA_AUTHENTICATION - {"oauth"},
    registry_check=require_kaf_registry,
    prepare_environment=prepare_kaf_environment,
)
KCL_ADAPTER = ClientAdapter(
    "kcl",
    KCL_EXECUTABLES,
    check_arguments=check_kcl_arguments,
    prepare_command=prepare_kcl_command,
    render_shim=render_kcl_shim,
    missing_command=_missing_kcl,
    # kcl's SASL mechanisms are PLAIN, SCRAM, and AWS MSK IAM; it has no OAUTHBEARER.
    kafka_authentication=_KAFKA_AUTHENTICATION - {"oauth"},
    registry_check=require_kcl_registry,
    prepare_environment=prepare_kcl_environment,
    # The argument policy, `[registry]` table, and `KCL_*` scrub follow v0.20.0.
    minimum_version=VersionGate(
        re.compile(r"\bkcl version v?(\d+)\.(\d+)\.(\d+)(\S*)"), (0, 20, 0)
    ),
)
CLIENT_ADAPTERS = (KCAT_ADAPTER, KASKADE_ADAPTER, JAVA_CLI_ADAPTER, KAF_ADAPTER, KCL_ADAPTER)
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


def prepare_command_environment(
    arguments: Sequence[str],
    environment: Mapping[str, str],
    configuration: ClientConfiguration,
) -> dict[str, str]:
    """Return the environment a supported explicit command runs with."""
    prepared = dict(environment)
    adapter = client_adapter(Path(arguments[0]).name) if arguments else None
    if adapter is not None and adapter.prepare_environment is not None:
        adapter.prepare_environment(prepared, configuration)
    return prepared


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
    kaf_config_path: Path | None = None,
    kcl_config_path: Path | None = None,
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
        kaf_config=kaf_config_path,
        kcl_config=kcl_config_path,
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
    """Decide each installed shim's capability, probing a Java install directory once."""

    environment: Mapping[str, str]
    registry: RegistryConnection | None
    require_java_pem: bool
    kafka_auth_type: str
    _java_probe: _JavaVersionProbe = field(default_factory=lambda: _JavaVersionProbe())

    def capability_error(self, adapter: ClientAdapter, executable: str) -> str | None:
        return _gate_error(
            lambda: _require_capability(
                adapter,
                executable,
                auth_type=self.kafka_auth_type,
                custom_pem=self.require_java_pem,
                environment=self.environment,
                registry=self.registry,
                java_probe=self._java_probe,
            )
        )


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
    _require_capability(
        adapter,
        executable,
        auth_type=auth_type,
        custom_pem=custom_pem,
        environment=environment,
        registry=registry,
        java_probe=_JavaVersionProbe(),
    )


def _require_capability(
    adapter: ClientAdapter,
    executable: str,
    *,
    auth_type: str,
    custom_pem: bool,
    environment: Mapping[str, str],
    registry: RegistryConnection | None,
    java_probe: _JavaVersionProbe,
) -> None:
    # The one capability decision for direct commands and shell shims; every
    # gate the profile needs applies, in this order.
    if auth_type not in adapter.kafka_authentication:
        raise AdapterError(f"{adapter.name} does not support Kafka authentication '{auth_type}'")
    if registry is not None and adapter.registry_check is not None:
        adapter.registry_check(registry)
    if adapter.minimum_version is not None:
        require_minimum_version(executable, adapter.minimum_version, environment=environment)
    if custom_pem and adapter.pem_version_gate:
        require_java_pem_support(executable, environment=environment, probe=java_probe)
    if auth_type == "oauth" and adapter.oauth_version_gate:
        require_java_oauth_support(executable, environment=environment, probe=java_probe)
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
    probe: _JavaVersionProbe | None = None,
) -> None:
    """Reject Java clients whose version cannot safely consume a PEM trust store."""
    version = _java_client_version(
        executable, environment, capability="PEM trust-store", probe=probe or _JavaVersionProbe()
    )
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
    probe: _JavaVersionProbe | None = None,
) -> None:
    """Require the verified Apache Kafka 4.x native client-credentials callback."""
    version = _java_client_version(
        executable, environment, capability="OAuth", probe=probe or _JavaVersionProbe()
    )
    if version[0] < 4:
        rendered_version = ".".join(str(part) for part in version)
        raise AdapterError(
            f"{Path(executable).name} {rendered_version} does not support Kantrip's native "
            "OAuth mapping; install Apache Kafka 4.0+"
        )


def require_minimum_version(
    executable: str,
    gate: VersionGate,
    *,
    environment: Mapping[str, str],
) -> None:
    """Reject an installed client older than the release its mapping relies on."""
    name = Path(executable).name
    guidance = f"install {name} {gate.rendered_minimum} or newer"
    version, suffix, rendered = _released_client_version(
        executable,
        environment,
        gate.pattern,
        failure=f"could not verify the installed {name} version; {guidance}",
    )
    if not suffix and version >= gate.minimum:
        return
    raise AdapterError(f"{name} {rendered} is not supported; {guidance}")


def require_kaskade_apicurio_security_support(
    executable: str,
    registry: RegistryConnection,
    *,
    environment: Mapping[str, str],
) -> None:
    """Gate native Apicurio OAuth scopes on their first stable Kaskade release."""
    if not _requires_new_kaskade_apicurio_security(registry):
        return
    version, suffix, rendered = _released_client_version(
        executable,
        environment,
        _KASKADE_VERSION_PATTERN,
        failure="could not verify Kaskade Apicurio security support",
    )
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


def _released_client_version(
    executable: str,
    environment: Mapping[str, str],
    pattern: re.Pattern[str],
    *,
    failure: str,
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
        raise AdapterError(failure) from error
    match = pattern.search(f"{result.stdout}\n{result.stderr}")
    if result.returncode != 0 or match is None:
        raise AdapterError(failure)
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
    probe: _JavaVersionProbe,
) -> tuple[int, int]:
    resolved = shutil.which(executable, path=environment.get("PATH"))
    if resolved is None:
        raise AdapterError(f"command '{Path(executable).name}' was not found")
    version = probe(resolved, environment)
    if version is None:
        raise AdapterError(f"could not verify {capability} support for {Path(executable).name}")
    return version


class _JavaVersionProbe:
    """Run a Java client's `--version` once per install directory."""

    def __init__(self) -> None:
        self._versions: dict[Path, tuple[int, int] | None] = {}

    def __call__(self, resolved: str, environment: Mapping[str, str]) -> tuple[int, int] | None:
        directory = Path(resolved).parent
        if directory not in self._versions:
            self._versions[directory] = _probe_java_client_version(resolved, environment)
        return self._versions[directory]


def _probe_java_client_version(
    resolved: str,
    environment: Mapping[str, str],
) -> tuple[int, int] | None:
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
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return _recognized_java_client_version(f"{result.stdout}\n{result.stderr}")


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
    "KAF_EXECUTABLES",
    "KASKADE_EXECUTABLES",
    "KCAT_EXECUTABLES",
    "KCL_EXECUTABLES",
    "SCHEMA_REGISTRY_CONSUMER_EXECUTABLES",
    "SCHEMA_REGISTRY_EXECUTABLES",
    "SCHEMA_REGISTRY_PRODUCER_EXECUTABLES",
    "AdapterError",
    "ClientAdapter",
    "ClientConfiguration",
    "VersionGate",
    "client_adapter",
    "create_subshell_shims",
    "prepare_command",
    "prepare_command_environment",
    "require_adapter_capability",
    "require_java_oauth_support",
    "require_java_pem_support",
    "require_kaskade_apicurio_security_support",
    "require_minimum_version",
]
