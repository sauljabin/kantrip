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
from collections.abc import Callable, Iterable, Mapping, MutableMapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

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
    java_option_variables,
    prepare_java_command,
    prepare_java_environment,
    prepare_kaf_command,
    prepare_kaf_environment,
    prepare_kaskade_command,
    prepare_kcat_command,
    prepare_kcat_environment,
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


def rendered_release(release: Sequence[int]) -> str:
    """Render a release such as `(6, 0)` as `6.0`."""
    return ".".join(str(part) for part in release)


def _at_least(product: str, release: Sequence[int]) -> str:
    return f"{product} {rendered_release(release)} or newer"


@dataclass(frozen=True)
class ClientVersion:
    """An installed client's release as its version option prints it."""

    release: tuple[int, int, int]
    suffix: str = ""

    def __str__(self) -> str:
        return rendered_release(self.release) + self.suffix


class VersionGate(Protocol):
    """How to read an installed client's release and whether Kantrip supports it."""

    @property
    def option(self) -> str:
        """The option that prints the release."""
        ...

    @property
    def shared_by_directory(self) -> bool:
        """Whether every executable in one install directory reports one release."""
        ...

    def read(self, output: str) -> ClientVersion | None:
        """Return the release printed by the version option, if recognized."""
        ...

    def supports(self, name: str, version: ClientVersion) -> bool:
        """Return whether executable `name` at `version` meets the floor."""
        ...

    def requirement(self, name: str) -> str:
        """Name the oldest supported release for executable `name`."""
        ...


@dataclass(frozen=True)
class ReleaseGate:
    """The oldest release whose native contract a client's mapping relies on.

    `pattern` reads the version output into major, minor, patch, and a suffix;
    a suffixed build, such as a development or pre-release one, never passes.
    """

    product: str
    pattern: re.Pattern[str]
    minimum: tuple[int, int, int]
    option: str = "--version"
    shared_by_directory: bool = False

    def read(self, output: str) -> ClientVersion | None:
        match = self.pattern.search(output)
        if match is None:
            return None
        major, minor, patch = (int(part) for part in match.group(1, 2, 3))
        return ClientVersion((major, minor, patch), match.group(4))

    def supports(self, name: str, version: ClientVersion) -> bool:
        del name
        return not version.suffix and version.release >= self.minimum

    def requirement(self, name: str) -> str:
        del name
        return _at_least(self.product, self.minimum)


_APACHE_KAFKA_MAJORS = frozenset({2, 3, 4})
_CONFLUENT_PLATFORM_MAJORS = frozenset({5, 6, 7, 8})


@dataclass(frozen=True)
class JavaFloor:
    """The oldest Apache Kafka and Confluent Platform releases for one Java capability.

    `apache` is None when only Confluent Platform ships the executable.
    """

    apache: tuple[int, int] | None
    confluent: tuple[int, int]

    def admits(self, version: ClientVersion) -> bool:
        major, minor, _ = version.release
        floor = self.apache if major in _APACHE_KAFKA_MAJORS else self.confluent
        return floor is not None and (major, minor) >= floor

    def requirement(self) -> str:
        confluent = _at_least("Confluent Platform", self.confluent)
        if self.apache is None:
            return confluent
        return f"Apache Kafka {rendered_release(self.apache)} or {confluent}"


_JAVA_CLI_FLOOR = JavaFloor(apache=(2, 6), confluent=(6, 0))
_SCHEMA_REGISTRY_CONSOLE_FLOOR = JavaFloor(apache=None, confluent=(5, 5))
# Native PEM trust stores arrived in Apache Kafka 2.7, which Confluent Platform 6.1 ships.
_JAVA_PEM_FLOOR = JavaFloor(apache=(2, 7), confluent=(6, 1))
# The `sasl.oauthbearer.client.credentials.*` properties arrived in Apache
# Kafka 4.1, which Confluent Platform 8.1 ships.
_JAVA_OAUTH_FLOOR = JavaFloor(apache=(4, 1), confluent=(8, 1))
# A suffix starts with a letter, so a Scala-prefixed `2.13-2.6.0` reads as 2.6.0.
_JAVA_VERSION_PATTERN = re.compile(r"(?<![\d.])(\d+)\.(\d+)(?:\.(\d+))?(-[A-Za-z][\w.]*)?")


@dataclass(frozen=True)
class JavaReleaseGate:
    """Apache Kafka and Confluent Platform floors for the Java CLIs.

    Apache Kafka majors are 2 to 4 and Confluent Platform majors 5 to 8, so the
    major names the distribution. The Schema Registry consoles ship only with
    Confluent Platform. Confluent's `-ccs` and `-ce` suffixes mark release
    builds, and JVM warnings may surround the release line.
    """

    option: str = "--version"
    shared_by_directory: bool = True

    def read(self, output: str) -> ClientVersion | None:
        recognized: ClientVersion | None = None
        for match in _JAVA_VERSION_PATTERN.finditer(output):
            major, minor = int(match.group(1)), int(match.group(2))
            if major in _APACHE_KAFKA_MAJORS | _CONFLUENT_PLATFORM_MAJORS:
                patch = int(match.group(3) or 0)
                recognized = ClientVersion((major, minor, patch), match.group(4) or "")
        return recognized

    def supports(self, name: str, version: ClientVersion) -> bool:
        return _java_floor(name).admits(version)

    def requirement(self, name: str) -> str:
        return _java_floor(name).requirement()


def _java_floor(name: str) -> JavaFloor:
    if name in SCHEMA_REGISTRY_EXECUTABLES:
        return _SCHEMA_REGISTRY_CONSOLE_FLOOR
    return _JAVA_CLI_FLOOR


@dataclass(frozen=True)
class KafkaNeeds:
    """What the selected profile's Kafka connection asks of an installed client."""

    auth_type: str
    custom_pem: bool = False
    oauth_ca: bool = False


# A feature check reads the installed release (and the raw version output, for
# linked libraries) and returns why the profile cannot use it, or None.
FeatureCheck = Callable[[str, ClientVersion, str, KafkaNeeds], str | None]


_KAFKA_AUTHENTICATION = frozenset(
    {"none", "plain", "scram-sha-256", "scram-sha-512", "mtls", "oauth"}
)


@dataclass(frozen=True)
class ClientAdapter:
    """One supported client family and the explicit functions that serve it.

    `check_arguments` rejects profile-owned connection options for both direct
    commands and the shim guard, and may return a signal the shim acts on.
    `prepare_command` and `render_shim` inject the private configuration:
    options for Java tools and Kaskade, `KCAT_CONFIG` plus `-r` for kcat.
    `minimum_version` gates every launch on the installed release, and
    `feature_checks` apply the profile's needs to that same release. The
    remaining fields are the family's capability matrix; `registry_check` gates
    a family that reads the profile's Registry on every run, and
    `prepare_environment` adjusts a direct child's environment.
    """

    name: str
    executables: frozenset[str]
    check_arguments: ArgumentCheck
    prepare_command: CommandPreparation
    render_shim: ShimRendering
    missing_command: Callable[[str], str]
    minimum_version: VersionGate
    kafka_authentication: frozenset[str] = _KAFKA_AUTHENTICATION
    registry_providers: frozenset[str] = frozenset({CONFLUENT_PROVIDER})
    feature_checks: tuple[FeatureCheck, ...] = ()
    registry_check: Callable[[RegistryConnection], object] | None = None
    prepare_environment: EnvironmentPreparation | None = None


def missing_command_message(name: str, hint: str | None = None) -> str:
    """Return the missing-command error, followed by an install hint when one is known."""
    message = f"command '{name}' was not found"
    return message if hint is None else f"{message}; {hint}"


def _missing_kcat(name: str) -> str:
    del name
    return missing_command_message(
        "kcat", "install it with 'brew install kcat' on macOS or your Linux package manager"
    )


def _missing_java_command(name: str) -> str:
    if name in SCHEMA_REGISTRY_EXECUTABLES:
        return missing_command_message(
            name,
            "install the Confluent Schema Registry package and ensure its bin directory is on PATH",
        )
    return missing_command_message(
        name, "install the Apache Kafka CLI and ensure its bin directory is on PATH"
    )


def _missing_kaf(name: str) -> str:
    del name
    return missing_command_message(
        "kaf",
        "install it with 'brew install kaf' on macOS or "
        "from https://github.com/birdayz/kaf/releases on Linux",
    )


def _missing_kcl(name: str) -> str:
    del name
    return missing_command_message(
        "kcl",
        "install it from https://github.com/twmb/kcl/releases "
        "and ensure its executable is on PATH",
    )


def _missing_kaskade(name: str) -> str:
    del name
    return missing_command_message("kaskade", "install it and ensure its executable is on PATH")


def _java_pem_support(
    name: str, version: ClientVersion, output: str, needs: KafkaNeeds
) -> str | None:
    del output
    if not needs.custom_pem or _JAVA_PEM_FLOOR.admits(version):
        return None
    return (
        f"{name} {version} does not support PEM trust stores; "
        f"custom CA profiles require {_JAVA_PEM_FLOOR.requirement()}"
    )


def _java_oauth_support(
    name: str, version: ClientVersion, output: str, needs: KafkaNeeds
) -> str | None:
    del output
    if needs.auth_type != "oauth" or _JAVA_OAUTH_FLOOR.admits(version):
        return None
    return (
        f"{name} {version} does not support Kantrip's native OAuth mapping; "
        f"install {_JAVA_OAUTH_FLOOR.requirement()}"
    )


_LIBRDKAFKA_VERSION_PATTERN = re.compile(r"\blibrdkafka (\d+)\.(\d+)\.(\d+)")
# `https.ca.location`, which trusts the token endpoint's CA, arrived in librdkafka 2.11.0.
_LIBRDKAFKA_HTTPS_CA_MIN_VERSION = (2, 11, 0)


def _librdkafka_oauth_ca_support(
    name: str, version: ClientVersion, output: str, needs: KafkaNeeds
) -> str | None:
    # kcat prints the linked librdkafka alongside its own release.
    del version
    if needs.auth_type != "oauth" or not needs.oauth_ca:
        return None
    match = _LIBRDKAFKA_VERSION_PATTERN.search(output)
    linked = tuple(int(part) for part in match.groups()) if match else None
    if linked is not None and linked >= _LIBRDKAFKA_HTTPS_CA_MIN_VERSION:
        return None
    rendered = f"librdkafka {rendered_release(linked)}" if linked else "an unidentified librdkafka"
    return (
        f"{name} links {rendered}, which cannot trust the profile's OAuth token-endpoint CA; "
        f"install {_at_least('librdkafka', _LIBRDKAFKA_HTTPS_CA_MIN_VERSION)}"
    )


KCAT_ADAPTER = ClientAdapter(
    "kcat",
    KCAT_EXECUTABLES,
    check_arguments=check_kcat_arguments,
    prepare_command=prepare_kcat_command,
    render_shim=render_kcat_shim,
    missing_command=_missing_kcat,
    minimum_version=ReleaseGate(
        "kcat", re.compile(r"\bVersion (\d+)\.(\d+)\.(\d+)(\S*)"), (1, 7, 0), option="-V"
    ),
    feature_checks=(_librdkafka_oauth_ca_support,),
    prepare_environment=prepare_kcat_environment,
)
KASKADE_ADAPTER = ClientAdapter(
    "Kaskade",
    KASKADE_EXECUTABLES,
    check_arguments=check_kaskade_arguments,
    prepare_command=prepare_kaskade_command,
    render_shim=render_kaskade_shim,
    missing_command=_missing_kaskade,
    # 5.0.1 is the first release that maps native Apicurio OAuth scopes.
    minimum_version=ReleaseGate(
        "Kaskade",
        re.compile(r"\bkaskade,\s+version\s+(\d+)\.(\d+)\.(\d+)(\S*)", re.IGNORECASE),
        (5, 0, 1),
    ),
    registry_providers=frozenset({CONFLUENT_PROVIDER, APICURIO_PROVIDER}),
)
JAVA_CLI_ADAPTER = ClientAdapter(
    "Apache/Confluent Java CLI",
    KAFKA_EXECUTABLES,
    check_arguments=check_java_arguments,
    prepare_command=prepare_java_command,
    render_shim=render_java_shim,
    missing_command=_missing_java_command,
    minimum_version=JavaReleaseGate(),
    feature_checks=(_java_pem_support, _java_oauth_support),
    prepare_environment=prepare_java_environment,
)
KAF_ADAPTER = ClientAdapter(
    "kaf",
    KAF_EXECUTABLES,
    check_arguments=check_kaf_arguments,
    prepare_command=prepare_kaf_command,
    render_shim=render_kaf_shim,
    missing_command=_missing_kaf,
    # The argument policy and one-cluster YAML follow v0.2.14.
    minimum_version=ReleaseGate(
        "kaf", re.compile(r"\bkaf version v?(\d+)\.(\d+)\.(\d+)(\S*)"), (0, 2, 14)
    ),
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
    # The argument policy, `[registry]` table, and `KCL_*` scrub follow v0.20.0.
    minimum_version=ReleaseGate(
        "kcl", re.compile(r"\bkcl version v?(\d+)\.(\d+)\.(\d+)(\S*)"), (0, 20, 0)
    ),
    # kcl's SASL mechanisms are PLAIN, SCRAM, and AWS MSK IAM; it has no OAUTHBEARER.
    kafka_authentication=_KAFKA_AUTHENTICATION - {"oauth"},
    registry_check=require_kcl_registry,
    prepare_environment=prepare_kcl_environment,
)
CLIENT_ADAPTERS = (KCAT_ADAPTER, KASKADE_ADAPTER, JAVA_CLI_ADAPTER, KAF_ADAPTER, KCL_ADAPTER)
_ADAPTERS_BY_EXECUTABLE = {
    executable: adapter for adapter in CLIENT_ADAPTERS for executable in adapter.executables
}
ADAPTER_EXECUTABLES = frozenset(_ADAPTERS_BY_EXECUTABLE)


def client_adapter(executable_name: str) -> ClientAdapter | None:
    """Return the adapter that owns an executable base name, if any."""
    return _ADAPTERS_BY_EXECUTABLE.get(executable_name)


# Matches the usual POSIX symlink resolution limit (SYMLOOP_MAX).
_SYMLINK_HOPS = 40


def resolve_client_command(command: str, search_path: str | None) -> str:
    """Return `command`, or the supported client it names through symlinks.

    A link such as `kas -> kaskade` runs that client, so it is adapted as the
    first link target whose base name is a supported executable. Aliases,
    functions, and wrapper scripts are not links and are never resolved.
    """
    if Path(command).name in ADAPTER_EXECUTABLES:
        return command
    located = shutil.which(command, path=search_path)
    if located is None:
        return command
    return _linked_client(Path(located)) or command


def _linked_client(path: Path) -> str | None:
    for _ in range(_SYMLINK_HOPS):
        try:
            target = os.readlink(path)
        except OSError:
            return None
        # A relative target is relative to the link's directory; leaving `..`
        # unnormalized lets the OS resolve it through linked directories.
        path = path.parent / target
        if path.name in ADAPTER_EXECUTABLES:
            return str(path)
    return None


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
    librdkafka_config_path: Path,
    kaskade_config_path: Path,
    kaskade_registry_config_path: Path,
    environment: Mapping[str, str],
    registry: RegistryConnection | None = None,
    require_java_pem: bool = False,
    kafka_auth_type: str = "none",
    kafka_oauth_ca: bool = False,
    schema_registry_java_config_path: Path | None = None,
    registry_oauth_ssl_cert_file: Path | None = None,
    kaf_config_path: Path | None = None,
    kcl_config_path: Path | None = None,
) -> Path:
    """Create session-owned shims for installed adapter executables."""
    configuration = ClientConfiguration(
        bootstrap_servers=bootstrap_servers,
        java_config=java_config_path,
        librdkafka_config=librdkafka_config_path,
        kaskade_config=kaskade_config_path,
        kaskade_registry_config=kaskade_registry_config_path,
        schema_registry_java_config=schema_registry_java_config_path,
        registry_oauth_ssl_cert_file=registry_oauth_ssl_cert_file,
        kaf_config=kaf_config_path,
        kcl_config=kcl_config_path,
    )
    search_path = environment.get("PATH", os.defpath)
    installed = [*_installed_executables(search_path), *_linked_executables(search_path)]
    versions = VersionProbe(environment)
    versions.prefetch((client.adapter.minimum_version, client.path) for client in installed)
    needs = KafkaNeeds(kafka_auth_type, require_java_pem, kafka_oauth_ca)
    directory.mkdir(mode=0o700)
    for client in installed:
        adapter = client.adapter
        inputs = ShimInputs(
            configuration,
            registry,
            kafka_auth_type,
            _capability_error(adapter, client.path, needs, registry, versions),
        )
        write_executable(
            directory / client.command, adapter.render_shim(client.name, client.path, inputs)
        )
    return directory


@dataclass(frozen=True)
class _InstalledClient:
    """One shim: the `command` typed in the shell runs executable `name` at `path`."""

    adapter: ClientAdapter
    command: str
    name: str
    path: str


def _installed_executables(search_path: str) -> list[_InstalledClient]:
    return [
        _InstalledClient(adapter, name, name, resolved)
        for adapter in CLIENT_ADAPTERS
        for name in sorted(adapter.executables)
        if (resolved := shutil.which(name, path=search_path)) is not None
    ]


def _linked_executables(search_path: str) -> list[_InstalledClient]:
    """Find other names on PATH that are symlinks to supported clients."""
    linked: dict[str, _InstalledClient] = {}
    for directory in filter(None, search_path.split(os.pathsep)):
        for entry in _symlinks(directory):
            if entry.name in ADAPTER_EXECUTABLES or entry.name in linked:
                continue
            target = _linked_client(Path(entry.path))
            # Only the command the shell would run gets a shim, not a shadowed one.
            if target is not None and shutil.which(entry.name, path=search_path) == entry.path:
                name = Path(target).name
                linked[entry.name] = _InstalledClient(
                    _ADAPTERS_BY_EXECUTABLE[name], entry.name, name, target
                )
    return sorted(linked.values(), key=lambda client: client.command)


def _symlinks(directory: str) -> list[os.DirEntry[str]]:
    try:
        with os.scandir(directory) as entries:
            return [entry for entry in entries if entry.is_symlink()]
    except OSError:
        return []


def _capability_error(
    adapter: ClientAdapter,
    executable: str,
    needs: KafkaNeeds,
    registry: RegistryConnection | None,
    versions: VersionProbe,
) -> str | None:
    try:
        _require_capability(adapter, executable, needs, registry, versions)
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
    oauth_ca: bool = False,
    versions: VersionProbe | None = None,
) -> None:
    """Apply the same mechanism and installed-version decision to direct clients."""
    adapter = client_adapter(Path(executable).name)
    if adapter is None:
        return
    _require_capability(
        adapter,
        executable,
        KafkaNeeds(auth_type, custom_pem, oauth_ca),
        registry,
        versions or VersionProbe(environment),
    )


def _require_capability(
    adapter: ClientAdapter,
    executable: str,
    needs: KafkaNeeds,
    registry: RegistryConnection | None,
    versions: VersionProbe,
) -> None:
    # The one capability decision for direct commands and shell shims; every
    # gate the profile needs applies, in this order, to one version probe.
    if needs.auth_type not in adapter.kafka_authentication:
        raise AdapterError(
            f"{adapter.name} does not support Kafka authentication '{needs.auth_type}'"
        )
    if registry is not None and adapter.registry_check is not None:
        adapter.registry_check(registry)
    version, output = _supported_version(executable, adapter.minimum_version, versions)
    for check in adapter.feature_checks:
        if (error := check(Path(executable).name, version, output, needs)) is not None:
            raise AdapterError(error)


def require_minimum_version(
    executable: str,
    gate: VersionGate,
    *,
    environment: Mapping[str, str],
    versions: VersionProbe | None = None,
) -> ClientVersion:
    """Return the installed release, rejecting one older than its mapping relies on."""
    version, _ = _supported_version(executable, gate, versions or VersionProbe(environment))
    return version


def _supported_version(
    executable: str, gate: VersionGate, versions: VersionProbe
) -> tuple[ClientVersion, str]:
    name = Path(executable).name
    resolved = shutil.which(executable, path=versions.environment.get("PATH"))
    if resolved is None:
        raise AdapterError(missing_command_message(name))
    output = versions.output(gate, resolved)
    version = gate.read(output) if output is not None else None
    guidance = f"install {gate.requirement(name)}"
    if version is None:
        raise AdapterError(f"could not verify the installed {name} version; {guidance}")
    if not gate.supports(name, version):
        raise AdapterError(f"{name} {version} is not supported; {guidance}")
    return version, output or ""


_VERSION_TIMEOUT_SECONDS = 10
_VERSION_PROBE_WORKERS = 8


class VersionProbe:
    """Run each installed client's version option once per launch.

    Java CLIs in one install directory report one release, so they share a run.
    """

    def __init__(self, environment: Mapping[str, str]) -> None:
        self.environment = environment
        self._outputs: dict[tuple[str, Path], str | None] = {}

    def output(self, gate: VersionGate, resolved: str) -> str | None:
        """Return the version output, or None when the client could not report it."""
        key = _probe_key(gate, resolved)
        if key not in self._outputs:
            self._outputs[key] = _version_output(resolved, gate.option, self.environment)
        return self._outputs[key]

    def prefetch(self, clients: Iterable[tuple[VersionGate, str]]) -> None:
        """Probe several clients at once, so a shell waits for the slowest one only."""
        pending: dict[tuple[str, Path], tuple[str, str]] = {}
        for gate, resolved in clients:
            key = _probe_key(gate, resolved)
            if key not in self._outputs:
                pending.setdefault(key, (resolved, gate.option))
        if not pending:
            return
        with ThreadPoolExecutor(max_workers=min(len(pending), _VERSION_PROBE_WORKERS)) as pool:
            outputs = pool.map(
                lambda probe: _version_output(probe[0], probe[1], self.environment),
                pending.values(),
            )
            self._outputs.update(zip(pending, outputs, strict=True))


def _probe_key(gate: VersionGate, resolved: str) -> tuple[str, Path]:
    path = Path(resolved)
    return gate.option, path.parent if gate.shared_by_directory else path


def _version_output(resolved: str, option: str, environment: Mapping[str, str]) -> str | None:
    try:
        result = subprocess.run(
            [resolved, option],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=_VERSION_TIMEOUT_SECONDS,
            check=False,
            env=dict(environment),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return f"{result.stdout}\n{result.stderr}"


__all__ = [
    "ADAPTER_EXECUTABLES",
    "CLIENT_ADAPTERS",
    "JAVA_CLI_ADAPTER",
    "KAFKA_ACLS_EXECUTABLES",
    "KAFKA_BROKER_API_VERSIONS_EXECUTABLES",
    "KAFKA_CONFIGS_EXECUTABLES",
    "KAFKA_CONSOLE_CONSUMER_EXECUTABLES",
    "KAFKA_CONSOLE_PRODUCER_EXECUTABLES",
    "KAFKA_CONSUMER_GROUPS_EXECUTABLES",
    "KAFKA_EXECUTABLES",
    "KAFKA_TOPICS_EXECUTABLES",
    "KAF_ADAPTER",
    "KAF_EXECUTABLES",
    "KASKADE_ADAPTER",
    "KASKADE_EXECUTABLES",
    "KCAT_ADAPTER",
    "KCAT_EXECUTABLES",
    "KCL_ADAPTER",
    "KCL_EXECUTABLES",
    "SCHEMA_REGISTRY_CONSUMER_EXECUTABLES",
    "SCHEMA_REGISTRY_EXECUTABLES",
    "SCHEMA_REGISTRY_PRODUCER_EXECUTABLES",
    "AdapterError",
    "ClientAdapter",
    "ClientConfiguration",
    "ClientVersion",
    "JavaReleaseGate",
    "KafkaNeeds",
    "ReleaseGate",
    "VersionGate",
    "VersionProbe",
    "client_adapter",
    "create_subshell_shims",
    "java_option_variables",
    "missing_command_message",
    "prepare_command",
    "prepare_command_environment",
    "rendered_release",
    "require_adapter_capability",
    "require_minimum_version",
    "resolve_client_command",
]
