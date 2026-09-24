"""Run local, read-only diagnostics for a Kantrip installation."""

from __future__ import annotations

import os
import shutil
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from cryptography import x509

from kantrip import APP_VERSION
from kantrip.adapters import (
    ADAPTER_EXECUTABLES,
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
    SCHEMA_REGISTRY_EXECUTABLES,
    AdapterError,
    require_adapter_capability,
)
from kantrip.kafka import KafkaProfileError, kafka_connection, resolve_kafka_connection
from kantrip.profiles import (
    DATABASE_BACKUP_PREFIX,
    DATABASE_MAINTENANCE_SUFFIX,
    ProfileCollection,
    ProfileStoreError,
    inspect_pending_secret_cleanup,
    inspect_profile_database,
    load_profiles,
    resolve_database_path,
)
from kantrip.reconciliation import ReconciliationError
from kantrip.registry import RegistryProfileError, registry_connection
from kantrip.runtime import (
    AUTOMATIC_SCAN_LIMIT,
    SessionRuntimeError,
    resolve_runtime_root,
    scan_sessions,
)
from kantrip.secret_store import (
    SecretNotFoundError,
    SecretStore,
    SecretStoreError,
    load_secret_store,
)
from kantrip.shells import ShellError, resolve_interactive_shell

CheckStatus = Literal["success", "warning", "error"]


@dataclass(frozen=True)
class DoctorCheck:
    """One diagnostic result suitable for human-readable presentation."""

    status: CheckStatus
    message: str
    section: str = "System"
    verbose_only: bool = False


@dataclass(frozen=True)
class DoctorReport:
    """The complete set of local diagnostics."""

    checks: tuple[DoctorCheck, ...]

    @property
    def healthy(self) -> bool:
        """Return whether no check found a condition that prevents safe use."""
        return all(check.status != "error" for check in self.checks)

    @property
    def warning_count(self) -> int:
        """Return the number of warning checks."""
        return sum(check.status == "warning" for check in self.checks)

    @property
    def error_count(self) -> int:
        """Return the number of failed checks."""
        return sum(check.status == "error" for check in self.checks)

    def sections(self, *, verbose: bool = False) -> tuple[tuple[str, tuple[DoctorCheck, ...]], ...]:
        """Group visible checks in their stable presentation order."""
        grouped: dict[str, list[DoctorCheck]] = {}
        for check in self.checks:
            if check.verbose_only and not verbose:
                continue
            grouped.setdefault(check.section, []).append(check)
        return tuple((name, tuple(checks)) for name, checks in grouped.items())


_KAFKA_COMMAND_GROUPS: tuple[tuple[str, frozenset[str]], ...] = (
    ("topics", KAFKA_TOPICS_EXECUTABLES),
    ("console consumer", KAFKA_CONSOLE_CONSUMER_EXECUTABLES),
    ("console producer", KAFKA_CONSOLE_PRODUCER_EXECUTABLES),
    ("consumer groups", KAFKA_CONSUMER_GROUPS_EXECUTABLES),
    ("configs", KAFKA_CONFIGS_EXECUTABLES),
    ("ACLs", KAFKA_ACLS_EXECUTABLES),
    ("broker API versions", KAFKA_BROKER_API_VERSIONS_EXECUTABLES),
)
_SCHEMA_REGISTRY_COMMAND_GROUPS = tuple(
    frozenset({executable}) for executable in sorted(SCHEMA_REGISTRY_EXECUTABLES)
)


def run_doctor(
    environment: Mapping[str, str] | None = None,
    *,
    profile_name: str | None = None,
    include_sessions: bool = False,
) -> DoctorReport:
    """Inspect Kantrip's local environment without contacting configured services."""
    env = os.environ if environment is None else environment
    system_checks = [
        DoctorCheck("success", f"Kantrip {APP_VERSION}"),
        _check_cli(env),
        _check_python(),
        _check_platform(),
        _check_shell(env),
    ]
    profiles, profile_checks = _check_profile_database(env, profile_name=profile_name)
    credential_checks, store = _check_credentials(env)
    profile_credential_checks = _check_profile_credentials(profiles, store)
    profile_id = None
    profile_revision = None
    if profiles is not None and profile_name in profiles.profiles:
        profile_id = str(profiles.profiles[profile_name]["id"])
        profile_revision = profiles.revision(profile_name)
    checks = [
        *_assign_section("System", system_checks),
        *_assign_section("Profiles", profile_checks),
        *_assign_section("Credentials", [*credential_checks, *profile_credential_checks]),
        *_assign_section(
            "Session",
            [
                *_check_session(profiles, env),
                *_check_runtime_sessions(
                    env,
                    profile_id=profile_id,
                    current_revision=profile_revision,
                    include_details=include_sessions,
                ),
            ],
        ),
        *_assign_section(
            "Clients", [*_check_commands(env), *_check_profile_clients(profiles, env)]
        ),
    ]
    return DoctorReport(tuple(checks))


def _check_credentials(
    environment: Mapping[str, str],
) -> tuple[list[DoctorCheck], SecretStore | None]:
    checks: list[DoctorCheck] = []
    store: SecretStore | None = None
    try:
        store = load_secret_store()
    except SecretStoreError:
        checks.append(DoctorCheck("error", "Credential store backend is unavailable or unsafe"))
    else:
        checks.append(DoctorCheck("success", f"Credential store: {store.info.display_name}"))
        checks.append(
            DoctorCheck(
                "success",
                f"Credential store backend: {store.info.backend}",
                verbose_only=True,
            )
        )
    try:
        pending = inspect_pending_secret_cleanup(environment=environment)
    except ProfileStoreError as error:
        if "requires migration" in str(error):
            checks.append(
                DoctorCheck("warning", "Credential reconciliation requires database migration")
            )
        else:
            checks.append(DoctorCheck("error", "Credential reconciliation state is unavailable"))
    except ReconciliationError:
        checks.append(DoctorCheck("error", "Credential reconciliation state is invalid"))
    else:
        count = len(pending)
        status: CheckStatus = "warning" if count else "success"
        noun = "entry" if count == 1 else "entries"
        message = f"Credential reconciliation: {count} pending {noun}"
        if count:
            message = f"{message}; run 'kantrip doctor --repair'"
        checks.append(DoctorCheck(status, message))
    return checks, store


def _check_profile_credentials(
    profiles: ProfileCollection | None,
    store: SecretStore | None,
) -> list[DoctorCheck]:
    if profiles is None or not profiles.profiles:
        return []
    checks: list[DoctorCheck] = []
    for name, profile in profiles.profiles.items():
        try:
            connection = kafka_connection(profile)
        except KafkaProfileError:
            checks.append(DoctorCheck("error", f"Kafka profile '{name}' is not executable"))
            continue
        checks.extend(_check_certificate_validity(name, profile))
        if not connection.requires_secrets:
            checks.append(DoctorCheck("success", f"Profile '{name}' requires no credentials"))
            continue
        if store is None:
            checks.append(DoctorCheck("error", f"Profile '{name}' credentials are unavailable"))
            continue
        checks.extend(_check_exact_references(name, profile, store))
        try:
            resolve_kafka_connection(connection, store)
        except KafkaProfileError:
            checks.append(DoctorCheck("error", f"Profile '{name}' credential identity is invalid"))
        else:
            checks.append(DoctorCheck("success", f"Profile '{name}' credentials are usable"))
    return checks


def _check_exact_references(
    name: str,
    profile: Mapping[str, object],
    store: SecretStore,
) -> list[DoctorCheck]:
    kafka = profile.get("kafka")
    auth = kafka.get("auth") if isinstance(kafka, Mapping) else None
    if not isinstance(auth, Mapping):
        return [DoctorCheck("error", f"Profile '{name}' authentication is invalid")]
    checks: list[DoctorCheck] = []
    for property_name, label in (
        ("passwordRef", "password"),
        ("privateKeyRef", "private key"),
        ("privateKeyPasswordRef", "private-key password"),
    ):
        reference = auth.get(property_name)
        if not isinstance(reference, str):
            continue
        try:
            store.get(reference)
        except SecretNotFoundError:
            checks.append(DoctorCheck("error", f"Profile '{name}' {label} is missing"))
        except SecretStoreError:
            checks.append(DoctorCheck("error", f"Profile '{name}' {label} is unavailable"))
        else:
            checks.append(DoctorCheck("success", f"Profile '{name}' {label} is stored"))
    return checks


def _check_certificate_validity(
    name: str,
    profile: Mapping[str, object],
) -> list[DoctorCheck]:
    kafka = profile.get("kafka")
    if not isinstance(kafka, Mapping):
        return []
    certificates: list[tuple[str, x509.Certificate]] = []
    tls = kafka.get("tls")
    if isinstance(tls, Mapping) and isinstance(tls.get("caCertificates"), str):
        try:
            parsed = x509.load_pem_x509_certificates(tls["caCertificates"].encode())
        except ValueError:
            return [DoctorCheck("error", f"Profile '{name}' Kafka CA bundle is invalid")]
        certificates.extend(("Kafka CA certificate", certificate) for certificate in parsed)
    auth = kafka.get("auth")
    if isinstance(auth, Mapping) and isinstance(auth.get("clientCertificate"), str):
        try:
            parsed = x509.load_pem_x509_certificates(auth["clientCertificate"].encode())
        except ValueError:
            return [DoctorCheck("error", f"Profile '{name}' client certificate is invalid")]
        certificates.extend(("client certificate", certificate) for certificate in parsed)
    return [
        check
        for label, certificate in certificates
        if (check := _certificate_time_check(name, label, certificate)) is not None
    ]


def _certificate_time_check(
    name: str,
    label: str,
    certificate: x509.Certificate,
) -> DoctorCheck | None:
    now = time.time()
    not_before = certificate.not_valid_before_utc.timestamp()
    not_after = certificate.not_valid_after_utc.timestamp()
    if now < not_before:
        return DoctorCheck("error", f"Profile '{name}' {label} is not yet valid")
    if now >= not_after:
        return DoctorCheck("error", f"Profile '{name}' {label} is expired")
    if not_after - now <= 30 * 24 * 60 * 60:
        return DoctorCheck("warning", f"Profile '{name}' {label} expires within 30 days")
    return None


def _assign_section(section: str, checks: Sequence[DoctorCheck]) -> list[DoctorCheck]:
    return [
        DoctorCheck(check.status, check.message, section, check.verbose_only) for check in checks
    ]


def _check_platform() -> DoctorCheck:
    if sys.platform.startswith(("darwin", "linux")):
        return DoctorCheck("success", f"Platform is supported ({sys.platform})")
    return DoctorCheck("warning", f"Platform is not currently supported ({sys.platform})")


def _check_python() -> DoctorCheck:
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    if (3, 10) <= sys.version_info[:2] < (3, 15):
        return DoctorCheck("success", f"Python version is supported ({version})")
    return DoctorCheck("error", f"Python version is not supported ({version})")


def _check_cli(environment: Mapping[str, str]) -> DoctorCheck:
    executable = shutil.which("kantrip", path=environment.get("PATH"))
    if executable is None:
        return DoctorCheck("warning", "kantrip executable was not found on PATH")
    return DoctorCheck("success", f"kantrip executable: {executable}", verbose_only=True)


def _check_shell(environment: Mapping[str, str]) -> DoctorCheck:
    try:
        shell = resolve_interactive_shell(environment)
    except ShellError as error:
        return DoctorCheck("warning", f"Interactive shell is unavailable: {error}")
    return DoctorCheck("success", f"Interactive shell: {shell}")


def _check_profile_database(
    environment: Mapping[str, str],
    *,
    profile_name: str | None,
) -> tuple[ProfileCollection | None, list[DoctorCheck]]:
    path = resolve_database_path(environment)
    path_check = DoctorCheck("success", f"Profile database: {path}", verbose_only=True)
    try:
        path.lstat()
    except FileNotFoundError:
        return None, [DoctorCheck("warning", "Profile database was not found"), path_check]
    except OSError:
        return None, [DoctorCheck("error", "Profile database could not be inspected"), path_check]
    try:
        migration_state = inspect_profile_database(path)
    except ProfileStoreError as error:
        return None, [DoctorCheck("error", str(error)), path_check]

    if migration_state.requires_migration:
        count = len(migration_state.pending_sequences)
        label = "migration" if count == 1 else "migrations"
        message = f"Database has {count} pending {label}"
        return None, [
            DoctorCheck("warning", f"{message}; run 'kantrip doctor --repair'"),
            path_check,
            *_check_database_artifacts(path),
        ]

    try:
        profiles = load_profiles(path, migrate=False)
    except ProfileStoreError as error:
        return None, [DoctorCheck("error", str(error)), path_check]

    if profile_name is not None:
        profile = profiles.profiles.get(profile_name)
        if profile is None:
            return None, [
                DoctorCheck("error", f"profile '{profile_name}' was not found"),
                path_check,
            ]
        profiles = ProfileCollection(
            profiles.path,
            {profile_name: profile},
            {profile_name: profiles.revision(profile_name)},
        )
    profile_count = len(profiles.profiles)
    profile_label = "profile" if profile_count == 1 else "profiles"
    checks = [
        DoctorCheck(
            "success",
            f"Profile database is healthy ({profile_count} {profile_label}, "
            f"schema version {migration_state.current_sequence})",
        ),
        path_check,
    ]
    checks.extend(_check_database_artifacts(path))
    checks.append(_check_profiles(profiles))
    checks.extend(_check_registry_profiles(profiles))
    return profiles, checks


def _check_database_artifacts(path: Path) -> list[DoctorCheck]:
    checks = [_check_private_database_file(path, "Profile database")]
    optional = (
        (Path(f"{path}-journal"), "SQLite journal"),
        (Path(f"{path}-shm"), "SQLite shared-memory file"),
        (Path(f"{path}-wal"), "SQLite WAL"),
        (Path(f"{path}{DATABASE_MAINTENANCE_SUFFIX}"), "Maintenance lock"),
    )
    for candidate, label in optional:
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError:
            checks.append(DoctorCheck("error", f"{label} metadata could not be read"))
        else:
            checks.append(_check_private_database_file(candidate, label))
    try:
        backups = sorted(
            candidate
            for candidate in path.parent.iterdir()
            if candidate.name.startswith(f"{path.name}{DATABASE_BACKUP_PREFIX}")
        )
    except OSError:
        checks.append(DoctorCheck("error", "Migration backups could not be inspected"))
    else:
        checks.extend(
            _check_private_database_file(candidate, "Migration backup") for candidate in backups
        )
    errors = [check for check in checks if check.status == "error"]
    return errors or [DoctorCheck("success", "Profile database files are private")]


def _check_private_database_file(path: Path, label: str) -> DoctorCheck:
    try:
        metadata = path.lstat()
    except OSError:
        return DoctorCheck("error", f"{label} metadata could not be read")
    if not stat.S_ISREG(metadata.st_mode):
        return DoctorCheck("error", f"{label} is not a regular file")
    getuid = getattr(os, "getuid", None)
    if getuid is not None and metadata.st_uid != getuid():
        return DoctorCheck("error", f"{label} is owned by another user")
    exposed_permissions = stat.S_IMODE(metadata.st_mode) & 0o077
    if exposed_permissions:
        return DoctorCheck(
            "error",
            f"{label} permissions are broader than 0600 " f"({stat.S_IMODE(metadata.st_mode):04o})",
        )
    return DoctorCheck("success", f"{label} permissions are private")


def _check_profiles(profiles: ProfileCollection) -> DoctorCheck:
    if not profiles.profiles:
        return DoctorCheck("warning", "No profiles are configured")
    return DoctorCheck("success", "Stored Kafka profile documents are valid")


def _check_registry_profiles(profiles: ProfileCollection) -> list[DoctorCheck]:
    configured = 0
    checks: list[DoctorCheck] = []
    for name, profile in profiles.profiles.items():
        if "registry" not in profile:
            continue
        configured += 1
        try:
            registry_connection(profile)
        except RegistryProfileError as error:
            checks.append(
                DoctorCheck(
                    "error",
                    f"Registry profile '{name}' is not executable: {error}",
                )
            )
    if checks:
        return checks
    if not configured:
        return [DoctorCheck("success", "Registry profiles: none configured")]
    label = "profile" if configured == 1 else "profiles"
    return [
        DoctorCheck(
            "success",
            f"Registry profiles: {configured} {label} configured and executable",
        )
    ]


def _check_session(
    profiles: ProfileCollection | None,
    environment: Mapping[str, str],
) -> list[DoctorCheck]:
    variables = {
        name: environment.get(name)
        for name in ("KANTRIP_PROFILE", "KANTRIP_SESSION_ID", "KANTRIP_SESSION_DIR")
    }
    if not any(variables.values()):
        return [DoctorCheck("success", "No Kantrip profile session is active")]
    missing = [name for name, value in variables.items() if not value]
    if missing:
        return [DoctorCheck("error", f"Active session is missing {', '.join(missing)}")]

    profile_name = variables["KANTRIP_PROFILE"] or ""
    session_directory = Path(variables["KANTRIP_SESSION_DIR"] or "")
    checks: list[DoctorCheck] = []
    if profiles is None or profile_name not in profiles.profiles:
        checks.append(
            DoctorCheck("error", f"Active profile '{profile_name}' is not in the profile database")
        )
    else:
        checks.append(DoctorCheck("success", f"Active profile exists: {profile_name}"))
    if not session_directory.is_dir():
        checks.append(DoctorCheck("error", "Active session directory was not found"))
        checks.append(
            DoctorCheck(
                "success",
                f"Configured active session directory: {session_directory}",
                verbose_only=True,
            )
        )
        return checks
    checks.append(DoctorCheck("success", "Active session directory exists"))
    checks.append(
        DoctorCheck(
            "success",
            f"Active session directory: {session_directory}",
            verbose_only=True,
        )
    )
    shim_directory = session_directory / "bin"
    if shim_directory.is_dir():
        checks.append(_check_session_path(shim_directory, environment))
    return checks


def _check_runtime_sessions(
    environment: Mapping[str, str],
    *,
    profile_id: str | None,
    current_revision: int | None,
    include_details: bool,
) -> list[DoctorCheck]:
    root = resolve_runtime_root(environment)
    path_check = DoctorCheck(
        "success",
        f"Session runtime: {root}",
        verbose_only=True,
    )
    try:
        report = scan_sessions(environment, limit=AUTOMATIC_SCAN_LIMIT)
    except SessionRuntimeError:
        return [DoctorCheck("error", "Session runtime is not private and user-owned"), path_check]
    checks = [path_check]
    if not report.exists:
        return [DoctorCheck("success", "No stored session artifacts were found"), *checks]
    observations = tuple(
        observation
        for observation in report.observations
        if profile_id is None or observation.profile_id == profile_id
    )
    state_counts = {
        state: sum(observation.state == state for observation in observations)
        for state in ("active", "recent", "stale")
    }
    checks.append(_runtime_count("active", state_counts["active"], "success"))
    checks.append(_runtime_count("recent inactive", state_counts["recent"], "warning"))
    checks.append(_runtime_count("stale", state_counts["stale"], "warning"))
    checks.append(_runtime_count("invalid", report.invalid, "error"))
    if report.truncated:
        checks.append(DoctorCheck("error", "Session runtime scan reached its safety limit"))
    if include_details:
        now = int(time.time())
        for observation in observations:
            age = max(0, now - observation.created_at)
            revision_note = ""
            if current_revision is not None and observation.profile_revision < current_revision:
                revision_note = f", older than current revision {current_revision}"
            checks.append(
                DoctorCheck(
                    "warning" if observation.state != "active" else "success",
                    f"Session {observation.state}: revision {observation.profile_revision}, "
                    f"age {age}s{revision_note}",
                )
            )
            checks.extend(
                (
                    DoctorCheck(
                        "success",
                        f"Session ID: {observation.session_id}",
                        verbose_only=True,
                    ),
                    DoctorCheck(
                        "success",
                        f"Supervisor PID: {observation.supervisor_pid}",
                        verbose_only=True,
                    ),
                    DoctorCheck(
                        "success",
                        f"Session path: {observation.path}",
                        verbose_only=True,
                    ),
                )
            )
    return checks


def _runtime_count(label: str, count: int, nonzero_status: CheckStatus) -> DoctorCheck:
    status: CheckStatus = nonzero_status if count else "success"
    noun = "session" if count == 1 else "sessions"
    return DoctorCheck(status, f"Runtime {label}: {count} {noun}")


def _check_session_path(shim_directory: Path, environment: Mapping[str, str]) -> DoctorCheck:
    path_entries = [
        Path(entry).expanduser() for entry in environment.get("PATH", "").split(os.pathsep) if entry
    ]
    shim_index = _path_index(path_entries, shim_directory)
    if shim_index is None:
        return DoctorCheck("error", f"Session adapter directory is not on PATH: {shim_directory}")
    shadows = _adapter_shadows(path_entries[:shim_index])
    if shadows:
        return DoctorCheck(
            "error",
            f"Session adapters are shadowed earlier on PATH: {', '.join(shadows)}",
        )
    return DoctorCheck("success", "No supported commands shadow session adapters on PATH")


def _path_index(entries: Sequence[Path], target: Path) -> int | None:
    normalized_target = target.resolve()
    for index, entry in enumerate(entries):
        if entry.resolve() == normalized_target:
            return index
    return None


def _adapter_shadows(directories: Sequence[Path]) -> list[str]:
    return sorted(
        executable
        for executable in ADAPTER_EXECUTABLES
        if any(_is_executable(directory / executable) for directory in directories)
    )


def _is_executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _check_commands(environment: Mapping[str, str]) -> list[DoctorCheck]:
    search_path = environment.get("PATH")
    return [
        _check_command_group("kcat", (KCAT_EXECUTABLES,), search_path),
        *_check_command_paths("kcat", (("executable", KCAT_EXECUTABLES),), search_path),
        _check_kafka_commands(search_path),
        *_check_command_paths("Kafka", _KAFKA_COMMAND_GROUPS, search_path),
        _check_command_group(
            "Schema Registry console", _SCHEMA_REGISTRY_COMMAND_GROUPS, search_path
        ),
        *_check_command_paths(
            "",
            tuple((next(iter(names)), names) for names in _SCHEMA_REGISTRY_COMMAND_GROUPS),
            search_path,
        ),
        _check_command_group("Kaskade", (KASKADE_EXECUTABLES,), search_path),
        *_check_command_paths("Kaskade", (("executable", KASKADE_EXECUTABLES),), search_path),
    ]


def _check_profile_clients(
    profiles: ProfileCollection | None,
    environment: Mapping[str, str],
) -> list[DoctorCheck]:
    """Report installed adapters that can execute each selected profile."""
    if profiles is None:
        return []
    search_path = environment.get("PATH")
    installed = (
        ("kcat", _find_first(KCAT_EXECUTABLES, search_path)),
        ("Kaskade", _find_first(KASKADE_EXECUTABLES, search_path)),
        ("Apache/Confluent Java CLI", _find_first(KAFKA_EXECUTABLES, search_path)),
    )
    checks: list[DoctorCheck] = []
    for name, profile in profiles.profiles.items():
        try:
            connection = kafka_connection(profile)
        except KafkaProfileError:
            continue
        compatible: list[str] = []
        rejected: list[str] = []
        custom_pem = connection.ca_certificates is not None or connection.auth_type == "mtls"
        for label, executable in installed:
            if executable is None:
                continue
            try:
                require_adapter_capability(
                    executable,
                    auth_type=connection.auth_type,
                    custom_pem=custom_pem,
                    environment=environment,
                )
            except AdapterError as error:
                rejected.append(f"{label}: {error}")
            else:
                compatible.append(label)
        if compatible:
            checks.append(
                DoctorCheck(
                    "success",
                    f"Profile '{name}' compatible installed clients: {', '.join(compatible)}",
                )
            )
        else:
            checks.append(
                DoctorCheck("warning", f"Profile '{name}' has no compatible client on PATH")
            )
        checks.extend(
            DoctorCheck("warning", f"Profile '{name}' client rejected: {message}")
            for message in rejected
        )
    return checks


def _check_command_paths(
    label: str,
    groups: Sequence[tuple[str, frozenset[str]]],
    search_path: str | None,
) -> list[DoctorCheck]:
    return [
        DoctorCheck(
            "success",
            f"{label + ' ' if label else ''}{group}: {resolved}",
            verbose_only=True,
        )
        for group, names in groups
        if (resolved := _find_first(names, search_path)) is not None
    ]


def _check_kafka_commands(search_path: str | None) -> DoctorCheck:
    missing = [
        label for label, names in _KAFKA_COMMAND_GROUPS if _find_first(names, search_path) is None
    ]
    installed_count = len(_KAFKA_COMMAND_GROUPS) - len(missing)
    if not installed_count:
        return DoctorCheck("warning", "Apache Kafka CLI commands were not found on PATH")
    if missing:
        return DoctorCheck(
            "warning",
            f"Apache Kafka CLI: {installed_count}/{len(_KAFKA_COMMAND_GROUPS)} command groups "
            f"installed; missing: {', '.join(missing)}",
        )
    return DoctorCheck(
        "success", f"Apache Kafka CLI: all {len(_KAFKA_COMMAND_GROUPS)} command groups installed"
    )


def _check_command_group(
    label: str,
    alternatives: Sequence[frozenset[str]],
    search_path: str | None,
) -> DoctorCheck:
    resolved = [(names, _find_first(names, search_path)) for names in alternatives]
    installed = [path for _, path in resolved if path is not None]
    missing = [" / ".join(sorted(names)) for names, path in resolved if path is None]
    if not installed:
        return DoctorCheck(
            "warning",
            f"{label} commands were not found on PATH; missing: {', '.join(missing)}",
        )
    if len(installed) != len(alternatives):
        return DoctorCheck(
            "warning",
            f"{label}: {len(installed)}/{len(alternatives)} command groups installed; "
            f"missing: {', '.join(missing)}",
        )
    if len(installed) == 1:
        return DoctorCheck("success", f"{label}: installed")
    return DoctorCheck("success", f"{label}: all {len(installed)} command groups installed")


def _find_first(names: frozenset[str], search_path: str | None) -> str | None:
    for name in sorted(names):
        if resolved := shutil.which(name, path=search_path):
            return resolved
    return None


__all__ = ["DoctorCheck", "DoctorReport", "run_doctor"]
