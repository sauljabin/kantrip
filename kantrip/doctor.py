"""Run local, read-only diagnostics for a Kantrip installation."""

from __future__ import annotations

import os
import shutil
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from kantrip import APP_VERSION
from kantrip.adapters import (
    ADAPTER_EXECUTABLES,
    KAFKA_ACLS_EXECUTABLES,
    KAFKA_BROKER_API_VERSIONS_EXECUTABLES,
    KAFKA_CONFIGS_EXECUTABLES,
    KAFKA_CONSOLE_CONSUMER_EXECUTABLES,
    KAFKA_CONSOLE_PRODUCER_EXECUTABLES,
    KAFKA_CONSUMER_GROUPS_EXECUTABLES,
    KAFKA_TOPICS_EXECUTABLES,
    KASKADE_EXECUTABLES,
    KCAT_EXECUTABLES,
    SCHEMA_REGISTRY_EXECUTABLES,
)
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
from kantrip.registry import RegistryProfileError, plain_registry_connection
from kantrip.runtime import (
    AUTOMATIC_SCAN_LIMIT,
    SessionRuntimeError,
    resolve_runtime_root,
    scan_sessions,
)
from kantrip.secret_store import SecretStoreError, load_secret_store
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


def run_doctor(environment: Mapping[str, str] | None = None) -> DoctorReport:
    """Inspect Kantrip's local environment without contacting configured services."""
    env = os.environ if environment is None else environment
    system_checks = [
        DoctorCheck("success", f"Kantrip {APP_VERSION}"),
        _check_cli(env),
        _check_python(),
        _check_platform(),
        _check_shell(env),
    ]
    profiles, profile_checks = _check_profile_database(env)
    checks = [
        *_assign_section("System", system_checks),
        *_assign_section("Profiles", profile_checks),
        *_assign_section("Credentials", _check_credentials(env)),
        *_assign_section(
            "Session",
            [*_check_session(profiles, env), *_check_runtime_sessions(env)],
        ),
        *_assign_section("Clients", _check_commands(env)),
    ]
    return DoctorReport(tuple(checks))


def _check_credentials(environment: Mapping[str, str]) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
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
    return checks


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
    return DoctorCheck("success", "All configured Kafka profiles are executable")


def _check_registry_profiles(profiles: ProfileCollection) -> list[DoctorCheck]:
    configured = 0
    checks: list[DoctorCheck] = []
    for name, profile in profiles.profiles.items():
        if "registry" not in profile:
            continue
        configured += 1
        try:
            plain_registry_connection(profile)
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


def _check_runtime_sessions(environment: Mapping[str, str]) -> list[DoctorCheck]:
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
    checks.append(_runtime_count("active", report.active, "success"))
    checks.append(_runtime_count("recent inactive", report.recent, "warning"))
    checks.append(_runtime_count("stale", report.stale, "warning"))
    checks.append(_runtime_count("invalid", report.invalid, "error"))
    if report.truncated:
        checks.append(DoctorCheck("error", "Session runtime scan reached its safety limit"))
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
