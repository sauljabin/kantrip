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
from kantrip.config import (
    Configuration,
    ConfigurationError,
    load_configuration,
    resolve_config_path,
)
from kantrip.schema_registry import SchemaRegistryProfileError, plain_schema_registry_url
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
        _check_python(),
        _check_platform(),
        _check_cli(env),
        _check_shell(env),
    ]
    configuration, config_checks = _check_configuration(env)
    checks = [
        *_assign_section("System", system_checks),
        *_assign_section("Configuration", config_checks),
        *_assign_section("Session", _check_session(configuration, env)),
        *_assign_section("Clients", _check_commands(env)),
    ]
    return DoctorReport(tuple(checks))


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


def _check_configuration(
    environment: Mapping[str, str],
) -> tuple[Configuration | None, list[DoctorCheck]]:
    path = resolve_config_path(environment)
    if not path.exists():
        return None, [DoctorCheck("warning", f"Configuration file was not found: {path}")]
    try:
        configuration = load_configuration(path)
    except ConfigurationError as error:
        return None, [DoctorCheck("error", str(error))]

    profile_count = len(configuration.profiles)
    profile_label = "profile" if profile_count == 1 else "profiles"
    checks = [
        DoctorCheck(
            "success",
            f"Configuration matches schema: {path} ({profile_count} {profile_label})",
        )
    ]
    checks.append(_check_config_file(path))
    checks.append(_check_profile_ids(configuration))
    checks.append(_check_profiles(configuration))
    checks.extend(_check_schema_registry_profiles(configuration))
    return configuration, checks


def _check_config_file(path: Path) -> DoctorCheck:
    try:
        metadata = path.stat()
    except OSError:
        return DoctorCheck("error", f"Configuration metadata could not be read: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        return DoctorCheck("error", f"Configuration is not a regular file: {path}")
    getuid = getattr(os, "getuid", None)
    if getuid is not None and metadata.st_uid != getuid():
        return DoctorCheck("warning", f"Configuration is owned by another user: {path}")
    exposed_permissions = stat.S_IMODE(metadata.st_mode) & 0o077
    if exposed_permissions:
        return DoctorCheck(
            "warning",
            f"Configuration permissions are broader than 0600: {path} "
            f"({stat.S_IMODE(metadata.st_mode):04o})",
        )
    return DoctorCheck("success", f"Configuration file permissions are private ({path})")


def _check_profile_ids(configuration: Configuration) -> DoctorCheck:
    ids = [str(profile["id"]) for profile in configuration.profiles.values()]
    if len(ids) != len(set(ids)):
        return DoctorCheck("error", "Configuration contains duplicate profile IDs")
    return DoctorCheck("success", "Profile IDs are unique", verbose_only=True)


def _check_profiles(configuration: Configuration) -> DoctorCheck:
    if not configuration.profiles:
        return DoctorCheck("warning", "No profiles are configured")
    return DoctorCheck("success", "All configured Kafka profiles are executable")


def _check_schema_registry_profiles(configuration: Configuration) -> list[DoctorCheck]:
    configured = 0
    checks: list[DoctorCheck] = []
    for name, profile in configuration.profiles.items():
        if "schemaRegistry" not in profile:
            continue
        configured += 1
        try:
            plain_schema_registry_url(profile)
        except SchemaRegistryProfileError as error:
            checks.append(
                DoctorCheck(
                    "error",
                    f"Schema Registry profile '{name}' is not executable: {error}",
                )
            )
    if checks:
        return checks
    if not configured:
        return [DoctorCheck("success", "Schema Registry profiles: none configured")]
    label = "profile" if configured == 1 else "profiles"
    return [
        DoctorCheck(
            "success",
            f"Schema Registry profiles: {configured} {label} configured and executable",
        )
    ]


def _check_session(
    configuration: Configuration | None,
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
    if configuration is None or profile_name not in configuration.profiles:
        checks.append(
            DoctorCheck("error", f"Active profile '{profile_name}' is not in the configuration")
        )
    else:
        checks.append(DoctorCheck("success", f"Active profile exists: {profile_name}"))
    if not session_directory.is_dir():
        checks.append(
            DoctorCheck("error", f"Active session directory was not found: {session_directory}")
        )
        return checks
    checks.append(DoctorCheck("success", f"Active session directory: {session_directory}"))
    shim_directory = session_directory / "bin"
    if shim_directory.is_dir():
        checks.append(_check_session_path(shim_directory, environment))
    return checks


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
        _check_kafka_commands(search_path),
        _check_command_group(
            "Schema Registry console", _SCHEMA_REGISTRY_COMMAND_GROUPS, search_path
        ),
        _check_command_group("Kaskade", (KASKADE_EXECUTABLES,), search_path),
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
        return DoctorCheck("success", f"{label}: {installed[0]}")
    return DoctorCheck("success", f"{label}: all {len(installed)} command groups installed")


def _find_first(names: frozenset[str], search_path: str | None) -> str | None:
    for name in sorted(names):
        if resolved := shutil.which(name, path=search_path):
            return resolved
    return None


__all__ = ["DoctorCheck", "DoctorReport", "run_doctor"]
