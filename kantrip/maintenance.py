"""Run Kantrip's explicit, deterministic local repair workflow."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from kantrip.profiles import (
    DATABASE_SCHEMA_VERSION,
    ProfileStoreError,
    database_maintenance_lock,
    migrate_profile_database,
    resolve_database_path,
)
from kantrip.runtime import SessionRuntimeError, scan_sessions

RepairStatus = Literal["success", "cleanup", "error"]


@dataclass(frozen=True)
class RepairAction:
    """One result from an explicit maintenance pass."""

    status: RepairStatus
    message: str


@dataclass(frozen=True)
class RepairReport:
    """Aggregate result of database migration and runtime cleanup."""

    actions: tuple[RepairAction, ...]

    @property
    def healthy(self) -> bool:
        """Return whether every requested repair completed safely."""
        return all(action.status != "error" for action in self.actions)


def run_repair(environment: Mapping[str, str] | None = None) -> RepairReport:
    """Migrate profiles and completely clean safe stale session artifacts."""
    database_path = resolve_database_path(environment)
    actions: list[RepairAction] = []
    if not _exists(database_path):
        actions.append(
            RepairAction("success", "Profile database was not found; no migration was needed")
        )
        actions.append(_repair_sessions(environment))
        return RepairReport(tuple(actions))

    try:
        with database_maintenance_lock(database_path):
            try:
                result = migrate_profile_database(
                    database_path,
                    missing_ok=False,
                    lock_held=True,
                )
            except ProfileStoreError as error:
                actions.append(RepairAction("error", str(error)))
            else:
                if result.applied_sequences:
                    sequences = ", ".join(str(sequence) for sequence in result.applied_sequences)
                    message = f"Applied database migrations: {sequences}"
                else:
                    message = (
                        "Profile database schema is current "
                        f"(sequence {DATABASE_SCHEMA_VERSION})"
                    )
                actions.append(RepairAction("success", message))
            actions.append(_repair_sessions(environment))
    except ProfileStoreError as error:
        actions.append(RepairAction("error", str(error)))
    return RepairReport(tuple(actions))


def _repair_sessions(environment: Mapping[str, str] | None) -> RepairAction:
    try:
        report = scan_sessions(environment, remove=True)
    except SessionRuntimeError:
        return RepairAction("error", "Session runtime could not be repaired safely")
    message = (
        f"Sessions: removed {report.removed} stale; active {report.active}; "
        f"recent {report.recent}; invalid {report.invalid}; failed {report.failed}"
    )
    if report.truncated:
        message = f"{message}; scan incomplete"
    return RepairAction("error" if report.has_errors else "cleanup", message)


def _exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


__all__ = ["RepairAction", "RepairReport", "run_repair"]
