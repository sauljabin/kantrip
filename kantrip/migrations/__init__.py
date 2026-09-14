"""Expose Kantrip's bundled migration chain through a stable package API."""

from __future__ import annotations

import sqlite3

from kantrip.migrations.engine import (
    MigrationChain,
    MigrationError,
    MigrationResult,
    MigrationState,
    SqlMigration,
)
from kantrip.migrations.engine import apply_migrations as _apply_migrations
from kantrip.migrations.engine import inspect_migrations as _inspect_migrations
from kantrip.migrations.versions import MIGRATIONS

LATEST_SEQUENCE = MIGRATIONS.latest_sequence


def inspect_migrations(connection: sqlite3.Connection) -> MigrationState:
    """Inspect the database against Kantrip's bundled migration chain."""
    return _inspect_migrations(connection, MIGRATIONS)


def apply_migrations(
    connection: sqlite3.Connection,
    *,
    applied_by: str,
) -> MigrationResult:
    """Apply Kantrip's bundled migration chain to a database."""
    return _apply_migrations(connection, MIGRATIONS, applied_by=applied_by)


__all__ = [
    "LATEST_SEQUENCE",
    "MIGRATIONS",
    "MigrationChain",
    "MigrationError",
    "MigrationResult",
    "MigrationState",
    "SqlMigration",
    "apply_migrations",
    "inspect_migrations",
]
