"""Define and apply Kantrip's bundled SQLite migration chain."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from . import migration_0001_initial_profile_store as initial
from . import migration_0002_add_reconciliation_journal as reconciliation


class MigrationError(ValueError):
    """Raised when migration history or database structure is unsafe."""


@dataclass(frozen=True)
class Migration:
    """One immutable, ordered database change."""

    sequence: int
    name: str
    statements: tuple[str, ...]

    @property
    def checksum(self) -> str:
        """Return a stable checksum for the migration identity and payload."""
        payload = json.dumps(
            {
                "sequence": self.sequence,
                "name": self.name,
                "statements": self.statements,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class MigrationState:
    """Read-only classification of a profile database migration state."""

    current_sequence: int
    target_sequence: int
    pending_sequences: tuple[int, ...] = ()
    new_database: bool = False

    @property
    def requires_migration(self) -> bool:
        """Return whether the database needs a known deterministic change."""
        return bool(self.pending_sequences or self.new_database)


@dataclass(frozen=True)
class MigrationResult:
    """Changes made by one migration pass."""

    previous_sequence: int
    current_sequence: int
    applied_sequences: tuple[int, ...] = ()

    @property
    def changed(self) -> bool:
        """Return whether history or schema changed."""
        return bool(self.applied_sequences)


_CREATE_HISTORY = """
    CREATE TABLE schema_migrations (
        sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
        name TEXT NOT NULL,
        checksum TEXT NOT NULL,
        applied_at TEXT NOT NULL,
        applied_by TEXT NOT NULL
    )
    """

MIGRATIONS = (
    Migration(initial.SEQUENCE, initial.NAME, initial.STATEMENTS),
    Migration(reconciliation.SEQUENCE, reconciliation.NAME, reconciliation.STATEMENTS),
)
LATEST_SEQUENCE = MIGRATIONS[-1].sequence


def inspect_migrations(connection: sqlite3.Connection) -> MigrationState:
    """Validate migration history without changing the database."""
    _validate_registry()
    version = _database_version(connection)
    tables = _table_names(connection)
    if "schema_migrations" not in tables:
        return _inspect_without_history(version, tables)

    _verify_history_schema(connection)
    rows = connection.execute(
        "SELECT sequence, name, checksum, applied_at, applied_by "
        "FROM schema_migrations ORDER BY sequence"
    ).fetchall()
    _validate_history_rows(rows)

    current = int(rows[-1]["sequence"]) if rows else 0
    if version > LATEST_SEQUENCE:
        raise MigrationError(f"profile database version {version} is not supported")
    if version != current:
        raise MigrationError("profile database version does not match its migration history")
    if current == LATEST_SEQUENCE:
        _verify_current_schema(connection)
    elif current == 0 and tables != {"schema_migrations"}:
        raise MigrationError("profile database schema is invalid")
    return MigrationState(current, LATEST_SEQUENCE, _pending_after(current))


def _inspect_without_history(version: int, tables: set[str]) -> MigrationState:
    if not tables and version == 0:
        return MigrationState(0, LATEST_SEQUENCE, _pending_after(0), new_database=True)
    if version > LATEST_SEQUENCE:
        raise MigrationError(f"profile database version {version} is not supported")
    raise MigrationError("profile database schema is invalid")


def _validate_history_rows(rows: list[sqlite3.Row]) -> None:
    if len(rows) > len(MIGRATIONS):
        raise MigrationError("profile database contains unknown migrations")
    for index, row in enumerate(rows):
        expected = MIGRATIONS[index]
        sequence = int(row["sequence"])
        if sequence != expected.sequence:
            raise MigrationError("profile database migration history is not contiguous")
        if row["name"] != expected.name or row["checksum"] != expected.checksum:
            raise MigrationError(f"profile database migration {sequence} was modified")
        if not _nonempty_text(row["applied_at"]) or not _nonempty_text(row["applied_by"]):
            raise MigrationError("profile database migration history is invalid")


def apply_migrations(connection: sqlite3.Connection, *, applied_by: str) -> MigrationResult:
    """Apply all known migrations in one immediate transaction."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        state = inspect_migrations(connection)
        previous = state.current_sequence
        if not state.requires_migration:
            connection.execute("COMMIT")
            return MigrationResult(previous, previous)

        if state.new_database:
            connection.execute(_CREATE_HISTORY)
        for migration in MIGRATIONS:
            if migration.sequence <= state.current_sequence:
                continue
            for statement in migration.statements:
                connection.execute(statement)
            _record_migration(connection, migration, applied_by)

        connection.execute(f"PRAGMA user_version = {LATEST_SEQUENCE}")
        final_state = inspect_migrations(connection)
        if final_state.requires_migration:
            raise MigrationError("profile database migration did not reach the current schema")
        connection.execute("COMMIT")
        return MigrationResult(
            previous,
            final_state.current_sequence,
            state.pending_sequences,
        )
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def _record_migration(
    connection: sqlite3.Connection,
    migration: Migration,
    applied_by: str,
) -> None:
    connection.execute(
        "INSERT INTO schema_migrations "
        "(sequence, name, checksum, applied_at, applied_by) VALUES (?, ?, ?, ?, ?)",
        (
            migration.sequence,
            migration.name,
            migration.checksum,
            datetime.now(timezone.utc).isoformat(),
            applied_by,
        ),
    )


def _pending_after(sequence: int) -> tuple[int, ...]:
    return tuple(migration.sequence for migration in MIGRATIONS if migration.sequence > sequence)


def _validate_registry() -> None:
    sequences = tuple(migration.sequence for migration in MIGRATIONS)
    if sequences != tuple(range(1, len(MIGRATIONS) + 1)):
        raise MigrationError("bundled database migrations are not contiguous")
    if any(not migration.name.strip() or not migration.statements for migration in MIGRATIONS):
        raise MigrationError("bundled database migration metadata is invalid")


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute(
            "SELECT name FROM sqlite_master " "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }


def _verify_current_schema(connection: sqlite3.Connection) -> None:
    if _table_names(connection) != {
        "credential_reconciliation",
        "profiles",
        "schema_migrations",
    }:
        raise MigrationError("profile database schema is invalid")
    _verify_profiles_schema(connection)
    _verify_history_schema(connection)
    _verify_reconciliation_schema(connection)


def _verify_profiles_schema(connection: sqlite3.Connection) -> None:
    columns = connection.execute("PRAGMA table_info(profiles)").fetchall()
    expected_columns = [
        (0, "name", "TEXT", 1, None, 1),
        (1, "id", "TEXT", 1, None, 0),
        (2, "revision", "INTEGER", 1, None, 0),
        (3, "document", "TEXT", 1, None, 0),
    ]
    if [tuple(row) for row in columns] != expected_columns:
        raise MigrationError("profile database schema is invalid")
    unique_columns = {
        tuple(
            row["name"]
            for row in connection.execute(f"PRAGMA index_info({index['name']})").fetchall()
        )
        for index in connection.execute("PRAGMA index_list(profiles)").fetchall()
        if index["unique"]
    }
    if unique_columns != {("name",), ("id",)}:
        raise MigrationError("profile database schema is invalid")


def _verify_history_schema(connection: sqlite3.Connection) -> None:
    columns = connection.execute("PRAGMA table_info(schema_migrations)").fetchall()
    expected_columns = [
        (0, "sequence", "INTEGER", 0, None, 1),
        (1, "name", "TEXT", 1, None, 0),
        (2, "checksum", "TEXT", 1, None, 0),
        (3, "applied_at", "TEXT", 1, None, 0),
        (4, "applied_by", "TEXT", 1, None, 0),
    ]
    if [tuple(row) for row in columns] != expected_columns:
        raise MigrationError("profile database migration history schema is invalid")


def _verify_reconciliation_schema(connection: sqlite3.Connection) -> None:
    columns = connection.execute("PRAGMA table_info(credential_reconciliation)").fetchall()
    expected_columns = [
        (0, "id", "TEXT", 1, None, 1),
        (1, "secret_reference", "TEXT", 1, None, 0),
        (2, "created_at", "TEXT", 1, None, 0),
    ]
    if [tuple(row) for row in columns] != expected_columns:
        raise MigrationError("credential reconciliation schema is invalid")
    unique_columns = {
        tuple(
            row["name"]
            for row in connection.execute(f"PRAGMA index_info({index['name']})").fetchall()
        )
        for index in connection.execute("PRAGMA index_list(credential_reconciliation)").fetchall()
        if index["unique"]
    }
    if unique_columns != {("id",), ("secret_reference",)}:
        raise MigrationError("credential reconciliation schema is invalid")


def _database_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("PRAGMA user_version").fetchone()
    if row is None:
        raise MigrationError("profile database version could not be read")
    return int(row[0])


def _nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


__all__ = [
    "LATEST_SEQUENCE",
    "MIGRATIONS",
    "Migration",
    "MigrationError",
    "MigrationResult",
    "MigrationState",
    "apply_migrations",
    "inspect_migrations",
]
