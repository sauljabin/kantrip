"""Define Kantrip's ordered database migrations."""

from __future__ import annotations

from textwrap import dedent, indent

from kantrip.migrations.engine import MigrationChain, SqlMigration


def _sql(value: str) -> str:
    """Keep SQL payloads stable regardless of their source indentation."""
    return f"\n{indent(dedent(value).strip(), '    ')}\n    "


class InitialProfileStore(SqlMigration):
    """Create the initial SQLite profile store."""

    sequence = 1
    name = "initial profile store"
    statements = (
        _sql("""
            CREATE TABLE profiles (
                name TEXT PRIMARY KEY NOT NULL,
                id TEXT UNIQUE NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                document TEXT NOT NULL
            )
            """),
    )


class AddReconciliationJournal(SqlMigration):
    """Add the exact-reference credential cleanup journal."""

    sequence = 2
    name = "add reconciliation journal"
    statements = (
        _sql("""
            CREATE TABLE credential_reconciliation (
                id TEXT PRIMARY KEY NOT NULL,
                secret_reference TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL
            )
            """),
    )


MIGRATIONS = MigrationChain(
    InitialProfileStore(),
    AddReconciliationJournal(),
)

__all__ = [
    "MIGRATIONS",
]
