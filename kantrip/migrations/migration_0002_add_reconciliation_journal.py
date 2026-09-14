"""Add the exact-reference credential cleanup journal."""

SEQUENCE = 2
NAME = "add reconciliation journal"
STATEMENTS = (
    """
    CREATE TABLE credential_reconciliation (
        id TEXT PRIMARY KEY NOT NULL,
        secret_reference TEXT UNIQUE NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
)

__all__ = ["NAME", "SEQUENCE", "STATEMENTS"]
