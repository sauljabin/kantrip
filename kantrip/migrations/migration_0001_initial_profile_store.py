"""Create the initial SQLite profile store."""

SEQUENCE = 1
NAME = "initial profile store"
STATEMENTS = (
    """
    CREATE TABLE profiles (
        name TEXT PRIMARY KEY NOT NULL,
        id TEXT UNIQUE NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        document TEXT NOT NULL
    )
    """,
)

__all__ = ["NAME", "SEQUENCE", "STATEMENTS"]
