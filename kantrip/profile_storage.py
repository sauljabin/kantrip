"""Own the private SQLite profile database: paths, locks, connections, and loading."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import sys
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import closing, contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from kantrip import APP_VERSION
from kantrip.kafka import KafkaProfileError, kafka_connection
from kantrip.migrations import (
    LATEST_SEQUENCE,
    MigrationError,
    MigrationResult,
    MigrationState,
    apply_migrations,
    inspect_migrations,
)
from kantrip.reconciliation import (
    CleanupRecord,
    ReconciliationError,
    ReconciliationResult,
    pending_secret_cleanup,
    reconcile_secret_cleanup,
)
from kantrip.registry import RegistryProfileError, registry_connection
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store

DATABASE_FILENAME = "profiles.db"


DATABASE_SCHEMA_VERSION = LATEST_SEQUENCE


DATABASE_TIMEOUT_SECONDS = 5.0


DATABASE_BACKUP_PREFIX = ".pre-migration-"


DATABASE_MAINTENANCE_SUFFIX = ".maintenance.lock"


SCHEMA_FILENAME = "profile.schema.json"


_PROFILE_NAME_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,127})\Z")


_SQLITE_PRIVATE_SUFFIXES = (
    "",
    "-journal",
    "-shm",
    "-wal",
    DATABASE_MAINTENANCE_SUFFIX,
)


class ProfileStoreError(ValueError):
    """Raised when the profile database cannot be used safely."""

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class ProfileCollection:
    """A validated snapshot of profiles loaded from one database."""

    path: Path
    profiles: dict[str, dict[str, Any]]
    revisions: dict[str, int] = field(default_factory=dict)

    def profile(self, name: str) -> dict[str, Any]:
        """Return one profile or raise an actionable profile error."""
        try:
            return self.profiles[name]
        except KeyError as error:
            raise ProfileStoreError(f"profile '{name}' was not found") from error

    def revision(self, name: str) -> int:
        """Return one profile revision or raise an actionable profile error."""
        self.profile(name)
        try:
            return self.revisions[name]
        except KeyError as error:
            raise ProfileStoreError(f"profile '{name}' has no revision metadata") from error


def resolve_database_path(environment: Mapping[str, str] | None = None) -> Path:
    """Resolve the documented profile database path without creating it."""
    env = os.environ if environment is None else environment
    configured = env.get("KANTRIP_DATABASE")
    if configured:
        return Path(configured).expanduser()

    data_home = env.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home) / "kantrip" / DATABASE_FILENAME

    home = env.get("HOME")
    if home:
        return Path(home) / ".local" / "share" / "kantrip" / DATABASE_FILENAME
    return Path.home() / ".local" / "share" / "kantrip" / DATABASE_FILENAME


def load_profiles(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    missing_ok: bool = False,
    migrate: bool = True,
) -> ProfileCollection:
    """Load a validated snapshot, applying known migrations when requested."""
    database_path = path if path is not None else resolve_database_path(environment)
    if not path_entry_exists(database_path):
        if missing_ok:
            return ProfileCollection(database_path, {})
        raise ProfileStoreError(f"profile database was not found: {database_path}")

    validate_private_parent(database_path.parent)
    validate_database_file(database_path)
    try:
        with closing(connect(database_path, writable=False)) as connection:
            state = inspect_migration_state(connection)
            if not state.requires_migration:
                return load_profile_collection(database_path, connection)
        if not migrate:
            raise ProfileStoreError(pending_migration_message(state))
        migrate_profile_database(database_path)
        with closing(connect(database_path, writable=False)) as connection:
            state = inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(pending_migration_message(state))
            return load_profile_collection(database_path, connection)
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be read safely") from error


def inspect_profile_database(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> MigrationState:
    """Inspect migration state without modifying the database or its directory."""
    database_path = path if path is not None else resolve_database_path(environment)
    if not path_entry_exists(database_path):
        raise ProfileStoreError(f"profile database was not found: {database_path}")
    validate_private_parent(database_path.parent)
    validate_database_file(database_path)
    try:
        with closing(connect(database_path, writable=False)) as connection:
            return inspect_migration_state(connection)
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be read safely") from error


def migrate_profile_database(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    missing_ok: bool = False,
    lock_held: bool = False,
) -> MigrationResult:
    """Bring one existing database to the latest bundled migration sequence."""
    database_path = path if path is not None else resolve_database_path(environment)
    if not path_entry_exists(database_path):
        if missing_ok:
            return MigrationResult(0, 0)
        raise ProfileStoreError(f"profile database was not found: {database_path}")
    validate_private_parent(database_path.parent)
    validate_database_file(database_path)
    lock = nullcontext() if lock_held else database_maintenance_lock(database_path)
    with lock:
        return _migrate_existing_database(database_path)


def inspect_pending_secret_cleanup(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> tuple[CleanupRecord, ...]:
    """Read and validate credential reconciliation state without modifying it."""
    database_path = path if path is not None else resolve_database_path(environment)
    if not path_entry_exists(database_path):
        return ()
    validate_private_parent(database_path.parent)
    validate_database_file(database_path)
    try:
        with closing(connect(database_path, writable=False)) as connection:
            state = inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(pending_migration_message(state))
            return pending_secret_cleanup(connection)
    except (ProfileStoreError, ReconciliationError):
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("credential reconciliation journal could not be read") from error


def reconcile_pending_secrets(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    store: SecretStore | None = None,
    lock_held: bool = False,
) -> ReconciliationResult:
    """Reconcile every exact pending credential reference under the mutation lock."""
    database_path = path if path is not None else resolve_database_path(environment)
    if not path_entry_exists(database_path):
        return ReconciliationResult(0, 0, 0)
    validate_private_parent(database_path.parent)
    validate_database_file(database_path)
    lock = nullcontext() if lock_held else database_maintenance_lock(database_path)
    try:
        with lock, closing(connect(database_path, writable=True)) as connection:
            state = inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(pending_migration_message(state))
            records = pending_secret_cleanup(connection)
            if not records:
                return ReconciliationResult(0, 0, 0)
            selected_store = store or load_secret_store()
            return reconcile_secret_cleanup(connection, selected_store)
    except (ProfileStoreError, ReconciliationError, SecretStoreError):
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("credential reconciliation journal could not be updated") from error
    finally:
        _harden_sqlite_files(database_path)


@contextmanager
def database_maintenance_lock(path: Path) -> Iterator[None]:
    """Serialize database migration and explicit maintenance work."""
    validate_private_parent(path.parent)
    lock_path = Path(f"{path}{DATABASE_MAINTENANCE_SUFFIX}")
    descriptor = _open_private_lock(lock_path)
    try:
        _acquire_bounded_lock(descriptor)
        yield
    finally:
        os.close(descriptor)


@contextmanager
def writable_connection(path: Path) -> Iterator[sqlite3.Connection]:
    _ensure_private_parent(path.parent)
    with database_maintenance_lock(path):
        _create_or_validate_database_file(path)
        connection: sqlite3.Connection | None = None
        body_completed = False
        try:
            connection = connect(path, writable=True)
            path.chmod(0o600)
            validate_database_file(path)
            _migrate_connection(connection, path)
            _configure_writable_connection(connection)
            yield connection
            body_completed = True
        except OSError as error:
            raise ProfileStoreError("profile database could not be opened safely") from error
        finally:
            cleanup_error: BaseException | None = None
            if connection is not None:
                try:
                    connection.close()
                except (OSError, sqlite3.Error) as error:
                    cleanup_error = error
            try:
                _harden_sqlite_files(path, strict=True)
            except OSError as error:
                cleanup_error = cleanup_error or error
            if body_completed and cleanup_error is not None:
                raise ProfileStoreError(
                    "profile database committed but its private files could not be verified",
                    exit_code=3,
                ) from cleanup_error


def connect(path: Path, *, writable: bool) -> sqlite3.Connection:
    if writable:
        connection = sqlite3.connect(
            path,
            timeout=DATABASE_TIMEOUT_SECONDS,
            isolation_level=None,
        )
    else:
        uri = f"{path.absolute().as_uri()}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=DATABASE_TIMEOUT_SECONDS,
            isolation_level=None,
        )
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout = {int(DATABASE_TIMEOUT_SECONDS * 1000)}")
    connection.execute("PRAGMA foreign_keys = ON")
    if writable:
        _configure_writable_connection(connection)
    return connection


def _migrate_existing_database(path: Path) -> MigrationResult:
    validate_private_parent(path.parent)
    validate_database_file(path)
    try:
        with closing(connect(path, writable=True)) as connection:
            result = _migrate_connection(connection, path)
            _configure_writable_connection(connection)
            return result
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be migrated safely") from error
    finally:
        _harden_sqlite_files(path)


def _migrate_connection(connection: sqlite3.Connection, path: Path) -> MigrationResult:
    state = inspect_migration_state(connection)
    if not state.requires_migration:
        return MigrationResult(state.current_sequence, state.current_sequence)
    if not state.new_database:
        _create_database_backup(connection, path)
    try:
        return apply_migrations(connection, applied_by=APP_VERSION)
    except MigrationError as error:
        raise ProfileStoreError(str(error)) from error


def inspect_migration_state(connection: sqlite3.Connection) -> MigrationState:
    try:
        return inspect_migrations(connection)
    except MigrationError as error:
        raise ProfileStoreError(str(error)) from error


def pending_migration_message(state: MigrationState) -> str:
    sequences = ", ".join(str(sequence) for sequence in state.pending_sequences)
    return f"profile database requires migration sequence {sequences}"


def rollback(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        connection.execute("ROLLBACK")


def load_profile_collection(path: Path, connection: sqlite3.Connection) -> ProfileCollection:
    revisions: dict[str, int] = {}
    profiles = load_profile_rows(connection, revisions=revisions)
    return ProfileCollection(path, profiles, revisions)


def load_profile_rows(
    connection: sqlite3.Connection,
    *,
    revisions: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    rows = connection.execute(
        "SELECT name, id, revision, document FROM profiles ORDER BY name"
    ).fetchall()
    for row in rows:
        name = str(row["name"])
        validate_profile_name(name)
        document = row["document"]
        if not isinstance(document, str):
            raise ProfileStoreError(f"stored profile '{name}' is not valid JSON")
        try:
            profile = json.loads(document)
        except (TypeError, json.JSONDecodeError) as error:
            raise ProfileStoreError(f"stored profile '{name}' is not valid JSON") from error
        if not isinstance(profile, dict):
            raise ProfileStoreError(f"stored profile '{name}' is not a JSON object")
        validate_profile(profile, name=name)
        if profile["id"] != row["id"]:
            raise ProfileStoreError(f"stored profile '{name}' has inconsistent identity")
        revision = row["revision"]
        if type(revision) is not int or revision < 1:
            raise ProfileStoreError(f"stored profile '{name}' has an invalid revision")
        validate_stored_registry(profile)
        profiles[name] = profile
        if revisions is not None:
            revisions[name] = revision
    return profiles


def validate_stored_registry(profile: Mapping[str, Any]) -> None:
    try:
        registry_connection(profile)
    except RegistryProfileError as error:
        raise ProfileStoreError(f"stored registry profile is not executable: {error}") from error


def encode_profile(profile: Mapping[str, Any]) -> str:
    return json.dumps(profile, sort_keys=True, separators=(",", ":"))


def validate_profile_name(name: str) -> None:
    if not _PROFILE_NAME_PATTERN.fullmatch(name):
        raise ProfileStoreError(
            "profile name must contain 1 to 128 safe characters: "
            "letters, digits, dots, underscores, or hyphens"
        )


def validate_profile(profile: dict[str, Any], *, name: str | None = None) -> None:
    validator = Draft202012Validator(_load_schema(), format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(profile),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    prefix = f"stored profile '{name}'" if name is not None else "profile"
    if errors:
        validation_error = errors[0]
        location = ".".join(str(part) for part in validation_error.absolute_path) or "document root"
        detail = _validation_detail(validation_error)
        suffix = f": {detail}" if detail else ""
        raise ProfileStoreError(f"{prefix} does not match schema at {location}{suffix}")
    try:
        kafka_connection(profile)
    except KafkaProfileError as error:
        raise ProfileStoreError(f"{prefix} has invalid Kafka configuration: {error}") from error


def _validation_detail(error: ValidationError) -> str | None:
    if error.validator == "additionalProperties" and isinstance(error.instance, dict):
        properties = error.schema.get("properties", {})
        unknown = sorted(str(key) for key in error.instance if key not in properties)
        if unknown:
            label = "field" if len(unknown) == 1 else "fields"
            return f"unknown {label}: {', '.join(unknown)}"
    if error.validator == "required" and isinstance(error.instance, dict):
        missing = sorted(str(key) for key in error.validator_value if key not in error.instance)
        if missing:
            label = "field" if len(missing) == 1 else "fields"
            return f"missing required {label}: {', '.join(missing)}"
    return None


def _ensure_private_parent(path: Path) -> None:
    missing: list[Path] = []
    current = path
    try:
        while True:
            try:
                current.lstat()
                break
            except FileNotFoundError:
                missing.append(current)
                parent = current.parent
                if parent == current:
                    raise ProfileStoreError("profile database directory has no existing parent")
                current = parent
        for directory in reversed(missing):
            try:
                directory.mkdir(mode=0o700)
            except FileExistsError:
                pass
            validate_private_parent(directory)
            _sync_directory(directory.parent)
    except ProfileStoreError:
        raise
    except OSError as error:
        raise ProfileStoreError("profile database directory could not be created") from error
    validate_private_parent(path)


def validate_private_parent(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ProfileStoreError("profile database directory could not be inspected") from error
    owner_uid = getattr(os, "getuid", lambda: metadata.st_uid)()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise ProfileStoreError("profile database directory is not private and user-owned")


def validate_database_file(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ProfileStoreError("profile database metadata could not be read") from error
    owner_uid = getattr(os, "getuid", lambda: metadata.st_uid)()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ProfileStoreError("profile database is not a regular file")
    if metadata.st_uid != owner_uid:
        raise ProfileStoreError("profile database is not user-owned")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ProfileStoreError("profile database permissions are not private")


def _create_or_validate_database_file(path: Path) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        validate_database_file(path)
        return
    except OSError as error:
        raise ProfileStoreError("profile database could not be created safely") from error
    os.fsync(descriptor)
    os.close(descriptor)
    _sync_directory(path.parent)
    validate_database_file(path)


def _open_private_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise ProfileStoreError("database maintenance lock could not be opened safely") from error
    metadata = os.fstat(descriptor)
    owner_uid = getattr(os, "getuid", lambda: metadata.st_uid)()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise ProfileStoreError("database maintenance lock is not private and user-owned")
    return descriptor


def _acquire_bounded_lock(descriptor: int) -> None:
    deadline = time.monotonic() + DATABASE_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except OSError as error:
            if error.errno not in (errno.EACCES, errno.EAGAIN):
                raise ProfileStoreError(
                    "database maintenance lock could not be acquired"
                ) from error
            if time.monotonic() >= deadline:
                raise ProfileStoreError("database maintenance is busy") from error
            time.sleep(0.05)


def _create_database_backup(connection: sqlite3.Connection, path: Path) -> None:
    timestamp = _backup_timestamp()
    backup_path = Path(f"{path}{DATABASE_BACKUP_PREFIX}{timestamp}-{uuid.uuid4().hex}")
    temporary_path = path.parent / f".{path.name}.backup-{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary_path, flags, 0o600)
        os.close(descriptor)
        descriptor = None
        with closing(sqlite3.connect(temporary_path)) as destination:
            connection.backup(destination)
        temporary_path.chmod(0o600)
        os.link(temporary_path, backup_path, follow_symlinks=False)
        with backup_path.open("rb") as backup:
            os.fsync(backup.fileno())
        temporary_path.unlink()
        _sync_directory(path.parent)
    except (OSError, sqlite3.Error) as error:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary_path.unlink()
        except OSError:
            pass
        raise ProfileStoreError("profile database backup could not be created safely") from error


def _backup_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S.%fZ")


def _harden_sqlite_files(path: Path, *, strict: bool = False) -> None:
    failure: OSError | None = None
    for suffix in _SQLITE_PRIVATE_SUFFIXES:
        candidate = Path(f"{path}{suffix}")
        try:
            metadata = candidate.lstat()
            if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                candidate.chmod(0o600)
        except FileNotFoundError:
            continue
        except OSError as error:
            failure = error
    if strict and failure is not None:
        raise failure


def _configure_writable_connection(connection: sqlite3.Connection) -> None:
    journal_mode = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
    if journal_mode != "wal":
        raise ProfileStoreError("profile database could not enable durable WAL mode")
    connection.execute("PRAGMA synchronous = FULL")
    synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
    if synchronous != 2:
        raise ProfileStoreError("profile database could not enable FULL synchronization")
    if sys.platform == "darwin":
        connection.execute("PRAGMA fullfsync = ON")
        fullfsync = int(connection.execute("PRAGMA fullfsync").fetchone()[0])
        if fullfsync != 1:
            raise ProfileStoreError("profile database could not enable fullfsync")


def _sync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def path_entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def _load_schema() -> dict[str, Any]:
    packaged_path = Path(__file__).parent / "schemas" / SCHEMA_FILENAME
    source_path = Path(__file__).parents[1] / "schemas" / SCHEMA_FILENAME
    schema_path = packaged_path if packaged_path.is_file() else source_path
    return cast(dict[str, Any], json.loads(schema_path.read_text(encoding="utf-8")))


__all__ = [
    "DATABASE_BACKUP_PREFIX",
    "DATABASE_FILENAME",
    "DATABASE_MAINTENANCE_SUFFIX",
    "DATABASE_SCHEMA_VERSION",
    "DATABASE_TIMEOUT_SECONDS",
    "SCHEMA_FILENAME",
    "ProfileCollection",
    "ProfileStoreError",
    "connect",
    "database_maintenance_lock",
    "encode_profile",
    "inspect_migration_state",
    "inspect_pending_secret_cleanup",
    "inspect_profile_database",
    "load_profile_collection",
    "load_profile_rows",
    "load_profiles",
    "migrate_profile_database",
    "path_entry_exists",
    "pending_migration_message",
    "reconcile_pending_secrets",
    "resolve_database_path",
    "rollback",
    "validate_database_file",
    "validate_private_parent",
    "validate_profile",
    "validate_profile_name",
    "validate_stored_registry",
    "writable_connection",
]
