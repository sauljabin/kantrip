"""Persist validated Kantrip profiles in a private SQLite database."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import sqlite3
import stat
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import closing, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from kantrip import APP_VERSION
from kantrip.credential_mutations import CredentialMutationError, update_profile_revision
from kantrip.kafka import KafkaProfileError, kafka_connection, validate_ca_bundle
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
from kantrip.registry import (
    CONFLUENT_PROVIDER,
    RegistryProfileError,
    plain_registry_connection,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store

DATABASE_FILENAME = "profiles.db"
DATABASE_SCHEMA_VERSION = LATEST_SEQUENCE
DATABASE_TIMEOUT_SECONDS = 5.0
DATABASE_BACKUP_PREFIX = ".pre-migration-"
DATABASE_MAINTENANCE_SUFFIX = ".maintenance.lock"
SCHEMA_FILENAME = "profile.schema.json"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
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
    if not _path_entry_exists(database_path):
        if missing_ok:
            return ProfileCollection(database_path, {})
        raise ProfileStoreError(f"profile database was not found: {database_path}")

    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
    try:
        with closing(_connect(database_path, writable=False)) as connection:
            state = _inspect_migration_state(connection)
            if not state.requires_migration:
                return _load_profile_collection(database_path, connection)
        if not migrate:
            raise ProfileStoreError(_pending_migration_message(state))
        migrate_profile_database(database_path)
        with closing(_connect(database_path, writable=False)) as connection:
            state = _inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(_pending_migration_message(state))
            return _load_profile_collection(database_path, connection)
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
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile database was not found: {database_path}")
    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
    try:
        with closing(_connect(database_path, writable=False)) as connection:
            return _inspect_migration_state(connection)
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
    if not _path_entry_exists(database_path):
        if missing_ok:
            return MigrationResult(0, 0)
        raise ProfileStoreError(f"profile database was not found: {database_path}")
    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
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
    if not _path_entry_exists(database_path):
        return ()
    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
    try:
        with closing(_connect(database_path, writable=False)) as connection:
            state = _inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(_pending_migration_message(state))
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
    if not _path_entry_exists(database_path):
        return ReconciliationResult(0, 0, 0)
    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
    lock = nullcontext() if lock_held else database_maintenance_lock(database_path)
    try:
        with lock, closing(_connect(database_path, writable=True)) as connection:
            state = _inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(_pending_migration_message(state))
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
    _validate_private_parent(path.parent)
    lock_path = Path(f"{path}{DATABASE_MAINTENANCE_SUFFIX}")
    descriptor = _open_private_lock(lock_path)
    try:
        _acquire_bounded_lock(descriptor)
        yield
    finally:
        os.close(descriptor)


def add_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    bootstrap_servers: tuple[str, ...] = (DEFAULT_BOOTSTRAP_SERVER,),
    description: str | None = None,
    labels: Mapping[str, str] | None = None,
    transport: str = "plaintext",
    ca_certificates: str | None = None,
    registry_provider: str | None = None,
    registry_url: str | None = None,
    environment: Mapping[str, str] | None = None,
) -> ProfileCollection:
    """Add one validated plaintext profile in a short write transaction."""
    _validate_profile_name(profile_name)
    database_path = path if path is not None else resolve_database_path(environment)
    profile = _new_profile(
        bootstrap_servers,
        description=description,
        labels=labels or {},
        transport=transport,
        ca_certificates=ca_certificates,
        registry_provider=registry_provider,
        registry_url=registry_url,
    )
    _validate_profile(profile)

    try:
        with _writable_connection(database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                _load_profile_rows(connection)
                connection.execute(
                    "INSERT INTO profiles (name, id, revision, document) VALUES (?, ?, 1, ?)",
                    (profile_name, profile["id"], _encode_profile(profile)),
                )
                connection.execute("COMMIT")
            except sqlite3.IntegrityError as error:
                _rollback(connection)
                raise ProfileStoreError(f"profile '{profile_name}' already exists") from error
            except BaseException:
                _rollback(connection)
                raise
            return _load_profile_collection(database_path, connection)
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be updated safely") from error


def remove_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> ProfileCollection:
    """Remove one profile in a short write transaction."""
    _validate_profile_name(profile_name)
    database_path = path if path is not None else resolve_database_path(environment)
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")

    try:
        with _writable_connection(database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                _load_profile_rows(connection)
                cursor = connection.execute("DELETE FROM profiles WHERE name = ?", (profile_name,))
                if cursor.rowcount != 1:
                    raise ProfileStoreError(f"profile '{profile_name}' was not found")
                connection.execute("COMMIT")
            except BaseException:
                _rollback(connection)
                raise
            return _load_profile_collection(database_path, connection)
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be updated safely") from error


def edit_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    bootstrap_servers: tuple[str, ...] | None = None,
    description: str | None = None,
    clear_description: bool = False,
    labels: Mapping[str, str] | None = None,
    remove_labels: tuple[str, ...] = (),
    transport: str | None = None,
    ca_certificates: str | None = None,
    default_trust: bool = False,
    registry_provider: str | None = None,
    registry_url: str | None = None,
    remove_registry: bool = False,
    environment: Mapping[str, str] | None = None,
) -> ProfileCollection:
    """Update explicit fields of one existing profile transactionally."""
    _validate_profile_name(profile_name)
    _validate_edit_request(
        bootstrap_servers=bootstrap_servers,
        description=description,
        clear_description=clear_description,
        labels=labels,
        remove_labels=remove_labels,
        transport=transport,
        ca_certificates=ca_certificates,
        default_trust=default_trust,
        registry_provider=registry_provider,
        registry_url=registry_url,
        remove_registry=remove_registry,
    )
    database_path = path if path is not None else resolve_database_path(environment)
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    try:
        with _writable_connection(database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                revisions: dict[str, int] = {}
                profiles = _load_profile_rows(connection, revisions=revisions)
                current = profiles.get(profile_name)
                if current is None:
                    raise ProfileStoreError(f"profile '{profile_name}' was not found")
                expected_revision = revisions[profile_name]
                updated = _apply_profile_edits(
                    current,
                    bootstrap_servers=bootstrap_servers,
                    description=description,
                    clear_description=clear_description,
                    labels=labels or {},
                    remove_labels=remove_labels,
                    transport=transport,
                    ca_certificates=ca_certificates,
                    default_trust=default_trust,
                    registry_provider=registry_provider,
                    registry_url=registry_url,
                    remove_registry=remove_registry,
                )
                _validate_profile(updated)
                try:
                    plain_registry_connection(updated)
                except RegistryProfileError as error:
                    raise ProfileStoreError(str(error)) from error
                try:
                    update_profile_revision(
                        connection,
                        profile_name=profile_name,
                        profile_id=current["id"],
                        expected_revision=expected_revision,
                        document=_encode_profile(updated),
                    )
                except CredentialMutationError as error:
                    raise ProfileStoreError(
                        f"profile '{profile_name}' changed unexpectedly"
                    ) from error
                connection.execute("COMMIT")
            except BaseException:
                _rollback(connection)
                raise
            return _load_profile_collection(database_path, connection)
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be updated safely") from error


def _validate_edit_request(
    *,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: Mapping[str, str] | None,
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
    registry_provider: str | None,
    registry_url: str | None,
    remove_registry: bool,
) -> None:
    has_change = any(
        (
            bootstrap_servers is not None,
            description is not None,
            clear_description,
            bool(labels),
            bool(remove_labels),
            transport is not None,
            ca_certificates is not None,
            default_trust,
            registry_provider is not None,
            registry_url is not None,
            remove_registry,
        )
    )
    if not has_change:
        raise ProfileStoreError("no profile changes were requested")
    if description is not None and clear_description:
        raise ProfileStoreError("--description cannot be combined with --clear-description")
    if remove_registry and (registry_provider is not None or registry_url is not None):
        raise ProfileStoreError("--remove-registry cannot be combined with Registry update options")
    if labels and set(labels).intersection(remove_labels):
        raise ProfileStoreError("a label cannot be set and removed in the same edit")
    if ca_certificates is not None and transport == "plaintext":
        raise ProfileStoreError("--ca-file cannot be combined with --transport plaintext")
    if ca_certificates is not None and default_trust:
        raise ProfileStoreError("--ca-file cannot be combined with --default-trust")
    if default_trust and transport == "plaintext":
        raise ProfileStoreError("--default-trust cannot be combined with --transport plaintext")


def _apply_profile_edits(
    current: Mapping[str, Any],
    *,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: Mapping[str, str],
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
    registry_provider: str | None,
    registry_url: str | None,
    remove_registry: bool,
) -> dict[str, Any]:
    updated = deepcopy(dict(current))
    if bootstrap_servers is not None:
        updated["kafka"]["bootstrapServers"] = list(bootstrap_servers)
    if clear_description:
        updated.pop("description", None)
    elif description is not None:
        updated["description"] = description
    _apply_label_edits(updated, labels, remove_labels)
    _apply_kafka_transport_edits(updated, transport, ca_certificates, default_trust)
    _apply_registry_edits(updated, registry_provider, registry_url, remove_registry)
    return updated


def _apply_kafka_transport_edits(
    profile: dict[str, Any],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
) -> None:
    kafka = profile["kafka"]
    if transport is not None:
        kafka["transport"] = transport
        if transport == "plaintext":
            kafka.pop("tls", None)
        elif "tls" not in kafka:
            kafka["tls"] = {}
    if ca_certificates is None:
        if not default_trust:
            return
        if kafka["transport"] != "tls":
            raise ProfileStoreError("--default-trust requires Kafka TLS transport")
        kafka.pop("tls", None)
        return
    if kafka["transport"] != "tls":
        raise ProfileStoreError("--ca-file requires Kafka TLS transport")
    kafka.setdefault("tls", {})["caCertificates"] = _validated_ca_bundle(ca_certificates)


def _apply_label_edits(
    profile: dict[str, Any],
    labels: Mapping[str, str],
    remove_labels: tuple[str, ...],
) -> None:
    current_labels = dict(profile.get("labels", {}))
    current_labels.update(labels)
    for name in remove_labels:
        current_labels.pop(name, None)
    if current_labels:
        profile["labels"] = current_labels
    else:
        profile.pop("labels", None)


def _apply_registry_edits(
    profile: dict[str, Any],
    provider: str | None,
    url: str | None,
    remove: bool,
) -> None:
    if remove:
        profile.pop("registry", None)
        return
    if provider is None and url is None:
        return
    existing = profile.get("registry")
    if not isinstance(existing, Mapping) and url is None:
        raise ProfileStoreError("--registry-provider requires --registry-url for a new Registry")
    selected_provider = provider or (
        str(existing["provider"]) if isinstance(existing, Mapping) else CONFLUENT_PROVIDER
    )
    if url is None:
        assert isinstance(existing, Mapping)
        old_property = (
            "apicurio.registry.url"
            if existing.get("provider") == "apicurio"
            else "schema.registry.url"
        )
        selected_url = existing.get(old_property)
        if not isinstance(selected_url, str):
            raise ProfileStoreError("stored Registry URL is invalid")
    else:
        selected_url = url
    property_name = (
        "apicurio.registry.url" if selected_provider == "apicurio" else "schema.registry.url"
    )
    profile["registry"] = {
        "provider": selected_provider,
        property_name: selected_url,
    }


def _new_profile(
    bootstrap_servers: tuple[str, ...],
    *,
    description: str | None,
    labels: Mapping[str, str],
    transport: str,
    ca_certificates: str | None,
    registry_provider: str | None,
    registry_url: str | None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "kafka": {
            "bootstrapServers": list(bootstrap_servers),
            "transport": transport,
            "auth": {"type": "none"},
        },
    }
    if ca_certificates is not None:
        if transport != "tls":
            raise ProfileStoreError("--ca-file requires --transport tls")
        profile["kafka"]["tls"] = {"caCertificates": _validated_ca_bundle(ca_certificates)}
    if description is not None:
        profile["description"] = description
    if labels:
        profile["labels"] = dict(labels)
    if registry_provider is not None and registry_url is None:
        raise ProfileStoreError("--registry-provider requires --registry-url")
    if registry_url is not None:
        provider = registry_provider or CONFLUENT_PROVIDER
        property_name = "apicurio.registry.url" if provider == "apicurio" else "schema.registry.url"
        profile["registry"] = {
            "provider": provider,
            property_name: registry_url,
        }
        try:
            plain_registry_connection(profile)
        except RegistryProfileError as error:
            raise ProfileStoreError(str(error)) from error
    return profile


def _validated_ca_bundle(contents: str) -> str:
    try:
        return validate_ca_bundle(contents)
    except KafkaProfileError as error:
        raise ProfileStoreError(str(error)) from error


@contextmanager
def _writable_connection(path: Path) -> Iterator[sqlite3.Connection]:
    _ensure_private_parent(path.parent)
    with database_maintenance_lock(path):
        _create_or_validate_database_file(path)
        connection: sqlite3.Connection | None = None
        try:
            connection = _connect(path, writable=True)
            path.chmod(0o600)
            _validate_database_file(path)
            _migrate_connection(connection, path)
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            yield connection
        except OSError as error:
            raise ProfileStoreError("profile database could not be opened safely") from error
        finally:
            if connection is not None:
                connection.close()
            _harden_sqlite_files(path)


def _connect(path: Path, *, writable: bool) -> sqlite3.Connection:
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
    return connection


def _migrate_existing_database(path: Path) -> MigrationResult:
    _validate_private_parent(path.parent)
    _validate_database_file(path)
    try:
        with closing(_connect(path, writable=True)) as connection:
            result = _migrate_connection(connection, path)
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            return result
    except ProfileStoreError:
        raise
    except sqlite3.Error as error:
        raise ProfileStoreError("profile database could not be migrated safely") from error
    finally:
        _harden_sqlite_files(path)


def _migrate_connection(connection: sqlite3.Connection, path: Path) -> MigrationResult:
    state = _inspect_migration_state(connection)
    if not state.requires_migration:
        return MigrationResult(state.current_sequence, state.current_sequence)
    if not state.new_database:
        _create_database_backup(connection, path)
    try:
        return apply_migrations(connection, applied_by=APP_VERSION)
    except MigrationError as error:
        raise ProfileStoreError(str(error)) from error


def _inspect_migration_state(connection: sqlite3.Connection) -> MigrationState:
    try:
        return inspect_migrations(connection)
    except MigrationError as error:
        raise ProfileStoreError(str(error)) from error


def _pending_migration_message(state: MigrationState) -> str:
    sequences = ", ".join(str(sequence) for sequence in state.pending_sequences)
    return f"profile database requires migration sequence {sequences}"


def _rollback(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        connection.execute("ROLLBACK")


def _load_profile_collection(path: Path, connection: sqlite3.Connection) -> ProfileCollection:
    revisions: dict[str, int] = {}
    profiles = _load_profile_rows(connection, revisions=revisions)
    return ProfileCollection(path, profiles, revisions)


def _load_profile_rows(
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
        _validate_profile_name(name)
        document = row["document"]
        if not isinstance(document, str):
            raise ProfileStoreError(f"stored profile '{name}' is not valid JSON")
        try:
            profile = json.loads(document)
        except (TypeError, json.JSONDecodeError) as error:
            raise ProfileStoreError(f"stored profile '{name}' is not valid JSON") from error
        if not isinstance(profile, dict):
            raise ProfileStoreError(f"stored profile '{name}' is not a JSON object")
        _validate_profile(profile, name=name)
        if profile["id"] != row["id"]:
            raise ProfileStoreError(f"stored profile '{name}' has inconsistent identity")
        revision = row["revision"]
        if type(revision) is not int or revision < 1:
            raise ProfileStoreError(f"stored profile '{name}' has an invalid revision")
        _validate_stored_registry(profile)
        profiles[name] = profile
        if revisions is not None:
            revisions[name] = revision
    return profiles


def _validate_stored_registry(profile: Mapping[str, Any]) -> None:
    try:
        plain_registry_connection(profile)
    except RegistryProfileError as error:
        raise ProfileStoreError(f"stored registry profile is not executable: {error}") from error


def _encode_profile(profile: Mapping[str, Any]) -> str:
    return json.dumps(profile, sort_keys=True, separators=(",", ":"))


def _validate_profile_name(name: str) -> None:
    if not _PROFILE_NAME_PATTERN.fullmatch(name):
        raise ProfileStoreError(
            "profile name must contain 1 to 128 safe characters: "
            "letters, digits, dots, underscores, or hyphens"
        )


def _validate_profile(profile: dict[str, Any], *, name: str | None = None) -> None:
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
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as error:
        raise ProfileStoreError("profile database directory could not be created") from error
    _validate_private_parent(path)


def _validate_private_parent(path: Path) -> None:
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


def _validate_database_file(path: Path) -> None:
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
        _validate_database_file(path)
        return
    except OSError as error:
        raise ProfileStoreError("profile database could not be created safely") from error
    os.close(descriptor)
    _validate_database_file(path)


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
        temporary_path.unlink()
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


def _harden_sqlite_files(path: Path) -> None:
    for suffix in _SQLITE_PRIVATE_SUFFIXES:
        candidate = Path(f"{path}{suffix}")
        try:
            metadata = candidate.lstat()
            if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                candidate.chmod(0o600)
        except FileNotFoundError:
            continue
        except OSError:
            continue


def _path_entry_exists(path: Path) -> bool:
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
    "ProfileCollection",
    "ProfileStoreError",
    "add_profile",
    "database_maintenance_lock",
    "edit_profile",
    "inspect_pending_secret_cleanup",
    "inspect_profile_database",
    "load_profiles",
    "migrate_profile_database",
    "reconcile_pending_secrets",
    "remove_profile",
    "resolve_database_path",
]
