"""Persist validated Kantrip profiles in a private SQLite database."""

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
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from kantrip import APP_VERSION
from kantrip.credential_mutations import (
    CredentialMutationError,
    SecretReplacement,
    commit_profile_removal,
    commit_secret_replacements,
    remove_profile_revision,
    stage_secret_replacements,
    update_profile_revision,
)
from kantrip.kafka import (
    KafkaConnection,
    KafkaProfileError,
    kafka_connection,
    resolve_kafka_connection,
    validate_ca_bundle,
    validate_client_identity,
    validate_sasl_credential,
)
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
from kantrip.secret_store import (
    SecretStore,
    SecretStoreError,
    load_secret_store,
    parse_secret_reference,
)

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

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class _MutationOutcome(Enum):
    """Durable outcome of the profile-row transaction."""

    NOT_COMMITTED = "not-committed"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


@dataclass
class _MutationTracker:
    operation: str
    outcome: _MutationOutcome = _MutationOutcome.NOT_COMMITTED

    def mark_committed(self) -> None:
        self.outcome = _MutationOutcome.COMMITTED


@dataclass(frozen=True)
class _ProfileRowState:
    name: str
    profile_id: str
    revision: int
    document: str


@dataclass(frozen=True)
class _MutationEvidence:
    before: _ProfileRowState | None
    after: _ProfileRowState | None
    removed_cleanup: tuple[CleanupRecord, ...] = ()
    added_cleanup: tuple[CleanupRecord, ...] = ()


@dataclass(frozen=True)
class KafkaAuthInput:
    """Validated-entry input whose secret values never enter the profile document."""

    auth_type: str
    username: str | None = None
    password: str | None = None
    client_certificate: str | None = None
    private_key: str | None = None
    private_key_password: str | None = None


@dataclass(frozen=True)
class _AuthPlan:
    auth_type: str
    username: str | None
    client_certificate: str | None
    retained_references: Mapping[str, str]
    replacements: tuple[SecretReplacement, ...]
    retire_references: tuple[str, ...]


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


@dataclass(frozen=True)
class ProfileSnapshot:
    """One immutable profile generation with credentials resolved in memory."""

    name: str
    profile_id: str
    revision: int
    document: Mapping[str, Any]
    kafka: KafkaConnection


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


def resolve_profile_snapshot(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
) -> ProfileSnapshot:
    """Load and resolve one generation while excluding credential mutation."""
    _validate_profile_name(profile_name)
    database_path = path if path is not None else resolve_database_path(environment)
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    _validate_private_parent(database_path.parent)
    _validate_database_file(database_path)
    try:
        with (
            database_maintenance_lock(database_path),
            closing(_connect(database_path, writable=False)) as connection,
        ):
            state = _inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(_pending_migration_message(state))
            collection = _load_profile_collection(database_path, connection)
            document = deepcopy(collection.profile(profile_name))
            revision = collection.revision(profile_name)
            parsed = kafka_connection(document)
            if parsed.requires_secrets:
                selected_store = secret_store or load_secret_store()
                parsed = resolve_kafka_connection(parsed, selected_store)
            return ProfileSnapshot(
                profile_name,
                str(document["id"]),
                revision,
                document,
                parsed,
            )
    except (KafkaProfileError, SecretStoreError) as error:
        raise ProfileStoreError(str(error)) from error
    except sqlite3.Error as error:
        raise ProfileStoreError("profile snapshot could not be resolved safely") from error


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
    auth: KafkaAuthInput | None = None,
    registry_provider: str | None = None,
    registry_url: str | None = None,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
) -> ProfileCollection:
    """Add one validated profile, staging any secrets before its database row."""
    _validate_profile_name(profile_name)
    database_path = path if path is not None else resolve_database_path(environment)
    profile_id = str(uuid.uuid4())
    auth_input = auth or KafkaAuthInput("none")
    auth_plan = _plan_authentication(profile_id, None, auth_input)
    profile = _new_profile(
        profile_id,
        bootstrap_servers,
        description=description,
        labels=labels or {},
        transport=transport,
        ca_certificates=ca_certificates,
        auth={"type": "none"},
        registry_provider=registry_provider,
        registry_url=registry_url,
    )
    _validate_profile(profile)
    _validate_auth_transport(auth_plan.auth_type, transport)
    mutation = _MutationTracker(f"profile '{profile_name}' creation")

    try:
        with _writable_connection(database_path) as connection:
            if profile_name in _load_profile_rows(connection):
                raise ProfileStoreError(f"profile '{profile_name}' already exists")
            if not auth_plan.replacements:
                profile["kafka"]["auth"] = _auth_document(auth_plan, {})
                _validate_profile(profile)
                document = _encode_profile(profile)
                evidence = _MutationEvidence(
                    before=None,
                    after=_ProfileRowState(profile_name, profile_id, 1, document),
                )
                _insert_profile(
                    connection,
                    profile_name,
                    profile,
                    database_path=database_path,
                    evidence=evidence,
                    mutation=mutation,
                )
                return _load_profile_collection(database_path, connection)
            store = secret_store or load_secret_store()
            staged = stage_secret_replacements(
                connection,
                store,
                profile_id,
                auth_plan.replacements,
            )
            staged_references = {item.field: item.reference for item in staged}
            profile["kafka"]["auth"] = _auth_document(auth_plan, staged_references)
            _validate_profile(profile)
            evidence = _MutationEvidence(
                before=None,
                after=_ProfileRowState(
                    profile_name,
                    profile_id,
                    1,
                    _encode_profile(profile),
                ),
            )

            def insert(references: Mapping[str, str]) -> None:
                if dict(references) != staged_references:
                    raise CredentialMutationError("staged credential references changed")
                _insert_profile(connection, profile_name, profile, transaction=False)

            commit_secret_replacements(
                connection,
                store,
                profile_id,
                staged,
                insert,
                inspect_outcome=_credential_outcome_inspector(database_path, evidence),
            )
            mutation.mark_committed()
            return _load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise _profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise _classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise _database_mutation_error(error, mutation) from error


def remove_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
    expected_profile_id: str | None = None,
    expected_revision: int | None = None,
) -> ProfileCollection:
    """Remove one profile before reconciling its exact owned credentials."""
    _validate_profile_name(profile_name)
    database_path = path if path is not None else resolve_database_path(environment)
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    mutation = _MutationTracker(f"profile '{profile_name}' removal")

    try:
        with _writable_connection(database_path) as connection:
            revisions: dict[str, int] = {}
            profiles = _load_profile_rows(connection, revisions=revisions)
            profile = profiles.get(profile_name)
            if profile is None:
                raise ProfileStoreError(f"profile '{profile_name}' was not found")
            current_revision = revisions[profile_name]
            before = _read_profile_row_state(connection, profile_name)
            if before is None:
                raise ProfileStoreError(f"profile '{profile_name}' changed unexpectedly")
            _validate_expected_generation(
                profile_name,
                profile,
                current_revision,
                expected_profile_id,
                expected_revision,
                message="changed after removal was requested",
            )
            references = _profile_secret_references(profile)
            if references:
                return _remove_profile_with_credentials(
                    connection,
                    database_path,
                    profile_name,
                    profile,
                    current_revision,
                    references,
                    secret_store,
                    before,
                    mutation,
                )
            connection.execute("BEGIN IMMEDIATE")
            try:
                remove_profile_revision(
                    connection,
                    profile_name=profile_name,
                    profile_id=profile["id"],
                    expected_revision=current_revision,
                )
                _commit_with_evidence(
                    connection,
                    database_path,
                    _MutationEvidence(before=before, after=None),
                    mutation,
                    "profile removal commit outcome could not be established",
                )
            except BaseException:
                _rollback(connection)
                raise
            return _load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise _profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise _classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise _database_mutation_error(error, mutation) from error


def _validate_expected_generation(
    profile_name: str,
    profile: Mapping[str, Any],
    revision: int,
    expected_profile_id: str | None,
    expected_revision: int | None,
    *,
    message: str,
) -> None:
    identity_changed = expected_profile_id is not None and profile["id"] != expected_profile_id
    revision_changed = expected_revision is not None and revision != expected_revision
    if identity_changed or revision_changed:
        raise ProfileStoreError(f"profile '{profile_name}' {message}")


def _remove_profile_with_credentials(
    connection: sqlite3.Connection,
    database_path: Path,
    profile_name: str,
    profile: Mapping[str, Any],
    current_revision: int,
    references: tuple[str, ...],
    secret_store: SecretStore | None,
    before: _ProfileRowState,
    mutation: _MutationTracker,
) -> ProfileCollection:
    store = secret_store or load_secret_store()
    result = commit_profile_removal(
        connection,
        store,
        profile["id"],
        references,
        lambda: remove_profile_revision(
            connection,
            profile_name=profile_name,
            profile_id=profile["id"],
            expected_revision=current_revision,
        ),
        inspect_outcome=_credential_outcome_inspector(
            database_path,
            _MutationEvidence(before=before, after=None),
        ),
    )
    mutation.mark_committed()
    collection = _load_profile_collection(database_path, connection)
    if result.failed:
        raise ProfileStoreError(
            f"profile '{profile_name}' was removed but credential cleanup is pending; "
            "run 'kantrip doctor --repair'",
            exit_code=3,
        )
    return collection


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
    auth: KafkaAuthInput | None = None,
    registry_provider: str | None = None,
    registry_url: str | None = None,
    remove_registry: bool = False,
    environment: Mapping[str, str] | None = None,
    expected_revision: int | None = None,
    expected_profile_id: str | None = None,
    secret_store: SecretStore | None = None,
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
        auth=auth,
        registry_provider=registry_provider,
        registry_url=registry_url,
        remove_registry=remove_registry,
    )
    database_path = path if path is not None else resolve_database_path(environment)
    if not _path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    mutation = _MutationTracker(f"profile '{profile_name}' update")
    try:
        with _writable_connection(database_path) as connection:
            revisions: dict[str, int] = {}
            profiles = _load_profile_rows(connection, revisions=revisions)
            current = profiles.get(profile_name)
            if current is None:
                raise ProfileStoreError(f"profile '{profile_name}' was not found")
            current_revision = revisions[profile_name]
            before = _read_profile_row_state(connection, profile_name)
            if before is None:
                raise ProfileStoreError(f"profile '{profile_name}' changed unexpectedly")
            _validate_expected_generation(
                profile_name,
                current,
                current_revision,
                expected_profile_id,
                expected_revision,
                message="changed while credentials were collected",
            )
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
            if auth is not None:
                plan = _plan_authentication(current["id"], current["kafka"]["auth"], auth)
                _validate_auth_transport(plan.auth_type, updated["kafka"]["transport"])
                return _commit_authenticated_edit(
                    connection,
                    database_path,
                    profile_name,
                    current,
                    current_revision,
                    updated,
                    plan,
                    secret_store,
                    before,
                    mutation,
                )
            _commit_plain_edit(
                connection,
                profile_name,
                current,
                current_revision,
                updated,
                database_path,
                before,
                mutation,
            )
            return _load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise _profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise _classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise _database_mutation_error(error, mutation) from error


def _commit_plain_edit(
    connection: sqlite3.Connection,
    profile_name: str,
    current: Mapping[str, Any],
    current_revision: int,
    updated: dict[str, Any],
    database_path: Path,
    before: _ProfileRowState,
    mutation: _MutationTracker,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        _validate_profile(updated)
        _validate_stored_registry(updated)
        try:
            update_profile_revision(
                connection,
                profile_name=profile_name,
                profile_id=current["id"],
                expected_revision=current_revision,
                document=_encode_profile(updated),
            )
        except CredentialMutationError as error:
            raise ProfileStoreError(f"profile '{profile_name}' changed unexpectedly") from error
        evidence = _MutationEvidence(
            before=before,
            after=_ProfileRowState(
                profile_name,
                str(current["id"]),
                current_revision + 1,
                _encode_profile(updated),
            ),
        )
        _commit_with_evidence(
            connection,
            database_path,
            evidence,
            mutation,
            "profile update commit outcome could not be established",
        )
    except BaseException:
        _rollback(connection)
        raise


def _commit_authenticated_edit(
    connection: sqlite3.Connection,
    database_path: Path,
    profile_name: str,
    current: Mapping[str, Any],
    current_revision: int,
    updated: dict[str, Any],
    plan: _AuthPlan,
    secret_store: SecretStore | None,
    before: _ProfileRowState,
    mutation: _MutationTracker,
) -> ProfileCollection:
    def switch(references: Mapping[str, str]) -> None:
        updated["kafka"]["auth"] = _auth_document(plan, references)
        _validate_profile(updated)
        _validate_stored_registry(updated)
        update_profile_revision(
            connection,
            profile_name=profile_name,
            profile_id=current["id"],
            expected_revision=current_revision,
            document=_encode_profile(updated),
        )

    if not plan.replacements and not plan.retire_references:
        updated["kafka"]["auth"] = _auth_document(plan, {})
        _validate_profile(updated)
        _validate_stored_registry(updated)
        evidence = _MutationEvidence(
            before=before,
            after=_ProfileRowState(
                profile_name,
                str(current["id"]),
                current_revision + 1,
                _encode_profile(updated),
            ),
        )
        connection.execute("BEGIN IMMEDIATE")
        try:
            switch({})
            _commit_with_evidence(
                connection,
                database_path,
                evidence,
                mutation,
                "profile update commit outcome could not be established",
            )
        except BaseException:
            _rollback(connection)
            raise
        return _load_profile_collection(database_path, connection)

    store = secret_store or load_secret_store()
    staged = stage_secret_replacements(
        connection,
        store,
        current["id"],
        plan.replacements,
    )
    staged_references = {item.field: item.reference for item in staged}
    updated["kafka"]["auth"] = _auth_document(plan, staged_references)
    _validate_profile(updated)
    _validate_stored_registry(updated)
    evidence = _MutationEvidence(
        before=before,
        after=_ProfileRowState(
            profile_name,
            str(current["id"]),
            current_revision + 1,
            _encode_profile(updated),
        ),
    )
    result = commit_secret_replacements(
        connection,
        store,
        current["id"],
        staged,
        switch,
        retire_references=plan.retire_references,
        inspect_outcome=_credential_outcome_inspector(database_path, evidence),
    )
    mutation.mark_committed()
    collection = _load_profile_collection(database_path, connection)
    if result.failed:
        raise ProfileStoreError(
            f"profile '{profile_name}' was updated but credential cleanup is pending; "
            "run 'kantrip doctor --repair'",
            exit_code=3,
        )
    return collection


def _profile_mutation_error(error: CredentialMutationError) -> ProfileStoreError:
    if error.committed is True:
        return ProfileStoreError(
            f"{error}; inspect the profile and run 'kantrip doctor --repair'",
            exit_code=3,
        )
    if error.committed is None:
        return ProfileStoreError(
            f"{error}; stop automatic retries and inspect the profile and doctor output",
            exit_code=4,
        )
    return ProfileStoreError(str(error), exit_code=1)


def _plan_authentication(
    profile_id: str,
    current: Mapping[str, Any] | None,
    requested: KafkaAuthInput,
) -> _AuthPlan:
    auth_type = requested.auth_type
    if auth_type not in {"none", "plain", "scram-sha-256", "scram-sha-512", "mtls"}:
        raise ProfileStoreError("Kafka authentication type is not supported")
    current_auth = current or {"type": "none"}
    current_type = current_auth.get("type")
    current_references = _auth_references(profile_id, current_auth)
    if auth_type == "none":
        if any(
            value is not None
            for value in (
                requested.username,
                requested.password,
                requested.client_certificate,
                requested.private_key,
                requested.private_key_password,
            )
        ):
            raise ProfileStoreError("Kafka auth none cannot include credentials")
        return _AuthPlan("none", None, None, {}, (), tuple(current_references.values()))
    if auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return _password_auth_plan(
            current_auth,
            current_type,
            current_references,
            requested,
        )
    return _mtls_auth_plan(
        current_auth,
        current_type,
        current_references,
        requested,
    )


def _password_auth_plan(
    current_auth: Mapping[str, Any],
    current_type: object,
    current_references: Mapping[str, str],
    requested: KafkaAuthInput,
) -> _AuthPlan:
    if requested.client_certificate is not None or requested.private_key is not None:
        raise ProfileStoreError("Kafka password authentication cannot include a client identity")
    username = requested.username
    if username is None and current_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        stored_username = current_auth.get("username")
        username = stored_username if isinstance(stored_username, str) else None
    if not username:
        raise ProfileStoreError("Kafka password authentication requires --username")
    previous = current_references.get("kafka/password")
    replacements: tuple[SecretReplacement, ...] = ()
    retained: dict[str, str] = {}
    if requested.password is not None:
        try:
            validate_sasl_credential(requested.password)
        except KafkaProfileError as error:
            raise ProfileStoreError(str(error)) from error
        replacements = (SecretReplacement("kafka/password", requested.password, previous),)
    elif previous is not None:
        retained["kafka/password"] = previous
    else:
        raise ProfileStoreError("Kafka password authentication requires a password")
    retired = _retired_references(current_references, retained, replacements)
    return _AuthPlan(
        requested.auth_type,
        username,
        None,
        retained,
        replacements,
        retired,
    )


def _mtls_auth_plan(
    current_auth: Mapping[str, Any],
    current_type: object,
    current_references: Mapping[str, str],
    requested: KafkaAuthInput,
) -> _AuthPlan:
    if requested.username is not None or requested.password is not None:
        raise ProfileStoreError("Kafka mTLS authentication cannot include username or password")
    changing_identity = (
        requested.client_certificate is not None or requested.private_key is not None
    )
    if changing_identity and (
        requested.client_certificate is None or requested.private_key is None
    ):
        raise ProfileStoreError("Kafka mTLS identity replacement requires certificate and key")
    if requested.private_key_password is not None and not changing_identity:
        raise ProfileStoreError("Kafka private-key password replacement requires a new key")
    if changing_identity:
        assert requested.client_certificate is not None
        assert requested.private_key is not None
        certificate, private_key = validate_client_identity(
            requested.client_certificate,
            requested.private_key,
            password=requested.private_key_password,
        )
        previous_key = current_references.get("kafka/tls/private-key")
        replacements = [SecretReplacement("kafka/tls/private-key", private_key, previous_key)]
        if requested.private_key_password is not None:
            replacements.append(
                SecretReplacement(
                    "kafka/tls/private-key-password",
                    requested.private_key_password,
                    current_references.get("kafka/tls/private-key-password"),
                )
            )
        retained: dict[str, str] = {}
    elif current_type == "mtls":
        stored_certificate = current_auth.get("clientCertificate")
        if not isinstance(stored_certificate, str):
            raise ProfileStoreError("stored Kafka client certificate is invalid")
        certificate = stored_certificate
        replacements = []
        retained = dict(current_references)
    else:
        raise ProfileStoreError("Kafka mTLS authentication requires certificate and key")
    retired = _retired_references(current_references, retained, tuple(replacements))
    return _AuthPlan(
        "mtls",
        None,
        certificate,
        retained,
        tuple(replacements),
        retired,
    )


def _auth_references(profile_id: str, auth: Mapping[str, Any]) -> dict[str, str]:
    references: dict[str, str] = {}
    for property_name, credential_field in (
        ("passwordRef", "kafka/password"),
        ("privateKeyRef", "kafka/tls/private-key"),
        ("privateKeyPasswordRef", "kafka/tls/private-key-password"),
    ):
        value = auth.get(property_name)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ProfileStoreError("stored Kafka credential reference is invalid")
        try:
            parsed = parse_secret_reference(value)
        except SecretStoreError as error:
            raise ProfileStoreError("stored Kafka credential reference is invalid") from error
        if parsed.profile_id != profile_id or parsed.field != credential_field:
            raise ProfileStoreError("stored Kafka credential reference does not match its profile")
        references[credential_field] = value
    return references


def _retired_references(
    current: Mapping[str, str],
    retained: Mapping[str, str],
    replacements: tuple[SecretReplacement, ...],
) -> tuple[str, ...]:
    replaced = {
        replacement.previous_reference
        for replacement in replacements
        if replacement.previous_reference is not None
    }
    retained_values = set(retained.values())
    return tuple(
        reference
        for reference in current.values()
        if reference not in retained_values and reference not in replaced
    )


def _auth_document(plan: _AuthPlan, staged: Mapping[str, str]) -> dict[str, Any]:
    references = dict(plan.retained_references) | dict(staged)
    if plan.auth_type == "none":
        return {"type": "none"}
    if plan.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return {
            "type": plan.auth_type,
            "username": plan.username,
            "passwordRef": references["kafka/password"],
        }
    document: dict[str, Any] = {
        "type": "mtls",
        "clientCertificate": plan.client_certificate,
        "privateKeyRef": references["kafka/tls/private-key"],
    }
    password_reference = references.get("kafka/tls/private-key-password")
    if password_reference is not None:
        document["privateKeyPasswordRef"] = password_reference
    return document


def _profile_secret_references(profile: Mapping[str, Any]) -> tuple[str, ...]:
    kafka = profile.get("kafka")
    if not isinstance(kafka, Mapping):
        raise ProfileStoreError("stored Kafka profile is invalid")
    auth = kafka.get("auth")
    if not isinstance(auth, Mapping):
        raise ProfileStoreError("stored Kafka authentication is invalid")
    return tuple(_auth_references(str(profile.get("id")), auth).values())


def _validate_auth_transport(auth_type: str, transport: str) -> None:
    if auth_type != "none" and transport != "tls":
        raise ProfileStoreError("Kafka authentication requires --transport tls")


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
    auth: KafkaAuthInput | None,
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
            auth is not None,
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
    profile_id: str,
    bootstrap_servers: tuple[str, ...],
    *,
    description: str | None,
    labels: Mapping[str, str],
    transport: str,
    ca_certificates: str | None,
    auth: Mapping[str, Any],
    registry_provider: str | None,
    registry_url: str | None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": profile_id,
        "kafka": {
            "bootstrapServers": list(bootstrap_servers),
            "transport": transport,
            "auth": dict(auth),
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


def _insert_profile(
    connection: sqlite3.Connection,
    profile_name: str,
    profile: Mapping[str, Any],
    *,
    transaction: bool = True,
    database_path: Path | None = None,
    evidence: _MutationEvidence | None = None,
    mutation: _MutationTracker | None = None,
) -> None:
    if transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "INSERT INTO profiles (name, id, revision, document) VALUES (?, ?, 1, ?)",
            (profile_name, profile["id"], _encode_profile(profile)),
        )
        if transaction:
            if database_path is None or evidence is None or mutation is None:
                raise RuntimeError("profile mutation evidence is required")
            _commit_with_evidence(
                connection,
                database_path,
                evidence,
                mutation,
                "profile creation commit outcome could not be established",
            )
    except BaseException:
        if transaction:
            _rollback(connection)
        raise


def _validated_ca_bundle(contents: str) -> str:
    try:
        return validate_ca_bundle(contents)
    except KafkaProfileError as error:
        raise ProfileStoreError(str(error)) from error


def _commit_with_evidence(
    connection: sqlite3.Connection,
    database_path: Path,
    evidence: _MutationEvidence,
    mutation: _MutationTracker,
    message: str,
) -> None:
    try:
        connection.execute("COMMIT")
    except Exception as error:
        _rollback_after_commit_error(connection)
        outcome = _inspect_mutation_outcome(database_path, evidence)
        mutation.outcome = outcome
        if outcome is _MutationOutcome.COMMITTED:
            raise ProfileStoreError(message, exit_code=3) from error
        if outcome is _MutationOutcome.NOT_COMMITTED:
            raise ProfileStoreError(f"{message}; the change was not committed") from error
        raise ProfileStoreError(message, exit_code=4) from error
    mutation.mark_committed()


def _rollback_after_commit_error(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        return
    try:
        connection.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _read_profile_row_state(
    connection: sqlite3.Connection,
    profile_name: str,
) -> _ProfileRowState | None:
    row = connection.execute(
        "SELECT name, id, revision, document FROM profiles WHERE name = ?",
        (profile_name,),
    ).fetchone()
    if row is None:
        return None
    name, profile_id, revision, document = tuple(row)
    if not isinstance(name, str) or not isinstance(profile_id, str):
        raise ProfileStoreError("stored profile identity is invalid")
    if type(revision) is not int or not isinstance(document, str):
        raise ProfileStoreError("stored profile revision is invalid")
    return _ProfileRowState(name, profile_id, revision, document)


def _credential_outcome_inspector(
    database_path: Path,
    base: _MutationEvidence,
) -> Callable[[tuple[CleanupRecord, ...], tuple[CleanupRecord, ...]], bool | None]:
    def inspect(
        removed_cleanup: tuple[CleanupRecord, ...],
        added_cleanup: tuple[CleanupRecord, ...],
    ) -> bool | None:
        evidence = _MutationEvidence(
            before=base.before,
            after=base.after,
            removed_cleanup=removed_cleanup,
            added_cleanup=added_cleanup,
        )
        outcome = _inspect_mutation_outcome(database_path, evidence)
        if outcome is _MutationOutcome.COMMITTED:
            return True
        if outcome is _MutationOutcome.NOT_COMMITTED:
            return False
        return None

    return inspect


def _inspect_mutation_outcome(
    database_path: Path,
    evidence: _MutationEvidence,
) -> _MutationOutcome:
    try:
        with closing(_connect(database_path, writable=False)) as inspection:
            if evidence.after is not None:
                profile_name = evidence.after.name
            elif evidence.before is not None:
                profile_name = evidence.before.name
            else:
                return _MutationOutcome.UNKNOWN
            current = _read_profile_row_state(inspection, profile_name)
            before_cleanup = _cleanup_records_match(
                inspection,
                present=evidence.removed_cleanup,
                absent=evidence.added_cleanup,
            )
            after_cleanup = _cleanup_records_match(
                inspection,
                present=evidence.added_cleanup,
                absent=evidence.removed_cleanup,
            )
    except (OSError, sqlite3.Error, ProfileStoreError, ReconciliationError):
        return _MutationOutcome.UNKNOWN
    before_matches = current == evidence.before and before_cleanup
    after_matches = current == evidence.after and after_cleanup
    if after_matches and not before_matches:
        return _MutationOutcome.COMMITTED
    if before_matches and not after_matches:
        return _MutationOutcome.NOT_COMMITTED
    return _MutationOutcome.UNKNOWN


def _cleanup_records_match(
    connection: sqlite3.Connection,
    *,
    present: tuple[CleanupRecord, ...],
    absent: tuple[CleanupRecord, ...],
) -> bool:
    for record in present:
        row = connection.execute(
            "SELECT secret_reference, created_at FROM credential_reconciliation WHERE id = ?",
            (record.record_id,),
        ).fetchone()
        if row is None or tuple(row) != (record.secret_reference, record.created_at):
            return False
    for record in absent:
        row = connection.execute(
            "SELECT 1 FROM credential_reconciliation WHERE id = ?",
            (record.record_id,),
        ).fetchone()
        if row is not None:
            return False
    return True


def _classify_post_commit_error(
    error: ProfileStoreError,
    mutation: _MutationTracker,
) -> ProfileStoreError:
    if mutation.outcome is not _MutationOutcome.COMMITTED or error.exit_code in {3, 4}:
        return ProfileStoreError(str(error), exit_code=error.exit_code)
    return ProfileStoreError(
        f"{mutation.operation} committed but completion could not be verified; "
        "inspect the profile and run 'kantrip doctor --repair'",
        exit_code=3,
    )


def _database_mutation_error(
    error: sqlite3.Error,
    mutation: _MutationTracker,
) -> ProfileStoreError:
    if mutation.outcome is _MutationOutcome.COMMITTED:
        return ProfileStoreError(
            f"{mutation.operation} committed but completion could not be verified; "
            "inspect the profile and run 'kantrip doctor --repair'",
            exit_code=3,
        )
    if mutation.outcome is _MutationOutcome.UNKNOWN:
        return ProfileStoreError(
            f"{mutation.operation} outcome could not be established; stop automatic retries",
            exit_code=4,
        )
    return ProfileStoreError("profile database could not be updated safely")


@contextmanager
def _writable_connection(path: Path) -> Iterator[sqlite3.Connection]:
    _ensure_private_parent(path.parent)
    with database_maintenance_lock(path):
        _create_or_validate_database_file(path)
        connection: sqlite3.Connection | None = None
        body_completed = False
        try:
            connection = _connect(path, writable=True)
            path.chmod(0o600)
            _validate_database_file(path)
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
    if writable:
        _configure_writable_connection(connection)
    return connection


def _migrate_existing_database(path: Path) -> MigrationResult:
    _validate_private_parent(path.parent)
    _validate_database_file(path)
    try:
        with closing(_connect(path, writable=True)) as connection:
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
            _validate_private_parent(directory)
            _sync_directory(directory.parent)
    except ProfileStoreError:
        raise
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
    os.fsync(descriptor)
    os.close(descriptor)
    _sync_directory(path.parent)
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
    "KafkaAuthInput",
    "ProfileCollection",
    "ProfileSnapshot",
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
    "resolve_profile_snapshot",
]
