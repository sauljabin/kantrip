"""Add, edit, remove, and resolve profiles through recoverable mutations."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Mapping
from contextlib import closing
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kantrip import profile_storage as storage
from kantrip.credential_mutations import (
    CredentialMutationError,
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
)
from kantrip.mutation_outcomes import (
    MutationEvidence,
    MutationTracker,
    ProfileRowState,
    classify_post_commit_error,
    commit_with_evidence,
    credential_outcome_inspector,
    database_mutation_error,
    profile_mutation_error,
    read_profile_row_state,
)
from kantrip.profile_auth import (
    AuthPlan,
    KafkaAuthInput,
    RegistryAuthInput,
    RegistryAuthPlan,
    apply_edit_authentication_plans,
    apply_new_authentication_plans,
    new_registry_auth_plan,
    plan_authentication,
    profile_secret_references,
    validate_auth_transport,
)
from kantrip.profile_documents import (
    DEFAULT_BOOTSTRAP_SERVER,
    apply_profile_edits,
    new_profile,
    requested_registry_edit_plan,
    validate_edit_request,
)
from kantrip.profile_storage import ProfileStoreError
from kantrip.registry import (
    RegistryConnection,
    RegistryProfileError,
    registry_connection,
    resolve_registry_connection,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store


@dataclass(frozen=True)
class ProfileSnapshot:
    """One immutable profile generation with credentials resolved in memory."""

    name: str
    profile_id: str
    revision: int
    document: Mapping[str, Any]
    kafka: KafkaConnection
    registry: RegistryConnection | None


def resolve_profile_snapshot(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
) -> ProfileSnapshot:
    """Load and resolve one generation while excluding credential mutation."""
    storage.validate_profile_name(profile_name)
    database_path = path if path is not None else storage.resolve_database_path(environment)
    if not storage.path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    storage.validate_private_parent(database_path.parent)
    storage.validate_database_file(database_path)
    try:
        with (
            storage.database_maintenance_lock(database_path),
            closing(storage.connect(database_path, writable=False)) as connection,
        ):
            state = storage.inspect_migration_state(connection)
            if state.requires_migration:
                raise ProfileStoreError(storage.pending_migration_message(state))
            collection = storage.load_profile_collection(database_path, connection)
            document = deepcopy(collection.profile(profile_name))
            revision = collection.revision(profile_name)
            parsed = kafka_connection(document)
            registry = registry_connection(document)
            if parsed.requires_secrets or registry is not None and registry.requires_secrets:
                selected_store = secret_store or load_secret_store()
                if parsed.requires_secrets:
                    parsed = resolve_kafka_connection(parsed, selected_store)
                if registry is not None and registry.requires_secrets:
                    registry = resolve_registry_connection(registry, selected_store)
            return ProfileSnapshot(
                profile_name,
                str(document["id"]),
                revision,
                document,
                parsed,
                registry,
            )
    except (KafkaProfileError, RegistryProfileError, SecretStoreError) as error:
        raise ProfileStoreError(str(error)) from error
    except sqlite3.Error as error:
        raise ProfileStoreError("profile snapshot could not be resolved safely") from error


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
    registry_auth: RegistryAuthInput | None = None,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
) -> storage.ProfileCollection:
    """Add one validated profile, staging any secrets before its database row."""
    storage.validate_profile_name(profile_name)
    database_path = path if path is not None else storage.resolve_database_path(environment)
    profile_id = str(uuid.uuid4())
    auth_input = auth or KafkaAuthInput("none")
    auth_plan = plan_authentication(profile_id, None, auth_input)
    registry_plan = new_registry_auth_plan(profile_id, registry_url, registry_auth)
    profile = new_profile(
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
    storage.validate_profile(profile)
    validate_auth_transport(auth_plan.auth_type, transport)
    mutation = MutationTracker(f"profile '{profile_name}' creation")

    try:
        with storage.writable_connection(database_path) as connection:
            if profile_name in storage.load_profile_rows(connection):
                raise ProfileStoreError(f"profile '{profile_name}' already exists")
            replacements = auth_plan.replacements + (
                registry_plan.replacements if registry_plan is not None else ()
            )
            if not replacements:
                apply_new_authentication_plans(profile, auth_plan, registry_plan, {})
                storage.validate_profile(profile)
                storage.validate_stored_registry(profile)
                document = storage.encode_profile(profile)
                evidence = MutationEvidence(
                    before=None,
                    after=ProfileRowState(profile_name, profile_id, 1, document),
                )
                _insert_profile(
                    connection,
                    profile_name,
                    profile,
                    database_path=database_path,
                    evidence=evidence,
                    mutation=mutation,
                )
                return storage.load_profile_collection(database_path, connection)
            store = secret_store or load_secret_store()
            staged = stage_secret_replacements(
                connection,
                store,
                profile_id,
                replacements,
            )
            staged_references = {item.field: item.reference for item in staged}
            apply_new_authentication_plans(profile, auth_plan, registry_plan, staged_references)
            storage.validate_profile(profile)
            storage.validate_stored_registry(profile)
            evidence = MutationEvidence(
                before=None,
                after=ProfileRowState(
                    profile_name,
                    profile_id,
                    1,
                    storage.encode_profile(profile),
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
                inspect_outcome=credential_outcome_inspector(database_path, evidence),
            )
            mutation.mark_committed()
            return storage.load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise database_mutation_error(error, mutation) from error


def remove_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    secret_store: SecretStore | None = None,
    expected_profile_id: str | None = None,
    expected_revision: int | None = None,
) -> storage.ProfileCollection:
    """Remove one profile before reconciling its exact owned credentials."""
    storage.validate_profile_name(profile_name)
    database_path = path if path is not None else storage.resolve_database_path(environment)
    if not storage.path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    mutation = MutationTracker(f"profile '{profile_name}' removal")

    try:
        with storage.writable_connection(database_path) as connection:
            revisions: dict[str, int] = {}
            profiles = storage.load_profile_rows(connection, revisions=revisions)
            profile = profiles.get(profile_name)
            if profile is None:
                raise ProfileStoreError(f"profile '{profile_name}' was not found")
            current_revision = revisions[profile_name]
            before = read_profile_row_state(connection, profile_name)
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
            references = profile_secret_references(profile)
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
                commit_with_evidence(
                    connection,
                    database_path,
                    MutationEvidence(before=before, after=None),
                    mutation,
                    "profile removal commit outcome could not be established",
                )
            except BaseException:
                storage.rollback(connection)
                raise
            return storage.load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise database_mutation_error(error, mutation) from error


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
    before: ProfileRowState,
    mutation: MutationTracker,
) -> storage.ProfileCollection:
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
        inspect_outcome=credential_outcome_inspector(
            database_path,
            MutationEvidence(before=before, after=None),
        ),
    )
    mutation.mark_committed()
    collection = storage.load_profile_collection(database_path, connection)
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
    registry_auth: RegistryAuthInput | None = None,
    remove_registry: bool = False,
    environment: Mapping[str, str] | None = None,
    expected_revision: int | None = None,
    expected_profile_id: str | None = None,
    secret_store: SecretStore | None = None,
) -> storage.ProfileCollection:
    """Update explicit fields of one existing profile transactionally."""
    storage.validate_profile_name(profile_name)
    validate_edit_request(
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
        registry_auth=registry_auth,
        remove_registry=remove_registry,
    )
    database_path = path if path is not None else storage.resolve_database_path(environment)
    if not storage.path_entry_exists(database_path):
        raise ProfileStoreError(f"profile '{profile_name}' was not found")
    mutation = MutationTracker(f"profile '{profile_name}' update")
    try:
        with storage.writable_connection(database_path) as connection:
            revisions: dict[str, int] = {}
            profiles = storage.load_profile_rows(connection, revisions=revisions)
            current = profiles.get(profile_name)
            if current is None:
                raise ProfileStoreError(f"profile '{profile_name}' was not found")
            current_revision = revisions[profile_name]
            before = read_profile_row_state(connection, profile_name)
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
            updated = apply_profile_edits(
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
            kafka_plan: AuthPlan | None = None
            if auth is not None:
                kafka_plan = plan_authentication(current["id"], current["kafka"]["auth"], auth)
                validate_auth_transport(kafka_plan.auth_type, updated["kafka"]["transport"])
            registry_plan = requested_registry_edit_plan(
                str(current["id"]), current, updated, registry_auth
            )
            if kafka_plan is not None or registry_plan is not None:
                return _commit_authenticated_edit(
                    connection,
                    database_path,
                    profile_name,
                    current,
                    current_revision,
                    updated,
                    kafka_plan,
                    registry_plan,
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
            return storage.load_profile_collection(database_path, connection)
    except CredentialMutationError as error:
        raise profile_mutation_error(error) from error
    except SecretStoreError as error:
        raise ProfileStoreError(str(error)) from error
    except ProfileStoreError as error:
        raise classify_post_commit_error(error, mutation) from error
    except sqlite3.Error as error:
        raise database_mutation_error(error, mutation) from error


def _commit_plain_edit(
    connection: sqlite3.Connection,
    profile_name: str,
    current: Mapping[str, Any],
    current_revision: int,
    updated: dict[str, Any],
    database_path: Path,
    before: ProfileRowState,
    mutation: MutationTracker,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        storage.validate_profile(updated)
        storage.validate_stored_registry(updated)
        try:
            update_profile_revision(
                connection,
                profile_name=profile_name,
                profile_id=current["id"],
                expected_revision=current_revision,
                document=storage.encode_profile(updated),
            )
        except CredentialMutationError as error:
            raise ProfileStoreError(f"profile '{profile_name}' changed unexpectedly") from error
        evidence = MutationEvidence(
            before=before,
            after=ProfileRowState(
                profile_name,
                str(current["id"]),
                current_revision + 1,
                storage.encode_profile(updated),
            ),
        )
        commit_with_evidence(
            connection,
            database_path,
            evidence,
            mutation,
            "profile update commit outcome could not be established",
        )
    except BaseException:
        storage.rollback(connection)
        raise


def _commit_authenticated_edit(
    connection: sqlite3.Connection,
    database_path: Path,
    profile_name: str,
    current: Mapping[str, Any],
    current_revision: int,
    updated: dict[str, Any],
    kafka_plan: AuthPlan | None,
    registry_plan: RegistryAuthPlan | None,
    secret_store: SecretStore | None,
    before: ProfileRowState,
    mutation: MutationTracker,
) -> storage.ProfileCollection:
    def switch(references: Mapping[str, str]) -> None:
        apply_edit_authentication_plans(updated, kafka_plan, registry_plan, references)
        storage.validate_profile(updated)
        storage.validate_stored_registry(updated)
        update_profile_revision(
            connection,
            profile_name=profile_name,
            profile_id=current["id"],
            expected_revision=current_revision,
            document=storage.encode_profile(updated),
        )

    replacements = (kafka_plan.replacements if kafka_plan is not None else ()) + (
        registry_plan.replacements if registry_plan is not None else ()
    )
    retire_references = (kafka_plan.retire_references if kafka_plan is not None else ()) + (
        registry_plan.retire_references if registry_plan is not None else ()
    )
    if not replacements and not retire_references:
        apply_edit_authentication_plans(updated, kafka_plan, registry_plan, {})
        storage.validate_profile(updated)
        storage.validate_stored_registry(updated)
        evidence = MutationEvidence(
            before=before,
            after=ProfileRowState(
                profile_name,
                str(current["id"]),
                current_revision + 1,
                storage.encode_profile(updated),
            ),
        )
        connection.execute("BEGIN IMMEDIATE")
        try:
            switch({})
            commit_with_evidence(
                connection,
                database_path,
                evidence,
                mutation,
                "profile update commit outcome could not be established",
            )
        except BaseException:
            storage.rollback(connection)
            raise
        return storage.load_profile_collection(database_path, connection)

    store = secret_store or load_secret_store()
    staged = stage_secret_replacements(
        connection,
        store,
        current["id"],
        replacements,
    )
    staged_references = {item.field: item.reference for item in staged}
    apply_edit_authentication_plans(updated, kafka_plan, registry_plan, staged_references)
    storage.validate_profile(updated)
    storage.validate_stored_registry(updated)
    evidence = MutationEvidence(
        before=before,
        after=ProfileRowState(
            profile_name,
            str(current["id"]),
            current_revision + 1,
            storage.encode_profile(updated),
        ),
    )
    result = commit_secret_replacements(
        connection,
        store,
        current["id"],
        staged,
        switch,
        retire_references=retire_references,
        inspect_outcome=credential_outcome_inspector(database_path, evidence),
    )
    mutation.mark_committed()
    collection = storage.load_profile_collection(database_path, connection)
    if result.failed:
        raise ProfileStoreError(
            f"profile '{profile_name}' was updated but credential cleanup is pending; "
            "run 'kantrip doctor --repair'",
            exit_code=3,
        )
    return collection


def _insert_profile(
    connection: sqlite3.Connection,
    profile_name: str,
    profile: Mapping[str, Any],
    *,
    transaction: bool = True,
    database_path: Path | None = None,
    evidence: MutationEvidence | None = None,
    mutation: MutationTracker | None = None,
) -> None:
    if transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "INSERT INTO profiles (name, id, revision, document) VALUES (?, ?, 1, ?)",
            (profile_name, profile["id"], storage.encode_profile(profile)),
        )
        if transaction:
            if database_path is None or evidence is None or mutation is None:
                raise RuntimeError("profile mutation evidence is required")
            commit_with_evidence(
                connection,
                database_path,
                evidence,
                mutation,
                "profile creation commit outcome could not be established",
            )
    except BaseException:
        if transaction:
            storage.rollback(connection)
        raise


__all__ = [
    "ProfileSnapshot",
    "add_profile",
    "edit_profile",
    "remove_profile",
    "resolve_profile_snapshot",
]
