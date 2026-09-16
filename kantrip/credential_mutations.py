"""Coordinate recoverable profile mutations across SQLite and a secret store."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass

from kantrip.reconciliation import (
    CleanupRecord,
    ReconciliationResult,
    queue_secret_cleanup,
    reconcile_secret_cleanup,
)
from kantrip.secret_store import (
    SecretStore,
    SecretStoreError,
    parse_secret_reference,
    secret_reference,
)


class CredentialMutationError(RuntimeError):
    """Raised when a cross-store credential mutation cannot finish safely."""

    def __init__(self, message: str, *, committed: bool | None = False) -> None:
        super().__init__(message)
        self.committed = committed


@dataclass(frozen=True)
class SecretReplacement:
    """One new secret and the exact prior reference it supersedes, if any."""

    field: str
    value: str
    previous_reference: str | None = None


@dataclass(frozen=True)
class StagedSecret:
    """One secret protected by a durable cleanup record until profile commit."""

    field: str
    reference: str
    previous_reference: str | None
    cleanup: CleanupRecord


ProfileSwitch = Callable[[Mapping[str, str]], None]
ProfileRemoval = Callable[[], None]


def update_profile_revision(
    connection: sqlite3.Connection,
    *,
    profile_name: str,
    profile_id: str,
    expected_revision: int,
    document: str,
) -> None:
    """Switch one exact profile generation inside the caller's transaction."""
    cursor = connection.execute(
        "UPDATE profiles SET revision = revision + 1, document = ? "
        "WHERE name = ? AND id = ? AND revision = ?",
        (document, profile_name, profile_id, expected_revision),
    )
    if cursor.rowcount != 1:
        raise CredentialMutationError("profile changed while credentials were collected")


def remove_profile_revision(
    connection: sqlite3.Connection,
    *,
    profile_name: str,
    profile_id: str,
    expected_revision: int,
) -> None:
    """Remove one exact profile generation inside the caller's transaction."""
    cursor = connection.execute(
        "DELETE FROM profiles WHERE name = ? AND id = ? AND revision = ?",
        (profile_name, profile_id, expected_revision),
    )
    if cursor.rowcount != 1:
        raise CredentialMutationError("profile changed before it could be removed")


def stage_secret_replacements(
    connection: sqlite3.Connection,
    store: SecretStore,
    profile_id: str,
    replacements: Sequence[SecretReplacement],
) -> tuple[StagedSecret, ...]:
    """Journal and store new immutable credentials before a profile switch."""
    validated = _validate_replacements(profile_id, replacements)
    if not validated:
        return ()
    staged = tuple(
        (
            replacement.field,
            secret_reference(profile_id, replacement.field),
            replacement.previous_reference,
        )
        for replacement in validated
    )
    try:
        connection.execute("BEGIN IMMEDIATE")
        journaled = tuple(
            StagedSecret(
                field,
                reference,
                previous_reference,
                queue_secret_cleanup(connection, reference),
            )
            for field, reference, previous_reference in staged
        )
        connection.execute("COMMIT")
    except BaseException:
        _rollback(connection)
        raise

    try:
        for item, replacement in zip(journaled, validated, strict=True):
            store.set(item.reference, replacement.value)
            if store.get(item.reference) != replacement.value:
                raise CredentialMutationError("staged credential verification failed")
    except SecretStoreError as error:
        raise CredentialMutationError("profile credentials could not be staged") from error
    return journaled


def commit_secret_replacements(
    connection: sqlite3.Connection,
    store: SecretStore,
    profile_id: str,
    staged: Sequence[StagedSecret],
    switch_profile: ProfileSwitch,
    *,
    retire_references: Iterable[str] = (),
) -> ReconciliationResult:
    """Switch a profile to staged references and retire superseded values."""
    committed = False
    try:
        references = {item.field: item.reference for item in staged}
        if len(references) != len(staged):
            raise CredentialMutationError("a credential field was staged more than once")
        retired = _validated_unique_references(
            (
                *(
                    item.previous_reference
                    for item in staged
                    if item.previous_reference is not None
                ),
                *retire_references,
            ),
            profile_id=profile_id,
        )
        if set(retired).intersection(references.values()):
            raise CredentialMutationError("an active credential reference cannot be retired")
        connection.execute("BEGIN IMMEDIATE")
        _verify_staged_records(connection, staged)
        switch_profile(references)
        for item in staged:
            _remove_cleanup_record(connection, item.cleanup)
        for reference in retired:
            queue_secret_cleanup(connection, reference)
        try:
            connection.execute("COMMIT")
        except BaseException as error:
            if connection.in_transaction and _rollback(connection):
                raise
            raise CredentialMutationError(
                "credential mutation commit outcome could not be established",
                committed=None,
            ) from error
        committed = True
    except BaseException:
        _rollback(connection)
        raise
    try:
        return reconcile_secret_cleanup(connection, store)
    except BaseException as error:
        if committed:
            raise CredentialMutationError(
                "profile change committed but credential cleanup could not be verified",
                committed=True,
            ) from error
        raise


def commit_profile_removal(
    connection: sqlite3.Connection,
    store: SecretStore,
    profile_id: str,
    references: Iterable[str],
    remove_profile: ProfileRemoval,
) -> ReconciliationResult:
    """Remove a profile transactionally before deleting its exact credentials."""
    validated = _validated_unique_references(references, profile_id=profile_id)
    committed = False
    try:
        connection.execute("BEGIN IMMEDIATE")
        remove_profile()
        for reference in validated:
            queue_secret_cleanup(connection, reference)
        try:
            connection.execute("COMMIT")
        except BaseException as error:
            if connection.in_transaction and _rollback(connection):
                raise
            raise CredentialMutationError(
                "profile removal commit outcome could not be established",
                committed=None,
            ) from error
        committed = True
    except BaseException:
        _rollback(connection)
        raise
    try:
        return reconcile_secret_cleanup(connection, store)
    except BaseException as error:
        if committed:
            raise CredentialMutationError(
                "profile removal committed but credential cleanup could not be verified",
                committed=True,
            ) from error
        raise


def _validate_replacements(
    profile_id: str,
    replacements: Sequence[SecretReplacement],
) -> tuple[SecretReplacement, ...]:
    fields: set[str] = set()
    validated: list[SecretReplacement] = []
    for replacement in replacements:
        if replacement.field in fields:
            raise CredentialMutationError("a credential field was supplied more than once")
        if not isinstance(replacement.value, str) or not replacement.value:
            raise CredentialMutationError("credential values must be non-empty text")
        secret_reference(profile_id, replacement.field)
        if replacement.previous_reference is not None:
            previous = parse_secret_reference(replacement.previous_reference)
            if previous.profile_id != profile_id or previous.field != replacement.field:
                raise CredentialMutationError("superseded credential reference does not match")
        fields.add(replacement.field)
        validated.append(replacement)
    return tuple(validated)


def _validated_unique_references(
    references: Iterable[str],
    *,
    profile_id: str,
) -> tuple[str, ...]:
    validated: list[str] = []
    seen: set[str] = set()
    for reference in references:
        parsed = parse_secret_reference(reference)
        if parsed.profile_id != profile_id:
            raise CredentialMutationError("credential reference does not belong to profile")
        if reference in seen:
            raise CredentialMutationError("credential reference was supplied more than once")
        seen.add(reference)
        validated.append(reference)
    return tuple(validated)


def _verify_staged_records(
    connection: sqlite3.Connection,
    staged: Sequence[StagedSecret],
) -> None:
    for item in staged:
        row = connection.execute(
            "SELECT secret_reference FROM credential_reconciliation WHERE id = ?",
            (item.cleanup.record_id,),
        ).fetchone()
        if row is None or row["secret_reference"] != item.reference:
            raise CredentialMutationError("staged credential cleanup record changed unexpectedly")


def _remove_cleanup_record(
    connection: sqlite3.Connection,
    record: CleanupRecord,
) -> None:
    cursor = connection.execute(
        "DELETE FROM credential_reconciliation WHERE id = ? AND secret_reference = ?",
        (record.record_id, record.secret_reference),
    )
    if cursor.rowcount != 1:
        raise CredentialMutationError("staged credential cleanup record changed unexpectedly")


def _rollback(connection: sqlite3.Connection) -> bool:
    """Roll back when possible and report whether non-commit is established."""
    try:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        return not connection.in_transaction
    except sqlite3.Error:
        return False


__all__ = [
    "CredentialMutationError",
    "SecretReplacement",
    "StagedSecret",
    "commit_profile_removal",
    "commit_secret_replacements",
    "remove_profile_revision",
    "stage_secret_replacements",
    "update_profile_revision",
]
