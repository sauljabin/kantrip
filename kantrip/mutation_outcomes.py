"""Classify profile mutation commits from durable database evidence."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from kantrip import profile_storage as storage
from kantrip.credential_mutations import CredentialMutationError
from kantrip.profile_storage import ProfileStoreError
from kantrip.reconciliation import CleanupRecord, ReconciliationError


class _MutationOutcome(Enum):
    """Durable outcome of the profile-row transaction."""

    NOT_COMMITTED = "not-committed"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


@dataclass
class MutationTracker:
    operation: str
    outcome: _MutationOutcome = _MutationOutcome.NOT_COMMITTED

    def mark_committed(self) -> None:
        self.outcome = _MutationOutcome.COMMITTED


@dataclass(frozen=True)
class ProfileRowState:
    name: str
    profile_id: str
    revision: int
    document: str


@dataclass(frozen=True)
class MutationEvidence:
    before: ProfileRowState | None
    after: ProfileRowState | None
    removed_cleanup: tuple[CleanupRecord, ...] = ()
    added_cleanup: tuple[CleanupRecord, ...] = ()


def profile_mutation_error(error: CredentialMutationError) -> ProfileStoreError:
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


def commit_with_evidence(
    connection: sqlite3.Connection,
    database_path: Path,
    evidence: MutationEvidence,
    mutation: MutationTracker,
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


def read_profile_row_state(
    connection: sqlite3.Connection,
    profile_name: str,
) -> ProfileRowState | None:
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
    return ProfileRowState(name, profile_id, revision, document)


def credential_outcome_inspector(
    database_path: Path,
    base: MutationEvidence,
) -> Callable[[tuple[CleanupRecord, ...], tuple[CleanupRecord, ...]], bool | None]:
    def inspect(
        removed_cleanup: tuple[CleanupRecord, ...],
        added_cleanup: tuple[CleanupRecord, ...],
    ) -> bool | None:
        evidence = MutationEvidence(
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
    evidence: MutationEvidence,
) -> _MutationOutcome:
    try:
        with closing(storage.connect(database_path, writable=False)) as inspection:
            if evidence.after is not None:
                profile_name = evidence.after.name
            elif evidence.before is not None:
                profile_name = evidence.before.name
            else:
                return _MutationOutcome.UNKNOWN
            current = read_profile_row_state(inspection, profile_name)
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


def classify_post_commit_error(
    error: ProfileStoreError,
    mutation: MutationTracker,
) -> ProfileStoreError:
    if mutation.outcome is not _MutationOutcome.COMMITTED or error.exit_code in {3, 4}:
        return ProfileStoreError(str(error), exit_code=error.exit_code)
    return ProfileStoreError(
        f"{mutation.operation} committed but completion could not be verified; "
        "inspect the profile and run 'kantrip doctor --repair'",
        exit_code=3,
    )


def database_mutation_error(
    error: sqlite3.Error,
    mutation: MutationTracker,
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


__all__ = [
    "MutationEvidence",
    "MutationTracker",
    "ProfileRowState",
    "classify_post_commit_error",
    "commit_with_evidence",
    "credential_outcome_inspector",
    "database_mutation_error",
    "profile_mutation_error",
    "read_profile_row_state",
]
