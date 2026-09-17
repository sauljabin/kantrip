"""Track and reconcile exact credential references after partial mutations."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

from kantrip.secret_store import SecretStore, SecretStoreError, validate_secret_reference


class ReconciliationError(RuntimeError):
    """Raised when reconciliation state is corrupt or cannot be updated safely."""


@dataclass(frozen=True)
class CleanupRecord:
    """One non-secret instruction to delete an exact credential-store entry."""

    record_id: str
    secret_reference: str
    created_at: str


@dataclass(frozen=True)
class ReconciliationResult:
    """Aggregate result of one bounded credential cleanup pass."""

    pending: int
    removed: int
    failed: int

    @property
    def healthy(self) -> bool:
        """Return whether every pending record was reconciled."""
        return self.failed == 0


def queue_secret_cleanup(
    connection: sqlite3.Connection,
    secret_reference: str,
    *,
    record_id: str | None = None,
    created_at: str | None = None,
) -> CleanupRecord:
    """Add one cleanup instruction inside the caller's SQLite transaction."""
    validate_secret_reference(secret_reference)
    selected_id = record_id or str(uuid.uuid4())
    _validate_record_id(selected_id)
    selected_created_at = created_at or datetime.now(timezone.utc).isoformat()
    _validate_created_at(selected_created_at)
    try:
        connection.execute(
            "INSERT INTO credential_reconciliation (id, secret_reference, created_at) "
            "VALUES (?, ?, ?)",
            (selected_id, secret_reference, selected_created_at),
        )
    except sqlite3.IntegrityError as error:
        raise ReconciliationError("credential cleanup reference is already pending") from error
    return CleanupRecord(selected_id, secret_reference, selected_created_at)


def pending_secret_cleanup(connection: sqlite3.Connection) -> tuple[CleanupRecord, ...]:
    """Return validated pending cleanup records without modifying them."""
    try:
        rows = connection.execute(
            "SELECT id, secret_reference, created_at "
            "FROM credential_reconciliation ORDER BY created_at, id"
        ).fetchall()
    except sqlite3.Error as error:
        raise ReconciliationError("credential reconciliation journal could not be read") from error
    records: list[CleanupRecord] = []
    for row in rows:
        record_id = row["id"]
        secret_reference = row["secret_reference"]
        created_at = row["created_at"]
        if not all(isinstance(value, str) for value in (record_id, secret_reference, created_at)):
            raise ReconciliationError("credential reconciliation record is invalid")
        _validate_record_id(record_id)
        try:
            validate_secret_reference(secret_reference)
        except SecretStoreError as error:
            raise ReconciliationError("credential reconciliation record is invalid") from error
        try:
            _validate_created_at(created_at)
        except ReconciliationError as error:
            raise ReconciliationError("credential reconciliation record is invalid") from error
        records.append(CleanupRecord(record_id, secret_reference, created_at))
    return tuple(records)


def reconcile_secret_cleanup(
    connection: sqlite3.Connection,
    store: SecretStore,
    *,
    record_ids: Iterable[str] | None = None,
) -> ReconciliationResult:
    """Delete selected orphan entries after validating the complete journal."""
    records = pending_secret_cleanup(connection)
    live_references = _live_secret_references(connection)
    conflicts = [
        record.secret_reference for record in records if record.secret_reference in live_references
    ]
    if conflicts:
        raise ReconciliationError("credential reconciliation references a live profile credential")
    selected = records
    if record_ids is not None:
        requested = tuple(record_ids)
        if len(set(requested)) != len(requested):
            raise ReconciliationError("credential reconciliation IDs are duplicated")
        by_id = {record.record_id: record for record in records}
        if any(record_id not in by_id for record_id in requested):
            raise ReconciliationError("credential reconciliation record changed unexpectedly")
        selected = tuple(by_id[record_id] for record_id in requested)
    removed = 0
    failed = 0
    for record in selected:
        try:
            store.delete(record.secret_reference)
        except SecretStoreError:
            failed += 1
            continue
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "DELETE FROM credential_reconciliation WHERE id = ? AND secret_reference = ?",
                (record.record_id, record.secret_reference),
            )
            if cursor.rowcount != 1:
                raise ReconciliationError("credential reconciliation record changed unexpectedly")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        removed += 1
    return ReconciliationResult(len(selected), removed, failed)


def _live_secret_references(connection: sqlite3.Connection) -> set[str]:
    try:
        rows = connection.execute("SELECT document FROM profiles").fetchall()
    except sqlite3.Error as error:
        raise ReconciliationError("live credential references could not be inspected") from error
    references: set[str] = set()
    for row in rows:
        document = row["document"]
        if not isinstance(document, str):
            raise ReconciliationError("live profile document is invalid")
        try:
            value = json.loads(document)
        except json.JSONDecodeError as error:
            raise ReconciliationError("live profile document is invalid") from error
        _collect_secret_references(value, references)
    return references


def _collect_secret_references(value: object, references: set[str]) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if isinstance(key, str) and key.endswith("Ref"):
                if not isinstance(nested, str):
                    raise ReconciliationError("live credential reference is invalid")
                try:
                    validate_secret_reference(nested)
                except SecretStoreError as error:
                    raise ReconciliationError("live credential reference is invalid") from error
                references.add(nested)
            else:
                _collect_secret_references(nested, references)
    elif isinstance(value, list):
        for nested in value:
            _collect_secret_references(nested, references)


def _validate_record_id(record_id: str) -> None:
    try:
        parsed = uuid.UUID(record_id)
    except (AttributeError, ValueError) as error:
        raise ReconciliationError("credential reconciliation ID is invalid") from error
    if str(parsed) != record_id:
        raise ReconciliationError("credential reconciliation ID is invalid")


def _validate_created_at(created_at: str) -> None:
    if not isinstance(created_at, str):
        raise ReconciliationError("credential reconciliation timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(created_at)
    except ValueError as error:
        raise ReconciliationError("credential reconciliation timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ReconciliationError("credential reconciliation timestamp is invalid")
    if parsed.isoformat() != created_at:
        raise ReconciliationError("credential reconciliation timestamp is invalid")


__all__ = [
    "CleanupRecord",
    "ReconciliationError",
    "ReconciliationResult",
    "pending_secret_cleanup",
    "queue_secret_cleanup",
    "reconcile_secret_cleanup",
]
