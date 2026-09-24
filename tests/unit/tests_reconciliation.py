import json
import sqlite3
import unittest

from kantrip.migrations import apply_migrations
from kantrip.reconciliation import (
    ReconciliationError,
    pending_secret_cleanup,
    queue_secret_cleanup,
    reconcile_secret_cleanup,
)
from kantrip.secret_store import SecretStoreError, secret_reference

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"


class TestReconciliation(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        apply_migrations(self.connection, applied_by="test")

    def test_queues_and_reconciles_an_exact_secret_reference(self) -> None:
        reference = secret_reference(PROFILE_ID, "kafka/password")
        self.connection.execute("BEGIN IMMEDIATE")
        queued = queue_secret_cleanup(
            self.connection,
            reference,
            record_id="018f8f13-7c21-7cee-8000-000000000011",
            created_at="2026-09-14T12:00:00+00:00",
        )
        self.connection.execute("COMMIT")
        store = _Store()

        result = reconcile_secret_cleanup(self.connection, store)

        self.assertEqual(reference, queued.secret_reference)
        self.assertEqual([reference], store.deleted)
        self.assertEqual((1, 1, 0), (result.pending, result.removed, result.failed))
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_failed_deletion_remains_pending_for_retry(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/token")
        self.connection.execute("BEGIN IMMEDIATE")
        queue_secret_cleanup(self.connection, reference)
        self.connection.execute("COMMIT")
        store = _Store(fail=True)

        result = reconcile_secret_cleanup(self.connection, store)

        self.assertEqual((1, 0, 1), (result.pending, result.removed, result.failed))
        self.assertEqual(reference, pending_secret_cleanup(self.connection)[0].secret_reference)

    def test_duplicate_and_invalid_references_are_rejected(self) -> None:
        reference = secret_reference(PROFILE_ID, "kafka/tls/private-key")
        self.connection.execute("BEGIN IMMEDIATE")
        queue_secret_cleanup(self.connection, reference)
        with self.assertRaises(ReconciliationError):
            queue_secret_cleanup(self.connection, reference)
        self.connection.execute("ROLLBACK")
        with self.assertRaises(SecretStoreError):
            queue_secret_cleanup(self.connection, "unsafe/reference")

    def test_corrupt_journal_record_fails_closed(self) -> None:
        self.connection.execute(
            "INSERT INTO credential_reconciliation VALUES (?, ?, ?)",
            ("not-a-uuid", "unsafe/reference", "now"),
        )

        with self.assertRaises(ReconciliationError):
            pending_secret_cleanup(self.connection)

    def test_noncanonical_or_non_utc_timestamps_fail_closed(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/token")
        for created_at in (
            "not-a-timestamp",
            "2026-09-14T01:02:03",
            "2026-09-14T01:02:03+01:00",
            "2026-09-14T01:02:03Z",
        ):
            with (
                self.subTest(created_at=created_at),
                self.assertRaises(ReconciliationError),
            ):
                queue_secret_cleanup(self.connection, reference, created_at=created_at)

    def test_live_reference_in_cleanup_journal_fails_without_deleting(self) -> None:
        reference = secret_reference(PROFILE_ID, "kafka/password")
        document = json.dumps({"kafka": {"auth": {"passwordRef": reference}}})
        self.connection.execute(
            "INSERT INTO profiles (name, id, revision, document) VALUES (?, ?, 1, ?)",
            ("local", PROFILE_ID, document),
        )
        self.connection.execute("BEGIN IMMEDIATE")
        queue_secret_cleanup(self.connection, reference)
        self.connection.execute("COMMIT")
        store = _Store()

        with self.assertRaisesRegex(ReconciliationError, "live profile credential"):
            reconcile_secret_cleanup(self.connection, store)

        self.assertEqual([], store.deleted)
        self.assertEqual(1, len(pending_secret_cleanup(self.connection)))


class _Store:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.deleted: list[str] = []

    def get(self, reference: str) -> str:
        raise NotImplementedError

    def set(self, reference: str, value: str) -> None:
        raise NotImplementedError

    def delete(self, reference: str) -> None:
        if self.fail:
            raise SecretStoreError("synthetic failure")
        self.deleted.append(reference)


if __name__ == "__main__":
    unittest.main()
