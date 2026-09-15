import sqlite3
import unittest

from kantrip.credential_mutations import (
    CredentialMutationError,
    SecretReplacement,
    commit_profile_removal,
    commit_secret_replacements,
    remove_profile_revision,
    stage_secret_replacements,
    update_profile_revision,
)
from kantrip.migrations import apply_migrations
from kantrip.reconciliation import pending_secret_cleanup
from kantrip.secret_store import SecretNotFoundError, SecretStoreError, secret_reference

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
OLD_CREDENTIAL_ID = "018f8f13-7c21-7cee-8000-000000000011"


class TestCredentialMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        apply_migrations(self.connection, applied_by="test")
        self.connection.execute(
            "INSERT INTO profiles (name, id, revision, document) VALUES (?, ?, 1, '{}')",
            ("local", PROFILE_ID),
        )

    def test_stages_new_immutable_reference_behind_cleanup_intent(self) -> None:
        store = _Store(self.connection)

        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("kafka/password", "synthetic-secret"),),
        )

        self.assertEqual(1, len(staged))
        self.assertNotIn("synthetic-secret", staged[0].reference)
        self.assertEqual("synthetic-secret", store.values[staged[0].reference])
        self.assertEqual(
            staged[0].reference, pending_secret_cleanup(self.connection)[0].secret_reference
        )
        self.assertTrue(store.saw_cleanup_before_set)

    def test_profile_switch_retires_superseded_reference(self) -> None:
        store = _Store(self.connection)
        old_reference = secret_reference(
            PROFILE_ID,
            "kafka/password",
            credential_id=OLD_CREDENTIAL_ID,
        )
        store.values[old_reference] = "old-secret"
        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("kafka/password", "new-secret", old_reference),),
        )

        def switch(references: dict[str, str]) -> None:
            update_profile_revision(
                self.connection,
                profile_name="local",
                profile_id=PROFILE_ID,
                expected_revision=1,
                document=references["kafka/password"],
            )

        result = commit_secret_replacements(self.connection, store, PROFILE_ID, staged, switch)

        row = self.connection.execute(
            "SELECT revision, document FROM profiles WHERE id = ?", (PROFILE_ID,)
        ).fetchone()
        self.assertEqual((2, staged[0].reference), tuple(row))
        self.assertNotIn(old_reference, store.values)
        self.assertEqual("new-secret", store.values[staged[0].reference])
        self.assertEqual((), pending_secret_cleanup(self.connection))
        self.assertEqual((1, 1, 0), (result.pending, result.removed, result.failed))

    def test_failed_profile_switch_cleans_staged_value_and_preserves_profile(self) -> None:
        store = _Store(self.connection)
        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("registry/token", "synthetic-token"),),
        )

        def fail_switch(references: dict[str, str]) -> None:
            del references
            raise RuntimeError("synthetic database failure")

        with self.assertRaisesRegex(RuntimeError, "database failure"):
            commit_secret_replacements(self.connection, store, PROFILE_ID, staged, fail_switch)

        row = self.connection.execute(
            "SELECT revision, document FROM profiles WHERE id = ?", (PROFILE_ID,)
        ).fetchone()
        self.assertEqual((1, "{}"), tuple(row))
        self.assertNotIn(staged[0].reference, store.values)
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_profile_switch_cannot_retire_its_new_active_reference(self) -> None:
        store = _Store(self.connection)
        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("kafka/password", "synthetic-secret"),),
        )

        with self.assertRaisesRegex(CredentialMutationError, "active credential"):
            commit_secret_replacements(
                self.connection,
                store,
                PROFILE_ID,
                staged,
                lambda references: None,
                retire_references=(staged[0].reference,),
            )

        self.assertNotIn(staged[0].reference, store.values)
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_concurrent_revision_change_rejects_switch_and_cleans_staging(self) -> None:
        store = _Store(self.connection)
        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("kafka/password", "synthetic-secret"),),
        )
        self.connection.execute("UPDATE profiles SET revision = 2 WHERE id = ?", (PROFILE_ID,))

        with self.assertRaisesRegex(CredentialMutationError, "changed"):
            commit_secret_replacements(
                self.connection,
                store,
                PROFILE_ID,
                staged,
                lambda references: update_profile_revision(
                    self.connection,
                    profile_name="local",
                    profile_id=PROFILE_ID,
                    expected_revision=1,
                    document=references["kafka/password"],
                ),
            )

        row = self.connection.execute(
            "SELECT revision, document FROM profiles WHERE id = ?", (PROFILE_ID,)
        ).fetchone()
        self.assertEqual((2, "{}"), tuple(row))
        self.assertNotIn(staged[0].reference, store.values)
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_failed_superseded_deletion_remains_retryable(self) -> None:
        store = _Store(self.connection)
        old_reference = secret_reference(
            PROFILE_ID,
            "registry/token",
            credential_id=OLD_CREDENTIAL_ID,
        )
        store.values[old_reference] = "old-token"
        staged = stage_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            (SecretReplacement("registry/token", "new-token", old_reference),),
        )
        store.fail_delete.add(old_reference)

        result = commit_secret_replacements(
            self.connection,
            store,
            PROFILE_ID,
            staged,
            lambda references: self.connection.execute(
                "UPDATE profiles SET revision = revision + 1, document = ? WHERE id = ?",
                (references["registry/token"], PROFILE_ID),
            ),
        )

        self.assertEqual((1, 0, 1), (result.pending, result.removed, result.failed))
        self.assertEqual(old_reference, pending_secret_cleanup(self.connection)[0].secret_reference)
        self.assertEqual("new-token", store.values[staged[0].reference])

    def test_partial_store_failure_cleans_every_staged_reference(self) -> None:
        store = _Store(self.connection, fail_set_after=1)

        with self.assertRaises(CredentialMutationError):
            stage_secret_replacements(
                self.connection,
                store,
                PROFILE_ID,
                (
                    SecretReplacement("kafka/password", "first-secret"),
                    SecretReplacement("registry/password", "second-secret"),
                ),
            )

        self.assertEqual({}, store.values)
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_profile_removal_commits_before_exact_cleanup(self) -> None:
        store = _Store(self.connection)
        reference = secret_reference(
            PROFILE_ID,
            "kafka/password",
            credential_id=OLD_CREDENTIAL_ID,
        )
        store.values[reference] = "synthetic-secret"

        result = commit_profile_removal(
            self.connection,
            store,
            PROFILE_ID,
            (reference,),
            lambda: remove_profile_revision(
                self.connection,
                profile_name="local",
                profile_id=PROFILE_ID,
                expected_revision=1,
            ),
        )

        self.assertEqual(0, self.connection.execute("SELECT COUNT(*) FROM profiles").fetchone()[0])
        self.assertTrue(store.saw_profile_removed_before_delete)
        self.assertEqual((1, 1, 0), (result.pending, result.removed, result.failed))

    def test_rejects_duplicate_or_mismatched_replacements_before_writes(self) -> None:
        store = _Store(self.connection)
        other_profile = "018f8f13-7c21-7cee-8000-000000000099"
        wrong_owner = secret_reference(
            other_profile,
            "kafka/password",
            credential_id=OLD_CREDENTIAL_ID,
        )
        cases = (
            (
                SecretReplacement("kafka/password", "one"),
                SecretReplacement("kafka/password", "two"),
            ),
            (SecretReplacement("kafka/password", "one", wrong_owner),),
        )
        for replacements in cases:
            with (
                self.subTest(replacements=replacements),
                self.assertRaises(CredentialMutationError),
            ):
                stage_secret_replacements(
                    self.connection,
                    store,
                    PROFILE_ID,
                    replacements,
                )
        self.assertEqual({}, store.values)
        self.assertEqual((), pending_secret_cleanup(self.connection))

    def test_profile_removal_rejects_a_reference_owned_by_another_profile(self) -> None:
        store = _Store(self.connection)
        other_profile = "018f8f13-7c21-7cee-8000-000000000099"
        wrong_owner = secret_reference(
            other_profile,
            "registry/token",
            credential_id=OLD_CREDENTIAL_ID,
        )

        with self.assertRaisesRegex(CredentialMutationError, "does not belong"):
            commit_profile_removal(
                self.connection,
                store,
                PROFILE_ID,
                (wrong_owner,),
                lambda: self.fail("profile removal must not run"),
            )

        self.assertEqual(1, self.connection.execute("SELECT COUNT(*) FROM profiles").fetchone()[0])


class _Store:
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        fail_set_after: int | None = None,
    ) -> None:
        self.connection = connection
        self.fail_set_after = fail_set_after
        self.set_count = 0
        self.values: dict[str, str] = {}
        self.fail_delete: set[str] = set()
        self.saw_cleanup_before_set = False
        self.saw_profile_removed_before_delete = False

    def get(self, reference: str) -> str:
        try:
            return self.values[reference]
        except KeyError as error:
            raise SecretNotFoundError("missing") from error

    def set(self, reference: str, value: str) -> None:
        pending = tuple(
            record.secret_reference for record in pending_secret_cleanup(self.connection)
        )
        self.saw_cleanup_before_set = self.saw_cleanup_before_set or reference in pending
        if self.fail_set_after is not None and self.set_count >= self.fail_set_after:
            raise SecretStoreError("synthetic set failure")
        self.set_count += 1
        self.values[reference] = value

    def delete(self, reference: str) -> None:
        if reference in self.fail_delete:
            raise SecretStoreError("synthetic delete failure")
        profile_count = self.connection.execute(
            "SELECT COUNT(*) FROM profiles WHERE id = ?", (PROFILE_ID,)
        ).fetchone()[0]
        if profile_count == 0:
            self.saw_profile_removed_before_delete = True
        self.values.pop(reference, None)


if __name__ == "__main__":
    unittest.main()
