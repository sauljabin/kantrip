import unittest

from keyring.errors import PasswordDeleteError, PasswordSetError

from kantrip.secret_store import (
    SERVICE_NAME,
    SecretNotFoundError,
    SecretStoreError,
    load_secret_store,
    secret_reference,
    validate_secret_reference,
)

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
CREDENTIAL_ID = "018f8f13-7c21-7cee-8000-000000000011"


class TestSecretStore(unittest.TestCase):
    def test_approved_macos_backend_stores_reads_and_deletes_exact_references(self) -> None:
        backend = _backend("keyring.backends.macOS")
        store = load_secret_store(backend=backend, platform_name="darwin")
        reference = secret_reference(PROFILE_ID, "kafka/password")

        store.set(reference, "synthetic-secret")

        self.assertEqual("synthetic-secret", store.get(reference))
        self.assertEqual({(SERVICE_NAME, reference): "synthetic-secret"}, backend.values)
        store.delete(reference)
        store.delete(reference)
        with self.assertRaises(SecretNotFoundError):
            store.get(reference)

    def test_approved_linux_secret_service_backends_are_accepted(self) -> None:
        for module in ("keyring.backends.SecretService", "keyring.backends.libsecret"):
            with self.subTest(module=module):
                store = load_secret_store(backend=_backend(module), platform_name="linux")
                self.assertEqual("Secret Service", store.info.display_name)

    def test_unapproved_null_chainer_and_platform_backends_are_rejected(self) -> None:
        cases = (
            (_backend("keyring.backends.null", priority=-1), "darwin"),
            (_backend("keyring.backends.chainer"), "linux"),
            (_backend("keyring.backends.Windows"), "win32"),
        )
        for backend, platform_name in cases:
            with (
                self.subTest(backend=type(backend).__module__, platform=platform_name),
                self.assertRaises(SecretStoreError),
            ):
                load_secret_store(backend=backend, platform_name=platform_name)

    def test_backend_with_invalid_priority_is_rejected(self) -> None:
        backend = _backend("keyring.backends.macOS")
        backend.priority = "high"

        with self.assertRaises(SecretStoreError):
            load_secret_store(backend=backend, platform_name="darwin")

    def test_references_are_limited_to_canonical_profile_keys(self) -> None:
        valid = secret_reference(
            PROFILE_ID,
            "registry/token",
            credential_id=CREDENTIAL_ID,
        )
        validate_secret_reference(valid)
        for invalid in (
            f"profile/not-a-uuid/{CREDENTIAL_ID}/registry/token",
            f"profile/{PROFILE_ID}/not-a-uuid/registry/token",
            f"profile/{PROFILE_ID}/{CREDENTIAL_ID}/unknown",
            f"other/{PROFILE_ID}/{CREDENTIAL_ID}/registry/token",
            f"profile/{PROFILE_ID.upper()}/{CREDENTIAL_ID}/registry/token",
        ):
            with self.subTest(reference=invalid), self.assertRaises(SecretStoreError):
                validate_secret_reference(invalid)

    def test_backend_errors_never_echo_secret_values(self) -> None:
        backend = _backend("keyring.backends.macOS")
        backend.set_error = PasswordSetError("synthetic-secret")
        store = load_secret_store(backend=backend, platform_name="darwin")
        reference = secret_reference(PROFILE_ID, "kafka/oauth/client-secret")

        with self.assertRaises(SecretStoreError) as raised:
            store.set(reference, "synthetic-secret")

        self.assertNotIn("synthetic-secret", str(raised.exception))


class _MemoryBackend:
    priority = 5

    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}
        self.set_error: Exception | None = None

    def get_password(self, service: str, username: str) -> str | None:
        return self.values.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.values[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        try:
            del self.values[(service, username)]
        except KeyError as error:
            raise PasswordDeleteError("missing") from error


def _backend(module: str, *, priority: float = 5) -> _MemoryBackend:
    backend_type = type("Keyring", (_MemoryBackend,), {"__module__": module, "priority": priority})
    return backend_type()


if __name__ == "__main__":
    unittest.main()
