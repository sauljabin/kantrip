import unittest
from pathlib import Path

from kantrip.linux_vault import LinuxVault
from kantrip.macos_vault import MacOSVault
from kantrip.secret_store import (
    SecretStoreError,
    load_secret_store,
    secret_reference,
    validate_secret_reference,
)

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
CREDENTIAL_ID = "018f8f13-7c21-7cee-8000-000000000011"


class TestSecretStore(unittest.TestCase):
    def test_macos_uses_the_dedicated_kantrip_vault(self) -> None:
        store = load_secret_store(platform_name="darwin")

        self.assertIsInstance(store, MacOSVault)
        assert isinstance(store, MacOSVault)
        self.assertEqual(Path.home() / "Library/Keychains/kantrip.keychain-db", store.path)
        self.assertEqual("macOS keychain", store.info.display_name)

    def test_linux_uses_the_dedicated_secret_service_vault(self) -> None:
        store = load_secret_store(platform_name="linux")

        self.assertIsInstance(store, LinuxVault)
        self.assertEqual("Secret Service", store.info.display_name)

    def test_other_platforms_are_rejected(self) -> None:
        for platform_name in ("win32", "cygwin", "freebsd14"):
            with self.subTest(platform=platform_name), self.assertRaises(SecretStoreError):
                load_secret_store(platform_name=platform_name)

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


if __name__ == "__main__":
    unittest.main()
