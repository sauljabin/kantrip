import ssl
import unittest
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtensionOID

from tests.pki import CONTROLLED_NOW, KEY_PASSWORD, synthetic_pki, temporary_pki_files


class TestSyntheticPki(unittest.TestCase):
    def test_variants_have_controlled_validity_and_host_identity(self) -> None:
        pki = synthetic_pki()
        server = x509.load_pem_x509_certificate(pki.server_certificate.encode())
        wrong_host = x509.load_pem_x509_certificate(pki.wrong_host_certificate.encode())
        expired = x509.load_pem_x509_certificate(pki.expired_certificate.encode())
        future = x509.load_pem_x509_certificate(pki.future_certificate.encode())

        self.assertLess(server.not_valid_before_utc, CONTROLLED_NOW)
        self.assertGreater(server.not_valid_after_utc, CONTROLLED_NOW)
        self.assertLess(expired.not_valid_after_utc, CONTROLLED_NOW)
        self.assertGreater(future.not_valid_before_utc, CONTROLLED_NOW)
        server_names = server.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        ).value
        wrong_names = wrong_host.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        ).value
        self.assertEqual(["localhost"], server_names.get_values_for_type(x509.DNSName))
        self.assertEqual(["wrong.invalid"], wrong_names.get_values_for_type(x509.DNSName))

    def test_wrong_ca_and_mismatched_key_are_semantically_distinct(self) -> None:
        pki = synthetic_pki()
        ca = x509.load_pem_x509_certificate(pki.ca.encode())
        wrong_ca = x509.load_pem_x509_certificate(pki.wrong_ca.encode())
        certificate = x509.load_pem_x509_certificate(pki.client_certificate.encode())
        key = serialization.load_pem_private_key(pki.client_key.encode(), password=None)
        mismatch = serialization.load_pem_private_key(
            pki.mismatched_client_key.encode(), password=None
        )
        encrypted = serialization.load_pem_private_key(
            pki.encrypted_client_key.encode(), password=KEY_PASSWORD.encode()
        )

        self.assertNotEqual(
            ca.fingerprint(ca.signature_hash_algorithm),
            wrong_ca.fingerprint(wrong_ca.signature_hash_algorithm),
        )
        self.assertEqual(
            certificate.public_key().public_numbers(), key.public_key().public_numbers()
        )
        self.assertEqual(key.public_key().public_numbers(), encrypted.public_key().public_numbers())
        self.assertNotEqual(
            certificate.public_key().public_numbers(), mismatch.public_key().public_numbers()
        )

    def test_temporary_files_are_private_and_removed(self) -> None:
        root: Path | None = None
        with temporary_pki_files(ca=synthetic_pki().ca) as paths:
            path = paths["ca"]
            root = path.parent
            self.assertEqual(0o700, root.stat().st_mode & 0o777)
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            context = ssl.create_default_context(cadata=path.read_text(encoding="utf-8"))
            self.assertIsInstance(context, ssl.SSLContext)
        assert root is not None
        self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
