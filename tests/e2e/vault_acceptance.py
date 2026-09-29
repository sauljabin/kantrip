"""Accept the macOS vault against the real `/usr/bin/security` and a candidate wheel.

This suite needs no Kafka sandbox. It locks and unlocks the real Kantrip vault,
so it runs only on a disposable machine that exported the vault password in
`KANTRIP_E2E_VAULT_PASSWORD`, such as the macOS CI job that created the vault.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from kantrip.macos_vault import NativeKeychainSystem, default_vault_path
from kantrip.secret_store import SERVICE_NAME
from tests.e2e.preconditions import E2ESetupError

SECURITY = "/usr/bin/security"
TIMEOUT_SECONDS = 60


@unittest.skipUnless(sys.platform == "darwin", "the Kantrip vault suite targets macOS")
class TestMacOSVault(unittest.TestCase):
    vault: Path
    password: str
    kantrip: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.vault = default_vault_path()
        cls.kantrip = _required_file("KANTRIP_E2E_KANTRIP", "an installed candidate wheel")
        password = os.environ.get("KANTRIP_E2E_VAULT_PASSWORD")
        if not password:
            raise E2ESetupError(
                "KANTRIP_E2E_VAULT_PASSWORD must hold the disposable vault password"
            )
        cls.password = password
        _unlock(cls.vault, password)

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="kantrip-vault-e2e-")
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        runtime = root / "runtime"
        runtime.mkdir(mode=0o700)
        self.environment = {
            **os.environ,
            "KANTRIP_DATABASE": str(root / "profiles.db"),
            "XDG_RUNTIME_DIR": str(runtime),
            "NO_COLOR": "1",
        }
        self.certificate, self.key = _mtls_files(root)
        self.profile = f"e2e-vault-{uuid.uuid4().hex[:12]}"
        self.addCleanup(self._remove_profile)

    def test_large_private_key_is_split_stored_resolved_and_removed(self) -> None:
        self._add_profile()
        reference = self._private_key_reference()

        self.assertEqual(0, _find_item(self.vault, reference))
        self.assertEqual(0, _find_item(self.vault, f"{reference}#1"))
        doctor = self._kantrip("doctor", self.profile)
        self.assertIn("kantrip.keychain-db (unlocked)", doctor.stdout)
        self.assertIn("Credential vault locks after", doctor.stdout)
        self.assertIn(f"Profile '{self.profile}' Kafka credentials are usable", doctor.stdout)
        executed = self._kantrip("exec", self.profile, "--", "true")
        self.assertEqual(0, executed.returncode, executed.stderr)

        removed = self._kantrip("remove", self.profile, "--yes")

        self.assertEqual(0, removed.returncode, removed.stderr)
        self.assertEqual(44, _find_item(self.vault, reference))
        self.assertEqual(44, _find_item(self.vault, f"{reference}#1"))

    def test_locked_vault_without_a_terminal_fails_at_once_and_stays_locked(self) -> None:
        self._add_profile()
        _security("lock-keychain", str(self.vault))
        self.addCleanup(_unlock, self.vault, self.password)

        executed = self._kantrip("exec", self.profile, "--", "true")
        doctor = self._kantrip("doctor", self.profile)

        self.assertEqual(1, executed.returncode)
        self.assertIn("is locked and there is no terminal to unlock it", executed.stderr)
        self.assertIn("kantrip.keychain-db (locked)", doctor.stdout)
        self.assertIn("Profile credentials were not checked", doctor.stdout)
        self.assertEqual(1, doctor.returncode)
        self.assertEqual("locked", _state(self.vault))

    def _add_profile(self) -> None:
        added = self._kantrip(
            "add",
            self.profile,
            "--bootstrap-server",
            "localhost:9095",
            "--auth",
            "mtls",
            "--client-certificate-file",
            str(self.certificate),
            "--client-key-file",
            str(self.key),
        )
        self.assertEqual(0, added.returncode, added.stderr)

    def _remove_profile(self) -> None:
        self._kantrip("remove", self.profile, "--yes")

    def _private_key_reference(self) -> str:
        with closing(sqlite3.connect(self.environment["KANTRIP_DATABASE"])) as connection:
            (document,) = connection.execute(
                "SELECT document FROM profiles WHERE name = ?", (self.profile,)
            ).fetchone()
        reference = json.loads(document)["kafka"]["auth"]["privateKeyRef"]
        assert isinstance(reference, str)
        return reference

    def _kantrip(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        # A new session has no controlling terminal, like a script or CI job.
        return subprocess.run(
            (self.kantrip, "--no-color", *arguments),
            env=self.environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            start_new_session=True,
            check=False,
        )


def _required_file(variable: str, description: str) -> str:
    value = os.environ.get(variable)
    if not value or not Path(value).is_file():
        raise E2ESetupError(f"{variable} must name {description}")
    return value


def _unlock(vault: Path, password: str) -> None:
    # `-p` is acceptable only for this disposable CI vault.
    result = subprocess.run(
        (SECURITY, "unlock-keychain", "-p", password, str(vault)),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode:
        raise E2ESetupError(f"the disposable Kantrip vault {vault} could not be unlocked")


def _security(*arguments: str) -> None:
    subprocess.run(
        (SECURITY, *arguments),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=TIMEOUT_SECONDS,
        check=True,
    )


def _state(vault: Path) -> str:
    # Security.framework reads the state without prompting or unlocking.
    return NativeKeychainSystem().keychain_state(vault)


def _find_item(vault: Path, account: str) -> int:
    """Return `security`'s status for one item without printing its value."""
    return subprocess.run(
        (SECURITY, "find-generic-password", "-a", account, "-s", SERVICE_NAME, str(vault)),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    ).returncode


def _mtls_files(directory: Path) -> tuple[Path, Path]:
    # A 4096-bit key is larger than one `security -i` line, so it is split.
    key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "kantrip-vault-e2e")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    certificate_path = directory / "client.crt"
    key_path = directory / "client.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    return certificate_path, key_path


if __name__ == "__main__":
    unittest.main()
