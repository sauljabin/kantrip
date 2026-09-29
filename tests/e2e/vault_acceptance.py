"""Accept the Kantrip vault against the real operating system and a candidate wheel.

This suite needs no Kafka sandbox. It locks, unlocks, and hides the real Kantrip
vault, so it runs only where that vault is disposable: on macOS, a machine that
exported the vault password in `KANTRIP_E2E_VAULT_PASSWORD`; on Linux, a
session whose vault is the empty-password GNOME keyring that CI pre-creates.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from kantrip.linux_vault import GNOME_VAULT_PATH, NativeSecretService, default_data_home
from kantrip.macos_vault import NativeKeychainSystem, default_vault_path
from kantrip.secret_store import SERVICE_NAME
from scripts import run_terminal
from tests.e2e.preconditions import E2ESetupError

SECURITY = "/usr/bin/security"
TIMEOUT_SECONDS = 60
# GNOME Keyring notices a keyring file that appears or disappears within seconds.
KEYRING_WATCH_SECONDS = 15
_ENCRYPTED_KEYRING = b"GnomeKeyring\n\r\0\n"


class _VaultCase(unittest.TestCase):
    """Profiles and CLI runs shared by both platforms' vault acceptance."""

    kantrip: str

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


@unittest.skipUnless(sys.platform == "darwin", "the macOS vault suite targets macOS")
class TestMacOSVault(_VaultCase):
    vault: Path
    password: str

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


@unittest.skipUnless(sys.platform.startswith("linux"), "the Linux vault suite targets Linux")
class TestLinuxVault(_VaultCase):
    keyring: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.kantrip = _required_file("KANTRIP_E2E_KANTRIP", "an installed candidate wheel")
        cls.keyring = default_data_home() / "keyrings" / "kantrip.keyring"
        try:
            header = cls.keyring.read_bytes()[: len(_ENCRYPTED_KEYRING)]
        except OSError as error:
            raise E2ESetupError(
                f"the Linux vault suite needs the disposable vault {cls.keyring}"
            ) from error
        if header == _ENCRYPTED_KEYRING:
            raise E2ESetupError(
                f"{cls.keyring} has a password; the Linux vault suite runs only against the "
                "disposable empty-password vault that CI pre-creates"
            )
        if GNOME_VAULT_PATH not in NativeSecretService().collections():
            raise E2ESetupError("GNOME Keyring does not serve the disposable Kantrip vault")

    def test_large_private_key_is_stored_resolved_and_removed(self) -> None:
        self._add_profile()
        reference = self._private_key_reference()

        self.assertEqual(1, _linux_items(reference))
        doctor = _flat(self._kantrip("doctor", self.profile).stdout)
        self.assertIn(f"{GNOME_VAULT_PATH} in GNOME Keyring", doctor)
        self.assertIn(f"Profile '{self.profile}' Kafka credentials are usable", doctor)
        executed = self._kantrip("exec", self.profile, "--", "true")
        self.assertEqual(0, executed.returncode, executed.stderr)

        removed = self._kantrip("remove", self.profile, "--yes")

        self.assertEqual(0, removed.returncode, removed.stderr)
        self.assertEqual(0, _linux_items(reference))

    def test_locked_vault_that_opens_without_a_window_is_used_and_reported(self) -> None:
        self._add_password_profile()
        _lock_linux_vault()

        doctor = _flat(self._kantrip("doctor", self.profile).stdout)
        executed = self._kantrip("exec", self.profile, "--", "true")

        self.assertEqual(0, executed.returncode, executed.stderr)
        self.assertIn(f"{GNOME_VAULT_PATH} in GNOME Keyring (locked)", doctor)
        self.assertIn("opened without asking for its password", doctor)
        self.assertIn("Credential vault has an empty password", doctor)
        self.assertIn(f"Profile '{self.profile}' Kafka credentials are usable", doctor)

    def test_missing_vault_fails_with_recovery_guidance(self) -> None:
        self._add_password_profile()
        contents = self.keyring.read_bytes()
        self.keyring.unlink()
        self.addCleanup(_restore_keyring, self.keyring, contents)
        _wait_for_linux_vault(self.keyring.parent, present=False)

        doctor = self._kantrip("doctor", self.profile)
        executed = self._kantrip("exec", self.profile, "--", "true")

        self.assertEqual(1, doctor.returncode)
        self.assertIn(f"{GNOME_VAULT_PATH} in GNOME Keyring (not found)", _flat(doctor.stdout))
        self.assertEqual(1, executed.returncode)
        self.assertIn("does not exist; restore it", _flat(executed.stderr))

    def _add_password_profile(self) -> None:
        # GNOME writes an empty-password keyring in plain text without escaping
        # newlines, so a stored PEM key corrupts the file once it is reloaded.
        # Tests that lock or reload the disposable vault store a single line.
        status, output = run_terminal(
            (
                self.kantrip,
                "--no-color",
                "add",
                self.profile,
                "--bootstrap-server",
                "localhost:9095",
                "--auth",
                "scram-sha-512",
                "--username",
                "vault-e2e",
            ),
            (f"synthetic-{uuid.uuid4().hex}",),
            environment=self.environment,
            ready_text="Kafka password",
            timeout=TIMEOUT_SECONDS,
        )
        self.assertEqual(0, status, output)


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


def _flat(text: str) -> str:
    """Join lines that the CLI wrapped at the terminal width."""
    return " ".join(text.split())


def _linux_items(reference: str) -> int:
    """Count the vault items of one reference without reading their values."""
    import secretstorage

    connection = secretstorage.dbus_init()
    try:
        collection = secretstorage.Collection(connection, GNOME_VAULT_PATH)
        if collection.is_locked():
            collection.unlock(timeout=TIMEOUT_SECONDS)
        return len(list(collection.search_items({"service": SERVICE_NAME, "account": reference})))
    finally:
        connection.close()


def _lock_linux_vault() -> None:
    import secretstorage

    connection = secretstorage.dbus_init()
    try:
        collection = secretstorage.Collection(connection, GNOME_VAULT_PATH)
        if not collection.is_locked():
            collection.lock()
    finally:
        connection.close()


def _wait_for_linux_vault(directory: Path, *, present: bool) -> None:
    deadline = time.monotonic() + KEYRING_WATCH_SECONDS
    while (GNOME_VAULT_PATH in NativeSecretService().collections()) != present:
        if time.monotonic() > deadline:
            raise E2ESetupError("GNOME Keyring did not notice the moved vault file")
        time.sleep(1.1)
        # GNOME Keyring rescans only when the directory's mtime, in whole
        # seconds, changes; a change within the second of its last scan is
        # otherwise invisible.
        os.utime(directory)


def _restore_keyring(keyring: Path, contents: bytes) -> None:
    time.sleep(1.1)
    staged = keyring.with_name(".kantrip.keyring.restore")
    staged.write_bytes(contents)
    staged.chmod(0o600)
    staged.rename(keyring)
    _wait_for_linux_vault(keyring.parent, present=True)


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
