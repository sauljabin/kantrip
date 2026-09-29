import tempfile
import unittest
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

from kantrip.linux_vault import (
    GNOME_VAULT_PATH,
    NO_OBJECT,
    PROMPT_TIMEOUT_SECONDS,
    VAULT_ALIAS,
    VAULT_LABEL,
    LinuxVault,
)
from kantrip.secret_store import (
    SERVICE_NAME,
    SecretNotFoundError,
    SecretStoreError,
    VaultError,
    secret_reference,
)

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
MARKER = "synthetic-vault-marker"
PEM = (
    "-----BEGIN PRIVATE KEY-----\n" + "\n".join([MARKER * 4] * 40) + "\n-----END PRIVATE KEY-----\n"
)
KDE_VAULT_PATH = "/org/freedesktop/secrets/collection/kantrip"
LOGIN = "/org/freedesktop/secrets/collection/login"
KDEWALLET = "/org/freedesktop/secrets/collection/kdewallet"
ENCRYPTED = b"GnomeKeyring\n\r\0\n" + b"\x00" * 16
PLAIN = b"[keyring]\ndisplay-name=kantrip\n"


class TestLinuxVaultStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeSecretService.gnome(self)
        self.vault = self.fake.vault()
        self.reference = secret_reference(PROFILE_ID, "kafka/password")

    def test_round_trips_values_of_every_size_and_shape_in_one_item(self) -> None:
        self.fake.add_vault(locked=False)
        samples = {
            "ascii": f"{MARKER}-Secret_123",
            "quotes": f"a b \"c\" 'd' `e` $HOME \\n {MARKER}",
            "unicode": f"contraseña-密码-🔑-{MARKER}",
            "pem": PEM,
            "one-megabyte": "z" * 1_000_000,
            "whitespace": f"  {MARKER} \t",
        }
        for name, value in samples.items():
            with self.subTest(name=name):
                reference = secret_reference(PROFILE_ID, "kafka/tls/private-key")
                self.vault.set(reference, value)
                self.assertEqual(value, self.vault.get(reference))
                self.assertEqual(1, len(self.fake.search(GNOME_VAULT_PATH, _attributes(reference))))

    def test_items_carry_only_the_service_reference_and_a_readable_label(self) -> None:
        self.fake.add_vault(locked=False)

        self.vault.set(self.reference, PEM)

        (item,) = self.fake.vault_collection().items.values()
        self.assertEqual({"service": SERVICE_NAME, "account": self.reference}, item.attributes)
        self.assertEqual(f"Kantrip kafka/password (profile {PROFILE_ID})", item.label)
        self.assertNotIn(MARKER, repr(item.attributes) + item.label)

    def test_replacing_a_value_keeps_one_item(self) -> None:
        self.fake.add_vault(locked=False)
        self.vault.set(self.reference, "first")

        self.vault.set(self.reference, "second")

        self.assertEqual("second", self.vault.get(self.reference))
        self.assertEqual(1, len(self.fake.vault_collection().items))

    def test_delete_is_idempotent(self) -> None:
        self.fake.add_vault(locked=False)
        self.vault.set(self.reference, "synthetic")

        self.vault.delete(self.reference)
        self.vault.delete(self.reference)

        self.assertEqual({}, self.fake.vault_collection().items)
        with self.assertRaises(SecretNotFoundError):
            self.vault.get(self.reference)

    def test_empty_stored_secret_is_an_error(self) -> None:
        # KDE reads items of a wallet whose file was replaced as empty secrets.
        self.fake.add_vault(locked=False)
        self.fake.vault_collection().add_item("label", _attributes(self.reference), b"")

        with self.assertRaisesRegex(SecretStoreError, "empty"):
            self.vault.get(self.reference)

    def test_duplicate_items_are_an_error(self) -> None:
        self.fake.add_vault(locked=False)
        for value in (b"first", b"second"):
            self.fake.vault_collection().add_item("label", _attributes(self.reference), value)

        with self.assertRaisesRegex(SecretStoreError, "duplicate"):
            self.vault.get(self.reference)

    def test_empty_values_are_refused(self) -> None:
        self.fake.add_vault(locked=False)

        with self.assertRaisesRegex(SecretStoreError, "non-empty"):
            self.vault.set(self.reference, "")

    def test_missing_vault_reads_fail_with_guidance_and_deletes_succeed(self) -> None:
        with self.assertRaisesRegex(VaultError, "does not exist; restore it, or store"):
            self.vault.get(self.reference)

        self.vault.delete(self.reference)
        self.assertEqual([], self.fake.prompts_run)

    def test_unsupported_provider_is_rejected(self) -> None:
        self.fake.process = "keepassxc"

        with self.assertRaisesRegex(VaultError, "'keepassxc' is not supported"):
            self.vault.get(self.reference)

    def test_unavailable_secret_service_is_reported(self) -> None:
        self.fake.process_error = VaultError("no Secret Service is running for this user")

        with self.assertRaisesRegex(VaultError, "no Secret Service is running"):
            self.vault.get(self.reference)


class TestLinuxVaultUnlock(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeSecretService.gnome(self)
        self.fake.add_vault(locked=False)
        self.reference = secret_reference(PROFILE_ID, "kafka/password")
        self.fake.vault().set(self.reference, "synthetic")
        self.fake.vault_collection().locked = True
        self.vault = self.fake.vault()

    def test_locked_vault_is_unlocked_in_a_desktop_window_before_any_item_access(self) -> None:
        self.fake.answers = ["accept"]

        self.assertEqual("synthetic", self.vault.get(self.reference))

        self.assertEqual(1, len(self.fake.prompts_run))
        self.assertEqual([], self.fake.locked_reads)
        self.assertIn("is locked. Enter its password in the window", self.fake.terminal_output)
        self.assertIn(f"up to {PROMPT_TIMEOUT_SECONDS} seconds", self.fake.terminal_output)

    def test_locked_vault_without_a_terminal_fails_at_once_without_a_window(self) -> None:
        self.fake.has_terminal = False

        with self.assertRaisesRegex(VaultError, "locked and there is no terminal to unlock"):
            self.vault.get(self.reference)

        self.assertEqual([], self.fake.prompts_run)
        self.assertEqual(1, len(self.fake.dismissed))
        self.assertTrue(self.fake.vault_collection().locked)

    def test_vault_that_opens_without_a_window_is_used_with_a_warning(self) -> None:
        for has_terminal in (True, False):
            with self.subTest(has_terminal=has_terminal):
                self.fake.vault_collection().locked = True
                self.fake.opens_without_window = True
                self.fake.has_terminal = has_terminal
                self.fake.terminal_output = ""
                vault = self.fake.vault()

                self.assertEqual("synthetic", vault.get(self.reference))

                self.assertEqual([], self.fake.prompts_run)
                (warning,) = vault.unlock_warnings()
                self.assertIn("opened without asking for its password", warning)
                self.assertIn("'Unlock password for: kantrip'", warning)
                self.assertEqual(has_terminal, "Warning: " in self.fake.terminal_output)

    def test_cancelled_window_is_remembered_for_the_rest_of_the_process(self) -> None:
        self.fake.answers = ["cancel"]

        with self.assertRaisesRegex(VaultError, "^Kantrip vault unlock was cancelled$"):
            self.vault.get(self.reference)
        with self.assertRaisesRegex(VaultError, "cancelled"):
            self.vault.get(self.reference)

        self.assertEqual(1, len(self.fake.prompts_run))

    def test_window_dismissed_at_once_asks_for_an_unlocked_desktop(self) -> None:
        self.fake.answers = ["no-window"]

        with self.assertRaisesRegex(VaultError, "could not be shown.*unlock the screen"):
            self.vault.get(self.reference)

    def test_unanswered_window_times_out_and_is_closed(self) -> None:
        self.fake.answers = ["timeout"]

        with self.assertRaisesRegex(VaultError, "no answer within 60 seconds"):
            self.vault.get(self.reference)

        self.assertEqual(self.fake.prompts_run, self.fake.dismissed)
        self.assertEqual([PROMPT_TIMEOUT_SECONDS], self.fake.timeouts)
        self.assertTrue(self.fake.vault_collection().locked)

    def test_ctrl_c_closes_the_window_and_leaves_the_vault_locked(self) -> None:
        self.fake.answers = ["interrupt"]

        with self.assertRaisesRegex(VaultError, "unlock was cancelled"):
            self.vault.get(self.reference)

        self.assertEqual(self.fake.prompts_run, self.fake.dismissed)
        self.assertTrue(self.fake.vault_collection().locked)


class TestLinuxVaultCreation(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeSecretService.gnome(self)
        self.vault = self.fake.vault()
        self.reference = secret_reference(PROFILE_ID, "kafka/password")

    def test_first_store_creates_the_gnome_keyring_without_an_alias(self) -> None:
        self.fake.answers = ["accept"]

        self.vault.set(self.reference, "synthetic")

        self.assertEqual([(VAULT_LABEL, "")], self.fake.created)
        self.assertEqual("synthetic", self.vault.get(self.reference))
        self.assertIn("Choose a password for it in the window", self.fake.terminal_output)
        self.assertIn(f"Created {GNOME_VAULT_PATH}.", self.fake.terminal_output)

    def test_creation_without_a_terminal_fails_at_once(self) -> None:
        self.fake.has_terminal = False

        with self.assertRaisesRegex(VaultError, "no terminal to create it"):
            self.vault.set(self.reference, "synthetic")

        self.assertEqual([], self.fake.created)

    def test_empty_password_removes_the_new_keyring(self) -> None:
        self.fake.answers = ["accept"]
        self.fake.creates_plain_file = True

        with self.assertRaisesRegex(VaultError, "must not be empty; the new vault was removed"):
            self.vault.set(self.reference, "synthetic")

        self.assertNotIn(GNOME_VAULT_PATH, self.fake.collections_by_path)

    def test_keyring_created_at_another_path_is_refused(self) -> None:
        self.fake.answers = ["accept"]
        self.fake.create_path = "/org/freedesktop/secrets/collection/kantrip_5f1"

        with self.assertRaisesRegex(VaultError, "created at .*kantrip_5f1 instead of"):
            self.vault.set(self.reference, "synthetic")

    def test_cancelled_creation_is_reported(self) -> None:
        self.fake.answers = ["cancel"]

        with self.assertRaisesRegex(VaultError, "creation was cancelled"):
            self.vault.set(self.reference, "synthetic")


class TestLinuxVaultLookup(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeSecretService.gnome(self)
        self.vault = self.fake.vault()

    def test_gnome_vault_is_found_by_its_path_and_label(self) -> None:
        self.fake.add_vault(locked=True)

        status = self.vault.vault_status()

        self.assertEqual(f"{GNOME_VAULT_PATH} in GNOME Keyring", status.location)
        self.assertEqual("locked", status.state)
        self.assertIsNone(status.lock_policy)
        self.assertEqual((), status.warnings)

    def test_gnome_refuses_to_guess_between_keyrings_named_kantrip(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.add_collection("/org/freedesktop/secrets/collection/kantrip_5f1", VAULT_LABEL)

        with self.assertRaisesRegex(VaultError, "cannot tell which keyring.*kantrip_5f1"):
            self.vault.vault_status()

    def test_gnome_refuses_a_renamed_keyring_at_the_vault_path(self) -> None:
        self.fake.add_collection(GNOME_VAULT_PATH, "Work")

        with self.assertRaisesRegex(VaultError, "the keyring there is named 'Work'"):
            self.vault.vault_status()

    def test_status_never_unlocks_or_opens_a_window(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.opens_without_window = True

        self.assertEqual("locked", self.vault.vault_status().state)

        self.assertEqual([], self.fake.unlock_requests)

    def test_default_keyring_vault_is_a_warning(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.aliases["default"] = GNOME_VAULT_PATH

        (warning,) = self.vault.vault_status().warnings

        self.assertIn("is the default keyring", warning)
        self.assertIn("Passwords and Keys", warning)

    def test_unencrypted_keyring_file_is_an_empty_password_warning(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.keyring_file.write_bytes(PLAIN)

        (warning,) = self.vault.vault_status().warnings

        self.assertIn("empty password", warning)


class TestKDEVault(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeSecretService.kde(self)
        self.vault = self.fake.vault()
        self.reference = secret_reference(PROFILE_ID, "kafka/password")

    def test_creation_uses_the_alias_keeps_the_default_and_explains_classic(self) -> None:
        self.fake.answers = ["accept"]

        self.vault.set(self.reference, "synthetic")

        self.assertEqual([(VAULT_LABEL, VAULT_ALIAS)], self.fake.created)
        self.assertEqual(KDEWALLET, self.fake.aliases["default"])
        self.assertEqual(1, self.fake.takeovers)
        self.assertIn("choose Classic", self.fake.terminal_output)
        self.assertEqual("synthetic", self.vault.get(self.reference))

    def test_unlock_keeps_the_default(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.answers = ["accept"]

        self.vault.set(self.reference, "synthetic")

        self.assertEqual(KDEWALLET, self.fake.aliases["default"])
        self.assertEqual(1, self.fake.takeovers)

    def test_vault_is_found_by_alias_only(self) -> None:
        # A deleted wallet leaves a same-label ghost until ksecretd restarts.
        self.fake.add_collection("/org/freedesktop/secrets/collection/kantrip0", VAULT_LABEL)

        status = self.vault.vault_status()

        self.assertEqual("missing", status.state)
        self.assertEqual("~/.local/share/kwalletd/kantrip.kwl in KDE Wallet", status.location)

    def test_listed_vault_without_its_file_is_missing_and_never_unlocked(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.wallet_file.unlink()

        status = self.vault.vault_status()
        with self.assertRaisesRegex(VaultError, "does not exist"):
            self.vault.get(self.reference)
        with self.assertRaisesRegex(VaultError, "still lists the Kantrip vault"):
            self.vault.set(self.reference, "synthetic")
        self.vault.delete(self.reference)

        self.assertEqual("missing", status.state)
        self.assertIn("still lists the credential vault", status.warnings[0])
        self.assertEqual([], self.fake.unlock_requests)
        self.assertEqual([], self.fake.created)

    def test_status_names_the_wallet_file_and_collection(self) -> None:
        self.fake.add_vault(locked=False)

        status = self.vault.vault_status()

        self.assertEqual(
            f"~/.local/share/kwalletd/kantrip.kwl in KDE Wallet, collection {KDE_VAULT_PATH}",
            status.location,
        )
        self.assertEqual("unlocked", status.state)

    def test_default_wallet_vault_is_a_warning(self) -> None:
        self.fake.add_vault(locked=True)
        self.fake.aliases["default"] = KDE_VAULT_PATH

        (warning,) = self.vault.vault_status().warnings

        self.assertIn("is the default wallet", warning)
        self.assertIn("System Settings > KDE Wallet", warning)


def _attributes(reference: str) -> dict[str, str]:
    return {"service": SERVICE_NAME, "account": reference}


@dataclass
class FakeItem:
    label: str
    attributes: dict[str, str]
    secret: bytes


@dataclass
class FakeCollection:
    label: str
    locked: bool = True
    items: dict[str, FakeItem] = field(default_factory=dict)
    next_item: int = 0

    def add_item(self, label: str, attributes: Mapping[str, str], secret: bytes) -> None:
        self.next_item += 1
        self.items[str(self.next_item)] = FakeItem(label, dict(attributes), secret)


class FakeTerminal:
    fd = -1

    def __init__(self, owner: "FakeSecretService") -> None:
        self.owner = owner

    def write(self, text: str) -> None:
        self.owner.terminal_output += text


class FakeSecretService:
    """Secret Service provider in memory, with windows answered from a script."""

    def __init__(self, process: str, home: Path) -> None:
        self.process = process
        self.process_error: VaultError | None = None
        self.home = home
        self.data_home = home / ".local" / "share"
        self.collections_by_path: dict[str, FakeCollection] = {}
        self.aliases: dict[str, str] = {}
        self.has_terminal = True
        self.terminal_output = ""
        self.answers: list[str] = []
        self.pending: dict[str, Callable[[], str]] = {}
        self.prompts_run: list[str] = []
        self.dismissed: list[str] = []
        self.timeouts: list[float] = []
        self.unlock_requests: list[str] = []
        self.locked_reads: list[str] = []
        self.created: list[tuple[str, str]] = []
        self.create_path = GNOME_VAULT_PATH
        self.creates_plain_file = False
        self.opens_without_window = False
        # KDE makes a wallet the default when it is first created or opened.
        self.first_use_takeover = False
        self.takeovers = 0
        self.now = 0.0

    @classmethod
    def gnome(cls, test: unittest.TestCase) -> "FakeSecretService":
        fake = cls("gnome-keyring-d", _home(test))
        fake.add_collection(LOGIN, "Login", locked=False)
        fake.aliases["default"] = LOGIN
        return fake

    @classmethod
    def kde(cls, test: unittest.TestCase) -> "FakeSecretService":
        fake = cls("ksecretd", _home(test))
        fake.add_collection(KDEWALLET, "kdewallet", locked=False)
        fake.aliases["default"] = KDEWALLET
        fake.first_use_takeover = True
        fake.create_path = KDE_VAULT_PATH
        return fake

    @property
    def kde_provider(self) -> bool:
        return self.process == "ksecretd"

    @property
    def keyring_file(self) -> Path:
        return self.data_home / "keyrings" / "kantrip.keyring"

    @property
    def wallet_file(self) -> Path:
        return self.data_home / "kwalletd" / "kantrip.kwl"

    def vault(self) -> LinuxVault:
        return LinuxVault(self, data_home=self.data_home, clock=lambda: self.now)

    def vault_collection(self) -> FakeCollection:
        return self.collections_by_path[self.create_path]

    def add_collection(self, path: str, label: str, *, locked: bool = True) -> None:
        self.collections_by_path[path] = FakeCollection(label, locked)

    def add_vault(self, *, locked: bool) -> None:
        self.add_collection(self.create_path, VAULT_LABEL, locked=locked)
        self._write_vault_file(plain=False)
        if self.kde_provider:
            self.aliases[VAULT_ALIAS] = self.create_path

    # SecretServiceSystem

    def provider_process(self) -> str:
        if self.process_error is not None:
            raise self.process_error
        return self.process

    def collections(self) -> tuple[str, ...]:
        return tuple(self.collections_by_path)

    def label(self, collection: str) -> str:
        return self.collections_by_path[collection].label

    def read_alias(self, alias: str) -> str:
        return self.aliases.get(alias, NO_OBJECT)

    def set_alias(self, alias: str, collection: str) -> None:
        if collection == NO_OBJECT:
            self.aliases.pop(alias, None)
        else:
            self.aliases[alias] = collection

    def is_locked(self, collection: str) -> bool:
        return self.collections_by_path[collection].locked

    def unlock(self, collection: str) -> str:
        self.unlock_requests.append(collection)
        if not self.collections_by_path[collection].locked:
            return NO_OBJECT
        if self.opens_without_window:
            self._open(collection)
            return NO_OBJECT
        return self._prompt(lambda: self._open(collection) or "")

    def create_collection(self, label: str, alias: str) -> tuple[str, str]:
        self.created.append((label, alias))

        def create() -> str:
            self.add_collection(self.create_path, label, locked=False)
            self._write_vault_file(plain=self.creates_plain_file)
            if alias:
                self.aliases[alias] = self.create_path
            self._take_over(self.create_path)
            return self.create_path

        return NO_OBJECT, self._prompt(create)

    def delete_collection(self, collection: str) -> str:
        del self.collections_by_path[collection]
        return NO_OBJECT

    def prompt(self, prompt: str, timeout: float) -> tuple[bool, str]:
        self.prompts_run.append(prompt)
        self.timeouts.append(timeout)
        answer = self.answers.pop(0)
        if answer == "timeout":
            raise TimeoutError
        if answer == "interrupt":
            raise KeyboardInterrupt
        if answer == "no-window":
            self.now += 0.01
            return True, ""
        self.now += 5
        if answer == "cancel":
            return True, ""
        return False, self.pending.pop(prompt)()

    def dismiss(self, prompt: str) -> None:
        self.dismissed.append(prompt)

    def search(self, collection: str, attributes: Mapping[str, str]) -> tuple[str, ...]:
        found = self._unlocked(collection)
        return tuple(
            f"{collection}/{key}"
            for key, item in found.items.items()
            if item.attributes == dict(attributes)
        )

    def secret(self, item: str) -> bytes:
        collection, key = item.rsplit("/", 1)
        return self._unlocked(collection).items[key].secret

    def store(
        self, collection: str, label: str, attributes: Mapping[str, str], secret: bytes
    ) -> None:
        found = self._unlocked(collection)
        for path in self.search(collection, attributes):
            del found.items[path.rsplit("/", 1)[1]]
        found.add_item(label, attributes, secret)

    def delete_item(self, item: str) -> None:
        collection, key = item.rsplit("/", 1)
        del self._unlocked(collection).items[key]

    @contextmanager
    def terminal(self) -> Iterator[FakeTerminal | None]:
        yield FakeTerminal(self) if self.has_terminal else None

    # Helpers

    def _prompt(self, action: Callable[[], str]) -> str:
        path = f"/org/freedesktop/secrets/prompt/u{len(self.pending) + len(self.prompts_run)}"
        self.pending[path] = action
        return path

    def _open(self, collection: str) -> None:
        self.collections_by_path[collection].locked = False
        self._take_over(collection)

    def _take_over(self, collection: str) -> None:
        if self.first_use_takeover:
            self.first_use_takeover = False
            self.aliases["default"] = collection
            self.takeovers += 1

    def _unlocked(self, collection: str) -> FakeCollection:
        found = self.collections_by_path[collection]
        if found.locked:
            self.locked_reads.append(collection)
            raise SecretStoreError("synthetic read of a locked collection")
        return found

    def _write_vault_file(self, *, plain: bool) -> None:
        path = self.wallet_file if self.kde_provider else self.keyring_file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(PLAIN if plain else ENCRYPTED)


def _home(test: unittest.TestCase) -> Path:
    """Return a temporary home directory that ``~`` paths are shown against."""
    directory = tempfile.TemporaryDirectory(prefix="kantrip-linux-vault-")
    test.addCleanup(directory.cleanup)
    home = Path(directory.name)
    patcher = patch.object(Path, "home", return_value=home)
    patcher.start()
    test.addCleanup(patcher.stop)
    return home


if __name__ == "__main__":
    unittest.main()
