import shlex
import sys
import tempfile
import unittest
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from kantrip.macos_vault import (
    LINE_BUDGET,
    CommandResult,
    MacOSVault,
    NativeKeychainSystem,
    Terminal,
)
from kantrip.secret_store import (
    SERVICE_NAME,
    LockPolicy,
    SecretNotFoundError,
    SecretStoreError,
    VaultError,
    VaultState,
    secret_reference,
)

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"
MARKER = "synthetic-vault-marker"
# `security -i` cuts longer input lines and runs the rest as new commands.
SECURITY_LINE_LIMIT = 4094
PEM = (
    "-----BEGIN PRIVATE KEY-----\n" + "\n".join([MARKER * 4] * 40) + "\n-----END PRIVATE KEY-----\n"
)


class TestMacOSVault(unittest.TestCase):
    def setUp(self) -> None:
        self.system = FakeKeychain()
        self.vault = MacOSVault(self.system.path, self.system)
        self.reference = secret_reference(PROFILE_ID, "kafka/password")

    def test_round_trips_values_of_every_size_and_shape(self) -> None:
        self.system.create_vault()
        samples = {
            "ascii": f"{MARKER}-Secret_123",
            "quotes": f"a b \"c\" 'd' `e` $HOME \\n {MARKER}",
            "unicode": f"contraseña-密码-🔑-{MARKER}",
            "pem": PEM,
            "large": "é🔑" * 3500,
            "whitespace": f"  {MARKER} \t",
            "hex-looking": "0x4142",
        }
        for name, value in samples.items():
            with self.subTest(name=name):
                reference = secret_reference(PROFILE_ID, "kafka/tls/private-key")
                self.vault.set(reference, value)
                self.assertEqual(value, self.vault.get(reference))

    def test_large_values_are_split_below_the_line_limit_with_the_head_last(self) -> None:
        self.system.create_vault()

        self.vault.set(self.reference, "k" * 9000)

        accounts = [account for account, _ in self.system.write_order]
        count = len(accounts)
        self.assertGreater(count, 2)
        self.assertEqual(self.reference, accounts[-1])
        self.assertEqual(str(count), self.system.items[self.reference].generic)
        self.assertTrue(all(len(line.encode()) <= LINE_BUDGET for line in self.system.lines))
        labels = {item.label for item in self.system.items.values()}
        self.assertIn(
            f"Kantrip kafka/password (profile {PROFILE_ID}), part {count} of {count}", labels
        )

    def test_shrinking_a_value_deletes_its_stale_pieces(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, "k" * 9000)

        self.vault.set(self.reference, "short")

        self.assertEqual({self.reference}, set(self.system.items))
        self.assertEqual("short", self.vault.get(self.reference))
        self.assertEqual(
            f"Kantrip kafka/password (profile {PROFILE_ID})",
            self.system.items[self.reference].label,
        )

    def test_delete_removes_every_piece_and_is_idempotent(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, "k" * 9000)

        self.vault.delete(self.reference)
        self.vault.delete(self.reference)

        self.assertEqual({}, self.system.items)
        with self.assertRaises(SecretNotFoundError):
            self.vault.get(self.reference)

    def test_delete_removes_pieces_left_by_an_interrupted_write(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, "k" * 9000)
        del self.system.items[self.reference]

        self.vault.delete(self.reference)

        self.assertEqual({}, self.system.items)

    def test_missing_piece_is_reported_instead_of_a_truncated_secret(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, "k" * 9000)
        del self.system.items[f"{self.reference}#2"]

        with self.assertRaisesRegex(SecretStoreError, "incomplete"):
            self.vault.get(self.reference)

    def test_empty_stored_secret_is_an_error(self) -> None:
        self.system.create_vault()
        self.system.items[self.reference] = FakeItem("label", "1", b"")

        with self.assertRaisesRegex(SecretStoreError, "empty"):
            self.vault.get(self.reference)

    def test_secrets_never_reach_process_arguments(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, PEM)
        self.vault.get(self.reference)
        self.vault.delete(self.reference)

        encoded = MARKER.encode().hex()
        for arguments in self.system.calls:
            self.assertFalse(any(MARKER in argument for argument in arguments))
            self.assertFalse(any(encoded in argument.lower() for argument in arguments))

    def test_first_stored_secret_creates_and_secures_the_vault(self) -> None:
        self.system.terminal_answers = ["create"]

        self.vault.set(self.reference, MARKER)

        self.assertEqual("unlocked", self.system.state)
        self.assertEqual(LockPolicy(True, 900), self.system.policy)
        self.assertEqual([str(self.system.path)], self.system.search_list[-1:])
        self.assertIn("Choose a password", self.system.terminal_output)
        self.assertEqual(MARKER, self.vault.get(self.reference))

    def test_existing_search_list_entry_is_not_duplicated(self) -> None:
        self.system.search_list.append(str(self.system.path))
        self.system.terminal_answers = ["create"]

        self.vault.set(self.reference, MARKER)

        self.assertEqual(1, self.system.search_list.count(str(self.system.path)))

    def test_empty_vault_password_removes_the_new_vault(self) -> None:
        self.system.terminal_answers = ["create-empty"]

        with self.assertRaisesRegex(VaultError, "must not be empty"):
            self.vault.set(self.reference, MARKER)

        self.assertEqual("missing", self.system.state)
        self.assertNotIn(str(self.system.path), self.system.search_list)

    def test_creation_without_a_terminal_fails_at_once(self) -> None:
        self.system.has_terminal = False

        with self.assertRaisesRegex(VaultError, "no terminal to create it"):
            self.vault.set(self.reference, MARKER)

        self.assertEqual("missing", self.system.state)

    def test_cancelled_creation_leaves_no_vault(self) -> None:
        self.system.terminal_answers = ["interrupt"]

        with self.assertRaisesRegex(VaultError, "creation was cancelled"):
            self.vault.set(self.reference, MARKER)

        self.assertEqual("missing", self.system.state)

    def test_reads_never_create_a_missing_vault(self) -> None:
        with self.assertRaisesRegex(VaultError, "does not exist.*--replace-secret"):
            self.vault.get(self.reference)

        self.assertEqual([], self.system.terminal_commands)

    def test_delete_from_a_missing_vault_succeeds_without_prompting(self) -> None:
        self.vault.delete(self.reference)

        self.assertEqual([], self.system.terminal_commands)

    def test_replaced_vault_reports_missing_secrets(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, MARKER)
        self.system.items.clear()

        with self.assertRaises(SecretNotFoundError):
            self.vault.get(self.reference)

    def test_locked_vault_unlocks_on_the_terminal_before_any_item_access(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, MARKER)
        self.system.state = "locked"
        self.system.terminal_answers = ["unlock"]

        # The fake fails any item command that reaches a locked vault.
        self.assertEqual(MARKER, self.vault.get(self.reference))

        self.assertEqual(["unlock-keychain"], self.system.terminal_commands)
        self.assertIn("is locked", self.system.terminal_output)

    def test_wrong_passwords_are_retried_three_times(self) -> None:
        self.system.create_vault()
        self.vault.set(self.reference, MARKER)
        self.system.state = "locked"
        self.system.terminal_answers = ["wrong", "wrong", "unlock"]

        self.assertEqual(MARKER, self.vault.get(self.reference))
        self.assertIn("2 attempts left", self.system.terminal_output)
        self.assertIn("1 attempt left", self.system.terminal_output)

        self.system.state = "locked"
        self.system.terminal_answers = ["wrong", "wrong", "wrong"]
        with self.assertRaisesRegex(VaultError, "still locked after 3 incorrect passwords"):
            self.vault.get(self.reference)

    def test_refused_unlock_is_not_asked_again_in_the_same_process(self) -> None:
        self.system.create_vault()
        self.system.state = "locked"
        self.system.terminal_answers = ["interrupt"]

        with self.assertRaisesRegex(VaultError, "unlock was cancelled"):
            self.vault.get(self.reference)
        with self.assertRaisesRegex(VaultError, "unlock was cancelled"):
            self.vault.delete(self.reference)

        self.assertEqual(["unlock-keychain"], self.system.terminal_commands)
        self.assertEqual("locked", self.system.state)

    def test_locked_vault_without_a_terminal_fails_at_once(self) -> None:
        self.system.create_vault()
        self.system.state = "locked"
        self.system.has_terminal = False

        with self.assertRaisesRegex(VaultError, "locked and there is no terminal"):
            self.vault.get(self.reference)

        self.assertEqual([], self.system.calls)

    def test_symbolic_link_vault_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            link = Path(directory) / "kantrip.keychain-db"
            link.symlink_to(Path(directory) / "elsewhere")
            vault = MacOSVault(link, self.system)

            with self.assertRaisesRegex(VaultError, "symbolic link"):
                vault.get(self.reference)

    def test_vault_path_must_be_safe_inside_security_commands(self) -> None:
        with self.assertRaises(VaultError):
            MacOSVault(Path('/tmp/a"b.keychain-db'), self.system)

    def test_status_reports_state_without_unlocking(self) -> None:
        self.assertEqual("missing", self.vault.vault_status().state)
        self.system.create_vault()
        self.system.state = "locked"

        locked = self.vault.vault_status()

        self.assertEqual(("locked", None, ()), (locked.state, locked.lock_policy, locked.warnings))
        self.assertEqual("locked", self.system.state)
        self.assertEqual([], self.system.terminal_commands)
        self.system.state = "unlocked"
        self.assertEqual(LockPolicy(True, 300), self.vault.vault_status().lock_policy)

    def test_status_warns_about_an_empty_password_and_a_stale_search_entry(self) -> None:
        self.system.create_vault(password="")
        self.assertIn("empty password", self.vault.vault_status().warnings[0])

        self.system.state = "missing"
        self.system.search_list.append(str(self.system.path))
        self.assertIn("doctor --repair", self.vault.vault_status().warnings[0])

    def test_forgetting_a_missing_vault_updates_only_its_search_list_entry(self) -> None:
        self.system.search_list.append(str(self.system.path))

        self.assertTrue(self.vault.forget_missing_vault())

        self.assertEqual(["/login.keychain-db"], self.system.search_list)
        self.assertFalse(self.vault.forget_missing_vault())
        self.system.create_vault()
        self.system.search_list.append(str(self.system.path))
        self.assertFalse(self.vault.forget_missing_vault())

    def test_invalid_references_and_empty_values_are_rejected(self) -> None:
        self.system.create_vault()
        with self.assertRaises(SecretStoreError):
            self.vault.get("profile/not-a-reference")
        with self.assertRaises(SecretStoreError):
            self.vault.set(self.reference, "")

    def test_failed_batch_write_is_reported_without_its_output(self) -> None:
        self.system.create_vault()
        self.system.batch_error = f"security: add failed {MARKER}"

        with self.assertRaises(SecretStoreError) as raised:
            self.vault.set(self.reference, MARKER)

        self.assertNotIn(MARKER, str(raised.exception))


@unittest.skipUnless(sys.platform == "darwin", "reads Security.framework")
class TestNativeKeychainSystem(unittest.TestCase):
    def test_missing_keychain_state_is_read_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "absent.keychain-db"
            system = NativeKeychainSystem()

            self.assertEqual("missing", system.keychain_state(path))
            self.assertIsNone(system.lock_policy(path))
            self.assertFalse(path.exists())


@dataclass
class FakeItem:
    label: str
    generic: str
    data: bytes


@dataclass
class _FakeTerminal:
    output: list[str]
    fd: int = -1

    def write(self, text: str) -> None:
        self.output.append(text)


@dataclass
class FakeKeychain:
    """An in-memory `security` tool with the behavior the vault depends on."""

    path: Path = Path("/Users/example/Library/Keychains/kantrip.keychain-db")
    state: VaultState = "missing"
    password: str | None = None
    policy: LockPolicy | None = None
    items: dict[str, FakeItem] = field(default_factory=dict)
    search_list: list[str] = field(default_factory=lambda: ["/login.keychain-db"])
    has_terminal: bool = True
    terminal_answers: list[str] = field(default_factory=list)
    terminal_commands: list[str] = field(default_factory=list)
    terminal_log: list[str] = field(default_factory=list)
    calls: list[tuple[str, ...]] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    write_order: list[tuple[str, str]] = field(default_factory=list)
    batch_error: str = ""

    @property
    def terminal_output(self) -> str:
        return "".join(self.terminal_log)

    def create_vault(self, *, password: str = "vault-password") -> None:
        # New macOS keychains lock after five idle minutes and on sleep.
        self.state = "unlocked"
        self.password = password
        self.policy = LockPolicy(True, 300)

    def keychain_state(self, path: Path) -> VaultState:
        return self.state

    def lock_policy(self, path: Path) -> LockPolicy | None:
        return self.policy if self.state == "unlocked" else None

    @contextmanager
    def terminal(self) -> Iterator[Terminal | None]:
        yield _FakeTerminal(self.terminal_log) if self.has_terminal else None

    def run_on_terminal(
        self,
        arguments: Sequence[str],
        terminal: Terminal,
        *,
        show_errors: bool,
    ) -> CommandResult:
        command = arguments[0]
        self.terminal_commands.append(command)
        answer = self.terminal_answers.pop(0)
        if answer == "interrupt":
            raise KeyboardInterrupt
        if command == "create-keychain":
            self.create_vault(password="" if answer == "create-empty" else "vault-password")
            return CommandResult(0)
        if answer == "wrong":
            return CommandResult(51, stderr="The user name or passphrase is not correct.")
        self.state = "unlocked"
        return CommandResult(0)

    def run(self, arguments: Sequence[str], *, stdin: str | None = None) -> CommandResult:
        self.calls.append(tuple(arguments))
        if arguments[0] == "-i":
            return self._batch(stdin or "")
        return self._command(list(arguments))

    def _batch(self, stdin: str) -> CommandResult:
        # Like `security -i`, exit 0 and report every command on stderr.
        stderr = []
        for line in stdin.splitlines():
            self.lines.append(line)
            if len(line.encode()) > SECURITY_LINE_LIMIT:
                raise AssertionError("security -i would truncate this line")
            stderr.append(self._command(shlex.split(line)).stderr)
        return CommandResult(0, stderr=self.batch_error or "".join(stderr))

    def _command(self, arguments: list[str]) -> CommandResult:
        command, options, target = arguments[0], _options(arguments[1:]), arguments[-1]
        handler = {
            "add-generic-password": self._add,
            "find-generic-password": self._find,
            "delete-generic-password": self._delete,
            "unlock-keychain": self._unlock,
            "set-keychain-settings": self._settings,
            "delete-keychain": self._delete_keychain,
            "list-keychains": self._list,
        }[command]
        if command != "list-keychains" and target != str(self.path):
            raise AssertionError(f"unexpected keychain {target}")
        if command.endswith("-password") and self.state != "unlocked":
            raise AssertionError("a locked vault would open a password window")
        return handler(options, arguments)

    def _add(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        if options["-s"] != SERVICE_NAME or "-U" not in arguments:
            raise AssertionError("unexpected item options")
        account = options["-a"]
        self.items[account] = FakeItem(options["-l"], options["-G"], bytes.fromhex(options["-X"]))
        self.write_order.append((account, options["-G"]))
        return CommandResult(0)

    def _find(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        item = self.items.get(options["-a"])
        if item is None:
            return CommandResult(44, stderr="The specified item could not be found.")
        stdout = f'keychain: "{self.path}"\nattributes:\n    "gena"<blob>="{item.generic}"\n'
        if "-g" not in arguments:
            return CommandResult(0, stdout)
        # Like `security`, print printable ASCII as text and anything else as hex.
        text = item.data.decode("latin-1")
        if text.isascii() and text.isprintable():
            password = f'password: "{text}"'
        else:
            password = f"password: 0x{item.data.hex().upper()} "
        return CommandResult(0, stdout, password + "\n")

    def _delete(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        if self.items.pop(options["-a"], None) is None:
            return CommandResult(44, stderr="The specified item could not be found.")
        return CommandResult(0, stderr="password has been deleted.\n")

    def _unlock(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        if options.get("-p") != self.password:
            return CommandResult(51, stderr="The user name or passphrase is not correct.")
        self.state = "unlocked"
        return CommandResult(0)

    def _settings(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        idle = int(options["-t"]) if "-t" in options else None
        self.policy = LockPolicy("-l" in arguments, idle)
        return CommandResult(0)

    def _delete_keychain(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        self.state = "missing"
        self.items.clear()
        self.search_list = [entry for entry in self.search_list if entry != str(self.path)]
        return CommandResult(0)

    def _list(self, options: dict[str, str], arguments: list[str]) -> CommandResult:
        if "-s" in arguments:
            self.search_list = arguments[arguments.index("-s") + 1 :]
            return CommandResult(0)
        return CommandResult(0, "".join(f'    "{entry}"\n' for entry in self.search_list))


def _options(arguments: list[str]) -> dict[str, str]:
    """Map each option to the value after it; a flag maps to the next option."""
    return {
        argument: arguments[index + 1]
        for index, argument in enumerate(arguments[:-1])
        if argument.startswith("-")
    }


if __name__ == "__main__":
    unittest.main()
