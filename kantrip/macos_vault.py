"""Keep credentials in a dedicated macOS keychain file, the Kantrip vault.

macOS owns the vault password: ``security create-keychain`` and
``security unlock-keychain`` ask for it on the terminal, and Kantrip never
reads, stores, or passes it. Every item is written and read by
``/usr/bin/security`` so items carry its stable ``apple-tool:`` partition;
items created in-process would be bound to the running Python build and prompt
after every upgrade. Python reads only the vault's lock state and lock policy
through Security.framework with user interaction disabled, so those reads never
open a password window.
"""

from __future__ import annotations

import ctypes
import os
import re
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from kantrip.secret_store import (
    SERVICE_NAME,
    LockPolicy,
    SecretNotFoundError,
    SecretStoreError,
    SecretStoreInfo,
    Terminal,
    VaultError,
    VaultState,
    VaultStatus,
    controlling_terminal,
    display_path,
    parse_secret_reference,
)

VAULT_FILENAME = "kantrip.keychain-db"
SECURITY = "/usr/bin/security"
# Lock after 15 idle minutes and on sleep; new keychains default to 5 minutes.
LOCK_TIMEOUT_SECONDS = 900
UNLOCK_ATTEMPTS = 3
# `security -i` truncates input lines at about 4,094 bytes and runs the rest
# as new commands, so every generated line stays below this budget.
LINE_BUDGET = 4000
MAX_PIECES = 999
_MIN_PIECE_BYTES = 256
_DELETED = "password has been deleted."
_ITEM_NOT_FOUND = 44
_WRONG_PASSWORD = 51
_UNSAFE_PATH_CHARACTERS = frozenset('"\\\n\r\0')
_PIECE_COUNT = re.compile(r'^\s*"gena"<blob>="(\d{1,3})"\s*$', re.MULTILINE)
_HEX_PASSWORD = re.compile(r"^password: 0x([0-9A-Fa-f]*)")
_TEXT_PASSWORD = re.compile(r'^password: "(.*)"$')
_LIST_ENTRY = re.compile(r'^\s*"(.*)"\s*$')


@dataclass(frozen=True)
class CommandResult:
    """Exit status and captured output of one ``security`` process."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


class KeychainSystem(Protocol):
    """Operating-system calls the vault needs; unit tests replace them."""

    def keychain_state(self, path: Path) -> VaultState:
        """Return the vault state without prompting or resetting its idle timer."""

    def lock_policy(self, path: Path) -> LockPolicy | None:
        """Return the lock policy of an unlocked vault, or None while locked."""

    def run(self, arguments: Sequence[str], *, stdin: str | None = None) -> CommandResult:
        """Run ``security`` without a terminal and capture its output."""

    def run_on_terminal(
        self,
        arguments: Sequence[str],
        terminal: Terminal,
        *,
        show_errors: bool,
    ) -> CommandResult:
        """Run ``security`` with the terminal as input so it can prompt there."""

    def terminal(self) -> Any:
        """Return a context manager yielding the controlling terminal or None."""


class _Settings(ctypes.Structure):
    _fields_ = (
        ("version", ctypes.c_uint32),
        ("lock_on_sleep", ctypes.c_ubyte),
        ("use_lock_interval", ctypes.c_ubyte),
        ("lock_interval", ctypes.c_uint32),
    )


class NativeKeychainSystem:
    """Security.framework state reads and ``/usr/bin/security`` processes."""

    _NO_SUCH_KEYCHAIN = -25294
    _UNLOCKED = 1
    _NO_LOCK_INTERVAL = 2**31 - 1

    def __init__(self) -> None:
        self._security: Any = None
        self._core_foundation: Any = None

    def keychain_state(self, path: Path) -> VaultState:
        status = ctypes.c_uint32()
        result = self._with_keychain(
            path,
            lambda keychain: self._framework().SecKeychainGetStatus(keychain, ctypes.byref(status)),
        )
        if result == self._NO_SUCH_KEYCHAIN:
            return "missing"
        if result != 0:
            raise VaultError(f"Kantrip vault {display_path(path)} state could not be read")
        return "unlocked" if status.value & self._UNLOCKED else "locked"

    def lock_policy(self, path: Path) -> LockPolicy | None:
        settings = _Settings(version=1)
        result = self._with_keychain(
            path,
            lambda keychain: self._framework().SecKeychainCopySettings(
                keychain, ctypes.byref(settings)
            ),
        )
        if result != 0:
            return None
        idle = None if settings.lock_interval >= self._NO_LOCK_INTERVAL else settings.lock_interval
        return LockPolicy(bool(settings.lock_on_sleep), idle)

    def run(self, arguments: Sequence[str], *, stdin: str | None = None) -> CommandResult:
        completed = subprocess.run(
            (SECURITY, *arguments),
            input=stdin,
            stdin=None if stdin is not None else subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)

    def run_on_terminal(
        self,
        arguments: Sequence[str],
        terminal: Terminal,
        *,
        show_errors: bool,
    ) -> CommandResult:
        # `security` prompts on /dev/tty itself; errors go to stderr, which is
        # shown for creation (to explain a password mismatch) and captured for
        # unlocking, where Kantrip reports the outcome in its own words.
        completed = subprocess.run(
            (SECURITY, *arguments),
            stdin=terminal.fd,
            stdout=subprocess.PIPE,
            stderr=terminal.fd if show_errors else subprocess.PIPE,
            text=True,
            check=False,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr or "")

    def terminal(self) -> Any:
        return controlling_terminal()

    def _with_keychain(self, path: Path, operation: Any) -> int:
        security = self._framework()
        keychain = ctypes.c_void_p()
        result = security.SecKeychainOpen(os.fsencode(path), ctypes.byref(keychain))
        if result != 0:
            return int(result)
        try:
            return int(operation(keychain))
        finally:
            self._core_foundation.CFRelease(keychain)

    def _framework(self) -> Any:
        if self._security is None:
            security = ctypes.CDLL("/System/Library/Frameworks/Security.framework/Security")
            core_foundation = ctypes.CDLL(
                "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
            )
            security.SecKeychainOpen.argtypes = (ctypes.c_char_p, ctypes.c_void_p)
            security.SecKeychainOpen.restype = ctypes.c_int32
            security.SecKeychainGetStatus.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
            security.SecKeychainGetStatus.restype = ctypes.c_int32
            security.SecKeychainCopySettings.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
            security.SecKeychainCopySettings.restype = ctypes.c_int32
            security.SecKeychainSetUserInteractionAllowed.argtypes = (ctypes.c_ubyte,)
            security.SecKeychainSetUserInteractionAllowed.restype = ctypes.c_int32
            core_foundation.CFRelease.argtypes = (ctypes.c_void_p,)
            core_foundation.CFRelease.restype = None
            # Reading a locked vault's settings then fails instead of opening a
            # password window. This affects only this process.
            security.SecKeychainSetUserInteractionAllowed(0)
            self._security = security
            self._core_foundation = core_foundation
        return self._security


def default_vault_path() -> Path:
    """Return the one Kantrip vault of the current macOS user."""
    return Path.home() / "Library" / "Keychains" / VAULT_FILENAME


class MacOSVault:
    """``SecretStore`` backed by the dedicated Kantrip keychain."""

    def __init__(
        self,
        path: Path | None = None,
        system: KeychainSystem | None = None,
    ) -> None:
        self.path = path or default_vault_path()
        if _UNSAFE_PATH_CHARACTERS.intersection(str(self.path)):
            raise VaultError("Kantrip vault path contains unsupported characters")
        self._system = system or NativeKeychainSystem()
        self._display = display_path(self.path)
        # A refused or cancelled unlock is final for this process, so a loop
        # over several credentials does not prompt again for each one.
        self._refusal: VaultError | None = None
        self.info = SecretStoreInfo("Kantrip vault through /usr/bin/security", "macOS keychain")

    def get(self, reference: str) -> str:
        parse_secret_reference(reference)
        self._ready()
        data = self._read(reference)
        if data is None:
            raise SecretNotFoundError("credential store entry was not found")
        try:
            value = data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SecretStoreError("credential store returned an invalid secret value") from error
        if not value:
            raise SecretStoreError("credential store returned an empty secret value")
        return value

    def set(self, reference: str, value: str) -> None:
        parsed = parse_secret_reference(reference)
        if not isinstance(value, str) or not value:
            raise SecretStoreError("secret value must be non-empty text")
        self._ready(create=True)
        self._write(reference, _label(parsed.field, parsed.profile_id), value.encode("utf-8"))

    def delete(self, reference: str) -> None:
        parse_secret_reference(reference)
        if self._state() == "missing":
            # A secret cannot outlive its vault; retiring it is already done.
            return
        self._ready()
        count = self._piece_count(reference)
        if count is None:
            # A write interrupted before its head item leaves only pieces,
            # which are always written in ascending order.
            index = 1
            while self._delete_item(_piece_account(reference, index)):
                index += 1
            return
        for index in reversed(range(1, count)):
            self._delete_item(_piece_account(reference, index))
        self._delete_item(reference)

    def vault_status(self) -> VaultStatus:
        """Describe the vault for ``doctor`` without unlocking it."""
        state = self._state()
        warnings: list[str] = []
        policy = None
        if state == "unlocked":
            policy = self._system.lock_policy(self.path)
            if self._has_empty_password():
                warnings.append(
                    "Credential vault has an empty password, so anyone at this account can "
                    "unlock it; set one in Keychain Access with Edit > Change Password for "
                    'Keychain "kantrip"'
                )
        if state == "missing" and self._listed():
            warnings.append(
                "Keychain Access still lists the missing credential vault; "
                "run 'kantrip doctor --repair'"
            )
        return VaultStatus(self._display, state, policy, tuple(warnings))

    def unlock_warnings(self) -> tuple[str, ...]:
        """Report nothing: a macOS vault never opens without its password prompt."""
        return ()

    def forget_missing_vault(self) -> bool:
        """Remove a missing vault from the keychain search list."""
        if self._state() != "missing" or not self._listed():
            return False
        self._set_search_list(entry for entry in self._search_list() if not self._is_vault(entry))
        return True

    def _state(self) -> VaultState:
        if self.path.is_symlink():
            raise VaultError(f"Kantrip vault {self._display} is a symbolic link")
        return self._system.keychain_state(self.path)

    def _ready(self, *, create: bool = False) -> None:
        if self._refusal is not None:
            raise self._refusal
        state = self._state()
        if state == "unlocked":
            return
        if state == "missing":
            if not create:
                raise VaultError(
                    f"Kantrip vault {self._display} does not exist; restore it, or store the "
                    "credential again with 'kantrip edit PROFILE --replace-secret FIELD'"
                )
            self._create()
            return
        try:
            self._unlock()
        except VaultError as error:
            self._refusal = error
            raise

    def _create(self) -> None:
        with self._system.terminal() as terminal:
            if terminal is None:
                raise VaultError(
                    f"Kantrip vault {self._display} does not exist and there is no terminal "
                    "to create it; run the command in a terminal"
                )
            terminal.write(
                f"Kantrip keeps credentials in its own keychain, {self._display}.\n"
                "Choose a password for it. Kantrip never sees or stores this password; "
                "macOS asks for it again after 15 idle minutes and after sleep.\n"
            )
            try:
                result = self._system.run_on_terminal(
                    ("create-keychain", str(self.path)), terminal, show_errors=True
                )
            except KeyboardInterrupt as error:
                raise VaultError("Kantrip vault creation was cancelled") from error
            if result.returncode != 0:
                raise VaultError(f"Kantrip vault {self._display} could not be created")
            self._secure_new_vault()
            terminal.write(f"Created {self._display}.\n")

    def _secure_new_vault(self) -> None:
        if self._has_empty_password():
            self._system.run(("delete-keychain", str(self.path)))
            raise VaultError(
                "Kantrip vault password must not be empty; the new vault was removed. "
                "Run the command again and choose a password"
            )
        policy = ("set-keychain-settings", "-l", "-u", "-t", str(LOCK_TIMEOUT_SECONDS))
        if self._system.run((*policy, str(self.path))).returncode != 0:
            raise VaultError(f"Kantrip vault {self._display} lock policy could not be set")
        if not self._listed():
            self._set_search_list((*self._search_list(), str(self.path)))

    def _has_empty_password(self) -> bool:
        # A wrong password leaves an unlocked keychain unlocked, so this
        # check changes nothing unless the password really is empty.
        return self._system.run(("unlock-keychain", "-p", "", str(self.path))).returncode == 0

    def _unlock(self) -> None:
        with self._system.terminal() as terminal:
            if terminal is None:
                raise VaultError(
                    f"Kantrip vault {self._display} is locked and there is no terminal to "
                    "unlock it; run the command in a terminal, or first run "
                    f"'security unlock-keychain {self._display}'"
                )
            terminal.write(f"Kantrip vault {self._display} is locked.\n")
            for attempt in range(1, UNLOCK_ATTEMPTS + 1):
                if self._unlock_attempt(terminal):
                    return
                left = UNLOCK_ATTEMPTS - attempt
                if left:
                    noun = "attempt" if left == 1 else "attempts"
                    terminal.write(f"Incorrect password; {left} {noun} left.\n")
        raise VaultError(
            f"Kantrip vault {self._display} is still locked after "
            f"{UNLOCK_ATTEMPTS} incorrect passwords"
        )

    def _unlock_attempt(self, terminal: Terminal) -> bool:
        try:
            result = self._system.run_on_terminal(
                ("unlock-keychain", str(self.path)), terminal, show_errors=False
            )
        except KeyboardInterrupt as error:
            raise VaultError("Kantrip vault unlock was cancelled") from error
        if result.returncode == 0:
            return True
        if result.returncode == _WRONG_PASSWORD:
            return False
        raise VaultError(f"Kantrip vault {self._display} could not be unlocked")

    def _read(self, reference: str) -> bytes | None:
        head = self._find(reference, reveal=True)
        if head is None:
            return None
        count = _parse_piece_count(head.stdout)
        pieces = [_parse_password(head.stderr)]
        for index in range(1, count):
            piece = self._find(_piece_account(reference, index), reveal=True)
            if piece is None:
                raise SecretStoreError(
                    "credential store entry is incomplete; store the credential again"
                )
            pieces.append(_parse_password(piece.stderr))
        return b"".join(pieces)

    def _write(self, reference: str, label: str, data: bytes) -> None:
        previous = self._piece_count(reference) or 0
        pieces = _split(data, self._piece_capacity(reference, label))
        count = len(pieces)
        # Pieces first and the head last: a reader never sees a head whose
        # pieces are not written yet.
        lines = [
            self._add_line(
                _piece_account(reference, index), _piece_label(label, index, count), 0, piece
            )
            for index, piece in enumerate(pieces)
            if index
        ]
        lines.append(self._add_line(reference, _piece_label(label, 0, count), count, pieces[0]))
        lines.extend(
            self._delete_line(_piece_account(reference, index)) for index in range(count, previous)
        )
        if any(len(line.encode()) > LINE_BUDGET for line in lines):
            raise SecretStoreError("credential store entry could not be split safely")
        result = self._system.run(("-i",), stdin="\n".join(lines) + "\n")
        if result.returncode != 0 or _batch_failed(result):
            raise SecretStoreError("credential store could not save the requested secret")

    def _piece_capacity(self, reference: str, label: str) -> int:
        longest = self._add_line(
            _piece_account(reference, MAX_PIECES),
            _piece_label(label, MAX_PIECES, MAX_PIECES),
            MAX_PIECES,
            b"",
        )
        capacity = (LINE_BUDGET - len(longest.encode())) // 2
        if capacity < _MIN_PIECE_BYTES:
            raise VaultError(f"Kantrip vault path {self._display} is too long")
        return capacity

    def _add_line(self, account: str, label: str, count: int, data: bytes) -> str:
        return (
            f'add-generic-password -U -a "{account}" -s {SERVICE_NAME} -l "{label}" '
            f'-G {count} -X {data.hex()} "{self.path}"'
        )

    def _delete_line(self, account: str) -> str:
        return f'delete-generic-password -a "{account}" -s {SERVICE_NAME} "{self.path}"'

    def _piece_count(self, reference: str) -> int | None:
        head = self._find(reference, reveal=False)
        return None if head is None else _parse_piece_count(head.stdout)

    def _find(self, account: str, *, reveal: bool) -> CommandResult | None:
        arguments = ["find-generic-password", "-a", account, "-s", SERVICE_NAME]
        if reveal:
            arguments.append("-g")
        result = self._system.run((*arguments, str(self.path)))
        if result.returncode == _ITEM_NOT_FOUND:
            return None
        if result.returncode != 0:
            raise SecretStoreError("credential store could not read the requested secret")
        return result

    def _delete_item(self, account: str) -> bool:
        result = self._system.run(
            ("delete-generic-password", "-a", account, "-s", SERVICE_NAME, str(self.path))
        )
        if result.returncode == _ITEM_NOT_FOUND:
            return False
        if result.returncode != 0:
            raise SecretStoreError("credential store could not delete the requested secret")
        return True

    def _search_list(self) -> tuple[str, ...]:
        result = self._system.run(("list-keychains", "-d", "user"))
        if result.returncode != 0:
            raise VaultError("the keychain search list could not be read")
        return tuple(
            match.group(1)
            for line in result.stdout.splitlines()
            if (match := _LIST_ENTRY.match(line)) is not None
        )

    def _set_search_list(self, entries: Iterable[str]) -> None:
        if self._system.run(("list-keychains", "-d", "user", "-s", *entries)).returncode != 0:
            raise VaultError("the keychain search list could not be updated")

    def _listed(self) -> bool:
        return any(self._is_vault(entry) for entry in self._search_list())

    def _is_vault(self, entry: str) -> bool:
        return os.path.realpath(entry) == os.path.realpath(self.path)


def _label(field: str, profile_id: str) -> str:
    return f"Kantrip {field} (profile {profile_id})"


def _piece_label(label: str, index: int, count: int) -> str:
    return label if count == 1 else f"{label}, part {index + 1} of {count}"


def _piece_account(reference: str, index: int) -> str:
    return reference if index == 0 else f"{reference}#{index}"


def _split(data: bytes, size: int) -> list[bytes]:
    pieces = [data[offset : offset + size] for offset in range(0, len(data), size)]
    if len(pieces) > MAX_PIECES:
        raise SecretStoreError("secret value is too large for the credential store")
    return pieces


def _parse_piece_count(attributes: str) -> int:
    match = _PIECE_COUNT.search(attributes)
    count = int(match.group(1)) if match else 1
    if not 1 <= count <= MAX_PIECES:
        raise SecretStoreError("credential store entry is invalid")
    return count


def _parse_password(output: str) -> bytes:
    # `-g` prints printable values as text and anything else as hex.
    for line in output.splitlines():
        if (match := _HEX_PASSWORD.match(line)) is not None:
            return bytes.fromhex(match.group(1))
        if (match := _TEXT_PASSWORD.match(line)) is not None:
            return match.group(1).encode("utf-8")
    raise SecretStoreError("credential store returned an invalid secret value")


def _batch_failed(result: CommandResult) -> bool:
    # `security -i` exits 0 even when a command fails, so any stderr line
    # other than a deletion's confirmation means the batch did not succeed.
    return any(line.strip() not in ("", _DELETED) for line in result.stderr.splitlines())


__all__ = [
    "LINE_BUDGET",
    "LOCK_TIMEOUT_SECONDS",
    "CommandResult",
    "KeychainSystem",
    "MacOSVault",
    "NativeKeychainSystem",
    "Terminal",
    "default_vault_path",
]
