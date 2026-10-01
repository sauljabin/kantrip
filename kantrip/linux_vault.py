"""Keep credentials in a dedicated Secret Service collection, the Kantrip vault.

The desktop's Secret Service provider owns the vault password: GNOME Keyring
and KDE Wallet ask for it in their own desktop windows, and Kantrip never
reads, stores, or passes it. Lookups and lock-state reads never prompt. Kantrip
starts a create or unlock window only with a controlling terminal to explain
it and bounds each window with its own timeout, because an unanswered window
otherwise blocks forever and stays on screen. When that expires, Kantrip calls
``Prompt.Dismiss()``, which closes GNOME's window; KDE only reports the prompt
as dismissed and leaves its window open. A GPG-encrypted KDE wallet asks through
gpg-agent's pinentry instead, which Kantrip can neither bound nor close, so
the vault must be a Classic wallet.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from kantrip.secret_store import (
    SERVICE_NAME,
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

VAULT_LABEL = "kantrip"
# KDE finds the vault by this alias; GNOME Keyring supports only `default`.
VAULT_ALIAS = "kantrip"
# GNOME Keyring derives the object path from the keyring file name.
GNOME_VAULT_PATH = "/org/freedesktop/secrets/collection/kantrip"
PROMPT_TIMEOUT_SECONDS = 60
NO_OBJECT = "/"
# GNOME Keyring dismisses a window it cannot show (no display, or a locked
# screen) at once; a person needs longer than this to cancel one.
_NO_WINDOW_SECONDS = 1.0
# An encrypted GNOME keyring file starts with this; an empty password leaves
# the file in plain text.
_GNOME_ENCRYPTED_HEADER = b"GnomeKeyring\n\r\0\n"
# A KDE wallet file starts with this magic and four version bytes: major,
# minor, cipher, and hash. Cipher 2 is GPG; Classic wallets use Blowfish.
_KDE_WALLET_MAGIC = b"KWALLET\n\r\0\r\n"
_KDE_GPG_CIPHER = 2
_KDE_CLASSIC_REMEDY = (
    f"delete the wallet '{VAULT_LABEL}' in KDE Wallet Manager, log out and back in, and "
    "store the credentials again with 'kantrip edit PROFILE --replace-secret FIELD', "
    "choosing Classic in the KDE wallet wizard"
)
_BUS_NAME = "org.freedesktop.secrets"
_SERVICE_PATH = "/org/freedesktop/secrets"
_INTERFACE = "org.freedesktop.Secret."
_NO_SERVICE = (
    "no Secret Service is running for this user; log in to the desktop session and run "
    "the command there"
)
_LOCKED_DURING_USE = "the Kantrip vault locked while Kantrip was using it; run the command again"


@dataclass(frozen=True)
class Provider:
    """A supported Secret Service implementation and its user-facing names."""

    name: str
    kde: bool
    manager: str
    noun: str


GNOME_KEYRING = Provider("GNOME Keyring", False, "Passwords and Keys", "keyring")
KDE_WALLET = Provider("KDE Wallet", True, "KDE Wallet Manager", "wallet")
# `/proc/PID/comm` holds at most 15 characters.
_PROVIDERS = {
    "gnome-keyring-d": GNOME_KEYRING,
    "gnome-keyring-daemon": GNOME_KEYRING,
    "ksecretd": KDE_WALLET,
    "kwalletd5": KDE_WALLET,
    "kwalletd6": KDE_WALLET,
}


class SecretServiceSystem(Protocol):
    """Secret Service calls the vault needs; unit tests replace them."""

    def provider_process(self) -> str:
        """Return the process name that owns the Secret Service bus name."""

    def collections(self) -> tuple[str, ...]:
        """Return every collection object path, locked or not."""

    def label(self, collection: str) -> str:
        """Return one collection's label, readable while it is locked."""

    def read_alias(self, alias: str) -> str:
        """Return the collection an alias names, or ``/`` for none."""

    def set_alias(self, alias: str, collection: str) -> None:
        """Point an alias at a collection, or remove it with ``/``."""

    def is_locked(self, collection: str) -> bool:
        """Return whether a collection is locked, without prompting."""

    def unlock(self, collection: str) -> str:
        """Request unlocking and return the prompt, or ``/`` when none was needed.

        The provider shows nothing until ``prompt`` runs the returned prompt.
        """

    def create_collection(self, label: str, alias: str) -> tuple[str, str]:
        """Request a new collection; return its path or ``/``, and a prompt or ``/``."""

    def delete_collection(self, collection: str) -> str:
        """Delete a collection and return a prompt, or ``/`` when none was needed."""

    def prompt(self, prompt: str, timeout: float) -> tuple[bool, str]:
        """Show a prompt and wait; return (dismissed, created object path or "").

        Raises ``TimeoutError`` when nobody answers in time.
        """

    def dismiss(self, prompt: str) -> None:
        """Close a prompt's window, ignoring failures."""

    def search(self, collection: str, attributes: Mapping[str, str]) -> tuple[str, ...]:
        """Return the items of one collection with exactly these attributes."""

    def secret(self, item: str) -> bytes:
        """Return one item's secret."""

    def store(
        self, collection: str, label: str, attributes: Mapping[str, str], secret: bytes
    ) -> None:
        """Create or replace the item with these attributes."""

    def delete_item(self, item: str) -> None:
        """Delete one item."""

    def terminal(self) -> Any:
        """Return a context manager yielding the controlling terminal or None."""


@contextmanager
def _translated(failure: str) -> Iterator[None]:
    """Turn D-Bus failures into store errors without exposing their details."""
    from secretstorage.exceptions import (
        LockedException,
        SecretServiceNotAvailableException,
    )

    try:
        yield
    except (SecretStoreError, TimeoutError):
        raise
    except SecretServiceNotAvailableException as error:
        raise VaultError(_NO_SERVICE) from error
    except LockedException as error:
        raise VaultError(_LOCKED_DURING_USE) from error
    except Exception as error:
        if str(getattr(error, "name", "")).endswith(".IsLocked"):
            raise VaultError(_LOCKED_DURING_USE) from error
        raise SecretStoreError(failure) from error


class NativeSecretService:
    """Secret Service D-Bus calls through ``secretstorage``, imported on first use."""

    def __init__(self) -> None:
        self._connection: Any = None
        self._session: Any = None

    def provider_process(self) -> str:
        with _translated("the Secret Service provider could not be identified"):
            # Reading a property starts a D-Bus-activated provider first.
            self._object(_SERVICE_PATH, "Service").get_property("Collections")
            bus = self._address("/org/freedesktop/DBus", "org.freedesktop.DBus")
            bus.bus_name = "org.freedesktop.DBus"
            (pid,) = bus.call("GetConnectionUnixProcessID", "s", _BUS_NAME)
            return Path(f"/proc/{int(pid)}/comm").read_text(encoding="utf-8").strip()

    def collections(self) -> tuple[str, ...]:
        with _translated("the Secret Service collections could not be listed"):
            paths = self._object(_SERVICE_PATH, "Service").get_property("Collections")
            return tuple(str(path) for path in paths)

    def label(self, collection: str) -> str:
        with _translated("a Secret Service collection could not be read"):
            return str(self._object(collection, "Collection").get_property("Label"))

    def read_alias(self, alias: str) -> str:
        with _translated("a Secret Service alias could not be read"):
            (path,) = self._object(_SERVICE_PATH, "Service").call("ReadAlias", "s", alias)
            return str(path)

    def set_alias(self, alias: str, collection: str) -> None:
        with _translated("a Secret Service alias could not be restored"):
            self._object(_SERVICE_PATH, "Service").call("SetAlias", "so", alias, collection)

    def is_locked(self, collection: str) -> bool:
        with _translated("the Kantrip vault state could not be read"):
            return bool(self._object(collection, "Collection").get_property("Locked"))

    def unlock(self, collection: str) -> str:
        with _translated("the Kantrip vault could not be unlocked"):
            _unlocked, prompt = self._object(_SERVICE_PATH, "Service").call(
                "Unlock", "ao", [collection]
            )
            return str(prompt)

    def create_collection(self, label: str, alias: str) -> tuple[str, str]:
        with _translated("the Kantrip vault could not be created"):
            properties = {f"{_INTERFACE}Collection.Label": ("s", label)}
            path, prompt = self._object(_SERVICE_PATH, "Service").call(
                "CreateCollection", "a{sv}s", properties, alias
            )
            return str(path), str(prompt)

    def delete_collection(self, collection: str) -> str:
        with _translated("the Kantrip vault could not be removed"):
            (prompt,) = self._object(collection, "Collection").call("Delete", "")
            return str(prompt)

    def prompt(self, prompt: str, timeout: float) -> tuple[bool, str]:
        from secretstorage.util import exec_prompt

        with _translated("the Secret Service window failed"):
            dismissed, (signature, result) = exec_prompt(self._bus(), prompt, timeout=timeout)
            return bool(dismissed), str(result) if signature == "o" else ""

    def dismiss(self, prompt: str) -> None:
        # The window may already be gone; there is nothing else to close.
        with suppress(SecretStoreError), _translated("the window could not be closed"):
            self._object(prompt, "Prompt").call("Dismiss", "")

    def search(self, collection: str, attributes: Mapping[str, str]) -> tuple[str, ...]:
        with _translated("credential store could not read the requested secret"):
            (items,) = self._object(collection, "Collection").call(
                "SearchItems", "a{ss}", dict(attributes)
            )
            return tuple(str(item) for item in items)

    def secret(self, item: str) -> bytes:
        from secretstorage.item import Item

        with _translated("credential store could not read the requested secret"):
            return bytes(Item(self._bus(), item, self._secret_session()).get_secret())

    def store(
        self, collection: str, label: str, attributes: Mapping[str, str], secret: bytes
    ) -> None:
        from secretstorage.util import format_secret

        with _translated("credential store could not save the requested secret"):
            properties = {
                f"{_INTERFACE}Item.Label": ("s", label),
                f"{_INTERFACE}Item.Attributes": ("a{ss}", dict(attributes)),
            }
            value = format_secret(self._secret_session(), secret, "text/plain")
            item, prompt = self._object(collection, "Collection").call(
                "CreateItem", "a{sv}(oayays)b", properties, value, True
            )
            if len(str(item)) <= 1:
                self.dismiss(str(prompt))
                raise SecretStoreError("credential store asked to confirm the save")

    def delete_item(self, item: str) -> None:
        with _translated("credential store could not delete the requested secret"):
            (prompt,) = self._object(item, "Item").call("Delete", "")
            if str(prompt) != NO_OBJECT:
                self.dismiss(str(prompt))
                raise SecretStoreError("credential store asked to confirm the deletion")

    def terminal(self) -> Any:
        return controlling_terminal()

    def _bus(self) -> Any:
        if self._connection is None:
            import secretstorage

            with _translated(_NO_SERVICE):
                self._connection = secretstorage.dbus_init()
        return self._connection

    def _secret_session(self) -> Any:
        from secretstorage.util import open_session

        if self._session is None:
            self._session = open_session(self._bus())
        return self._session

    def _object(self, path: str, interface: str) -> Any:
        return self._address(path, f"{_INTERFACE}{interface}")

    def _address(self, path: str, interface: str) -> Any:
        from secretstorage.util import DBusAddressWrapper

        return DBusAddressWrapper(path, interface, self._bus())


@dataclass(frozen=True)
class _Location:
    """Where the vault is and what state it is in; never read by prompting."""

    provider: Provider
    collection: str | None
    state: VaultState
    wallet: Path | None


def default_data_home() -> Path:
    """Return the XDG data directory where providers keep their vault files."""
    configured = os.environ.get("XDG_DATA_HOME", "")
    if configured and Path(configured).is_absolute():
        return Path(configured)
    return Path.home() / ".local" / "share"


class LinuxVault:
    """``SecretStore`` backed by the dedicated Kantrip Secret Service collection."""

    def __init__(
        self,
        system: SecretServiceSystem | None = None,
        *,
        data_home: Path | None = None,
        prompt_timeout: float = PROMPT_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._system = system or NativeSecretService()
        self._data_home = data_home or default_data_home()
        self._timeout = prompt_timeout
        self._clock = clock
        self._provider: Provider | None = None
        # A refused, cancelled, or unanswered window is final for this process,
        # so a loop over several credentials does not open another one each time.
        self._refusal: VaultError | None = None
        self._warnings: list[str] = []
        self.info = SecretStoreInfo("Kantrip vault through the Secret Service", "Secret Service")

    def get(self, reference: str) -> str:
        parse_secret_reference(reference)
        items = self._system.search(self._ready(), _attributes(reference))
        if not items:
            raise SecretNotFoundError("credential store entry was not found")
        if len(items) > 1:
            raise SecretStoreError("credential store holds duplicate entries for one secret")
        try:
            value = self._system.secret(items[0]).decode("utf-8")
        except UnicodeDecodeError as error:
            raise SecretStoreError("credential store returned an invalid secret value") from error
        if not value:
            # KDE reads items of a wallet whose file was replaced as empty secrets.
            raise SecretStoreError("credential store returned an empty secret value")
        return value

    def set(self, reference: str, value: str) -> None:
        parsed = parse_secret_reference(reference)
        if not isinstance(value, str) or not value:
            raise SecretStoreError("secret value must be non-empty text")
        collection = self._ready(create=True)
        label = f"Kantrip {parsed.field} (profile {parsed.profile_id})"
        self._system.store(collection, label, _attributes(reference), value.encode("utf-8"))

    def delete(self, reference: str) -> None:
        parse_secret_reference(reference)
        if self._locate().state == "missing":
            # A secret cannot outlive its vault; retiring it is already done.
            return
        collection = self._ready()
        for item in self._system.search(collection, _attributes(reference)):
            self._system.delete_item(item)

    def vault_status(self) -> VaultStatus:
        """Describe the vault for ``doctor`` without unlocking it or opening a window."""
        location = self._locate()
        warnings: list[str] = []
        if location.collection is not None and location.state == "missing":
            warnings.append(
                f"KDE Wallet still lists the credential vault without its file; restore "
                f"{self._display(location)}, or log out and back in before Kantrip creates "
                "a new vault"
            )
        elif location.collection is not None and self._is_default(location.collection):
            provider = location.provider
            warnings.append(
                f"Credential vault is the default {provider.noun}, so other applications "
                f"store their secrets in it; make another {provider.noun} the default in "
                f"{_default_setting(provider)}"
            )
        if self._uses_gpg(location):
            warnings.append(
                "Credential vault is encrypted with GPG, which Kantrip does not support; "
                + _KDE_CLASSIC_REMEDY
            )
        if self._has_plain_gnome_file(location):
            warnings.append(
                "Credential vault has an empty password, so its file is not encrypted and "
                "anyone at this account can read it; set a password with Change Password "
                "in Passwords and Keys"
            )
        return VaultStatus(self._location_label(location), location.state, None, tuple(warnings))

    def unlock_warnings(self) -> tuple[str, ...]:
        """Return what this process learned while opening the vault."""
        return tuple(self._warnings)

    def forget_missing_vault(self) -> bool:
        """Report that nothing needed repair: no registration outlives this vault."""
        return False

    def _ready(self, *, create: bool = False) -> str:
        if self._refusal is not None:
            raise self._refusal
        location = self._locate()
        if self._uses_gpg(location):
            # gpg-agent's pinentry would block the provider beyond any timeout.
            self._refusal = VaultError(
                f"Kantrip vault {self._display(location)} is encrypted with GPG, which "
                f"Kantrip does not support; {_KDE_CLASSIC_REMEDY}"
            )
            raise self._refusal
        if location.state == "unlocked" and location.collection is not None:
            return location.collection
        if location.state == "missing" and not create:
            raise VaultError(
                f"Kantrip vault {self._display(location)} does not exist; restore it, or "
                "store the credential again with "
                "'kantrip edit PROFILE --replace-secret FIELD'"
            )
        try:
            if location.state == "missing" or location.collection is None:
                return self._create(location)
            self._unlock(location, location.collection)
        except VaultError as error:
            self._refusal = error
            raise
        return location.collection

    def _locate(self) -> _Location:
        provider = self._identify()
        if provider.kde:
            return self._locate_kde(provider)
        return self._locate_gnome(provider)

    def _identify(self) -> Provider:
        if self._provider is None:
            process = self._system.provider_process()
            provider = _PROVIDERS.get(process)
            if provider is None:
                raise VaultError(
                    f"the Secret Service provider '{process}' is not supported; Kantrip "
                    "keeps its vault in GNOME Keyring or KDE Wallet"
                )
            self._provider = provider
        return self._provider

    def _locate_gnome(self, provider: Provider) -> _Location:
        collections = self._system.collections()
        labels = {collection: self._system.label(collection) for collection in collections}
        named = [collection for collection, label in labels.items() if label == VAULT_LABEL]
        if named == [GNOME_VAULT_PATH]:
            return _Location(provider, GNOME_VAULT_PATH, self._lock_state(GNOME_VAULT_PATH), None)
        if not named and GNOME_VAULT_PATH not in labels:
            return _Location(provider, None, "missing", None)
        if GNOME_VAULT_PATH in labels and labels[GNOME_VAULT_PATH] != VAULT_LABEL:
            found = f"the keyring there is named '{labels[GNOME_VAULT_PATH]}'"
        else:
            others = ", ".join(path for path in named if path != GNOME_VAULT_PATH)
            found = f"other keyrings named '{VAULT_LABEL}' exist at {others}"
        raise VaultError(
            f"Kantrip cannot tell which keyring is its vault: it expects the keyring "
            f"'{VAULT_LABEL}' at {GNOME_VAULT_PATH}, but {found}; rename or remove the "
            "other keyrings in Passwords and Keys"
        )

    def _locate_kde(self, provider: Provider) -> _Location:
        # Deleted wallets leave same-label ghosts until ksecretd restarts, so
        # only the alias identifies the vault, and only its file proves it exists.
        wallet = self._kde_wallet()
        collection = self._system.read_alias(VAULT_ALIAS)
        if collection == NO_OBJECT:
            return _Location(provider, None, "missing", wallet)
        if not wallet.is_file():
            return _Location(provider, collection, "missing", wallet)
        return _Location(provider, collection, self._lock_state(collection), wallet)

    def _lock_state(self, collection: str) -> VaultState:
        return "locked" if self._system.is_locked(collection) else "unlocked"

    def _create(self, location: _Location) -> str:
        provider = location.provider
        display = self._display(location)
        if location.collection is not None:
            raise VaultError(
                f"KDE Wallet still lists the Kantrip vault, but its file {display} is "
                "missing; restore the file, or log out and back in before Kantrip creates "
                "a new vault"
            )
        with self._system.terminal() as terminal:
            if terminal is None:
                raise VaultError(
                    f"Kantrip vault {display} does not exist and there is no terminal to "
                    "create it; run the command in a terminal of your desktop session"
                )
            terminal.write(self._creation_notice(provider))
            with self._default_kept(provider):
                collection, prompt = self._system.create_collection(
                    VAULT_LABEL, VAULT_ALIAS if provider.kde else ""
                )
                if prompt != NO_OBJECT:
                    collection = self._answer(prompt, "creation")
            self._check_created(provider, collection)
            terminal.write(f"Created {display}.\n")
        return collection

    def _creation_notice(self, provider: Provider) -> str:
        notice = (
            f"Kantrip keeps credentials in its own {provider.noun}, '{VAULT_LABEL}'.\n"
            f"Choose a password for it in the window on your desktop; Kantrip waits up to "
            f"{self._timeout:g} seconds. Kantrip never sees or stores this password.\n"
        )
        if provider.kde:
            notice += "In the KDE wallet wizard, choose Classic; Kantrip does not support GPG.\n"
        return notice

    def _check_created(self, provider: Provider, collection: str) -> None:
        if provider.kde:
            if _is_gpg_wallet(self._kde_wallet()):
                self._remove_gpg_wallet(collection)
            return
        if collection != GNOME_VAULT_PATH:
            raise VaultError(
                f"the new keyring was created at {collection} instead of {GNOME_VAULT_PATH}; "
                f"remove it in Passwords and Keys, and remove any stale "
                f"{display_path(self._gnome_file())}"
            )
        location = _Location(provider, collection, "unlocked", None)
        if self._has_plain_gnome_file(location):
            if self._system.delete_collection(collection) != NO_OBJECT:
                raise VaultError(
                    "Kantrip vault password must not be empty; remove the new keyring "
                    f"'{VAULT_LABEL}' in Passwords and Keys and run the command again"
                )
            raise VaultError(
                "Kantrip vault password must not be empty; the new vault was removed. "
                "Run the command again and choose a password"
            )

    def _remove_gpg_wallet(self, collection: str) -> None:
        if self._system.delete_collection(collection) != NO_OBJECT:
            raise VaultError(
                "Kantrip vault must be a Classic wallet, not GPG; delete the new wallet "
                f"'{VAULT_LABEL}' in KDE Wallet Manager, log out and back in, and run the "
                "command again, choosing Classic"
            )
        raise VaultError(
            "Kantrip vault must be a Classic wallet, not GPG; the new wallet was removed. "
            "Run the command again and choose Classic in the KDE wallet wizard"
        )

    def _unlock(self, location: _Location, collection: str) -> None:
        provider = location.provider
        display = self._display(location)
        with self._system.terminal() as terminal, self._default_kept(provider):
            prompt = self._system.unlock(collection)
            if prompt == NO_OBJECT:
                self._opened_without_window(provider, display, terminal)
                return
            if terminal is None:
                # The provider shows nothing until the prompt runs.
                self._system.dismiss(prompt)
                raise VaultError(
                    f"Kantrip vault {display} is locked and there is no terminal to unlock "
                    "it; run the command in a terminal of your desktop session, or first "
                    f"unlock the {provider.noun} '{VAULT_LABEL}' in {provider.manager}"
                )
            terminal.write(
                f"Kantrip vault {display} is locked. Enter its password in the window on "
                f"your desktop; Kantrip waits up to {self._timeout:g} seconds.\n"
            )
            self._answer(prompt, "unlock")

    def _opened_without_window(
        self, provider: Provider, display: str, terminal: Terminal | None
    ) -> None:
        if provider.kde:
            remedy = "give it a password in KDE Wallet Manager"
        else:
            remedy = (
                "give it a password, and stop automatic unlocking by deleting "
                f"'Unlock password for: {VAULT_LABEL}' from the Login keyring, in "
                "Passwords and Keys"
            )
        warning = (
            f"Kantrip vault {display} opened without asking for its password, so it does "
            f"not protect credentials while you are logged in; {remedy}"
        )
        if warning not in self._warnings:
            self._warnings.append(warning)
        if terminal is not None:
            terminal.write(f"Warning: {warning}.\n")

    def _answer(self, prompt: str, action: str) -> str:
        started = self._clock()
        try:
            dismissed, result = self._system.prompt(prompt, self._timeout)
        except TimeoutError as error:
            self._system.dismiss(prompt)
            raise VaultError(
                f"Kantrip vault {action} got no answer within {self._timeout:g} seconds; "
                "close the window if it is still open, and run the command again"
            ) from error
        except KeyboardInterrupt as error:
            self._system.dismiss(prompt)
            raise VaultError(f"Kantrip vault {action} was cancelled") from error
        if not dismissed:
            return result
        if self._clock() - started < _NO_WINDOW_SECONDS:
            raise VaultError(
                f"Kantrip vault {action} window could not be shown; it needs an unlocked "
                "desktop session, so log in to the desktop or unlock the screen and run "
                "the command again"
            )
        raise VaultError(f"Kantrip vault {action} was cancelled")

    @contextmanager
    def _default_kept(self, provider: Provider) -> Iterator[None]:
        """Undo KDE's first-use takeover of the `default` alias by the vault.

        While kwalletrc lacks `First Use=false`, creating or opening a wallet
        makes it the desktop default, and other applications would then store
        their secrets in the Kantrip vault.
        """
        if not provider.kde:
            yield
            return
        previous = self._system.read_alias("default")
        try:
            yield
        finally:
            self._restore_default(provider, previous)

    def _restore_default(self, provider: Provider, previous: str) -> None:
        try:
            if self._system.read_alias("default") != previous:
                self._system.set_alias("default", previous)
        except SecretStoreError:
            warning = (
                f"KDE Wallet may have made the Kantrip vault the default {provider.noun}; "
                "run 'kantrip doctor'"
            )
            if warning not in self._warnings:
                self._warnings.append(warning)

    def _is_default(self, collection: str) -> bool:
        return self._system.read_alias("default") == collection

    def _has_plain_gnome_file(self, location: _Location) -> bool:
        if location.provider.kde or location.state == "missing":
            return False
        try:
            with self._gnome_file().open("rb") as handle:
                header = handle.read(len(_GNOME_ENCRYPTED_HEADER))
        except OSError:
            return False
        return header != _GNOME_ENCRYPTED_HEADER

    def _uses_gpg(self, location: _Location) -> bool:
        return location.wallet is not None and _is_gpg_wallet(location.wallet)

    def _gnome_file(self) -> Path:
        return self._data_home / "keyrings" / f"{VAULT_LABEL}.keyring"

    def _kde_wallet(self) -> Path:
        return self._data_home / "kwalletd" / f"{VAULT_LABEL}.kwl"

    def _display(self, location: _Location) -> str:
        if location.wallet is not None:
            return display_path(location.wallet)
        return GNOME_VAULT_PATH

    def _location_label(self, location: _Location) -> str:
        label = f"{self._display(location)} in {location.provider.name}"
        if location.provider.kde and location.collection is not None:
            label += f", collection {location.collection}"
        return label


def _is_gpg_wallet(wallet: Path) -> bool:
    """Read only the wallet's header, which holds no secret and needs no window."""
    try:
        with wallet.open("rb") as handle:
            header = handle.read(len(_KDE_WALLET_MAGIC) + 4)
    except OSError:
        return False
    return (
        header.startswith(_KDE_WALLET_MAGIC)
        and len(header) > len(_KDE_WALLET_MAGIC) + 2
        and header[len(_KDE_WALLET_MAGIC) + 2] == _KDE_GPG_CIPHER
    )


def _attributes(reference: str) -> dict[str, str]:
    # KDE stores attributes and labels in plain JSON beside the wallet, so
    # they hold only the service name and the non-secret reference.
    return {"service": SERVICE_NAME, "account": reference}


def _default_setting(provider: Provider) -> str:
    if provider.kde:
        return "System Settings > KDE Wallet"
    return "Passwords and Keys"


__all__ = [
    "GNOME_KEYRING",
    "GNOME_VAULT_PATH",
    "KDE_WALLET",
    "NO_OBJECT",
    "PROMPT_TIMEOUT_SECONDS",
    "VAULT_ALIAS",
    "VAULT_LABEL",
    "LinuxVault",
    "NativeSecretService",
    "Provider",
    "SecretServiceSystem",
    "default_data_home",
]
