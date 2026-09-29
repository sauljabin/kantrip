"""Store profile secrets only in the dedicated Kantrip vault of each platform."""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

SERVICE_NAME = "kantrip"
SECRET_FIELDS = frozenset(
    {
        "kafka/password",
        "kafka/oauth/client-secret",
        "kafka/tls/private-key",
        "kafka/tls/private-key-password",
        "registry/password",
        "registry/token",
        "registry/oauth/client-secret",
        "registry/tls/private-key",
        "registry/tls/private-key-password",
    }
)

VaultState = Literal["missing", "locked", "unlocked"]


class SecretStoreError(RuntimeError):
    """Raised when the credential vault cannot complete an operation."""


class SecretNotFoundError(SecretStoreError):
    """Raised when a referenced secret does not exist."""


class VaultError(SecretStoreError):
    """Raised when the vault itself, not one secret, is unavailable.

    Its message is user guidance (missing, locked without a terminal, unlock
    refused or cancelled) and never contains a secret, so callers show it
    instead of a generic credential error.
    """


class SecretStore(Protocol):
    """Narrow interface required by Kantrip profile mutations and sessions."""

    def get(self, reference: str) -> str:
        """Return one secret without exposing it through diagnostics."""

    def set(self, reference: str, value: str) -> None:
        """Create or replace one exact credential entry."""

    def delete(self, reference: str) -> None:
        """Idempotently delete one exact credential entry."""


@dataclass(frozen=True)
class SecretStoreInfo:
    """Non-sensitive identity of one platform's credential vault."""

    backend: str
    display_name: str


@dataclass(frozen=True)
class LockPolicy:
    """When the operating system locks the vault by itself."""

    lock_on_sleep: bool
    idle_seconds: int | None


@dataclass(frozen=True)
class VaultStatus:
    """Vault identity and state, read without unlocking or prompting."""

    location: str
    state: VaultState
    lock_policy: LockPolicy | None
    warnings: tuple[str, ...]


class Vault(SecretStore, Protocol):
    """The dedicated Kantrip vault of one platform."""

    info: SecretStoreInfo

    def vault_status(self) -> VaultStatus:
        """Describe the vault without unlocking it or prompting."""

    def unlock_warnings(self) -> tuple[str, ...]:
        """Return warnings this process learned while opening the vault."""

    def forget_missing_vault(self) -> bool:
        """Repair what a missing vault left registered; return whether anything changed."""


@dataclass(frozen=True)
class SecretReference:
    """Validated identity encoded by one immutable credential reference."""

    profile_id: str
    credential_id: str
    field: str


class Terminal(Protocol):
    """The controlling terminal, used only around vault prompts."""

    fd: int

    def write(self, text: str) -> None:
        """Show one Kantrip message on the terminal."""


class _TerminalDevice:
    def __init__(self, fd: int) -> None:
        self.fd = fd

    def write(self, text: str) -> None:
        os.write(self.fd, text.encode())


@contextmanager
def controlling_terminal() -> Iterator[Terminal | None]:
    """Yield the controlling terminal, or None in scripts, services, and CI."""
    try:
        fd = os.open("/dev/tty", os.O_RDWR | os.O_NOCTTY)
    except OSError:
        yield None
        return
    try:
        yield _TerminalDevice(fd)
    finally:
        os.close(fd)


def display_path(path: Path) -> str:
    """Show a vault path below the home directory with ``~``."""
    try:
        return f"~/{path.relative_to(Path.home())}"
    except ValueError:
        return str(path)


def load_secret_store(*, platform_name: str | None = None) -> Vault:
    """Load the dedicated Kantrip vault of this platform."""
    if _platform_family(platform_name or sys.platform) == "darwin":
        from kantrip.macos_vault import MacOSVault

        return MacOSVault()
    from kantrip.linux_vault import LinuxVault

    return LinuxVault()


def secret_reference(
    profile_id: str,
    field: str,
    *,
    credential_id: str | None = None,
) -> str:
    """Construct one canonical immutable profile secret reference."""
    if field not in SECRET_FIELDS:
        raise SecretStoreError("secret field is not supported")
    canonical_profile_id = _canonical_uuid(profile_id, label="profile ID")
    canonical_credential_id = _canonical_uuid(
        credential_id or str(uuid.uuid4()),
        label="credential ID",
    )
    return f"profile/{canonical_profile_id}/{canonical_credential_id}/{field}"


def validate_secret_reference(reference: str) -> None:
    """Reject references outside Kantrip's exact profile-key namespace."""
    parse_secret_reference(reference)


def parse_secret_reference(reference: str) -> SecretReference:
    """Parse a canonical reference without accessing its secret value."""
    if not isinstance(reference, str):
        raise SecretStoreError("secret reference is invalid")
    parts = reference.split("/", 3)
    if len(parts) != 4 or parts[0] != "profile" or parts[3] not in SECRET_FIELDS:
        raise SecretStoreError("secret reference is invalid")
    try:
        profile_id = _canonical_uuid(parts[1], label="profile ID")
        credential_id = _canonical_uuid(parts[2], label="credential ID")
    except SecretStoreError as error:
        raise SecretStoreError("secret reference is invalid") from error
    if profile_id != parts[1] or credential_id != parts[2]:
        raise SecretStoreError("secret reference is invalid")
    return SecretReference(profile_id, credential_id, parts[3])


def _canonical_uuid(value: str, *, label: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise SecretStoreError(f"{label} is not a canonical UUID") from error
    canonical = str(parsed)
    if canonical != value:
        raise SecretStoreError(f"{label} is not a canonical UUID")
    return canonical


def _platform_family(platform_name: str) -> str:
    if platform_name == "darwin":
        return "darwin"
    if platform_name.startswith("linux"):
        return "linux"
    raise SecretStoreError("credential storage is not supported on this platform")


__all__ = [
    "SECRET_FIELDS",
    "SERVICE_NAME",
    "LockPolicy",
    "SecretNotFoundError",
    "SecretReference",
    "SecretStore",
    "SecretStoreError",
    "SecretStoreInfo",
    "Terminal",
    "Vault",
    "VaultError",
    "VaultState",
    "VaultStatus",
    "controlling_terminal",
    "display_path",
    "load_secret_store",
    "parse_secret_reference",
    "secret_reference",
    "validate_secret_reference",
]
