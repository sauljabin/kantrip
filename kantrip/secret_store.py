"""Store profile secrets only in approved operating-system credential backends."""

from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol

import keyring
from keyring.errors import KeyringLocked, PasswordDeleteError

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
_APPROVED_LINUX_BACKENDS = frozenset(
    {
        "keyring.backends.SecretService.Keyring",
        "keyring.backends.libsecret.Keyring",
    }
)

if TYPE_CHECKING:
    from kantrip.macos_vault import MacOSVault

VaultState = Literal["missing", "locked", "unlocked"]


class SecretStoreError(RuntimeError):
    """Raised when an approved credential store cannot complete an operation."""


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
    """Non-sensitive identity of one approved credential backend."""

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


@dataclass(frozen=True)
class SecretReference:
    """Validated identity encoded by one immutable credential reference."""

    profile_id: str
    credential_id: str
    field: str


class KeyringSecretStore:
    """Keyring adapter restricted to native approved backend implementations."""

    def __init__(self, backend: object, info: SecretStoreInfo) -> None:
        self._backend = backend
        self.info = info

    def get(self, reference: str) -> str:
        validate_secret_reference(reference)
        try:
            value = self._backend.get_password(SERVICE_NAME, reference)  # type: ignore[attr-defined]
        except KeyringLocked as error:
            raise SecretStoreError("credential store is locked") from error
        except Exception as error:
            raise SecretStoreError(
                "credential store could not read the requested secret"
            ) from error
        if value is None:
            raise SecretNotFoundError("credential store entry was not found")
        if not isinstance(value, str):
            raise SecretStoreError("credential store returned an invalid secret value")
        return value

    def set(self, reference: str, value: str) -> None:
        validate_secret_reference(reference)
        if not isinstance(value, str) or not value:
            raise SecretStoreError("secret value must be non-empty text")
        try:
            self._backend.set_password(SERVICE_NAME, reference, value)  # type: ignore[attr-defined]
        except KeyringLocked as error:
            raise SecretStoreError("credential store is locked") from error
        except Exception as error:
            raise SecretStoreError(
                "credential store could not save the requested secret"
            ) from error

    def delete(self, reference: str) -> None:
        validate_secret_reference(reference)
        try:
            self._backend.delete_password(SERVICE_NAME, reference)  # type: ignore[attr-defined]
        except PasswordDeleteError:
            return
        except KeyringLocked as error:
            raise SecretStoreError("credential store is locked") from error
        except Exception as error:
            raise SecretStoreError(
                "credential store could not delete the requested secret"
            ) from error

    def vault_status(self) -> VaultStatus | None:
        """Report no dedicated vault: the keyring library uses the default store."""
        return None

    def forget_missing_vault(self) -> bool:
        """Report that no vault registration needed repair."""
        return False


def load_secret_store(
    *,
    backend: object | None = None,
    platform_name: str | None = None,
) -> KeyringSecretStore | MacOSVault:
    """Load the Kantrip vault on macOS or an approved Secret Service keyring on Linux."""
    platform_family = _platform_family(platform_name or sys.platform)
    if platform_family == "darwin":
        from kantrip.macos_vault import MacOSVault

        return MacOSVault()
    try:
        selected_backend = keyring.get_keyring() if backend is None else backend
        identifier = _backend_identifier(selected_backend)
        priority = selected_backend.priority  # type: ignore[attr-defined]
    except Exception as error:
        raise SecretStoreError("credential store backend is unavailable") from error
    if (
        type(priority) not in (int, float)
        or priority < 1
        or identifier not in _APPROVED_LINUX_BACKENDS
    ):
        raise SecretStoreError("configured credential store backend is not approved")
    return KeyringSecretStore(selected_backend, SecretStoreInfo(identifier, "Secret Service"))


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


def _backend_identifier(backend: object) -> str:
    backend_type = type(backend)
    return f"{backend_type.__module__}.{backend_type.__qualname__}"


__all__ = [
    "SECRET_FIELDS",
    "SERVICE_NAME",
    "KeyringSecretStore",
    "LockPolicy",
    "SecretNotFoundError",
    "SecretReference",
    "SecretStore",
    "SecretStoreError",
    "SecretStoreInfo",
    "VaultError",
    "VaultState",
    "VaultStatus",
    "load_secret_store",
    "parse_secret_reference",
    "secret_reference",
    "validate_secret_reference",
]
