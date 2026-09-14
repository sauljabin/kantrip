"""Store profile secrets only in approved operating-system credential backends."""

from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass
from typing import Protocol

import keyring
from keyring.errors import KeyringLocked, PasswordDeleteError

SERVICE_NAME = "kantrip"
SECRET_FIELDS = frozenset(
    {
        "kafka/password",
        "oauth/client-secret",
        "tls/private-key",
        "tls/private-key-password",
        "registry/password",
        "registry/token",
    }
)
_APPROVED_BACKENDS = {
    "darwin": frozenset({"keyring.backends.macOS.Keyring"}),
    "linux": frozenset(
        {
            "keyring.backends.SecretService.Keyring",
            "keyring.backends.libsecret.Keyring",
        }
    ),
}


class SecretStoreError(RuntimeError):
    """Raised when an approved credential store cannot complete an operation."""


class SecretNotFoundError(SecretStoreError):
    """Raised when a referenced secret does not exist."""


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


def load_secret_store(
    *,
    backend: object | None = None,
    platform_name: str | None = None,
) -> KeyringSecretStore:
    """Load the configured keyring only when its concrete backend is approved."""
    selected_platform = platform_name or sys.platform
    platform_family = _platform_family(selected_platform)
    try:
        selected_backend = keyring.get_keyring() if backend is None else backend
        identifier = _backend_identifier(selected_backend)
        priority = selected_backend.priority  # type: ignore[attr-defined]
    except Exception as error:
        raise SecretStoreError("credential store backend is unavailable") from error
    if (
        type(priority) not in (int, float)
        or priority < 1
        or identifier not in _APPROVED_BACKENDS[platform_family]
    ):
        raise SecretStoreError("configured credential store backend is not approved")
    display_name = "macOS Keychain" if platform_family == "darwin" else "Secret Service"
    return KeyringSecretStore(selected_backend, SecretStoreInfo(identifier, display_name))


def secret_reference(profile_id: str, field: str) -> str:
    """Construct one canonical immutable profile secret reference."""
    if field not in SECRET_FIELDS:
        raise SecretStoreError("secret field is not supported")
    canonical_id = _canonical_profile_id(profile_id)
    return f"profile/{canonical_id}/{field}"


def validate_secret_reference(reference: str) -> None:
    """Reject references outside Kantrip's exact profile-key namespace."""
    if not isinstance(reference, str):
        raise SecretStoreError("secret reference is invalid")
    parts = reference.split("/", 2)
    if len(parts) != 3 or parts[0] != "profile" or parts[2] not in SECRET_FIELDS:
        raise SecretStoreError("secret reference is invalid")
    if _canonical_profile_id(parts[1]) != parts[1]:
        raise SecretStoreError("secret reference is invalid")


def _canonical_profile_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise SecretStoreError("profile ID is not a canonical UUID") from error
    canonical = str(parsed)
    if canonical != value:
        raise SecretStoreError("profile ID is not a canonical UUID")
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
    "SecretNotFoundError",
    "SecretStore",
    "SecretStoreError",
    "SecretStoreInfo",
    "load_secret_store",
    "secret_reference",
    "validate_secret_reference",
]
