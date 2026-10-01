"""Read bounded import documents and hold the Kafka connection they describe.

An import never becomes a profile on its own: its parser returns an
`ImportedConnection`, the command line merges it with explicit options, and the
ordinary `add` path validates and stores the result.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from kantrip.profile_auth import RegistryAuthInput
from kantrip.secret_value import Secret

MAX_IMPORT_BYTES = 1024 * 1024
STDIN_SOURCE = "-"


class ProfileImportError(ValueError):
    """Raised when an import document is unreadable, unsafe, or unsupported."""


@dataclass(frozen=True)
class ImportedRegistry:
    """The Registry an import sets; ``auth`` is ``None`` without authentication or TLS."""

    provider: str
    url: str
    auth: RegistryAuthInput | None = None

    @property
    def has_secrets(self) -> bool:
        auth = self.auth
        if auth is None:
            return False
        secrets = (auth.password, auth.token, auth.private_key, auth.private_key_password)
        return any(secret is not None for secret in (*secrets, auth.oauth_client_secret))


@dataclass(frozen=True)
class ImportedConnection:
    """Connection fields an import sets; ``None`` leaves a field to the options.

    ``registry`` is the imported Registry, and ``ignored_keys`` names the
    application settings the source held but a profile doesn't keep.
    """

    bootstrap_servers: tuple[str, ...] | None = None
    transport: str | None = None
    ca_certificates: str | None = None
    auth_type: str | None = None
    username: str | None = None
    password: Secret | None = None
    client_certificate: str | None = None
    private_key: Secret | None = None
    private_key_password: Secret | None = None
    oauth_token_url: str | None = None
    oauth_client_id: str | None = None
    oauth_scopes: tuple[str, ...] | None = None
    oauth_client_secret: Secret | None = None
    oauth_ca_certificates: str | None = None
    registry: ImportedRegistry | None = None
    ignored_keys: tuple[str, ...] = ()

    @property
    def has_secrets(self) -> bool:
        """Return whether the import carried a credential."""
        secrets = (self.password, self.private_key, self.private_key_password)
        registry = self.registry is not None and self.registry.has_secrets
        return registry or any(
            secret is not None for secret in (*secrets, self.oauth_client_secret)
        )


def source_directory(source: str) -> Path | None:
    """Return the directory relative paths in a source resolve against, or ``None`` for stdin."""
    if source == STDIN_SOURCE:
        return None
    return Path(os.path.abspath(source)).parent


def read_import_source(source: str, *, stdin: BinaryIO, label: str) -> str:
    """Read one bounded UTF-8 document from a regular file, or from stdin for ``-``."""
    if source == STDIN_SOURCE:
        contents = stdin.read(MAX_IMPORT_BYTES + 1)
    else:
        contents = _read_regular_file(source, label)
    if len(contents) > MAX_IMPORT_BYTES:
        raise ProfileImportError(f"{label} exceeds the 1 MiB limit")
    try:
        return contents.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ProfileImportError(f"{label} must be UTF-8 text") from error


def _read_regular_file(path: str, label: str) -> bytes:
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProfileImportError(f"{label} could not be read") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ProfileImportError(f"{label} must be a regular file; use - for stdin")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            return source.read(MAX_IMPORT_BYTES + 1)
    except OSError as error:
        raise ProfileImportError(f"{label} could not be read") from error
    finally:
        os.close(descriptor)


__all__ = [
    "MAX_IMPORT_BYTES",
    "STDIN_SOURCE",
    "ImportedConnection",
    "ImportedRegistry",
    "ProfileImportError",
    "read_import_source",
    "source_directory",
]
