"""Shared OAuth client-credentials profile model and validation."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from kantrip.secret_value import Secret


class OAuthProfileError(ValueError):
    """Raised when an OAuth client-credentials configuration is unsafe."""


@dataclass(frozen=True)
class OAuthConnection:
    """One independently trusted client-credentials token endpoint."""

    token_url: str
    client_id: str
    scopes: tuple[str, ...]
    client_secret_reference: str
    ca_certificates: str | None = None
    client_secret: Secret | None = None


def validate_oauth_endpoint(url: str) -> str:
    """Require one credential-free HTTPS token endpoint URL."""
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except (TypeError, ValueError) as error:
        raise OAuthProfileError("OAuth token URL is invalid") from error
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise OAuthProfileError(
            "OAuth token URL must use https:// without credentials, a query, or a fragment"
        )
    return url


def validate_oauth_identity(client_id: object, scopes: object) -> tuple[str, tuple[str, ...]]:
    """Validate a client ID and an ordered, unique scope sequence."""
    if (
        not isinstance(client_id, str)
        or not client_id
        or any(character in client_id for character in ("\x00", "\r", "\n"))
    ):
        raise OAuthProfileError("OAuth client ID is invalid")
    if (
        not isinstance(scopes, list)
        or not all(
            isinstance(scope, str)
            and bool(scope)
            and not any(character.isspace() for character in scope)
            and "\x00" not in scope
            for scope in scopes
        )
        or len(scopes) != len(set(scopes))
    ):
        raise OAuthProfileError("OAuth scopes must be ordered unique non-empty tokens")
    return client_id, tuple(scopes)


__all__ = [
    "OAuthConnection",
    "OAuthProfileError",
    "validate_oauth_endpoint",
    "validate_oauth_identity",
]
