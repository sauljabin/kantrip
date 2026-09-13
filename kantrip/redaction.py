"""Sensitive-value classification for structured profile output."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TypeAlias
from urllib.parse import urlsplit, urlunsplit

REDACTED = "<redacted>"

_CLASSIFIED_KEY_PARTS = (
    "authorization",
    "bearer",
    "clientsecret",
    "credential",
    "jaas",
    "password",
    "privatekey",
    "refreshtoken",
    "saslpassword",
    "secret",
    "token",
)

_URL_PATTERN = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
_AUTHORIZATION_PATTERN = re.compile(
    r"\b(authorization)\b(\s*[:=]\s*)(?:Bearer\s+)?[^\s,;]+",
    re.IGNORECASE,
)
_BEARER_PATTERN = re.compile(r"\bBearer\s+[^\s,;]+", re.IGNORECASE)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"\b(client[._-]?secret|password|private[._-]?key|refresh[._-]?token|"
    r"sasl[._-]?password|secret|token)\b"
    r"(\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;]+)",
    re.IGNORECASE,
)
_MAX_DIAGNOSTIC_LENGTH = 500

Redactable: TypeAlias = object


def _normalized_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def is_classified_key(key: str) -> bool:
    """Return whether a key names secret material or a secret reference."""
    normalized = _normalized_key(key)
    return any(part in normalized for part in _CLASSIFIED_KEY_PARTS)


def redact_mapping(values: Mapping[str, Redactable]) -> dict[str, Redactable]:
    """Recursively redact classified fields without mutating the source."""
    return {
        key: REDACTED if is_classified_key(key) else _redact_nested(value)
        for key, value in values.items()
    }


def redact_text(value: object) -> str:
    """Return a bounded diagnostic string with common secret forms removed."""
    text = _URL_PATTERN.sub(_redact_url, str(value))
    text = _AUTHORIZATION_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}",
        text,
    )
    text = _BEARER_PATTERN.sub(f"Bearer {REDACTED}", text)
    text = _SECRET_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}",
        text,
    )
    printable = "".join(
        character if ord(character) >= 32 and ord(character) != 127 else " " for character in text
    )
    text = " ".join(printable.split())
    if len(text) > _MAX_DIAGNOSTIC_LENGTH:
        return f"{text[: _MAX_DIAGNOSTIC_LENGTH - 1]}…"
    return text


def _redact_url(match: re.Match[str]) -> str:
    parsed = urlsplit(match.group(0))
    hostname = parsed.hostname
    if hostname is None:
        return "<redacted-url>"
    host = f"[{hostname}]" if ":" in hostname else hostname
    try:
        port = f":{parsed.port}" if parsed.port is not None else ""
    except ValueError:
        return f"{parsed.scheme}://<redacted>"
    return urlunsplit((parsed.scheme, f"{host}{port}", parsed.path, "", ""))


def _redact_nested(value: Redactable) -> Redactable:
    if isinstance(value, Mapping):
        string_mapping = {str(key): nested for key, nested in value.items()}
        return redact_mapping(string_mapping)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_redact_nested(item) for item in value]
    return value


__all__ = ["redact_mapping", "redact_text"]
