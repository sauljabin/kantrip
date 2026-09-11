"""Sensitive-value classification for structured profile output."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TypeAlias

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


def _redact_nested(value: Redactable) -> Redactable:
    if isinstance(value, Mapping):
        string_mapping = {str(key): nested for key, nested in value.items()}
        return redact_mapping(string_mapping)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_redact_nested(item) for item in value]
    return value


__all__ = ["redact_mapping"]
