"""Secret classification and safe diagnostic redaction."""

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

_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|passphrase|client[_-]?secret|access[_-]?token|refresh[_-]?token|"
    r"sasl\.jaas\.config|sasl\.password|ssl\.key\.password)\b(\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)\b(Bearer)(\s+)[A-Za-z0-9._~+\-/]+=*")
_PRIVATE_KEY_PATTERN = re.compile(
    r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----.*?" r"-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
    re.DOTALL,
)
_URL_CREDENTIAL_PATTERN = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<username>[^\s:/@]+):(?P<secret>[^\s/@]+)@"
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


def redact_text(text: str) -> str:
    """Redact common secret-bearing diagnostic text formats."""
    redacted = _PRIVATE_KEY_PATTERN.sub(REDACTED, text)
    redacted = _URL_CREDENTIAL_PATTERN.sub(
        lambda match: f"{match.group('scheme')}{match.group('username')}:{REDACTED}@",
        redacted,
    )
    redacted = _BEARER_PATTERN.sub(lambda match: f"{match.group(1)} {REDACTED}", redacted)
    return _ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}", redacted
    )


__all__ = ["REDACTED", "is_classified_key", "redact_mapping", "redact_text"]
