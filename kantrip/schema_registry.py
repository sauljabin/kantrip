"""Validate the currently supported Schema Registry profile connection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit


class SchemaRegistryProfileError(ValueError):
    """Raised when a configured registry connection is not currently executable."""


def plain_schema_registry_url(profile: Mapping[str, Any]) -> str | None:
    """Return a plain registry URL or reject unsupported security settings."""
    registry = profile.get("schemaRegistry")
    if registry is None:
        return None
    if not isinstance(registry, Mapping):
        raise SchemaRegistryProfileError(
            "schemaRegistry must be an object in the selected Kantrip profile"
        )

    url = registry.get("url")
    auth = registry.get("auth")
    auth_type = auth.get("type") if isinstance(auth, Mapping) else None
    transport = registry.get("transport")
    tls = registry.get("tls")
    parsed_url = urlsplit(url) if isinstance(url, str) else None
    plain_url = (
        parsed_url is not None
        and parsed_url.scheme == "http"
        and parsed_url.hostname is not None
        and parsed_url.username is None
        and parsed_url.password is None
    )
    tls_configured = tls not in (None, False, {})
    if (
        auth_type != "none"
        or transport not in (None, "plaintext")
        or tls_configured
        or not plain_url
    ):
        raise SchemaRegistryProfileError(
            "the selected profile uses authenticated or TLS-secured Schema Registry settings; "
            "Kantrip currently supports only an http:// schemaRegistry URL with auth.type: none"
        )
    return url


def display_schema_registry_url(profile: Mapping[str, Any]) -> str:
    """Return a registry URL safe for compact profile listings."""
    registry = profile.get("schemaRegistry")
    url = registry.get("url") if isinstance(registry, Mapping) else None
    if not isinstance(url, str):
        return "-"
    parsed = urlsplit(url)
    netloc = parsed.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


__all__ = [
    "SchemaRegistryProfileError",
    "display_schema_registry_url",
    "plain_schema_registry_url",
]
