"""Resolve the plaintext registry connection selected by a profile."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast
from urllib.parse import urlsplit, urlunsplit

RegistryProvider = Literal["apicurio", "confluent"]

APICURIO_PROVIDER: RegistryProvider = "apicurio"
CONFLUENT_PROVIDER: RegistryProvider = "confluent"
APICURIO_URL_PROPERTY = "apicurio.registry.url"
CONFLUENT_URL_PROPERTY = "schema.registry.url"


class RegistryProfileError(ValueError):
    """Raised when a configured registry connection is not executable."""


@dataclass(frozen=True)
class RegistryConnection:
    """A validated registry provider and its official serializer URL property."""

    provider: RegistryProvider
    url: str
    property_name: str

    @property
    def display_name(self) -> str:
        """Return the provider's user-facing product name."""
        return (
            "Apicurio Registry"
            if self.provider == APICURIO_PROVIDER
            else "Confluent Schema Registry"
        )


def plain_registry_connection(profile: Mapping[str, Any]) -> RegistryConnection | None:
    """Return a plaintext registry connection from a profile."""
    registry = profile.get("registry")
    if registry is None:
        return None
    if not isinstance(registry, Mapping):
        raise RegistryProfileError("registry must be an object in the selected Kantrip profile")

    provider_value = registry.get("provider", CONFLUENT_PROVIDER)
    if provider_value not in {APICURIO_PROVIDER, CONFLUENT_PROVIDER}:
        raise RegistryProfileError("registry.provider must be confluent or apicurio")
    provider = cast(RegistryProvider, provider_value)
    property_name = (
        APICURIO_URL_PROPERTY if provider == APICURIO_PROVIDER else CONFLUENT_URL_PROPERTY
    )
    expected_keys = {"provider", property_name}
    unknown_keys = sorted(str(key) for key in registry if key not in expected_keys)
    if unknown_keys:
        raise RegistryProfileError(
            f"registry contains properties incompatible with provider {provider}: "
            f"{', '.join(unknown_keys)}"
        )
    url = registry.get(property_name)
    if not isinstance(url, str):
        raise RegistryProfileError(f"registry requires {property_name}")
    _validate_plain_url(url)
    return RegistryConnection(provider, url, property_name)


def display_registry(profile: Mapping[str, Any]) -> str:
    """Return a compact, redacted registry label for profile listings."""
    registry = profile.get("registry")
    if not isinstance(registry, Mapping):
        return "-"
    provider = registry.get("provider", CONFLUENT_PROVIDER)
    property_name = (
        APICURIO_URL_PROPERTY if provider == APICURIO_PROVIDER else CONFLUENT_URL_PROPERTY
    )
    url = registry.get(property_name)
    if not isinstance(url, str):
        return "-"
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "-"
    netloc = parsed.netloc.rsplit("@", 1)[-1]
    redacted_url = urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))
    name = "Apicurio" if provider == APICURIO_PROVIDER else "Confluent"
    return f"{name}: {redacted_url}"


def _validate_plain_url(url: str) -> None:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as error:
        raise RegistryProfileError("registry URL is invalid") from error
    valid = (
        parsed.scheme == "http"
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.query == ""
        and parsed.fragment == ""
        and (port is None or 1 <= port <= 65535)
    )
    if not valid:
        raise RegistryProfileError(
            "Kantrip currently supports only an http:// registry URL without credentials, "
            "a query, or a fragment"
        )


__all__ = [
    "APICURIO_PROVIDER",
    "APICURIO_URL_PROPERTY",
    "CONFLUENT_PROVIDER",
    "CONFLUENT_URL_PROPERTY",
    "RegistryConnection",
    "RegistryProfileError",
    "RegistryProvider",
    "display_registry",
    "plain_registry_connection",
]
