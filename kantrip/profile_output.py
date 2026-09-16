"""Build safe profile observations for human and structured output."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

import yaml

from kantrip.secret_store import (
    SecretNotFoundError,
    SecretStore,
    SecretStoreError,
    load_secret_store,
)

OutputFormat = Literal["json", "yaml"]


def filter_profiles(
    profiles: Mapping[str, Mapping[str, Any]],
    required_labels: Mapping[str, str],
) -> dict[str, Mapping[str, Any]]:
    """Return profiles whose labels contain every requested exact pair."""
    return {
        name: profile
        for name, profile in profiles.items()
        if _matches_labels(profile, required_labels)
    }


def list_observation(
    profiles: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return a stable, non-secret summary for structured list output."""
    return [
        _profile_observation(name, profile, revision=None) for name, profile in profiles.items()
    ]


def describe_observation(
    name: str,
    revision: int,
    profile: Mapping[str, Any],
    *,
    secret_store: SecretStore | None = None,
) -> dict[str, Any]:
    """Return one stable profile observation without arbitrary properties."""
    observation = _profile_observation(name, profile, revision=revision)
    observation["kafka"]["auth"]["credentials"] = _credential_states(profile, secret_store)
    return observation


def dump_observation(value: Any, output_format: OutputFormat) -> str:
    """Serialize an observation without terminal styling."""
    if output_format == "json":
        return json.dumps(value, indent=2, ensure_ascii=False) + "\n"
    return yaml.safe_dump(value, sort_keys=False, allow_unicode=True)


def _matches_labels(profile: Mapping[str, Any], required: Mapping[str, str]) -> bool:
    labels = profile.get("labels", {})
    if not isinstance(labels, Mapping):
        return False
    return all(labels.get(key) == value for key, value in required.items())


def _profile_observation(
    name: str,
    profile: Mapping[str, Any],
    *,
    revision: int | None,
) -> dict[str, Any]:
    observation: dict[str, Any] = {"name": name}
    if revision is not None:
        observation["id"] = profile["id"]
        observation["revision"] = revision
    observation["description"] = profile.get("description")
    observation["labels"] = dict(sorted(_labels(profile).items()))
    observation["kafka"] = _kafka_observation(profile)
    observation["registry"] = _registry_observation(profile)
    return observation


def _labels(profile: Mapping[str, Any]) -> dict[str, str]:
    labels = profile.get("labels", {})
    if not isinstance(labels, Mapping):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


def _kafka_observation(profile: Mapping[str, Any]) -> dict[str, Any]:
    kafka = profile.get("kafka", {})
    if not isinstance(kafka, Mapping):
        return {}
    auth = kafka.get("auth", {})
    auth_type = auth.get("type") if isinstance(auth, Mapping) else None
    return {
        "bootstrapServers": list(kafka.get("bootstrapServers", ())),
        "transport": kafka.get("transport"),
        "tls": _tls_observation(kafka),
        "auth": {"type": auth_type},
    }


def _tls_observation(kafka: Mapping[str, Any]) -> dict[str, str] | None:
    if kafka.get("transport") != "tls":
        return None
    tls = kafka.get("tls")
    custom_ca = isinstance(tls, Mapping) and bool(tls.get("caCertificates"))
    return {"trust": "custom" if custom_ca else "system"}


def _registry_observation(profile: Mapping[str, Any]) -> dict[str, Any] | None:
    registry = profile.get("registry")
    if not isinstance(registry, Mapping):
        return None
    provider = registry.get("provider")
    url_key = "apicurio.registry.url" if provider == "apicurio" else "schema.registry.url"
    return {"provider": provider, "url": registry.get(url_key)}


def _credential_states(
    profile: Mapping[str, Any],
    store: SecretStore | None,
) -> dict[str, str]:
    kafka = profile.get("kafka")
    auth = kafka.get("auth") if isinstance(kafka, Mapping) else None
    if not isinstance(auth, Mapping):
        return {}
    references = {
        field: auth[property_name]
        for property_name, field in (
            ("passwordRef", "kafka/password"),
            ("privateKeyRef", "kafka/tls/private-key"),
            ("privateKeyPasswordRef", "kafka/tls/private-key-password"),
        )
        if isinstance(auth.get(property_name), str)
    }
    if not references:
        return {}
    try:
        selected_store = store or load_secret_store()
    except SecretStoreError:
        return {field: "unavailable" for field in references}
    states: dict[str, str] = {}
    for field, reference in references.items():
        try:
            selected_store.get(reference)
        except SecretNotFoundError:
            states[field] = "missing"
        except SecretStoreError:
            states[field] = "unavailable"
        else:
            states[field] = "stored"
    return states


__all__ = [
    "OutputFormat",
    "describe_observation",
    "dump_observation",
    "filter_profiles",
    "list_observation",
]
