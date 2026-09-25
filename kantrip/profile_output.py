"""Build safe profile observations for human and structured output.

Observations never read the OS credential store: every secret the profile
references is reported as `configured`, and `doctor` is the only command that
checks whether a value is actually stored.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

import yaml
from cryptography import x509

OutputFormat = Literal["json", "yaml"]

# Stored reference property → public field name, shared with `--replace-secret`.
_SECRET_FIELDS = (
    ("passwordRef", "password"),
    ("tokenRef", "token"),
    ("privateKeyRef", "private-key"),
    ("privateKeyPasswordRef", "private-key-password"),
    ("clientSecretRef", "oauth.client-secret"),
)
_OAUTH_OPTIONAL_FIELDS = ("logicalCluster", "identityPoolId")


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
) -> dict[str, Any]:
    """Return every public profile field without secrets, references, or properties."""
    observation = _profile_observation(name, profile, revision=revision)
    kafka = _mapping(profile.get("kafka"))
    observation["kafka"]["auth"] = _auth_observation(
        "kafka",
        _mapping(kafka.get("auth")),
        client_certificate=_mapping(kafka.get("auth")).get("clientCertificate"),
    )
    registry = profile.get("registry")
    if isinstance(registry, Mapping):
        observation["registry"]["auth"] = _auth_observation(
            "registry",
            _mapping(registry.get("auth")),
            client_certificate=_mapping(registry.get("tls")).get("clientCertificate"),
        )
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


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _kafka_observation(profile: Mapping[str, Any]) -> dict[str, Any]:
    kafka = profile.get("kafka", {})
    if not isinstance(kafka, Mapping):
        return {}
    tls = None
    if kafka.get("transport") == "tls":
        tls = _trust_observation(_mapping(kafka.get("tls")))
    return {
        "bootstrapServers": list(kafka.get("bootstrapServers", ())),
        "transport": kafka.get("transport"),
        "tls": tls,
        "auth": {"type": _mapping(kafka.get("auth")).get("type")},
    }


def _registry_observation(profile: Mapping[str, Any]) -> dict[str, Any] | None:
    registry = profile.get("registry")
    if not isinstance(registry, Mapping):
        return None
    provider = registry.get("provider")
    url_key = "apicurio.registry.url" if provider == "apicurio" else "schema.registry.url"
    url = registry.get(url_key)
    tls = None
    if isinstance(url, str) and url.lower().startswith("https://"):
        tls = _trust_observation(_mapping(registry.get("tls")))
    return {
        "provider": provider,
        "url": url,
        "tls": tls,
        "auth": {"type": _mapping(registry.get("auth")).get("type", "none")},
    }


def _trust_observation(tls: Mapping[str, Any]) -> dict[str, str]:
    return {"trust": "custom" if tls.get("caCertificates") else "system"}


def _auth_observation(
    scope: str,
    auth: Mapping[str, Any],
    *,
    client_certificate: object,
) -> dict[str, Any]:
    auth_type = auth.get("type", "none")
    observation: dict[str, Any] = {"type": auth_type}
    if isinstance(auth.get("username"), str):
        observation["username"] = auth["username"]
    if auth_type == "mtls" and isinstance(client_certificate, str):
        observation["clientCertificate"] = _certificate_observation(client_certificate)
    if auth_type == "oauth":
        observation["oauth"] = _oauth_observation(auth)
    observation["credentials"] = {
        f"{scope}.auth.{field}": "configured"
        for property_name, field in _SECRET_FIELDS
        if isinstance(auth.get(property_name), str)
    }
    return observation


def _oauth_observation(auth: Mapping[str, Any]) -> dict[str, Any]:
    observation: dict[str, Any] = {
        "tokenUrl": auth.get("tokenUrl"),
        "clientId": auth.get("clientId"),
        "scopes": list(auth.get("scopes", ())),
        **_trust_observation(auth),
    }
    for field in _OAUTH_OPTIONAL_FIELDS:
        if isinstance(auth.get(field), str):
            observation[field] = auth[field]
    return observation


def _certificate_observation(pem: str) -> dict[str, str | None]:
    try:
        certificate = x509.load_pem_x509_certificates(pem.encode("utf-8"))[0]
    except (ValueError, IndexError):
        # Stored profiles are validated on write; `doctor` reports a damaged one.
        return {"subject": None, "expires": None}
    return {
        "subject": certificate.subject.rfc4514_string(),
        "expires": certificate.not_valid_after_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


__all__ = [
    "OutputFormat",
    "describe_observation",
    "dump_observation",
    "filter_profiles",
    "list_observation",
]
