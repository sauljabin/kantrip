"""Build new profile documents and apply requested edits without performing I/O."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from kantrip import profile_storage as storage
from kantrip.profile_auth import (
    KafkaAuthInput,
    RegistryAuthInput,
    RegistryAuthPlan,
    plan_registry_authentication,
    registry_auth_references,
    validated_ca_bundle,
)
from kantrip.profile_storage import ProfileStoreError
from kantrip.registry import CONFLUENT_PROVIDER, RegistryProfileError, registry_connection

DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"


def requested_registry_edit_plan(
    profile_id: str,
    current: Mapping[str, Any],
    updated: Mapping[str, Any],
    registry_auth: RegistryAuthInput | None,
) -> RegistryAuthPlan | None:
    current_registry = current.get("registry")
    stored_registry = current_registry if isinstance(current_registry, Mapping) else None
    if registry_auth is not None:
        return plan_registry_authentication(profile_id, stored_registry, registry_auth)
    _validate_retained_registry_auth(current, updated)
    current_references = registry_auth_references(profile_id, stored_registry)
    if not current_references:
        return None
    updated_registry = updated.get("registry")
    updated_references = registry_auth_references(
        profile_id, updated_registry if isinstance(updated_registry, Mapping) else None
    )
    if set(current_references.values()) == set(updated_references.values()):
        return None
    return RegistryAuthPlan(
        RegistryAuthInput("none"),
        {},
        (),
        tuple(current_references.values()),
    )


def _validate_retained_registry_auth(
    current: Mapping[str, Any],
    updated: Mapping[str, Any],
) -> None:
    """Reject a provider change whose retained authentication the new provider lacks."""
    before, after = current.get("registry"), updated.get("registry")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return
    if before.get("provider") == after.get("provider"):
        return
    try:
        storage.validate_profile(dict(updated))
        registry_connection(updated)
    except (ProfileStoreError, RegistryProfileError) as error:
        auth = after.get("auth")
        auth_type = auth.get("type") if isinstance(auth, Mapping) else "none"
        raise ProfileStoreError(
            f"Registry provider '{after.get('provider')}' does not support the current "
            f"'{auth_type}' authentication; pass --registry-auth to choose a supported "
            "method or none"
        ) from error


def validate_edit_request(
    *,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: Mapping[str, str] | None,
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
    auth: KafkaAuthInput | None,
    registry_provider: str | None,
    registry_url: str | None,
    registry_auth: RegistryAuthInput | None,
    remove_registry: bool,
) -> None:
    has_change = any(
        (
            bootstrap_servers is not None,
            description is not None,
            clear_description,
            bool(labels),
            bool(remove_labels),
            transport is not None,
            ca_certificates is not None,
            default_trust,
            auth is not None,
            registry_provider is not None,
            registry_url is not None,
            registry_auth is not None,
            remove_registry,
        )
    )
    if not has_change:
        raise ProfileStoreError("no profile changes were requested")
    if description is not None and clear_description:
        raise ProfileStoreError("--description cannot be combined with --clear-description")
    if remove_registry and (
        registry_provider is not None or registry_url is not None or registry_auth is not None
    ):
        raise ProfileStoreError("--remove-registry cannot be combined with Registry update options")
    if labels and set(labels).intersection(remove_labels):
        raise ProfileStoreError("a label cannot be set and removed in the same edit")
    if ca_certificates is not None and transport == "plaintext":
        raise ProfileStoreError("--ca-file cannot be combined with --transport plaintext")
    if ca_certificates is not None and default_trust:
        raise ProfileStoreError("--ca-file cannot be combined with --default-trust")
    if default_trust and transport == "plaintext":
        raise ProfileStoreError("--default-trust cannot be combined with --transport plaintext")


def apply_profile_edits(
    current: Mapping[str, Any],
    *,
    bootstrap_servers: tuple[str, ...] | None,
    description: str | None,
    clear_description: bool,
    labels: Mapping[str, str],
    remove_labels: tuple[str, ...],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
    registry_provider: str | None,
    registry_url: str | None,
    remove_registry: bool,
) -> dict[str, Any]:
    updated = deepcopy(dict(current))
    if bootstrap_servers is not None:
        updated["kafka"]["bootstrapServers"] = list(bootstrap_servers)
    if clear_description:
        updated.pop("description", None)
    elif description is not None:
        updated["description"] = description
    _apply_label_edits(updated, labels, remove_labels)
    _apply_kafka_transport_edits(updated, transport, ca_certificates, default_trust)
    _apply_registry_edits(updated, registry_provider, registry_url, remove_registry)
    return updated


def _apply_kafka_transport_edits(
    profile: dict[str, Any],
    transport: str | None,
    ca_certificates: str | None,
    default_trust: bool,
) -> None:
    kafka = profile["kafka"]
    if transport is not None:
        kafka["transport"] = transport
        if transport == "plaintext":
            kafka.pop("tls", None)
        elif "tls" not in kafka:
            kafka["tls"] = {}
    if ca_certificates is None:
        if not default_trust:
            return
        if kafka["transport"] != "tls":
            raise ProfileStoreError("--default-trust requires Kafka TLS transport")
        kafka.pop("tls", None)
        return
    if kafka["transport"] != "tls":
        raise ProfileStoreError("--ca-file requires Kafka TLS transport")
    kafka.setdefault("tls", {})["caCertificates"] = validated_ca_bundle(ca_certificates)


def _apply_label_edits(
    profile: dict[str, Any],
    labels: Mapping[str, str],
    remove_labels: tuple[str, ...],
) -> None:
    current_labels = dict(profile.get("labels", {}))
    current_labels.update(labels)
    for name in remove_labels:
        current_labels.pop(name, None)
    if current_labels:
        profile["labels"] = current_labels
    else:
        profile.pop("labels", None)


def _apply_registry_edits(
    profile: dict[str, Any],
    provider: str | None,
    url: str | None,
    remove: bool,
) -> None:
    if remove:
        profile.pop("registry", None)
        return
    if provider is None and url is None:
        return
    existing = profile.get("registry")
    if not isinstance(existing, Mapping) and url is None:
        raise ProfileStoreError("--registry-provider requires --registry-url for a new Registry")
    if (
        isinstance(existing, Mapping)
        and provider is not None
        and provider != existing.get("provider")
        and url is None
    ):
        raise ProfileStoreError("changing Registry provider requires --registry-url")
    selected_provider = provider or (
        str(existing["provider"]) if isinstance(existing, Mapping) else CONFLUENT_PROVIDER
    )
    if url is None:
        assert isinstance(existing, Mapping)
        old_property = (
            "apicurio.registry.url"
            if existing.get("provider") == "apicurio"
            else "schema.registry.url"
        )
        selected_url = existing.get(old_property)
        if not isinstance(selected_url, str):
            raise ProfileStoreError("stored Registry URL is invalid")
    else:
        selected_url = url
    property_name = (
        "apicurio.registry.url" if selected_provider == "apicurio" else "schema.registry.url"
    )
    replacement: dict[str, Any] = {
        "provider": selected_provider,
        property_name: selected_url,
    }
    if isinstance(existing, Mapping):
        # Keep trust and authentication across a provider change; credentials are
        # never dropped implicitly. Unsupported combinations are rejected later.
        for field in ("tls", "auth"):
            if field in existing:
                replacement[field] = deepcopy(existing[field])
    else:
        replacement["auth"] = {"type": "none"}
    profile["registry"] = replacement


def new_profile(
    profile_id: str,
    bootstrap_servers: tuple[str, ...],
    *,
    description: str | None,
    labels: Mapping[str, str],
    transport: str,
    ca_certificates: str | None,
    auth: Mapping[str, Any],
    registry_provider: str | None,
    registry_url: str | None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": profile_id,
        "kafka": {
            "bootstrapServers": list(bootstrap_servers),
            "transport": transport,
            "auth": dict(auth),
        },
    }
    if ca_certificates is not None:
        if transport != "tls":
            raise ProfileStoreError("--ca-file requires --transport tls")
        profile["kafka"]["tls"] = {"caCertificates": validated_ca_bundle(ca_certificates)}
    if description is not None:
        profile["description"] = description
    if labels:
        profile["labels"] = dict(labels)
    if registry_provider is not None and registry_url is None:
        raise ProfileStoreError("--registry-provider requires --registry-url")
    if registry_url is not None:
        provider = registry_provider or CONFLUENT_PROVIDER
        property_name = "apicurio.registry.url" if provider == "apicurio" else "schema.registry.url"
        profile["registry"] = {
            "provider": provider,
            property_name: registry_url,
            "auth": {"type": "none"},
        }
        try:
            registry_connection(profile)
        except RegistryProfileError as error:
            raise ProfileStoreError(str(error)) from error
    return profile


__all__ = [
    "DEFAULT_BOOTSTRAP_SERVER",
    "apply_profile_edits",
    "new_profile",
    "requested_registry_edit_plan",
    "validate_edit_request",
]
