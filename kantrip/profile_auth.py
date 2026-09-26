"""Plan Kafka and Registry authentication changes without performing I/O."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from kantrip.credential_mutations import SecretReplacement
from kantrip.kafka import (
    KafkaProfileError,
    validate_ca_bundle,
    validate_client_identity,
    validate_sasl_credential,
)
from kantrip.oauth import OAuthProfileError, validate_oauth_endpoint, validate_oauth_identity
from kantrip.profile_storage import (
    ProfileInputError,
    ProfileStoreError,
    connection_rule_violation,
)
from kantrip.secret_store import SecretStoreError, parse_secret_reference
from kantrip.secret_value import Secret


@dataclass(frozen=True)
class KafkaAuthInput:
    """Validated-entry input whose secret values never enter the profile document."""

    auth_type: str
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
    oauth_default_trust: bool = False


@dataclass(frozen=True)
class RegistryAuthInput:
    """Registry input whose secret values remain outside the profile document."""

    auth_type: str = "none"
    ca_certificates: str | None = None
    username: str | None = None
    password: Secret | None = None
    token: Secret | None = None
    client_certificate: str | None = None
    private_key: Secret | None = None
    private_key_password: Secret | None = None
    oauth_token_url: str | None = None
    oauth_client_id: str | None = None
    oauth_scopes: tuple[str, ...] | None = None
    oauth_client_secret: Secret | None = None
    oauth_ca_certificates: str | None = None
    oauth_logical_cluster: str | None = None
    oauth_identity_pool_id: str | None = None


@dataclass(frozen=True)
class AuthPlan:
    auth_type: str
    username: str | None
    client_certificate: str | None
    retained_references: Mapping[str, str]
    replacements: tuple[SecretReplacement, ...]
    retire_references: tuple[str, ...]
    oauth_token_url: str | None = None
    oauth_client_id: str | None = None
    oauth_scopes: tuple[str, ...] = ()
    oauth_ca_certificates: str | None = None


@dataclass(frozen=True)
class RegistryAuthPlan:
    """A Registry credential mutation composed with the Kafka mutation."""

    requested: RegistryAuthInput
    retained_references: Mapping[str, str]
    replacements: tuple[SecretReplacement, ...]
    retire_references: tuple[str, ...]


def apply_edit_authentication_plans(
    profile: dict[str, Any],
    kafka_plan: AuthPlan | None,
    registry_plan: RegistryAuthPlan | None,
    staged: Mapping[str, str],
) -> None:
    if kafka_plan is not None:
        profile["kafka"]["auth"] = _auth_document(kafka_plan, staged)
    if registry_plan is not None and isinstance(profile.get("registry"), dict):
        _apply_registry_authentication(profile, registry_plan, staged)


def plan_authentication(
    profile_id: str,
    current: Mapping[str, Any] | None,
    requested: KafkaAuthInput,
) -> AuthPlan:
    auth_type = requested.auth_type
    if auth_type not in {
        "none",
        "plain",
        "scram-sha-256",
        "scram-sha-512",
        "mtls",
        "oauth",
    }:
        raise ProfileInputError("Kafka authentication type is not supported")
    current_auth = current or {"type": "none"}
    current_type = current_auth.get("type")
    current_references = _auth_references(profile_id, current_auth)
    if auth_type == "none":
        if (
            any(
                value is not None
                for value in (
                    requested.username,
                    requested.password,
                    requested.client_certificate,
                    requested.private_key,
                    requested.private_key_password,
                    requested.oauth_token_url,
                    requested.oauth_client_id,
                    requested.oauth_client_secret,
                    requested.oauth_ca_certificates,
                )
            )
            or requested.oauth_scopes is not None
            or requested.oauth_default_trust
        ):
            raise ProfileInputError("Kafka auth none cannot include credentials")
        return AuthPlan("none", None, None, {}, (), tuple(current_references.values()))
    if auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return _password_auth_plan(
            current_auth,
            current_type,
            current_references,
            requested,
        )
    if auth_type == "mtls":
        return _mtls_auth_plan(
            current_auth,
            current_type,
            current_references,
            requested,
        )
    return _oauth_auth_plan(
        current_auth,
        current_type,
        current_references,
        requested,
    )


def _password_auth_plan(
    current_auth: Mapping[str, Any],
    current_type: object,
    current_references: Mapping[str, str],
    requested: KafkaAuthInput,
) -> AuthPlan:
    if (
        requested.client_certificate is not None
        or requested.private_key is not None
        or _has_kafka_oauth_input(requested)
    ):
        raise ProfileInputError("Kafka password authentication cannot include a client identity")
    username = requested.username
    if username is None and current_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        stored_username = current_auth.get("username")
        username = stored_username if isinstance(stored_username, str) else None
    if not username:
        raise ProfileInputError("Kafka password authentication requires --username")
    previous = current_references.get("kafka/password")
    replacements: tuple[SecretReplacement, ...] = ()
    retained: dict[str, str] = {}
    if requested.password is not None:
        try:
            validate_sasl_credential(requested.password)
        except KafkaProfileError as error:
            raise ProfileInputError(str(error)) from error
        replacements = (SecretReplacement("kafka/password", requested.password, previous),)
    elif previous is not None:
        retained["kafka/password"] = previous
    else:
        raise ProfileInputError("Kafka password authentication requires a password")
    retired = _retired_references(current_references, retained, replacements)
    return AuthPlan(
        requested.auth_type,
        username,
        None,
        retained,
        replacements,
        retired,
    )


def _mtls_auth_plan(
    current_auth: Mapping[str, Any],
    current_type: object,
    current_references: Mapping[str, str],
    requested: KafkaAuthInput,
) -> AuthPlan:
    if (
        requested.username is not None
        or requested.password is not None
        or _has_kafka_oauth_input(requested)
    ):
        raise ProfileInputError("Kafka mTLS authentication cannot include username or password")
    changing_identity = (
        requested.client_certificate is not None or requested.private_key is not None
    )
    if changing_identity and (
        requested.client_certificate is None or requested.private_key is None
    ):
        raise ProfileInputError("Kafka mTLS identity replacement requires certificate and key")
    if requested.private_key_password is not None and not changing_identity:
        raise ProfileInputError("Kafka private-key password replacement requires a new key")
    if changing_identity:
        assert requested.client_certificate is not None
        assert requested.private_key is not None
        certificate, private_key = validate_client_identity(
            requested.client_certificate,
            requested.private_key,
            password=requested.private_key_password,
        )
        previous_key = current_references.get("kafka/tls/private-key")
        replacements = [SecretReplacement("kafka/tls/private-key", private_key, previous_key)]
        if requested.private_key_password is not None:
            replacements.append(
                SecretReplacement(
                    "kafka/tls/private-key-password",
                    requested.private_key_password,
                    current_references.get("kafka/tls/private-key-password"),
                )
            )
        retained: dict[str, str] = {}
    elif current_type == "mtls":
        stored_certificate = current_auth.get("clientCertificate")
        if not isinstance(stored_certificate, str):
            raise ProfileStoreError("stored Kafka client certificate is invalid")
        certificate = stored_certificate
        replacements = []
        retained = dict(current_references)
    else:
        raise ProfileInputError("Kafka mTLS authentication requires certificate and key")
    retired = _retired_references(current_references, retained, tuple(replacements))
    return AuthPlan(
        "mtls",
        None,
        certificate,
        retained,
        tuple(replacements),
        retired,
    )


def _oauth_auth_plan(
    current_auth: Mapping[str, Any],
    current_type: object,
    current_references: Mapping[str, str],
    requested: KafkaAuthInput,
) -> AuthPlan:
    if any(
        value is not None
        for value in (
            requested.username,
            requested.password,
            requested.client_certificate,
            requested.private_key,
            requested.private_key_password,
        )
    ):
        raise ProfileInputError("Kafka OAuth authentication cannot include other credentials")
    token_url = requested.oauth_token_url
    client_id = requested.oauth_client_id
    scopes = requested.oauth_scopes
    ca_certificates = requested.oauth_ca_certificates
    if current_type == "oauth":
        token_url = token_url or _stored_text(current_auth, "tokenUrl")
        client_id = client_id or _stored_text(current_auth, "clientId")
        if scopes is None:
            stored_scopes = current_auth.get("scopes", [])
            scopes = tuple(stored_scopes) if isinstance(stored_scopes, list) else None
        if ca_certificates is None and not requested.oauth_default_trust:
            stored_ca = current_auth.get("caCertificates")
            ca_certificates = stored_ca if isinstance(stored_ca, str) else None
    if token_url is None or client_id is None:
        raise ProfileInputError("Kafka OAuth requires a token URL and client ID")
    try:
        validate_oauth_endpoint(token_url)
        validated_client_id, validated_scopes = validate_oauth_identity(
            client_id, list(scopes or ())
        )
    except OAuthProfileError as error:
        raise ProfileInputError(str(error).replace("OAuth", "Kafka OAuth", 1)) from error
    validated_ca = validated_ca_bundle(ca_certificates) if ca_certificates is not None else None
    previous = current_references.get("kafka/oauth/client-secret")
    retained: dict[str, str] = {}
    replacements: tuple[SecretReplacement, ...] = ()
    if requested.oauth_client_secret is not None:
        try:
            validate_sasl_credential(requested.oauth_client_secret)
        except KafkaProfileError as error:
            raise ProfileInputError(str(error)) from error
        replacements = (
            SecretReplacement("kafka/oauth/client-secret", requested.oauth_client_secret, previous),
        )
    elif previous is not None:
        retained["kafka/oauth/client-secret"] = previous
    else:
        raise ProfileInputError("Kafka OAuth authentication requires a client secret")
    retired = _retired_references(current_references, retained, replacements)
    return AuthPlan(
        "oauth",
        None,
        None,
        retained,
        replacements,
        retired,
        token_url,
        validated_client_id,
        validated_scopes,
        validated_ca,
    )


def _stored_text(document: Mapping[str, Any], field: str) -> str | None:
    value = document.get(field)
    return value if isinstance(value, str) else None


def _has_kafka_oauth_input(requested: KafkaAuthInput) -> bool:
    return (
        any(
            value is not None
            for value in (
                requested.oauth_token_url,
                requested.oauth_client_id,
                requested.oauth_scopes,
                requested.oauth_client_secret,
                requested.oauth_ca_certificates,
            )
        )
        or requested.oauth_default_trust
    )


def _auth_references(profile_id: str, auth: Mapping[str, Any]) -> dict[str, str]:
    references: dict[str, str] = {}
    for property_name, credential_field in (
        ("passwordRef", "kafka/password"),
        ("clientSecretRef", "kafka/oauth/client-secret"),
        ("privateKeyRef", "kafka/tls/private-key"),
        ("privateKeyPasswordRef", "kafka/tls/private-key-password"),
    ):
        value = auth.get(property_name)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ProfileStoreError("stored Kafka credential reference is invalid")
        try:
            parsed = parse_secret_reference(value)
        except SecretStoreError as error:
            raise ProfileStoreError("stored Kafka credential reference is invalid") from error
        if parsed.profile_id != profile_id or parsed.field != credential_field:
            raise ProfileStoreError("stored Kafka credential reference does not match its profile")
        references[credential_field] = value
    return references


def _retired_references(
    current: Mapping[str, str],
    retained: Mapping[str, str],
    replacements: tuple[SecretReplacement, ...],
) -> tuple[str, ...]:
    replaced = {
        replacement.previous_reference
        for replacement in replacements
        if replacement.previous_reference is not None
    }
    retained_values = set(retained.values())
    return tuple(
        reference
        for reference in current.values()
        if reference not in retained_values and reference not in replaced
    )


def _auth_document(plan: AuthPlan, staged: Mapping[str, str]) -> dict[str, Any]:
    references = dict(plan.retained_references) | dict(staged)
    if plan.auth_type == "none":
        return {"type": "none"}
    if plan.auth_type in {"plain", "scram-sha-256", "scram-sha-512"}:
        return {
            "type": plan.auth_type,
            "username": plan.username,
            "passwordRef": references["kafka/password"],
        }
    if plan.auth_type == "oauth":
        document = {
            "type": "oauth",
            "tokenUrl": plan.oauth_token_url,
            "clientId": plan.oauth_client_id,
            "scopes": list(plan.oauth_scopes),
            "clientSecretRef": references["kafka/oauth/client-secret"],
        }
        if plan.oauth_ca_certificates is not None:
            document["caCertificates"] = plan.oauth_ca_certificates
        return document
    document = {
        "type": "mtls",
        "clientCertificate": plan.client_certificate,
        "privateKeyRef": references["kafka/tls/private-key"],
    }
    password_reference = references.get("kafka/tls/private-key-password")
    if password_reference is not None:
        document["privateKeyPasswordRef"] = password_reference
    return document


def plan_registry_authentication(
    profile_id: str,
    current: Mapping[str, Any] | None,
    requested: RegistryAuthInput,
) -> RegistryAuthPlan:
    """Validate one Registry auth replacement without writing a secret."""
    if requested.auth_type not in {"none", "basic", "token", "mtls", "oauth"}:
        raise ProfileInputError("Registry authentication type is not supported")
    if requested.ca_certificates is not None:
        validated_ca_bundle(requested.ca_certificates)
    current_references = registry_auth_references(profile_id, current)
    if requested.auth_type == "none":
        _validate_registry_none_input(requested)
        return RegistryAuthPlan(requested, {}, (), tuple(current_references.values()))
    field = {
        "basic": "registry/password",
        "token": "registry/token",
        "mtls": "registry/tls/private-key",
        "oauth": "registry/oauth/client-secret",
    }[requested.auth_type]
    value = {
        "basic": requested.password,
        "token": requested.token,
        "mtls": requested.private_key,
        "oauth": requested.oauth_client_secret,
    }[requested.auth_type]
    previous = current_references.get(field)
    _validate_registry_auth_input(requested, credential_exists=previous is not None)
    if requested.auth_type == "basic" and (
        requested.username is None
        or any(character in requested.username for character in (":", "\x00", "\r", "\n"))
    ):
        raise ProfileInputError("Registry basic username is invalid")
    if value is not None and requested.auth_type in {"basic", "token", "oauth"}:
        try:
            validate_sasl_credential(value)
        except KafkaProfileError as error:
            raise ProfileInputError("Registry credential is invalid") from error
    retained, replacements = _registry_secret_replacements(
        requested, field, value, previous, current_references
    )
    retired = _retired_references(current_references, retained, replacements)
    return RegistryAuthPlan(requested, retained, replacements, retired)


def _validate_registry_none_input(requested: RegistryAuthInput) -> None:
    supplied = (
        requested.username,
        requested.password,
        requested.token,
        requested.client_certificate,
        requested.private_key,
        requested.private_key_password,
        requested.oauth_token_url,
        requested.oauth_client_id,
        requested.oauth_client_secret,
    )
    if any(value is not None for value in supplied) or requested.oauth_scopes:
        raise ProfileInputError("Registry auth none cannot include credentials")


def _registry_secret_replacements(
    requested: RegistryAuthInput,
    field: str,
    value: Secret | None,
    previous: str | None,
    current_references: Mapping[str, str],
) -> tuple[dict[str, str], tuple[SecretReplacement, ...]]:
    retained: dict[str, str] = {}
    replacements: tuple[SecretReplacement, ...] = ()
    if value is not None:
        replacements = (SecretReplacement(field, value, previous),)
    elif previous is not None:
        retained[field] = previous
    else:
        raise ProfileInputError(
            f"Registry {requested.auth_type} authentication requires a credential"
        )

    password_field = "registry/tls/private-key-password"
    if requested.auth_type == "mtls" and requested.private_key_password is not None:
        replacements += (
            SecretReplacement(
                password_field,
                requested.private_key_password,
                current_references.get(password_field),
            ),
        )
    elif (
        requested.auth_type == "mtls"
        and requested.private_key is None
        and password_field in current_references
    ):
        retained[password_field] = current_references[password_field]
    return retained, replacements


def new_registry_auth_plan(
    profile_id: str,
    registry_url: str | None,
    requested: RegistryAuthInput | None,
) -> RegistryAuthPlan | None:
    if requested is None:
        return None
    if registry_url is None:
        raise ProfileInputError("Registry authentication requires --registry-url")
    return plan_registry_authentication(profile_id, None, requested)


def _validate_registry_auth_input(requested: RegistryAuthInput, *, credential_exists: bool) -> None:
    if requested.auth_type == "basic":
        _validate_registry_basic_input(requested)
    elif requested.auth_type == "token":
        _validate_registry_token_input(requested)
    elif requested.auth_type == "mtls":
        _validate_registry_mtls_input(requested, credential_exists=credential_exists)
    elif requested.auth_type == "oauth":
        _validate_registry_oauth_input(requested)


def _validate_registry_basic_input(requested: RegistryAuthInput) -> None:
    incompatible = (
        requested.token,
        requested.client_certificate,
        requested.private_key,
        requested.private_key_password,
        requested.oauth_token_url,
        requested.oauth_client_id,
        requested.oauth_client_secret,
        requested.oauth_ca_certificates,
        requested.oauth_logical_cluster,
        requested.oauth_identity_pool_id,
    )
    if (
        not requested.username
        or any(value is not None for value in incompatible)
        or requested.oauth_scopes
    ):
        raise ProfileInputError("Registry basic authentication requires --registry-username")


def _validate_registry_token_input(requested: RegistryAuthInput) -> None:
    incompatible = (
        requested.username,
        requested.password,
        requested.client_certificate,
        requested.private_key,
        requested.private_key_password,
        requested.oauth_token_url,
        requested.oauth_client_id,
        requested.oauth_client_secret,
        requested.oauth_ca_certificates,
        requested.oauth_logical_cluster,
        requested.oauth_identity_pool_id,
    )
    if any(value is not None for value in incompatible) or requested.oauth_scopes:
        raise ProfileInputError("Registry token authentication cannot include basic credentials")


def _validate_registry_mtls_input(requested: RegistryAuthInput, *, credential_exists: bool) -> None:
    incompatible = (
        requested.username,
        requested.password,
        requested.token,
        requested.oauth_token_url,
        requested.oauth_client_id,
        requested.oauth_client_secret,
        requested.oauth_ca_certificates,
        requested.oauth_logical_cluster,
        requested.oauth_identity_pool_id,
    )
    if any(value is not None for value in incompatible) or requested.oauth_scopes:
        raise ProfileInputError("Registry mTLS cannot include another authentication mode")
    if requested.client_certificate is None or (
        requested.private_key is None and not credential_exists
    ):
        raise ProfileInputError("Registry mTLS requires certificate and private key")
    if requested.private_key is None:
        return
    try:
        validate_client_identity(
            requested.client_certificate,
            requested.private_key,
            password=requested.private_key_password,
        )
    except KafkaProfileError as error:
        raise ProfileInputError(str(error).replace("Kafka", "Registry")) from error


def _validate_registry_oauth_input(requested: RegistryAuthInput) -> None:
    incompatible = (
        requested.username,
        requested.password,
        requested.token,
        requested.client_certificate,
        requested.private_key,
        requested.private_key_password,
    )
    if any(value is not None for value in incompatible):
        raise ProfileInputError("Registry OAuth cannot include another authentication mode")
    if not requested.oauth_token_url or not requested.oauth_client_id:
        raise ProfileInputError("Registry OAuth requires a token URL and client ID")
    scopes = requested.oauth_scopes or ()
    try:
        validate_oauth_endpoint(requested.oauth_token_url)
        validate_oauth_identity(requested.oauth_client_id, list(scopes))
    except OAuthProfileError as error:
        raise ProfileInputError(str(error).replace("OAuth", "Registry OAuth", 1)) from error
    if requested.oauth_ca_certificates is not None:
        validated_ca_bundle(requested.oauth_ca_certificates)


def _registry_auth_document(
    plan: RegistryAuthPlan,
    staged: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    references = dict(plan.retained_references) | dict(staged)
    requested = plan.requested
    auth: dict[str, Any] = {"type": requested.auth_type}
    tls: dict[str, Any] | None = (
        {"caCertificates": validated_ca_bundle(requested.ca_certificates)}
        if requested.ca_certificates is not None
        else None
    )
    if requested.auth_type == "basic":
        auth.update(username=requested.username, passwordRef=references["registry/password"])
    elif requested.auth_type == "token":
        auth["tokenRef"] = references["registry/token"]
    elif requested.auth_type == "mtls":
        assert requested.client_certificate is not None
        certificate = requested.client_certificate
        if requested.private_key is not None:
            certificate, _ = validate_client_identity(
                requested.client_certificate,
                requested.private_key,
                password=requested.private_key_password,
            )
        tls = dict(tls or {}) | {"clientCertificate": certificate}
        auth["privateKeyRef"] = references["registry/tls/private-key"]
        password_reference = references.get("registry/tls/private-key-password")
        if password_reference is not None:
            auth["privateKeyPasswordRef"] = password_reference
    elif requested.auth_type == "oauth":
        auth.update(
            tokenUrl=requested.oauth_token_url,
            clientId=requested.oauth_client_id,
            scopes=list(requested.oauth_scopes or ()),
            clientSecretRef=references["registry/oauth/client-secret"],
        )
        if requested.oauth_ca_certificates is not None:
            auth["caCertificates"] = validated_ca_bundle(requested.oauth_ca_certificates)
        if requested.oauth_logical_cluster is not None:
            auth["logicalCluster"] = requested.oauth_logical_cluster
        if requested.oauth_identity_pool_id is not None:
            auth["identityPoolId"] = requested.oauth_identity_pool_id
    return auth, tls


def registry_auth_references(
    profile_id: str,
    registry: Mapping[str, Any] | None,
) -> dict[str, str]:
    if registry is None:
        return {}
    auth = registry.get("auth")
    if not isinstance(auth, Mapping):
        return {}
    fields = (
        ("passwordRef", "registry/password"),
        ("tokenRef", "registry/token"),
        ("privateKeyRef", "registry/tls/private-key"),
        ("privateKeyPasswordRef", "registry/tls/private-key-password"),
        ("clientSecretRef", "registry/oauth/client-secret"),
    )
    references: dict[str, str] = {}
    for property_name, field_name in fields:
        value = auth.get(property_name)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ProfileStoreError("stored Registry credential reference is invalid")
        try:
            parsed = parse_secret_reference(value)
        except SecretStoreError as error:
            raise ProfileStoreError("stored Registry credential reference is invalid") from error
        if parsed.profile_id != profile_id or parsed.field != field_name:
            raise ProfileStoreError(
                "stored Registry credential reference does not match its profile"
            )
        references[field_name] = value
    return references


def _apply_registry_authentication(
    profile: dict[str, Any],
    plan: RegistryAuthPlan,
    staged: Mapping[str, str],
) -> None:
    registry = profile.get("registry")
    if not isinstance(registry, dict):
        raise ProfileInputError("Registry authentication requires a Registry connection")
    auth, tls = _registry_auth_document(plan, staged)
    registry["auth"] = auth
    if tls is None:
        registry.pop("tls", None)
    else:
        registry["tls"] = tls


def apply_new_authentication_plans(
    profile: dict[str, Any],
    kafka_plan: AuthPlan,
    registry_plan: RegistryAuthPlan | None,
    staged: Mapping[str, str],
) -> None:
    profile["kafka"]["auth"] = _auth_document(kafka_plan, staged)
    if registry_plan is not None:
        _apply_registry_authentication(profile, registry_plan, staged)


def profile_secret_references(profile: Mapping[str, Any]) -> tuple[str, ...]:
    kafka = profile.get("kafka")
    if not isinstance(kafka, Mapping):
        raise ProfileStoreError("stored Kafka profile is invalid")
    auth = kafka.get("auth")
    if not isinstance(auth, Mapping):
        raise ProfileStoreError("stored Kafka authentication is invalid")
    profile_id = str(profile.get("id"))
    registry = profile.get("registry")
    registry_references = (
        registry_auth_references(profile_id, registry) if isinstance(registry, Mapping) else {}
    )
    return tuple(_auth_references(profile_id, auth).values()) + tuple(registry_references.values())


def validate_auth_transport(auth_type: str, transport: str) -> None:
    violation = connection_rule_violation(transport=transport, kafka_auth=auth_type)
    if violation is not None:
        raise ProfileInputError(violation)


def validated_ca_bundle(contents: str) -> str:
    try:
        return validate_ca_bundle(contents)
    except KafkaProfileError as error:
        raise ProfileInputError(str(error)) from error


__all__ = [
    "AuthPlan",
    "KafkaAuthInput",
    "RegistryAuthInput",
    "RegistryAuthPlan",
    "apply_edit_authentication_plans",
    "apply_new_authentication_plans",
    "new_registry_auth_plan",
    "plan_authentication",
    "plan_registry_authentication",
    "profile_secret_references",
    "registry_auth_references",
    "validate_auth_transport",
    "validated_ca_bundle",
]
