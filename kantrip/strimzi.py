"""Parse one Kubernetes Secret that the Strimzi user operator generated for a KafkaUser.

Only the SCRAM-SHA-512 and TLS shapes are accepted. Errors name keys and rules,
never values, because every value in a Secret may be a credential.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping
from typing import Any

import yaml

from kantrip.kafka import KafkaProfileError, validate_client_identity, validate_sasl_credential
from kantrip.profile_imports import ImportedConnection, ProfileImportError
from kantrip.secret_value import Secret

LABEL = "Strimzi Secret"
_KIND_LABEL = "strimzi.io/kind"
_USER_LABEL = "app.kubernetes.io/instance"
_SCRAM_KEYS = frozenset({"password"})
_TLS_KEYS = frozenset({"user.crt", "user.key"})
# Generated alongside the credentials Kantrip reads. `ca.crt` is the clients CA,
# which signs user certificates and never establishes trust in a listener.
_IGNORED_KEYS = frozenset({"sasl.jaas.config", "user.p12", "user.password", "ca.crt"})
_DATA_KEY = re.compile(r"[-._a-zA-Z0-9]{1,253}")
_MAX_DEPTH = 16


def parse_strimzi_secret(text: str) -> ImportedConnection:
    """Return the Kafka authentication a generated KafkaUser Secret describes."""
    document = _load_document(text)
    username, data = _secret_parts(document)
    keys = set(data)
    unknown = sorted(keys - _SCRAM_KEYS - _TLS_KEYS - _IGNORED_KEYS)
    if unknown:
        name = unknown[0] if _DATA_KEY.fullmatch(unknown[0]) else "with unsupported characters"
        raise ProfileImportError(f"{LABEL} has an unsupported data key {name}")
    scram = bool(keys & _SCRAM_KEYS)
    tls = bool(keys & _TLS_KEYS)
    if scram and tls:
        raise ProfileImportError(f"{LABEL} mixes SCRAM and TLS credentials")
    if scram:
        return _scram_connection(username, data)
    if tls:
        return _tls_connection(data)
    raise ProfileImportError(f"{LABEL} has neither a SCRAM password nor a TLS certificate and key")


def _scram_connection(username: str, data: Mapping[str, str]) -> ImportedConnection:
    password = Secret(_decoded(data, "password"))
    try:
        validate_sasl_credential(password)
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} password is empty or contains controls") from error
    return ImportedConnection(
        transport="tls", auth_type="scram-sha-512", username=username, password=password
    )


def _tls_connection(data: Mapping[str, str]) -> ImportedConnection:
    if not _TLS_KEYS <= set(data):
        raise ProfileImportError(f"{LABEL} TLS credentials need both user.crt and user.key")
    try:
        certificate, key = validate_client_identity(
            _decoded(data, "user.crt"), Secret(_decoded(data, "user.key"))
        )
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} {error}") from error
    return ImportedConnection(
        transport="tls", auth_type="mtls", client_certificate=certificate, private_key=key
    )


def _decoded(data: Mapping[str, str], key: str) -> str:
    try:
        return base64.b64decode(data[key], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as error:
        raise ProfileImportError(f"{LABEL} data key {key} is not base64-encoded UTF-8") from error


def _secret_parts(document: object) -> tuple[str, dict[str, str]]:
    """Validate the generated Secret shape and return its username and data."""
    if not isinstance(document, dict):
        raise ProfileImportError(f"{LABEL} must be one Kubernetes Secret object")
    if document.get("apiVersion") != "v1" or document.get("kind") != "Secret":
        raise ProfileImportError(
            f"{LABEL} must have apiVersion v1 and kind Secret; export the generated Secret, "
            "not the KafkaUser"
        )
    if "stringData" in document:
        raise ProfileImportError(f"{LABEL} must not use stringData")
    if document.get("type", "Opaque") != "Opaque":
        raise ProfileImportError(f"{LABEL} must have type Opaque")
    return _username(document.get("metadata")), _data(document.get("data"))


def _username(metadata: object) -> str:
    name = metadata.get("name") if isinstance(metadata, dict) else None
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    if not isinstance(name, str) or not isinstance(labels, dict):
        raise ProfileImportError(f"{LABEL} needs metadata.name and metadata.labels")
    if labels.get(_KIND_LABEL) != "KafkaUser":
        raise ProfileImportError(
            f"{LABEL} was not generated for a KafkaUser; it has no {_KIND_LABEL}: KafkaUser label"
        )
    # The user operator may prefix Secret names; the instance label is the user itself.
    username = labels.get(_USER_LABEL, name)
    if not isinstance(username, str) or not username or not name.endswith(username):
        raise ProfileImportError(f"{LABEL} name and {_USER_LABEL} label disagree")
    return username


def _data(data: object) -> dict[str, str]:
    if not isinstance(data, dict) or not data:
        raise ProfileImportError(f"{LABEL} needs a data section")
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in data.items()):
        raise ProfileImportError(f"{LABEL} data keys and values must be strings")
    return data


def _load_document(text: str) -> object:
    """Load exactly one JSON or YAML document without echoing parser context."""
    if text.lstrip().startswith("{"):
        return _load_json(text)
    _check_yaml_events(text)
    try:
        return yaml.load(text, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise ProfileImportError(f"{LABEL} is not valid JSON or YAML") from error


def _load_json(text: str) -> object:
    try:
        return json.loads(text, object_pairs_hook=_unique_pairs)
    except ProfileImportError:
        raise
    except (ValueError, RecursionError) as error:
        raise ProfileImportError(f"{LABEL} is not valid JSON or YAML") from error


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProfileImportError(f"{LABEL} contains a duplicate key")
        result[key] = value
    return result


def _check_yaml_events(text: str) -> None:
    """Reject several documents, anchors and aliases, and deep nesting before loading."""
    documents = depth = 0
    try:
        for event in yaml.parse(text, Loader=yaml.SafeLoader):
            if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None):
                raise ProfileImportError(f"{LABEL} must not use YAML anchors or aliases")
            documents += isinstance(event, yaml.DocumentStartEvent)
            depth += isinstance(event, (yaml.MappingStartEvent, yaml.SequenceStartEvent))
            depth -= isinstance(event, (yaml.MappingEndEvent, yaml.SequenceEndEvent))
            if documents > 1 or depth > _MAX_DEPTH:
                raise ProfileImportError(f"{LABEL} must contain exactly one shallow document")
    except yaml.YAMLError as error:
        raise ProfileImportError(f"{LABEL} is not valid JSON or YAML") from error


class _UniqueKeyLoader(yaml.SafeLoader):
    """A safe loader that rejects duplicate mapping keys instead of keeping the last."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        seen: set[object] = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in seen:
                raise ProfileImportError(f"{LABEL} contains a duplicate or non-text key")
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


__all__ = ["LABEL", "parse_strimzi_secret"]
