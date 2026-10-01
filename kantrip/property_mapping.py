"""Take recognized client-property keys one at a time, with value-free errors.

The Kafka and Registry mappings of `--from-properties` share this reader: it
tracks which keys a mapping used, so leftovers fail instead of being dropped,
and it reads PEM material and secrets through Kantrip's bounded validators.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TypeVar

from kantrip.kafka import (
    KafkaProfileError,
    read_ca_bundle,
    validate_ca_bundle,
    validate_client_identity,
    validate_sasl_credential,
)
from kantrip.oauth import OAuthProfileError, validate_oauth_endpoint
from kantrip.profile_imports import ProfileImportError
from kantrip.properties_syntax import reportable_key
from kantrip.secret_value import Secret

LABEL = "Client properties"

Parsed = TypeVar("Parsed")

_PEM_BLOCK = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]+)-----(?P<body>[A-Za-z0-9+/=\s]*?)-----END (?P=label)-----"
)


class PropertyReader:
    """Keys a mapping takes one by one; any key left untaken fails.

    ``names`` maps a key to the spelling the file used, for messages.
    """

    def __init__(
        self,
        values: Mapping[str, str],
        base_directory: Path | None,
        *,
        owner: str = "",
        names: Mapping[str, str] | None = None,
    ) -> None:
        self._values = dict(values)
        self._taken: set[str] = set()
        self._base = base_directory
        self._owner = owner
        self._names = dict(names or {})

    def name(self, key: str) -> str:
        return self._names.get(key, f"{self._owner}{key}")

    def has(self, key: str) -> bool:
        return key in self._values

    def take(self, key: str) -> str | None:
        self._taken.add(key)
        return self._values.get(key)

    def require(self, key: str, purpose: str) -> str:
        value = self.take(key)
        if not value:
            raise ProfileImportError(f"{LABEL} need {self.name(key)} for {purpose}")
        return value

    def child(self, values: Mapping[str, str], owner: str) -> PropertyReader:
        return PropertyReader(values, self._base, owner=owner)

    def reject_unused(self, context: str) -> None:
        unused = sorted(set(self._values) - self._taken)
        if unused:
            key = unused[0]
            shown = self.name(key) if reportable_key(key) else "a key with unsupported characters"
            raise ProfileImportError(f"{LABEL} set {shown}, which {context} does not use")

    def path(self, key: str, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        if self._base is None:
            raise ProfileImportError(
                f"{LABEL} read from stdin need an absolute path in {self.name(key)}"
            )
        return self._base / path

    def pem(
        self,
        inline_key: str,
        location_key: str,
        *,
        validate: Callable[[str], Parsed],
        read: Callable[[Path], Parsed],
    ) -> Parsed | None:
        """Read PEM given inline or as a file, never both."""
        inline = self.take(inline_key)
        location = self.take(location_key)
        if inline is not None and location is not None:
            raise ProfileImportError(
                f"{LABEL} set both {self.name(inline_key)} and {self.name(location_key)}"
            )
        key = inline_key if inline is not None else location_key
        try:
            if inline is not None:
                return validate(normalized_pem(inline))
            if location is not None:
                return read(self.path(location_key, location))
        except KafkaProfileError as error:
            raise ProfileImportError(f"{LABEL} {self.name(key)} is invalid: {error}") from error
        return None

    def secret(self, key: str, purpose: str) -> Secret:
        value = Secret(self.require(key, purpose))
        try:
            validate_sasl_credential(value)
        except KafkaProfileError as error:
            raise ProfileImportError(
                f"{LABEL} {self.name(key)} is empty or contains controls"
            ) from error
        return value

    def https_url(self, key: str, purpose: str) -> str:
        """Require a credential-free HTTPS endpoint, such as an OAuth token URL."""
        url = self.require(key, purpose)
        try:
            return validate_oauth_endpoint(url)
        except OAuthProfileError as error:
            raise ProfileImportError(
                f"{LABEL} {self.name(key)} must use https:// without credentials, a query, "
                "or a fragment"
            ) from error


def normalized_pem(value: str) -> str:
    """Rewrap PEM blocks that a properties file flattened onto one line."""
    blocks = []
    for match in _PEM_BLOCK.finditer(value):
        body = "".join(match.group("body").split())
        lines = [body[index : index + 64] for index in range(0, len(body), 64)]
        label = match.group("label")
        blocks.append("\n".join((f"-----BEGIN {label}-----", *lines, f"-----END {label}-----")))
    if not blocks or _PEM_BLOCK.sub("", value).strip():
        return value
    return "\n".join(blocks) + "\n"


def check_hostname_verification(reader: PropertyReader) -> None:
    """Reject disabled hostname verification in Java's SSL settings."""
    key = "ssl.endpoint.identification.algorithm"
    algorithm = reader.take(key)
    if algorithm is not None and algorithm.lower() != "https":
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} must be https; "
            "Kantrip does not import disabled hostname verification"
        )


def split_scopes(value: str | None, key: str) -> tuple[str, ...]:
    scopes = tuple(value.split()) if value else ()
    if len(set(scopes)) != len(scopes):
        raise ProfileImportError(f"{LABEL} {key} repeats a scope")
    return scopes


def require_pem_store(
    reader: PropertyReader, key: str, store_type: str | None, *, required: bool
) -> None:
    """Require a PEM store type whenever store material is present."""
    if store_type is None and not required:
        return
    if store_type != "PEM":
        raise ProfileImportError(
            f"{LABEL} {reader.name(key)} must be PEM; JKS and PKCS12 stores are not supported"
        )


def java_pem_trust(reader: PropertyReader) -> str | None:
    """Read Java's PEM ``ssl.truststore.*`` trust, given inline or as a file."""
    store_type = reader.take("ssl.truststore.type")
    present = reader.has("ssl.truststore.certificates") or reader.has("ssl.truststore.location")
    require_pem_store(reader, "ssl.truststore.type", store_type, required=present)
    return reader.pem(
        "ssl.truststore.certificates",
        "ssl.truststore.location",
        validate=validate_ca_bundle,
        read=read_ca_bundle,
    )


def java_pem_identity(
    reader: PropertyReader,
) -> tuple[str | None, Secret | None, str | None]:
    """Read Java's inline PEM ``ssl.keystore.*`` identity and its key password."""
    store_type = reader.take("ssl.keystore.type")
    chain = reader.take("ssl.keystore.certificate.chain")
    key = reader.take("ssl.keystore.key")
    present = chain is not None or key is not None
    require_pem_store(reader, "ssl.keystore.type", store_type, required=present)
    return (
        normalized_pem(chain) if chain is not None else None,
        Secret(normalized_pem(key)) if key is not None else None,
        reader.take("ssl.key.password") if present else None,
    )


def client_identity(
    certificate: str | None, key: Secret | None, password: str | None
) -> tuple[str, Secret, Secret | None] | None:
    """Validate a client certificate, its key, and the key's password together."""
    if certificate is None and key is None:
        return None
    if certificate is None or key is None:
        raise ProfileImportError(f"{LABEL} need both a client certificate and a private key")
    if password == "":
        raise ProfileImportError(f"{LABEL} set an empty private-key password")
    key_password = Secret(password) if password is not None else None
    try:
        validated_certificate, validated_key = validate_client_identity(
            certificate, key, password=key_password
        )
    except KafkaProfileError as error:
        raise ProfileImportError(f"{LABEL} client identity is invalid: {error}") from error
    return validated_certificate, validated_key, key_password


__all__ = [
    "LABEL",
    "PropertyReader",
    "check_hostname_verification",
    "client_identity",
    "java_pem_identity",
    "java_pem_trust",
    "normalized_pem",
    "require_pem_store",
    "split_scopes",
]
