"""Generate synthetic test-only PKI without repository certificate fixtures."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

CONTROLLED_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
KEY_PASSWORD = "synthetic-key-password"


@dataclass(frozen=True)
class SyntheticPki:
    """In-memory PEM variants used by TLS and profile tests."""

    ca: str
    wrong_ca: str
    client_certificate: str
    client_key: str
    encrypted_client_key: str
    mismatched_client_key: str
    server_certificate: str
    server_key: str
    wrong_host_certificate: str
    expired_certificate: str
    future_certificate: str


@lru_cache(maxsize=1)
def synthetic_pki() -> SyntheticPki:
    """Generate one process-local PKI on first use, keeping all keys in memory."""
    ca_key, ca = _certificate_authority("Kantrip Test CA")
    _, wrong_ca = _certificate_authority("Kantrip Wrong Test CA")
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    mismatch_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return SyntheticPki(
        ca=_certificate_pem(ca),
        wrong_ca=_certificate_pem(wrong_ca),
        client_certificate=_certificate_pem(
            _leaf(ca, ca_key, client_key, "kantrip-client", client=True)
        ),
        client_key=_private_key_pem(client_key),
        encrypted_client_key=_private_key_pem(client_key, password=KEY_PASSWORD),
        mismatched_client_key=_private_key_pem(mismatch_key),
        server_certificate=_certificate_pem(
            _leaf(ca, ca_key, server_key, "localhost", dns_name="localhost")
        ),
        server_key=_private_key_pem(server_key),
        wrong_host_certificate=_certificate_pem(
            _leaf(ca, ca_key, server_key, "wrong.invalid", dns_name="wrong.invalid")
        ),
        expired_certificate=_certificate_pem(
            _leaf(
                ca,
                ca_key,
                server_key,
                "expired.invalid",
                not_before=datetime(2019, 1, 1, tzinfo=timezone.utc),
                not_after=datetime(2020, 1, 1, tzinfo=timezone.utc),
            )
        ),
        future_certificate=_certificate_pem(
            _leaf(
                ca,
                ca_key,
                server_key,
                "future.invalid",
                not_before=datetime(2040, 1, 1, tzinfo=timezone.utc),
                not_after=datetime(2041, 1, 1, tzinfo=timezone.utc),
            )
        ),
    )


@contextmanager
def temporary_pki_files(**materials: str) -> Iterator[dict[str, Path]]:
    """Materialize selected PEM values with private modes and unconditional cleanup."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        root.chmod(0o700)
        paths: dict[str, Path] = {}
        for name, contents in materials.items():
            path = root / f"{name}.pem"
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
                destination.write(contents)
            paths[name] = path
        yield paths


def _certificate_authority(common_name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime(2025, 1, 1, tzinfo=timezone.utc))
        .not_valid_after(datetime(2036, 1, 1, tzinfo=timezone.utc))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def _leaf(
    ca: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    key: rsa.RSAPrivateKey,
    common_name: str,
    *,
    dns_name: str | None = None,
    client: bool = False,
    not_before: datetime = datetime(2025, 1, 1, tzinfo=timezone.utc),
    not_after: datetime = datetime(2035, 1, 1, tzinfo=timezone.utc),
) -> x509.Certificate:
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
                if client
                else [x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]
            ),
            critical=False,
        )
    )
    if dns_name is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(dns_name)]),
            critical=False,
        )
    return builder.sign(ca_key, hashes.SHA256())


def _certificate_pem(certificate: x509.Certificate) -> str:
    return certificate.public_bytes(serialization.Encoding.PEM).decode()


def _private_key_pem(key: rsa.RSAPrivateKey, *, password: str | None = None) -> str:
    encryption = (
        serialization.NoEncryption()
        if password is None
        else serialization.BestAvailableEncryption(password.encode())
    )
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        encryption,
    ).decode()
