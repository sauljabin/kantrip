import copy
import json
import pickle
import re
import unittest
from dataclasses import asdict
from pathlib import Path

from kantrip.credential_mutations import SecretReplacement
from kantrip.kafka import KafkaConnection
from kantrip.oauth import OAuthConnection
from kantrip.profile_auth import KafkaAuthInput, RegistryAuthInput
from kantrip.profiles import ProfileSnapshot
from kantrip.registry import RegistryConnection
from kantrip.secret_value import Secret

MARKER = "synthetic-marker-7c1f"
PACKAGE = Path(__file__).resolve().parents[2] / "kantrip"
REVEAL_MODULES = frozenset(
    {
        "credential_mutations.py",
        "kafka.py",
        "ping.py",
        "registry.py",
        "secret_value.py",
        "session.py",
    }
)


def _secret(suffix: str) -> Secret:
    return Secret(f"{MARKER}-{suffix}")


def _oauth() -> OAuthConnection:
    return OAuthConnection(
        "https://idp.invalid/token",
        "client",
        ("scope",),
        "reference",
        client_secret=_secret("oauth"),
    )


def _kafka() -> KafkaConnection:
    return KafkaConnection(
        ("broker.invalid:9093",),
        "tls",
        auth_type="oauth",
        oauth=_oauth(),
        password=_secret("password"),
        private_key=_secret("key"),
        private_key_password=_secret("key-password"),
    )


def _registry() -> RegistryConnection:
    return RegistryConnection(
        "confluent",
        "https://registry.invalid",
        "schema.registry.url",
        auth_type="oauth",
        oauth=_oauth(),
        password=_secret("password"),
        token=_secret("token"),
        private_key=_secret("key"),
        private_key_password=_secret("key-password"),
    )


class TestSecret(unittest.TestCase):
    def test_repr_masks_and_text_conversion_fails_loudly(self) -> None:
        secret = _secret("direct")

        self.assertEqual("Secret('***')", repr(secret))
        with self.assertRaises(TypeError):
            str(secret)
        with self.assertRaises(TypeError):
            f"{secret}"
        self.assertEqual(f"{MARKER}-direct", secret.reveal())

    def test_cannot_be_serialized_or_mutated(self) -> None:
        secret = _secret("serialize")

        with self.assertRaises(TypeError):
            pickle.dumps(secret)
        with self.assertRaises(TypeError):
            json.dumps(secret)
        with self.assertRaises(AttributeError):
            secret._value = "changed"  # type: ignore[misc]

    def test_copies_and_dataclass_conversion_keep_the_wrapper(self) -> None:
        secret = _secret("copy")
        replacement = SecretReplacement("kafka/password", secret)

        self.assertIs(secret, copy.copy(secret))
        self.assertIs(secret, copy.deepcopy(secret))
        self.assertIs(secret, asdict(replacement)["value"])

    def test_equality_compares_values(self) -> None:
        self.assertEqual(Secret("same"), Secret("same"))
        self.assertNotEqual(Secret("same"), Secret("other"))
        self.assertNotEqual(Secret("same"), "same")
        self.assertEqual(hash(Secret("same")), hash(Secret("same")))
        self.assertFalse(Secret(""))

    def test_rejects_non_text_values(self) -> None:
        with self.assertRaises(TypeError):
            Secret(b"bytes")  # type: ignore[arg-type]


class TestSecretBearingRepresentations(unittest.TestCase):
    def test_direct_objects_omit_secret_values(self) -> None:
        objects = (
            _oauth(),
            _kafka(),
            _registry(),
            KafkaAuthInput(
                "mtls",
                password=_secret("password"),
                private_key=_secret("key"),
                private_key_password=_secret("key-password"),
                oauth_client_secret=_secret("oauth"),
            ),
            RegistryAuthInput(
                "basic",
                username="registry-user",
                password=_secret("password"),
                token=_secret("token"),
                private_key=_secret("key"),
                private_key_password=_secret("key-password"),
                oauth_client_secret=_secret("oauth"),
            ),
            SecretReplacement("kafka/password", _secret("replacement")),
        )

        for value in objects:
            with self.subTest(type(value).__name__):
                self.assertNotIn(MARKER, repr(value))
                self.assertIn("Secret('***')", repr(value))

    def test_nested_snapshot_keeps_public_context(self) -> None:
        snapshot = ProfileSnapshot(
            "production",
            "00000000-0000-4000-8000-000000000000",
            3,
            {"name": "production"},
            _kafka(),
            _registry(),
        )

        representation = repr(snapshot)

        self.assertNotIn(MARKER, representation)
        self.assertIn("broker.invalid:9093", representation)
        self.assertIn("https://registry.invalid", representation)

    def test_reveal_is_limited_to_boundary_modules(self) -> None:
        pattern = re.compile(r"\.reveal\(\)|\breveal_optional\(")
        revealing = {path.name for path in PACKAGE.glob("*.py") if pattern.search(path.read_text())}

        self.assertLessEqual(revealing, REVEAL_MODULES)


if __name__ == "__main__":
    unittest.main()
