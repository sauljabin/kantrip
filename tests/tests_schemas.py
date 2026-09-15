import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_SCHEMA = PROJECT_ROOT / "schemas" / "profile.schema.json"
CA_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "kafka-ca.pem"


class TestProfileSchema(unittest.TestCase):
    def test_schema_is_a_valid_draft_2020_12_document(self) -> None:
        Draft202012Validator.check_schema(self._schema())

    def test_programmatic_profile_matches_schema(self) -> None:
        self._validator().validate(_profile_configuration())

    def test_documented_profile_examples_match_schema(self) -> None:
        for path in sorted((PROJECT_ROOT / "examples").glob("*.json")):
            with self.subTest(path=path.name):
                example = json.loads(path.read_text(encoding="utf-8"))
                self._validator().validate(example)

    def test_plain_confluent_registry_is_accepted(self) -> None:
        profile = _profile_configuration()
        profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "http://schema.example.com",
        }

        self.assertTrue(self._validator().is_valid(profile))

    def test_plain_apicurio_registry_is_accepted(self) -> None:
        profile = _profile_configuration()
        profile["registry"] = {
            "provider": "apicurio",
            "apicurio.registry.url": "http://schema.example.com/apis/registry/v3",
        }

        self.assertTrue(self._validator().is_valid(profile))

    def test_registry_provider_and_url_property_must_match(self) -> None:
        for registry in (
            {"schema.registry.url": "http://registry.example.com"},
            {"provider": "confluent", "apicurio.registry.url": "http://registry.example.com"},
            {"provider": "apicurio", "schema.registry.url": "http://registry.example.com"},
            {
                "provider": "apicurio",
                "schema.registry.url": "http://registry.example.com",
                "apicurio.registry.url": "http://registry.example.com/apis/registry/v3",
            },
        ):
            with self.subTest(registry=registry):
                profile = _profile_configuration()
                profile["registry"] = registry
                self.assertFalse(self._validator().is_valid(profile))

    def test_secure_or_credentialed_registry_urls_are_rejected(self) -> None:
        for url in (
            "https://registry.example.com",
            "http://user:secret@registry.example.com",
            "http://registry.example.com?token=synthetic",
            "http://registry.example.com#fragment",
        ):
            with self.subTest(url=url):
                profile = _profile_configuration()
                profile["registry"] = {
                    "provider": "confluent",
                    "schema.registry.url": url,
                }
                self.assertFalse(self._validator().is_valid(profile))

    def test_legacy_schema_registry_contract_is_rejected(self) -> None:
        profile = _profile_configuration()
        profile["schemaRegistry"] = {
            "url": "http://schema.example.com",
            "auth": {"type": "none"},
        }

        self.assertFalse(self._validator().is_valid(profile))

    def test_top_level_version_is_not_accepted(self) -> None:
        profile = _profile_configuration()
        profile["version"] = "0.1.0a0"

        self.assertFalse(self._validator().is_valid(profile))

    def test_tls_with_system_or_custom_ca_is_accepted(self) -> None:
        system_trust = _profile_configuration()
        system_trust["kafka"]["transport"] = "tls"
        custom_trust = _profile_configuration()
        custom_trust["kafka"]["transport"] = "tls"
        custom_trust["kafka"]["tls"] = {"caCertificates": CA_FIXTURE.read_text(encoding="utf-8")}

        self.assertTrue(self._validator().is_valid(system_trust))
        self.assertTrue(self._validator().is_valid(custom_trust))

    def test_plaintext_cannot_include_tls_configuration(self) -> None:
        profile = _profile_configuration()
        profile["kafka"]["tls"] = {}

        self.assertFalse(self._validator().is_valid(profile))

    def test_authenticated_profiles_are_not_yet_accepted(self) -> None:
        profile = _profile_configuration()
        kafka = profile["kafka"]
        kafka["transport"] = "tls"
        kafka["auth"] = {"type": "plain", "username": "synthetic"}

        self.assertFalse(self._validator().is_valid(profile))

    def test_arbitrary_client_properties_are_rejected(self) -> None:
        profile = _profile_configuration()
        profile["kafka"]["properties"] = {"common": {"client.id": "unsafe-passthrough"}}

        self.assertFalse(self._validator().is_valid(profile))

    @staticmethod
    def _schema() -> dict:
        return json.loads(PROFILE_SCHEMA.read_text(encoding="utf-8"))

    @classmethod
    def _validator(cls) -> Draft202012Validator:
        return Draft202012Validator(cls._schema(), format_checker=FormatChecker())


def _profile_configuration() -> dict:
    return {
        "id": "018f8f13-7c21-7cee-8000-000000000010",
        "description": "Synthetic unit profile",
        "labels": {"environment": "test"},
        "kafka": {
            "bootstrapServers": ["localhost:19092"],
            "transport": "plaintext",
            "auth": {"type": "none"},
        },
    }


if __name__ == "__main__":
    unittest.main()
