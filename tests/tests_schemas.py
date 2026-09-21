import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_SCHEMA = PROJECT_ROOT / "schemas" / "profile.schema.json"
from tests.pki import synthetic_pki


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

    def test_https_registry_and_credentialed_urls_are_distinguished(self) -> None:
        secure = _profile_configuration()
        secure["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.example.com",
            "auth": {"type": "none"},
        }
        self.assertTrue(self._validator().is_valid(secure))
        for url in (
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

    def test_registry_schema_enforces_transport_and_provider_auth_constraints(self) -> None:
        profile_id = _profile_configuration()["id"]
        credential_id = "018f8f13-7c21-7cee-8000-000000000011"
        password_reference = f"profile/{profile_id}/{credential_id}/registry/password"
        token_reference = f"profile/{profile_id}/{credential_id}/registry/token"
        registries = (
            {
                "provider": "confluent",
                "schema.registry.url": "http://registry.example.com",
                "auth": {
                    "type": "basic",
                    "username": "synthetic",
                    "passwordRef": password_reference,
                },
            },
            {
                "provider": "apicurio",
                "apicurio.registry.url": "https://registry.example.com",
                "auth": {"type": "token", "tokenRef": token_reference},
            },
        )
        for registry in registries:
            profile = _profile_configuration()
            profile["registry"] = registry
            with self.subTest(registry=registry):
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
        custom_trust["kafka"]["tls"] = {"caCertificates": synthetic_pki().ca}

        self.assertTrue(self._validator().is_valid(system_trust))
        self.assertTrue(self._validator().is_valid(custom_trust))

    def test_plaintext_cannot_include_tls_configuration(self) -> None:
        profile = _profile_configuration()
        profile["kafka"]["tls"] = {}

        self.assertFalse(self._validator().is_valid(profile))

    def test_password_and_mtls_profiles_are_accepted_only_over_tls(self) -> None:
        profile_id = _profile_configuration()["id"]
        credential_id = "018f8f13-7c21-7cee-8000-000000000011"
        password_reference = f"profile/{profile_id}/{credential_id}/kafka/password"
        key_reference = f"profile/{profile_id}/{credential_id}/kafka/tls/private-key"
        oauth_reference = f"profile/{profile_id}/{credential_id}/kafka/oauth/client-secret"
        for auth in (
            {"type": "plain", "username": "synthetic", "passwordRef": password_reference},
            {
                "type": "scram-sha-256",
                "username": "synthetic",
                "passwordRef": password_reference,
            },
            {
                "type": "scram-sha-512",
                "username": "synthetic",
                "passwordRef": password_reference,
            },
            {
                "type": "mtls",
                "clientCertificate": synthetic_pki().client_certificate,
                "privateKeyRef": key_reference,
            },
            {
                "type": "oauth",
                "tokenUrl": "https://idp.invalid/oauth/token",
                "clientId": "synthetic-client",
                "scopes": ["openid", "profile"],
                "clientSecretRef": oauth_reference,
                "caCertificates": synthetic_pki().ca,
            },
        ):
            with self.subTest(auth=auth["type"]):
                profile = _profile_configuration()
                profile["kafka"]["transport"] = "tls"
                profile["kafka"]["auth"] = auth
                self.assertTrue(self._validator().is_valid(profile))
                profile["kafka"]["transport"] = "plaintext"
                self.assertFalse(self._validator().is_valid(profile))

    def test_authenticated_profiles_require_complete_owned_reference_shapes(self) -> None:
        profile = _profile_configuration()
        profile["kafka"]["transport"] = "tls"
        for auth in (
            {"type": "plain", "username": "synthetic"},
            {
                "type": "plain",
                "username": "synthetic",
                "passwordRef": "profile/not-a-uuid/value/kafka/password",
            },
            {
                "type": "mtls",
                "clientCertificate": "not-pem",
                "privateKeyRef": "not-a-reference",
            },
        ):
            with self.subTest(auth=auth):
                profile["kafka"]["auth"] = auth
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
