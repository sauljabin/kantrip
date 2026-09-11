import json
import unittest
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_SCHEMA = PROJECT_ROOT / "schemas" / "profile.schema.json"


class TestProfileSchema(unittest.TestCase):
    def test_schema_is_a_valid_draft_2020_12_document(self) -> None:
        Draft202012Validator.check_schema(self._schema())

    def test_programmatic_profile_matches_schema(self) -> None:
        self._validator().validate(_profile_configuration())

    def test_documented_profile_example_matches_schema(self) -> None:
        example = yaml.safe_load(
            (PROJECT_ROOT / "examples" / "config.yaml").read_text(encoding="utf-8")
        )

        self._validator().validate(example)

    def test_schema_registry_is_not_accepted(self) -> None:
        profile = _profile_configuration()
        profile["profiles"]["unit-local"]["schemaRegistry"] = {
            "url": "https://schema.example.com",
            "auth": {"type": "none"},
        }

        self.assertFalse(self._validator().is_valid(profile))

    def test_top_level_version_is_not_accepted(self) -> None:
        profile = _profile_configuration()
        profile["version"] = "0.1.0a0"

        self.assertFalse(self._validator().is_valid(profile))

    def test_authenticated_or_encrypted_profiles_are_not_accepted(self) -> None:
        profile = _profile_configuration()
        kafka = profile["profiles"]["unit-local"]["kafka"]
        kafka["transport"] = "tls"
        kafka["auth"] = {"type": "plain", "username": "synthetic"}

        self.assertFalse(self._validator().is_valid(profile))

    @staticmethod
    def _schema() -> dict:
        return json.loads(PROFILE_SCHEMA.read_text(encoding="utf-8"))

    @classmethod
    def _validator(cls) -> Draft202012Validator:
        return Draft202012Validator(cls._schema(), format_checker=FormatChecker())


def _profile_configuration() -> dict:
    return {
        "profiles": {
            "unit-local": {
                "id": "018f8f13-7c21-7cee-8000-000000000010",
                "description": "Synthetic unit profile",
                "labels": {"environment": "test"},
                "kafka": {
                    "bootstrapServers": ["localhost:19092"],
                    "transport": "plaintext",
                    "auth": {"type": "none"},
                },
            }
        }
    }


if __name__ == "__main__":
    unittest.main()
