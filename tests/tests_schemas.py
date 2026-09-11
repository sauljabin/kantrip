import json
import unittest
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, FormatChecker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_SCHEMA = PROJECT_ROOT / "schemas" / "profile-v1.schema.json"
PROFILE_FIXTURE = Path(__file__).parent / "profiles" / "config-v1.yaml"


class TestProfileSchema(unittest.TestCase):
    def test_schema_is_a_valid_draft_2020_12_document(self) -> None:
        Draft202012Validator.check_schema(self._schema())

    def test_unit_profile_fixture_matches_schema(self) -> None:
        profile = yaml.safe_load(PROFILE_FIXTURE.read_text(encoding="utf-8"))

        self._validator().validate(profile)

    def test_documented_profile_example_matches_schema(self) -> None:
        example = yaml.safe_load(
            (PROJECT_ROOT / "examples" / "config.yaml").read_text(encoding="utf-8")
        )

        self._validator().validate(example)

    def test_schema_registry_mtls_fields_match_schema(self) -> None:
        profile = yaml.safe_load(PROFILE_FIXTURE.read_text(encoding="utf-8"))
        profile_id = profile["profiles"]["unit-local"]["id"]
        profile["profiles"]["unit-local"]["schemaRegistry"] = {
            "url": "https://schema.example.com",
            "auth": {"type": "none"},
            "tls": {
                "certificateRef": f"keyring://profile/{profile_id}/schema-registry/certificate",
                "privateKeyRef": f"keyring://profile/{profile_id}/schema-registry/private-key",
            },
        }

        self._validator().validate(profile)

    @staticmethod
    def _schema() -> dict:
        return json.loads(PROFILE_SCHEMA.read_text(encoding="utf-8"))

    @classmethod
    def _validator(cls) -> Draft202012Validator:
        return Draft202012Validator(cls._schema(), format_checker=FormatChecker())


if __name__ == "__main__":
    unittest.main()
