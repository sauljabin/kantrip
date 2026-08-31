import json
import unittest
from pathlib import Path

from kantrip.redaction import REDACTED, is_classified_key, redact_mapping, redact_text

CASES = Path(__file__).parent / "redaction" / "cases.json"


class TestRedaction(unittest.TestCase):
    def test_classifies_secret_values_and_references(self) -> None:
        for key in ("password", "passwordRef", "clientSecret", "private_key", "access-token"):
            with self.subTest(key=key):
                self.assertTrue(is_classified_key(key))
        self.assertFalse(is_classified_key("bootstrapServers"))
        self.assertFalse(is_classified_key("clientId"))

    def test_recursively_redacts_without_mutating_source(self) -> None:
        source = {
            "bootstrapServers": ["localhost:9092"],
            "auth": {
                "username": "synthetic-user",
                "passwordRef": "keyring://profile/synthetic/password",
            },
        }

        result = redact_mapping(source)

        self.assertEqual(REDACTED, result["auth"]["passwordRef"])
        self.assertEqual("keyring://profile/synthetic/password", source["auth"]["passwordRef"])
        self.assertEqual(["localhost:9092"], result["bootstrapServers"])

    def test_text_redaction_fixtures(self) -> None:
        cases = json.loads(CASES.read_text(encoding="utf-8"))

        for case in cases:
            with self.subTest(text=case["input"]):
                self.assertEqual(case["expected"], redact_text(case["input"]))

    def test_private_key_block_is_removed(self) -> None:
        text = "before\n-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----\nafter"

        self.assertEqual(f"before\n{REDACTED}\nafter", redact_text(text))


if __name__ == "__main__":
    unittest.main()
