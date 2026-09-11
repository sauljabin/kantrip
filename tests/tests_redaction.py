import unittest

from kantrip.redaction import REDACTED, is_classified_key, redact_mapping


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
                "password": "synthetic-password",
            },
        }

        result = redact_mapping(source)

        self.assertEqual(REDACTED, result["auth"]["password"])
        self.assertEqual("synthetic-password", source["auth"]["password"])
        self.assertEqual(["localhost:9092"], result["bootstrapServers"])


if __name__ == "__main__":
    unittest.main()
