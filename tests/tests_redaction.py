import unittest

from kantrip.redaction import REDACTED, is_classified_key, redact_mapping, redact_text


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

    def test_redacts_sensitive_exception_text(self) -> None:
        source = (
            "request https://user:pass@example.test/token?access_token=visible failed; "
            "client_secret=also-visible Authorization: Bearer bearer-value"
        )

        result = redact_text(source)

        self.assertEqual(
            "request https://example.test/token failed; client_secret=<redacted> "
            "Authorization: <redacted>",
            result,
        )

    def test_bounds_and_removes_control_characters_from_exception_text(self) -> None:
        result = redact_text(f"failure\x1b[31m {'x' * 600}")

        self.assertNotIn("\x1b", result)
        self.assertEqual(500, len(result))
        self.assertTrue(result.endswith("…"))


if __name__ == "__main__":
    unittest.main()
