import unittest

from kantrip.registry import RegistryProfileError, display_registry, plain_registry_connection


class TestRegistry(unittest.TestCase):
    def test_resolves_confluent(self) -> None:
        connection = plain_registry_connection(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": "http://registry.invalid:8081",
                }
            }
        )

        self.assertIsNotNone(connection)
        assert connection is not None
        self.assertEqual("confluent", connection.provider)
        self.assertEqual("schema.registry.url", connection.property_name)

    def test_rejects_a_missing_provider(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "registry.provider must"):
            plain_registry_connection(
                {"registry": {"schema.registry.url": "http://registry.invalid:8081"}}
            )

    def test_resolves_native_apicurio(self) -> None:
        connection = plain_registry_connection(
            {
                "registry": {
                    "provider": "apicurio",
                    "apicurio.registry.url": "http://registry.invalid/apis/registry/v3",
                }
            }
        )

        self.assertIsNotNone(connection)
        assert connection is not None
        self.assertEqual("apicurio", connection.provider)
        self.assertEqual("apicurio.registry.url", connection.property_name)

    def test_rejects_provider_specific_property_mismatches(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "incompatible with provider apicurio"):
            plain_registry_connection(
                {
                    "registry": {
                        "provider": "apicurio",
                        "schema.registry.url": "http://registry.invalid:8081",
                    }
                }
            )

    def test_accepts_https_without_auth_and_rejects_embedded_metadata(self) -> None:
        secure = plain_registry_connection(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": "https://registry.invalid",
                }
            }
        )
        self.assertIsNotNone(secure)
        for url in (
            "http://user:secret@registry.invalid",
            "http://registry.invalid?token=synthetic",
            "http://registry.invalid#fragment",
            "http://registry.invalid:70000",
        ):
            with self.subTest(url=url), self.assertRaises(RegistryProfileError):
                plain_registry_connection(
                    {"registry": {"provider": "confluent", "schema.registry.url": url}}
                )

    def test_rejects_authenticated_http_registry(self) -> None:
        with self.assertRaisesRegex(RegistryProfileError, "require an https"):
            plain_registry_connection(
                {
                    "id": "018f8f13-7c21-7cee-8000-000000000010",
                    "registry": {
                        "provider": "confluent",
                        "schema.registry.url": "http://registry.invalid",
                        "auth": {
                            "type": "basic",
                            "username": "synthetic",
                            "passwordRef": "profile/018f8f13-7c21-7cee-8000-000000000010/018f8f13-7c21-7cee-8000-000000000011/registry/password",
                        },
                    },
                }
            )

    def test_display_redacts_credentials_query_and_fragment(self) -> None:
        rendered = display_registry(
            {
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": (
                        "http://user:secret@registry.invalid:8081/path?token=synthetic#fragment"
                    ),
                }
            }
        )

        self.assertEqual("Confluent: http://registry.invalid:8081/path", rendered)


if __name__ == "__main__":
    unittest.main()
