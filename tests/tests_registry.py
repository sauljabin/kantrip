import unittest

from kantrip.registry import RegistryProfileError, display_registry, plain_registry_connection


class TestRegistry(unittest.TestCase):
    def test_resolves_confluent_as_the_default_provider(self) -> None:
        connection = plain_registry_connection(
            {"registry": {"schema.registry.url": "http://registry.invalid:8081"}}
        )

        self.assertIsNotNone(connection)
        assert connection is not None
        self.assertEqual("confluent", connection.provider)
        self.assertEqual("schema.registry.url", connection.property_name)

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

    def test_rejects_non_plain_or_embedded_registry_metadata(self) -> None:
        for url in (
            "https://registry.invalid",
            "http://user:secret@registry.invalid",
            "http://registry.invalid?token=synthetic",
            "http://registry.invalid#fragment",
            "http://registry.invalid:70000",
        ):
            with self.subTest(url=url), self.assertRaises(RegistryProfileError):
                plain_registry_connection({"registry": {"schema.registry.url": url}})

    def test_display_redacts_credentials_query_and_fragment(self) -> None:
        rendered = display_registry(
            {
                "registry": {
                    "schema.registry.url": (
                        "http://user:secret@registry.invalid:8081/path?token=synthetic#fragment"
                    )
                }
            }
        )

        self.assertEqual("Confluent: http://registry.invalid:8081/path", rendered)


if __name__ == "__main__":
    unittest.main()
