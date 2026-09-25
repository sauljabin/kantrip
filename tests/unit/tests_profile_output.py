import json
import unittest

import yaml

from kantrip.profile_output import (
    describe_observation,
    dump_observation,
    filter_profiles,
    list_observation,
)


class TestProfileOutput(unittest.TestCase):
    def test_filters_profiles_by_every_exact_label(self) -> None:
        profiles = {
            "production": {"labels": {"environment": "production", "owner": "platform"}},
            "analytics": {"labels": {"environment": "production", "owner": "data"}},
            "local": {},
        }

        selected = filter_profiles(
            profiles,
            {"environment": "production", "owner": "platform"},
        )

        self.assertEqual(["production"], list(selected))

    def test_describe_uses_an_allowlist_and_omits_arbitrary_properties(self) -> None:
        profile = {
            "id": "018f8f13-7c21-7cee-8000-000000000010",
            "description": "Production cluster",
            "labels": {"owner": "platform"},
            "kafka": {
                "bootstrapServers": ["kafka.example.com:9093"],
                "transport": "tls",
                "tls": {"caCertificates": "classified public material"},
                "auth": {
                    "type": "scram-sha-512",
                    "username": "application",
                    "passwordRef": "profile/id/kafka/password",
                },
                "properties": {"common": {"password": "classified"}},
            },
            "internal": {"token": "classified"},
        }

        observation = describe_observation("production", 4, profile)
        serialized = json.dumps(observation)

        self.assertEqual("production", observation["name"])
        self.assertEqual(4, observation["revision"])
        self.assertEqual(
            {
                "type": "scram-sha-512",
                "username": "application",
                "credentials": {"kafka.auth.password": "configured"},
            },
            observation["kafka"]["auth"],
        )
        self.assertEqual({"trust": "custom"}, observation["kafka"]["tls"])
        self.assertNotIn("passwordRef", serialized)
        self.assertNotIn("properties", serialized)
        self.assertNotIn("classified", serialized)

    def test_list_observation_omits_identity_and_revision(self) -> None:
        profiles = {
            "local": {
                "id": "018f8f13-7c21-7cee-8000-000000000010",
                "kafka": {
                    "bootstrapServers": ["localhost:9092"],
                    "transport": "plaintext",
                    "auth": {"type": "none"},
                },
            }
        }

        observation = list_observation(profiles)[0]

        self.assertEqual("local", observation["name"])
        self.assertNotIn("id", observation)
        self.assertNotIn("revision", observation)

    def test_serializes_json_and_yaml_without_style_sequences(self) -> None:
        value = [{"name": "local", "labels": {"environment": "development"}}]

        as_json = dump_observation(value, "json")
        as_yaml = dump_observation(value, "yaml")

        self.assertEqual(value, json.loads(as_json))
        self.assertEqual(value, yaml.safe_load(as_yaml))
        self.assertNotIn("\x1b[", as_json)
        self.assertNotIn("\x1b[", as_yaml)


if __name__ == "__main__":
    unittest.main()
