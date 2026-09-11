import tempfile
import unittest
from pathlib import Path

from kantrip.config import (
    ConfigurationError,
    add_profile,
    load_configuration,
    remove_profile,
    resolve_config_path,
)


class TestConfiguration(unittest.TestCase):
    def test_resolves_documented_configuration_precedence(self) -> None:
        environment = {
            "HOME": "/home/example",
            "XDG_CONFIG_HOME": "/xdg/config",
            "KANTRIP_CONFIG": "/explicit/config.yaml",
        }

        self.assertEqual(Path("/explicit/config.yaml"), resolve_config_path(environment))
        del environment["KANTRIP_CONFIG"]
        self.assertEqual(Path("/xdg/config/kantrip/config.yaml"), resolve_config_path(environment))
        del environment["XDG_CONFIG_HOME"]
        self.assertEqual(
            Path("/home/example/.config/kantrip/config.yaml"), resolve_config_path(environment)
        )

    def test_loads_and_selects_a_valid_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(_VALID_CONFIG, encoding="utf-8")

            configuration = load_configuration(path)

            self.assertEqual(
                ["localhost:9092"], configuration.profile("local")["kafka"]["bootstrapServers"]
            )

    def test_reports_invalid_yaml_without_echoing_file_contents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("password: secret\nprofiles: [", encoding="utf-8")

            with self.assertRaisesRegex(ConfigurationError, "invalid YAML") as raised:
                load_configuration(path)

            self.assertNotIn("secret", str(raised.exception))

    def test_reports_schema_location(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(_VALID_CONFIG.replace("plaintext", "unknown"), encoding="utf-8")

            with self.assertRaisesRegex(ConfigurationError, r"profiles\.local\.kafka\.transport"):
                load_configuration(path)

    def test_reports_unknown_root_field_without_echoing_its_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("version: secret-value\nprofiles: {}\n", encoding="utf-8")

            with self.assertRaisesRegex(
                ConfigurationError, r"document root: unknown field: version"
            ) as raised:
                load_configuration(path)

            self.assertNotIn("secret-value", str(raised.exception))

    def test_unknown_profile_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(_VALID_CONFIG, encoding="utf-8")

            with self.assertRaisesRegex(ConfigurationError, "profile 'missing' was not found"):
                load_configuration(path).profile("missing")

    def test_adds_and_removes_profiles_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "config.yaml"

            configuration = add_profile("local", path)

            self.assertEqual(
                ["localhost:9092"], configuration.profile("local")["kafka"]["bootstrapServers"]
            )
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            self.assertEqual("local", next(iter(load_configuration(path).profiles)))

            configuration = remove_profile("local", path)

            self.assertEqual({}, configuration.profiles)
            self.assertEqual({}, load_configuration(path).profiles)

    def test_add_refuses_to_replace_an_existing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"

            add_profile("local", path)

            with self.assertRaisesRegex(ConfigurationError, "already exists"):
                add_profile("local", path)

    def test_missing_configuration_can_be_loaded_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"

            configuration = load_configuration(path, missing_ok=True)

            self.assertEqual({}, configuration.profiles)
            self.assertFalse(path.exists())


_VALID_CONFIG = """\
profiles:
  local:
    id: 018f8f13-7c21-7cee-8000-000000000001
    kafka:
      bootstrapServers:
        - localhost:9092
      transport: plaintext
      auth:
        type: none
"""


if __name__ == "__main__":
    unittest.main()
