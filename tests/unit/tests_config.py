import tempfile
import unittest
from pathlib import Path

from kantrip.config import ConfigurationError, load_configuration, resolve_config_path


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

    def test_unknown_profile_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(_VALID_CONFIG, encoding="utf-8")

            with self.assertRaisesRegex(ConfigurationError, "profile 'missing' was not found"):
                load_configuration(path).profile("missing")


_VALID_CONFIG = """\
version: 1
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
