import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.doctor import run_doctor


class TestDoctor(unittest.TestCase):
    def test_reports_valid_configuration_and_installed_tools(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        messages = [check.message for check in report.checks]
        self.assertTrue(report.healthy)
        self.assertTrue(any("Configuration matches schema" in message for message in messages))
        self.assertTrue(any(message.startswith("kcat: ") for message in messages))
        self.assertTrue(any("Apache Kafka CLI: all 7" in message for message in messages))
        self.assertTrue(any("Schema Registry console: all 6" in message for message in messages))
        self.assertTrue(
            any("Schema Registry profiles: 1 profile" in message for message in messages)
        )

    def test_invalid_configuration_is_unhealthy_without_contacting_kafka(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text("profiles: []\n", encoding="utf-8")

            with patch("kantrip.doctor.shutil.which", return_value=None):
                report = run_doctor(
                    {"KANTRIP_CONFIG": str(config_path), "PATH": "", "SHELL": "/bin/zsh"}
                )

        self.assertFalse(report.healthy)
        self.assertTrue(
            any("configuration does not match schema" in check.message for check in report.checks)
        )

    def test_unsupported_schema_registry_profile_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(
                _VALID_CONFIG.replace("http://localhost:8081", "https://localhost:8081"),
                encoding="utf-8",
            )
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                "Schema Registry profile 'local' is not executable" in check.message
                for check in report.checks
            )
        )

    def test_names_a_missing_schema_registry_console_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            def installed_without_protobuf_consumer(
                name: str, path: str | None = None
            ) -> str | None:
                if name == "kafka-protobuf-console-consumer":
                    return None
                return _installed_tool(name, path)

            with patch(
                "kantrip.doctor.shutil.which",
                side_effect=installed_without_protobuf_consumer,
            ):
                report = run_doctor(environment)

        self.assertTrue(
            any(
                "missing: kafka-protobuf-console-consumer" in check.message
                for check in report.checks
            )
        )

    def test_active_session_allows_a_benign_path_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            session_directory = root / "session"
            shim_directory = session_directory / "bin"
            shim_directory.mkdir(parents=True)
            virtual_environment = root / "venv" / "bin"
            virtual_environment.mkdir(parents=True)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "KANTRIP_PROFILE": "local",
                "KANTRIP_SESSION_ID": "synthetic-session",
                "KANTRIP_SESSION_DIR": str(session_directory),
                "PATH": f"{virtual_environment}:{shim_directory}:/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertTrue(report.healthy)
        self.assertTrue(
            any(
                check.message == "No supported commands shadow session adapters on PATH"
                for check in report.checks
            )
        )

    def test_active_session_rejects_an_adapter_shadow_earlier_on_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            session_directory = root / "session"
            shim_directory = session_directory / "bin"
            shim_directory.mkdir(parents=True)
            shadow_directory = root / "shadow"
            shadow_directory.mkdir()
            shadow = shadow_directory / "kcat"
            shadow.write_text("#!/bin/sh\n", encoding="utf-8")
            shadow.chmod(0o700)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "KANTRIP_PROFILE": "local",
                "KANTRIP_SESSION_ID": "synthetic-session",
                "KANTRIP_SESSION_DIR": str(session_directory),
                "PATH": f"{shadow_directory}:{shim_directory}:/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any(
                check.message == "Session adapters are shadowed earlier on PATH: kcat"
                for check in report.checks
            )
        )


def _installed_tool(name: str, path: str | None = None) -> str | None:
    del path
    installed = {
        "kantrip",
        "zsh",
        "kcat",
        "kaskade",
        "kafka-topics",
        "kafka-console-consumer",
        "kafka-console-producer",
        "kafka-consumer-groups",
        "kafka-configs",
        "kafka-acls",
        "kafka-broker-api-versions",
        "kafka-avro-console-consumer",
        "kafka-avro-console-producer",
        "kafka-json-schema-console-consumer",
        "kafka-json-schema-console-producer",
        "kafka-protobuf-console-consumer",
        "kafka-protobuf-console-producer",
    }
    return f"/tools/{name}" if name in installed else None


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
    schemaRegistry:
      url: http://localhost:8081
      auth:
        type: none
"""


if __name__ == "__main__":
    unittest.main()
