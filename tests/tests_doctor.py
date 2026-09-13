import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.doctor import run_doctor
from kantrip.runtime import SESSION_STALE_SECONDS, create_session_runtime


class TestDoctor(unittest.TestCase):
    def test_reports_valid_configuration_and_installed_tools(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "XDG_RUNTIME_DIR": directory,
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
        self.assertTrue(any("Registry profiles: 1 profile" in message for message in messages))
        verbose_messages = [
            check.message
            for _, checks in report.sections(verbose=True)
            for check in checks
            if check.verbose_only
        ]
        self.assertTrue(any(message.startswith("Kafka topics: ") for message in verbose_messages))
        self.assertTrue(
            any(
                message.startswith("kafka-protobuf-console-consumer: ")
                for message in verbose_messages
            )
        )

    def test_invalid_configuration_is_unhealthy_without_contacting_kafka(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text("profiles: []\n", encoding="utf-8")

            with patch("kantrip.doctor.shutil.which", return_value=None):
                report = run_doctor(
                    {
                        "KANTRIP_CONFIG": str(config_path),
                        "XDG_RUNTIME_DIR": directory,
                        "PATH": "",
                        "SHELL": "/bin/zsh",
                    }
                )

        self.assertFalse(report.healthy)
        self.assertTrue(
            any("configuration does not match schema" in check.message for check in report.checks)
        )

    def test_unsupported_registry_profile_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(
                _VALID_CONFIG.replace("http://localhost:8081", "https://localhost:8081"),
                encoding="utf-8",
            )
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any("configuration does not match schema" in check.message for check in report.checks)
        )

    def test_names_a_missing_schema_registry_console_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "XDG_RUNTIME_DIR": directory,
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
                "XDG_RUNTIME_DIR": directory,
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
                "XDG_RUNTIME_DIR": directory,
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

    def test_runtime_diagnostics_are_read_only_and_hide_paths_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            config_path = root / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }
            with patch("kantrip.runtime.time.time", return_value=1000):
                runtime = create_session_runtime(environment)
            runtime_path = runtime.path
            runtime._closed = True
            os.close(runtime._lock_descriptor)
            os.close(runtime._session_descriptor)
            os.close(runtime._root_descriptor)

            with (
                patch("kantrip.doctor.shutil.which", side_effect=_installed_tool),
                patch("kantrip.runtime.time.time", return_value=1000 + SESSION_STALE_SECONDS),
            ):
                report = run_doctor(environment)
            artifact_preserved = runtime_path.exists()

        visible = [check.message for _, checks in report.sections() for check in checks]
        verbose = [check.message for _, checks in report.sections(verbose=True) for check in checks]
        self.assertTrue(artifact_preserved)
        self.assertTrue(any("Runtime stale: 1 session" in message for message in visible))
        self.assertFalse(any(str(runtime_path.parent) in message for message in visible))
        self.assertTrue(any(str(runtime_path.parent) in message for message in verbose))

    def test_invalid_runtime_entry_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            runtime_root = root / "kantrip" / "sessions"
            runtime_root.mkdir(mode=0o700, parents=True)
            runtime_root.parent.chmod(0o700)
            (runtime_root / "unexpected").mkdir()
            config_path = root / "config.yaml"
            config_path.write_text(_VALID_CONFIG, encoding="utf-8")
            config_path.chmod(0o600)
            environment = {
                "KANTRIP_CONFIG": str(config_path),
                "XDG_RUNTIME_DIR": directory,
                "PATH": "/tools",
                "SHELL": "/tools/zsh",
            }

            with patch("kantrip.doctor.shutil.which", side_effect=_installed_tool):
                report = run_doctor(environment)

        self.assertFalse(report.healthy)
        self.assertTrue(
            any("Runtime invalid: 1 session" in check.message for check in report.checks)
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
    registry:
      provider: confluent
      schema.registry.url: http://localhost:8081
"""


if __name__ == "__main__":
    unittest.main()
