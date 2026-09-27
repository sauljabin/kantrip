"""The kcl adapter: private TOML config, Registry, argument policy, and shim (#54)."""

import json
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from kantrip.adapter_policy import AdapterError, check_kcl_arguments
from kantrip.kafka import KafkaConnection
from kantrip.oauth import OAuthConnection
from kantrip.registry import RegistryConnection
from kantrip.secret_value import Secret
from kantrip.session import SessionError, _kcl_toml_string, run_profile_session
from tests.unit.pki import KEY_PASSWORD, synthetic_pki

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000054"
PROFILE: dict[str, Any] = {
    "id": PROFILE_ID,
    "kafka": {
        "bootstrapServers": ["localhost:9092"],
        "transport": "plaintext",
        "auth": {"type": "none"},
    },
}
# kcl v0.20.0 (client/client.go envRef): `$${` is a literal `${`, and `${NAME}` is
# replaced with the environment variable NAME.
_KCL_ENVIRONMENT_REFERENCE = re.compile(r"\$\$\{|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class KclSessionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        runtime_directory = tempfile.TemporaryDirectory()
        self.addCleanup(runtime_directory.cleanup)
        runtime_patch = patch(
            "kantrip.runtime.tempfile.gettempdir", return_value=runtime_directory.name
        )
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)

    def run_kcl(
        self,
        arguments: list[str],
        resolved: KafkaConnection | None = None,
        registry: RegistryConnection | None = None,
        environment: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        observed: dict[str, Any] = {}

        def inspect_run(command: list[str], **options: object) -> subprocess.CompletedProcess:
            child_environment = options["env"]
            assert isinstance(child_environment, dict)
            session = Path(child_environment["KANTRIP_SESSION_DIR"])
            config = Path(child_environment["KCL_CONFIG_PATH"])
            observed.update(
                arguments=command,
                environment=child_environment,
                config_path=config,
                config=config.read_text(encoding="utf-8"),
                mode=stat.S_IMODE(config.stat().st_mode),
                session=session,
                files={
                    path.name: path.read_text(encoding="utf-8")
                    for path in session.iterdir()
                    if path.is_file()
                },
            )
            return subprocess.CompletedProcess(command, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kcl"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            result = run_profile_session(
                "local",
                PROFILE,
                arguments,
                environment=environment or {"HOME": "/Users/someone"},
                resolved_kafka=resolved,
                resolved_registry=registry,
            )
        self.assertEqual(0, result)
        return observed

    def assert_fails_before_launch(
        self,
        message: str,
        *,
        arguments: list[str] | None = None,
        resolved: KafkaConnection | None = None,
        registry: RegistryConnection | None = None,
    ) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kcl"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, message),
        ):
            run_profile_session(
                "local",
                PROFILE,
                arguments or ["kcl", "topic", "list"],
                environment={},
                resolved_kafka=resolved,
                resolved_registry=registry,
            )
        run.assert_not_called()


class TestKclSession(KclSessionTestCase):
    def test_plaintext_profile_is_selected_through_the_environment_only(self) -> None:
        observed = self.run_kcl(
            ["kcl", "topic", "list"],
            environment={
                "HOME": "/Users/someone",
                "KCL_CONFIG_PATH": "/Users/someone/other.toml",
                "KCL_NO_CONFIG_FILE": "1",
                "KCL_PROFILE": "other",
                "KCL_SEED_BROKERS": "other.invalid:9092",
            },
        )

        session = observed["session"]
        self.assertEqual(["kcl", "topic", "list"], observed["arguments"])
        self.assertEqual(session / "kcl.toml", observed["config_path"])
        self.assertEqual('seed_brokers = ["localhost:9092"]\n', observed["config"])
        self.assertEqual(0o600, observed["mode"])
        self.assertEqual(
            ["KCL_CONFIG_PATH"],
            [name for name in observed["environment"] if name.startswith("KCL_")],
        )
        self.assertEqual("/Users/someone", observed["environment"]["HOME"])
        self.assertFalse(session.exists())

    def test_scram_uses_the_private_ca_with_verification_on(self) -> None:
        pki = synthetic_pki()
        resolved = KafkaConnection(
            ("broker.invalid:9094",),
            "tls",
            pki.ca,
            "scram-sha-512",
            username="synthetic-user",
            password=Secret("synthetic-password"),
        )

        observed = self.run_kcl(["kcl", "topic", "list"], resolved)

        ca_path = observed["session"] / "kafka-ca.pem"
        self.assertEqual(
            'seed_brokers = ["broker.invalid:9094"]\n'
            "\n"
            "[tls]\n"
            "insecure = false\n"
            f'ca_cert_path = "{ca_path}"\n'
            "\n"
            "[sasl]\n"
            'mechanism = "scram-sha-512"\n'
            'user = "synthetic-user"\n'
            'pass = "synthetic-password"\n',
            observed["config"],
        )
        self.assertEqual(pki.ca, observed["files"]["kafka-ca.pem"])
        self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))

    def test_system_trust_tls_keeps_verification_without_a_ca_file(self) -> None:
        observed = self.run_kcl(
            ["kcl", "topic", "list"], KafkaConnection(("broker.invalid:9093",), "tls")
        )

        self.assertEqual(
            'seed_brokers = ["broker.invalid:9093"]\n\n[tls]\ninsecure = false\n',
            observed["config"],
        )

    def test_mtls_reads_a_decrypted_copy_of_an_encrypted_key(self) -> None:
        pki = synthetic_pki()
        resolved = KafkaConnection(
            ("broker.invalid:9095",),
            "tls",
            pki.ca,
            "mtls",
            client_certificate=pki.client_certificate,
            private_key=Secret(pki.encrypted_client_key),
            private_key_password=Secret(KEY_PASSWORD),
        )

        observed = self.run_kcl(["kcl", "topic", "list"], resolved)

        session = observed["session"]
        self.assertIn(f'client_cert_path = "{session / "kafka-client.crt"}"\n', observed["config"])
        self.assertIn(
            f'client_key_path = "{session / "client-unencrypted.key"}"\n', observed["config"]
        )
        self.assertNotIn("[sasl]", observed["config"])
        self.assertEqual(pki.client_key, observed["files"]["client-unencrypted.key"])
        self.assertNotIn(KEY_PASSWORD, observed["config"])

    def test_mtls_without_a_key_password_reuses_the_session_key(self) -> None:
        pki = synthetic_pki()
        resolved = KafkaConnection(
            ("broker.invalid:9095",),
            "tls",
            pki.ca,
            "mtls",
            client_certificate=pki.client_certificate,
            private_key=Secret(pki.client_key),
        )

        observed = self.run_kcl(["kcl", "topic", "list"], resolved)

        self.assertIn(
            f'client_key_path = "{observed["session"] / "kafka-client.key"}"\n',
            observed["config"],
        )
        self.assertNotIn("client-unencrypted.key", observed["files"])

    def test_oauth_fails_before_launch(self) -> None:
        resolved = KafkaConnection(
            ("broker.invalid:9096",),
            "tls",
            auth_type="oauth",
            oauth=OAuthConnection(
                token_url="https://idp.invalid/token",
                client_id="synthetic-client",
                scopes=(),
                client_secret_reference="synthetic-reference",
                client_secret=Secret("synthetic-secret"),
            ),
        )

        self.assert_fails_before_launch(
            "kcl does not support Kafka authentication 'oauth'", resolved=resolved
        )

    def test_connection_overrides_fail_before_launch(self) -> None:
        self.assert_fails_before_launch(
            "cannot override the selected Kantrip profile",
            arguments=["kcl", "-B", "other:9092", "topic", "list"],
        )

    def test_profile_commands_fail_before_launch(self) -> None:
        self.assert_fails_before_launch(
            "kcl profile commands cannot change or show the selected Kantrip profile",
            arguments=["kcl", "profile", "dump"],
        )


class TestKclRegistry(KclSessionTestCase):
    def test_unauthenticated_registry_sets_only_the_url(self) -> None:
        registry = RegistryConnection("confluent", "http://registry.invalid:8081", "")

        observed = self.run_kcl(["kcl", "registry", "subject", "list"], registry=registry)

        self.assertEqual(
            'seed_brokers = ["localhost:9092"]\n'
            "\n"
            "[registry]\n"
            'urls = ["http://registry.invalid:8081"]\n',
            observed["config"],
        )

    def test_basic_registry_uses_its_private_ca_and_keeps_credentials_in_the_file(self) -> None:
        pki = synthetic_pki()
        registry = RegistryConnection(
            "confluent",
            "https://registry.invalid",
            "",
            "basic",
            pki.ca,
            username="synthetic-user",
            password=Secret("synthetic-password"),
        )

        observed = self.run_kcl(["kcl", "registry", "subject", "list"], registry=registry)

        ca_path = observed["session"] / "registry-ca.pem"
        self.assertTrue(
            observed["config"].endswith(
                "[registry]\n"
                'urls = ["https://registry.invalid"]\n'
                'user = "synthetic-user"\n'
                'pass = "synthetic-password"\n'
                "\n"
                "[registry.tls]\n"
                "insecure = false\n"
                f'ca_cert_path = "{ca_path}"\n'
            ),
            observed["config"],
        )
        self.assertEqual(0o600, observed["mode"])
        self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))

    def test_token_registry_with_system_trust_sends_a_bearer_token(self) -> None:
        registry = RegistryConnection(
            "confluent",
            "https://registry.invalid",
            "",
            "token",
            token=Secret("synthetic-token"),
        )

        observed = self.run_kcl(["kcl", "registry", "subject", "list"], registry=registry)

        self.assertTrue(
            observed["config"].endswith(
                '[registry]\nurls = ["https://registry.invalid"]\n'
                'bearer_token = "synthetic-token"\n'
            ),
            observed["config"],
        )

    def test_mtls_registry_reads_a_decrypted_copy_of_an_encrypted_key(self) -> None:
        pki = synthetic_pki()
        registry = RegistryConnection(
            "confluent",
            "https://registry.invalid",
            "",
            "mtls",
            pki.ca,
            client_certificate=pki.client_certificate,
            private_key=Secret(pki.encrypted_client_key),
            private_key_password=Secret(KEY_PASSWORD),
        )

        observed = self.run_kcl(["kcl", "registry", "subject", "list"], registry=registry)

        session = observed["session"]
        self.assertIn(
            "[registry.tls]\n"
            "insecure = false\n"
            f'ca_cert_path = "{session / "registry-ca.pem"}"\n'
            f'client_cert_path = "{session / "registry-client.crt"}"\n'
            f'client_key_path = "{session / "registry-client-unencrypted.key"}"\n',
            observed["config"],
        )
        self.assertEqual(pki.client_key, observed["files"]["registry-client-unencrypted.key"])
        self.assertNotIn(KEY_PASSWORD, observed["config"])

    def test_registries_kcl_cannot_map_fail_before_launch(self) -> None:
        cases = (
            (
                RegistryConnection("apicurio", "https://registry.invalid/apis/registry/v3", ""),
                "kcl supports only Confluent-compatible registry profiles",
            ),
            (
                RegistryConnection(
                    "confluent",
                    "https://registry.invalid",
                    "",
                    "oauth",
                    oauth=OAuthConnection(
                        token_url="https://idp.invalid/token",
                        client_id="synthetic-client",
                        scopes=(),
                        client_secret_reference="synthetic-reference",
                        client_secret=Secret("synthetic-secret"),
                    ),
                ),
                "kcl does not support Registry authentication 'oauth'",
            ),
        )
        for registry, message in cases:
            with self.subTest(message=message):
                self.assert_fails_before_launch(message, registry=registry)


class TestKclShim(KclSessionTestCase):
    def test_interactive_shim_strips_kcl_variables_and_selects_the_config(self) -> None:
        observed: dict[str, Any] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            return "/opt/bin/kcl" if executable == "kcl" else None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim = Path(environment["PATH"].split(":", 1)[0]) / "kcl"
            observed["contents"] = shim.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(shim.stat().st_mode)
            observed["session"] = Path(environment["KANTRIP_SESSION_DIR"])
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", PROFILE, [], environment={"SHELL": "/bin/bash", "PATH": "/bin"}
            )

        config = observed["session"] / "kcl.toml"
        self.assertIn("kantrip._adapter_guard kcl", observed["contents"])
        self.assertIn('unset "$kantrip_variable"', observed["contents"])
        self.assertIn(
            f'export KCL_CONFIG_PATH={config}\nexec /opt/bin/kcl "$@"\n', observed["contents"]
        )
        self.assertEqual(0o700, observed["mode"])


class TestKclArgumentPolicy(unittest.TestCase):
    def test_connection_options_are_rejected_in_every_form(self) -> None:
        for arguments in (
            ["-B", "other:9092", "topic", "list"],
            ["-Bother:9092", "topic", "list"],
            ["-B=other:9092", "topic", "list"],
            ["--bootstrap-servers", "other:9092", "topic", "list"],
            ["--bootstrap-servers=other:9092", "topic", "list"],
            ["topic", "list", "-jB", "other:9092"],
            ["-R", "http://other.invalid", "registry", "subject", "list"],
            ["--registry=http://other.invalid", "registry", "subject", "list"],
            ["-C", "other", "topic", "list"],
            ["--profile", "other", "topic", "list"],
            ["--config-path", "other.toml", "topic", "list"],
            ["--config-path=other.toml", "topic", "list"],
            ["--no-config-file", "topic", "list"],
            ["--config-env-prefix", "OTHER_", "topic", "list"],
            # An option-like value is checked anyway: a rejection beats a bypass.
            ["consume", "orders", "--offset", "-B", "other:9092"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(
                    AdapterError, "cannot override the selected Kantrip profile"
                ),
            ):
                check_kcl_arguments("kcl", arguments)

    def test_config_options_accept_only_timeouts_and_help(self) -> None:
        for arguments in (
            ["-X", "seed_brokers=other:9092", "topic", "list"],
            ["-Xsasl.user=other", "topic", "list"],
            ["-X=tls.insecure", "topic", "list"],
            ["-jX", "registry.urls=http://other.invalid", "registry", "subject", "list"],
            ["--config-opt", "SASL_PASS=other", "topic", "list"],
            ["--config-opt=use_tls=false", "topic", "list"],
            ["topic", "list", "-X", "tls.ca_cert_path=other.pem"],
            ["-X", "-Bother:9092", "topic", "list"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(
                    AdapterError, "kcl config key .* cannot override the selected Kantrip profile"
                ),
            ):
                check_kcl_arguments("kcl", arguments)
        for arguments in (
            ["-X", "broker_timeout=10s", "topic", "list"],
            ["-Xdial_timeout=3s", "-X", "retry_timeout=1m", "topic", "list"],
            ["--config-opt=RETRY_TIMEOUT=1m", "topic", "list"],
            ["-X", "help"],
            ["-X", "list"],
        ):
            with self.subTest(arguments=arguments):
                self.assertIsNone(check_kcl_arguments("kcl", arguments))

    def test_profile_commands_are_rejected_wherever_the_command_is_found(self) -> None:
        for arguments in (
            ["profile", "use", "other"],
            ["profile", "dump"],
            ["myconfig", "link", "other"],
            ["--format", "json", "profile", "list"],
            ["-j", "profile", "current"],
            ["--log-level=debug", "profile", "list"],
            # An unknown option is taken as a boolean, so the command after it is still found.
            ["--help-json", "profile", "list"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(AdapterError, "kcl .* commands cannot change or show"),
            ):
                check_kcl_arguments("kcl", arguments)

    def test_everyday_commands_and_resources_named_profile_are_accepted(self) -> None:
        for arguments in (
            ["topic", "list"],
            ["--format", "json", "topic", "list"],
            ["cluster", "metadata"],
            ["consume", "orders", "-o", "start", "-n", "1", "-g", "group"],
            ["consume", "orders", "-ostart", "-n1"],
            ["consume", "profile"],
            ["topic", "create", "profile", "-p", "3", "-r", "1"],
            ["produce", "orders", "-k", "key", "-H", "a:b"],
            ["registry", "subject", "list", "--context", ".staging"],
            ["group", "describe", "profile"],
            ["--help"],
            ["topic", "list", "--", "-B"],
        ):
            with self.subTest(arguments=arguments):
                self.assertIsNone(check_kcl_arguments("kcl", arguments))


class TestKclConfigStrings(unittest.TestCase):
    def test_strings_escape_toml_syntax_and_kcl_environment_references(self) -> None:
        for value in (
            "plain",
            'quote " and backslash \\',
            "line\nbreak\ttab\x7fdelete",
            "${HOME}",
            "$${HOME}",
            "$$${HOME}$",
            "trailing ${",
            "unicode ü ☃",
        ):
            with self.subTest(value=value):
                rendered = _kcl_toml_string(value)
                self.assertEqual(value, _kcl_expand(_toml_basic_string(rendered)))


def _toml_basic_string(rendered: str) -> str:
    if sys.version_info >= (3, 11):
        import tomllib

        return str(tomllib.loads(f"value = {rendered}")["value"])
    # The renderer's escapes are the subset TOML basic strings share with JSON.
    return str(json.loads(rendered))


def _kcl_expand(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        if match.group(0) == "$${":
            return "${"
        raise AssertionError(f"kcl would expand {match.group(0)}")

    return _KCL_ENVIRONMENT_REFERENCE.sub(replace, value)


if __name__ == "__main__":
    unittest.main()
