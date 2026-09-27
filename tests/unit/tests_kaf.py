"""The kaf adapter: private one-cluster config, Registry, argument policy, and shim (#39)."""

import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml

from kantrip.adapter_policy import AdapterError, check_kaf_arguments
from kantrip.kafka import KafkaConnection
from kantrip.oauth import OAuthConnection
from kantrip.registry import RegistryConnection
from kantrip.secret_value import Secret
from kantrip.session import SessionError, run_profile_session
from tests.unit.client_versions import use_supported_client_versions
from tests.unit.pki import KEY_PASSWORD, synthetic_pki

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000039"
PROFILE: dict[str, Any] = {
    "id": PROFILE_ID,
    "kafka": {
        "bootstrapServers": ["localhost:9092"],
        "transport": "plaintext",
        "auth": {"type": "none"},
    },
}


class KafSessionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        runtime_directory = tempfile.TemporaryDirectory()
        self.addCleanup(runtime_directory.cleanup)
        runtime_patch = patch(
            "kantrip.runtime.tempfile.gettempdir", return_value=runtime_directory.name
        )
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        use_supported_client_versions(self)

    def run_kaf(
        self,
        arguments: list[str],
        resolved: KafkaConnection | None = None,
        registry: RegistryConnection | None = None,
    ) -> dict[str, Any]:
        observed: dict[str, Any] = {}

        def inspect_run(command: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            session = Path(environment["KANTRIP_SESSION_DIR"])
            config = Path(command[2])
            observed.update(
                arguments=command,
                home=environment.get("HOME"),
                config=yaml.safe_load(config.read_text(encoding="utf-8")),
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
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaf"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            result = run_profile_session(
                "local",
                PROFILE,
                arguments,
                environment={"HOME": "/Users/someone"},
                resolved_kafka=resolved,
                resolved_registry=registry,
            )
        self.assertEqual(0, result)
        return observed


class TestKafSession(KafSessionTestCase):
    def test_plaintext_profile_is_the_only_cluster_and_home_is_unwritable(self) -> None:
        observed = self.run_kaf(["kaf", "topics"])

        session = observed["session"]
        self.assertEqual(
            ["kaf", "--config", str(session / "kaf.yaml"), "topics"], observed["arguments"]
        )
        self.assertEqual(
            {
                "current-cluster": "kantrip",
                "clusters": [{"name": "kantrip", "brokers": ["localhost:9092"]}],
            },
            observed["config"],
        )
        self.assertEqual(0o600, observed["mode"])
        # kaf saves to $HOME/.kaf/config whatever --config names; this home fails the save.
        self.assertEqual("/dev/null", observed["home"])
        self.assertFalse(session.exists())

    def test_scram_uses_sasl_ssl_with_the_private_ca_and_verification_on(self) -> None:
        pki = synthetic_pki()
        resolved = KafkaConnection(
            ("broker.invalid:9094",),
            "tls",
            pki.ca,
            "scram-sha-512",
            username="synthetic-user",
            password=Secret("synthetic-password"),
        )

        observed = self.run_kaf(["kaf", "topics"], resolved)

        cluster = observed["config"]["clusters"][0]
        ca_path = str(observed["session"] / "kafka-ca.pem")
        self.assertEqual("SASL_SSL", cluster["security-protocol"])
        self.assertEqual(
            {
                "mechanism": "SCRAM-SHA-512",
                "username": "synthetic-user",
                "password": "synthetic-password",
            },
            cluster["SASL"],
        )
        self.assertEqual({"insecure": False, "cafile": ca_path}, cluster["TLS"])
        self.assertEqual(pki.ca, observed["files"]["kafka-ca.pem"])
        self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))

    def test_system_trust_tls_keeps_verification_without_a_ca_file(self) -> None:
        observed = self.run_kaf(["kaf", "topics"], KafkaConnection(("broker.invalid:9093",), "tls"))

        cluster = observed["config"]["clusters"][0]
        self.assertEqual({"insecure": False}, cluster["TLS"])
        self.assertNotIn("security-protocol", cluster)

    def test_mtls_decrypts_an_encrypted_key_into_a_private_kaf_file(self) -> None:
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

        observed = self.run_kaf(["kaf", "topics"], resolved)

        session = observed["session"]
        cluster = observed["config"]["clusters"][0]
        self.assertNotIn("security-protocol", cluster)
        self.assertEqual(str(session / "kafka-client.crt"), cluster["TLS"]["clientfile"])
        self.assertEqual(str(session / "client-unencrypted.key"), cluster["TLS"]["clientkeyfile"])
        self.assertEqual(pki.client_key, observed["files"]["client-unencrypted.key"])
        # librdkafka clients keep the encrypted key and read its password themselves.
        self.assertEqual(pki.encrypted_client_key, observed["files"]["kafka-client.key"])

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

        observed = self.run_kaf(["kaf", "topics"], resolved)

        cluster = observed["config"]["clusters"][0]
        self.assertEqual(
            str(observed["session"] / "kafka-client.key"), cluster["TLS"]["clientkeyfile"]
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
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaf"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(
                SessionError, "kaf does not support Kafka authentication 'oauth'"
            ),
        ):
            run_profile_session(
                "local", PROFILE, ["kaf", "topics"], environment={}, resolved_kafka=resolved
            )
        run.assert_not_called()

    def test_connection_overrides_fail_before_launch(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaf"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, "cannot override the selected Kantrip profile"),
        ):
            run_profile_session(
                "local", PROFILE, ["kaf", "-b", "other:9092", "topics"], environment={}
            )
        run.assert_not_called()


class TestKafRegistry(KafSessionTestCase):
    def test_unauthenticated_registry_sets_only_the_url(self) -> None:
        registry = RegistryConnection("confluent", "http://registry.invalid:8081", "")

        observed = self.run_kaf(["kaf", "consume", "orders"], registry=registry)

        cluster = observed["config"]["clusters"][0]
        self.assertEqual("http://registry.invalid:8081", cluster["schema-registry-url"])
        self.assertNotIn("schema-registry-credentials", cluster)

    def test_basic_registry_with_system_trust_writes_credentials_only_to_the_file(self) -> None:
        registry = RegistryConnection(
            "confluent",
            "https://registry.invalid",
            "",
            "basic",
            username="synthetic-user",
            password=Secret("synthetic-password"),
        )

        observed = self.run_kaf(["kaf", "consume", "orders"], registry=registry)

        cluster = observed["config"]["clusters"][0]
        self.assertEqual("https://registry.invalid", cluster["schema-registry-url"])
        self.assertEqual(
            {"username": "synthetic-user", "password": "synthetic-password"},
            cluster["schema-registry-credentials"],
        )
        self.assertEqual(0o600, observed["mode"])
        self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))

    def test_registries_kaf_cannot_reach_safely_fail_before_launch(self) -> None:
        pki = synthetic_pki()
        cases = (
            (
                RegistryConnection("apicurio", "https://registry.invalid/apis/registry/v3", ""),
                "kaf supports only Confluent-compatible registry profiles",
            ),
            (
                RegistryConnection(
                    "confluent",
                    "https://registry.invalid",
                    "",
                    "basic",
                    pki.ca,
                    username="synthetic-user",
                    password=Secret("synthetic-password"),
                ),
                "kaf trusts only the system CA store for the Registry",
            ),
            (
                RegistryConnection(
                    "confluent",
                    "https://registry.invalid",
                    "",
                    "token",
                    token=Secret("synthetic-token"),
                ),
                "kaf does not support Registry authentication 'token'",
            ),
            (
                RegistryConnection(
                    "confluent",
                    "https://registry.invalid",
                    "",
                    "mtls",
                    pki.ca,
                    client_certificate=pki.client_certificate,
                    private_key=Secret(pki.client_key),
                ),
                "kaf does not support Registry authentication 'mtls'",
            ),
        )
        for registry, message in cases:
            with (
                self.subTest(message=message),
                patch("kantrip.session.shutil.which", return_value="/opt/bin/kaf"),
                patch("kantrip.session._run_child") as run,
                self.assertRaisesRegex(SessionError, message),
            ):
                run_profile_session(
                    "local",
                    PROFILE,
                    ["kaf", "topics"],
                    environment={},
                    resolved_registry=registry,
                )
            run.assert_not_called()


class TestKafShim(KafSessionTestCase):
    def test_interactive_shim_selects_the_config_under_an_unwritable_home(self) -> None:
        observed: dict[str, Any] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            return "/opt/bin/kaf" if executable == "kaf" else None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim = Path(environment["PATH"].split(":", 1)[0]) / "kaf"
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

        config = observed["session"] / "kaf.yaml"
        self.assertIn(
            f'HOME=/dev/null exec /opt/bin/kaf --config {config} "$@"', observed["contents"]
        )
        self.assertIn("kantrip._adapter_guard kaf", observed["contents"])
        self.assertEqual(0o700, observed["mode"])


class TestKafArgumentPolicy(unittest.TestCase):
    def test_connection_options_are_rejected_in_every_form(self) -> None:
        for arguments in (
            ["--config", "other.yaml", "topics"],
            ["--config=other.yaml", "topics"],
            ["-b", "other:9092", "topics"],
            ["-bother:9092", "topics"],
            ["-b=other:9092", "topics"],
            ["--brokers", "other:9092", "topics"],
            ["--brokers=other:9092", "topics"],
            ["-vb", "other:9092", "topics"],
            ["-c", "other", "topics"],
            ["--cluster=other", "topics"],
            ["--schema-registry", "http://other.invalid", "consume", "t"],
            ["consume", "t", "-fb", "other:9092"],
            # An option-like value is checked anyway: a rejection beats a bypass.
            ["consume", "t", "--raw", "-b", "other:9092"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(
                    AdapterError, "cannot override the selected Kantrip profile"
                ),
            ):
                check_kaf_arguments("kaf", arguments)

    def test_config_commands_are_rejected_because_they_save_the_loaded_config(self) -> None:
        for arguments in (
            ["config", "use-cluster", "kantrip"],
            ["config", "ls"],
            ["-v", "config", "add-cluster", "x"],
            ["-t", "x", "config", "ls"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(AdapterError, "kaf config commands cannot change or save"),
            ):
                check_kaf_arguments("kaf", arguments)

    def test_everyday_commands_and_resources_named_config_are_accepted(self) -> None:
        for arguments in (
            ["topics"],
            ["-v", "topics"],
            ["consume", "orders", "-f", "-g", "group", "--offset", "oldest"],
            ["consume", "orders", "-ooldest", "-kbob"],
            ["consume", "config"],
            ["topic", "describe", "config"],
            ["produce", "orders", "-k", "key", "-H", "a:b"],
            ["group", "describe", "-t", "config", "group"],
            ["--help"],
            ["topics", "--", "-b"],
        ):
            with self.subTest(arguments=arguments):
                self.assertIsNone(check_kaf_arguments("kaf", arguments))


if __name__ == "__main__":
    unittest.main()
