import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from kantrip.adapters import (
    SCHEMA_REGISTRY_EXECUTABLES,
    AdapterError,
    create_subshell_shims,
    require_kaskade_apicurio_security_support,
)
from kantrip.kafka import KafkaConnection
from kantrip.oauth import OAuthConnection
from kantrip.registry import RegistryConnection
from kantrip.runtime import create_session_runtime
from kantrip.secret_store import secret_reference
from kantrip.session import SessionError, _oauth_trust_bundle, run_profile_session
from tests.unit.pki import synthetic_pki

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000010"


class TestProfileSession(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_directory = tempfile.TemporaryDirectory()
        self.runtime_patch = patch(
            "kantrip.runtime.tempfile.gettempdir",
            return_value=self.runtime_directory.name,
        )
        self.runtime_patch.start()
        self.addCleanup(self.runtime_patch.stop)
        self.addCleanup(self.runtime_directory.cleanup)
        self.profile = {
            "id": PROFILE_ID,
            "kafka": {
                "bootstrapServers": ["localhost:9092", "localhost:9093"],
                "transport": "plaintext",
                "auth": {"type": "none"},
            },
            "registry": {
                "provider": "confluent",
                "schema.registry.url": "http://registry.invalid:8081",
            },
        }

    def test_explicit_resolved_registry_never_reads_store_or_profile_again(self) -> None:
        self.profile["registry"] = {"invalid": True}
        resolved = RegistryConnection(
            "confluent",
            "https://registry.invalid:8083",
            "schema.registry.url",
            auth_type="basic",
            username="synthetic-user",
            password="synthetic-password",
        )
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session._profile_registry", side_effect=AssertionError),
            patch("kantrip.session.load_secret_store", side_effect=AssertionError),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kcat"], 0),
            ),
        ):
            self.assertEqual(
                0,
                run_profile_session(
                    "local",
                    self.profile,
                    ["kcat", "-L"],
                    environment={},
                    resolved_registry=resolved,
                ),
            )

    def test_kaskade_apicurio_scopes_reject_unsupported_releases(self) -> None:
        connection = RegistryConnection(
            "apicurio",
            "https://registry.invalid/apis/registry/v3",
            "apicurio.registry.url",
            auth_type="oauth",
            oauth=OAuthConnection(
                "https://idp.invalid/token",
                "registry-client",
                ("registry.read",),
                "secret-reference",
            ),
        )
        for rendered in ("5.0.0", "5.0.1.dev5"):
            version = subprocess.CompletedProcess(
                ["kaskade", "--version"],
                0,
                stdout=f"kaskade, version {rendered}\n",
                stderr="",
            )
            with (
                self.subTest(version=rendered),
                patch("kantrip.adapters.shutil.which", return_value="/opt/bin/kaskade"),
                patch("kantrip.adapters.subprocess.run", return_value=version),
                self.assertRaisesRegex(AdapterError, "Kaskade 5.0.1 or newer"),
            ):
                require_kaskade_apicurio_security_support(
                    "kaskade",
                    connection,
                    environment={"PATH": "/opt/bin"},
                )

    def test_oauth_trust_bundle_uses_a_portable_system_path_not_environment(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            trusted_path = root_path / "system.pem"
            inherited_path = root_path / "inherited.pem"
            trusted_path.write_text("SYSTEM ROOT\n", encoding="utf-8")
            inherited_path.write_text("INHERITED ROOT\n", encoding="utf-8")
            paths = Mock(
                openssl_cafile=str(root_path / "missing-compiled.pem"),
                cafile=str(inherited_path),
            )

            with (
                patch("kantrip.session.ssl.get_default_verify_paths", return_value=paths),
                patch(
                    "kantrip.session._PLATFORM_CA_BUNDLE_CANDIDATES",
                    (trusted_path,),
                ),
            ):
                bundle = _oauth_trust_bundle("PROFILE ROOT\n")

        self.assertEqual("SYSTEM ROOT\nPROFILE ROOT\n", bundle)
        self.assertNotIn("INHERITED ROOT", bundle)

    def test_oauth_trust_bundle_fails_when_system_roots_are_unavailable(self) -> None:
        paths = Mock(openssl_cafile=None)
        with (
            patch("kantrip.session.ssl.get_default_verify_paths", return_value=paths),
            patch("kantrip.session._PLATFORM_CA_BUNDLE_CANDIDATES", ()),
            self.assertRaisesRegex(SessionError, "could not be located or read"),
        ):
            _oauth_trust_bundle("PROFILE ROOT\n")

    def test_kaskade_apicurio_official_shared_ca_needs_no_new_release(self) -> None:
        connection = RegistryConnection(
            "apicurio",
            "https://registry.invalid/apis/registry/v3",
            "apicurio.registry.url",
            auth_type="oauth",
            ca_certificates="shared-ca",
            oauth=OAuthConnection(
                "https://idp.invalid/token",
                "registry-client",
                (),
                "secret-reference",
            ),
        )

        with patch("kantrip.adapters.subprocess.run") as run:
            require_kaskade_apicurio_security_support(
                "kaskade",
                connection,
                environment={"PATH": "/opt/bin"},
            )

        run.assert_not_called()

    def test_kaskade_apicurio_scopes_accept_kaskade_5_0_1(self) -> None:
        connection = RegistryConnection(
            "apicurio",
            "https://registry.invalid/apis/registry/v3",
            "apicurio.registry.url",
            auth_type="oauth",
            oauth=OAuthConnection(
                "https://idp.invalid/token",
                "registry-client",
                ("registry.read",),
                "secret-reference",
            ),
        )
        version = subprocess.CompletedProcess(
            ["kaskade", "--version"],
            0,
            stdout="kaskade, version 5.0.1\n",
            stderr="",
        )

        with (
            patch("kantrip.adapters.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.adapters.subprocess.run", return_value=version),
        ):
            require_kaskade_apicurio_security_support(
                "kaskade",
                connection,
                environment={"PATH": "/opt/bin"},
            )

    def test_runs_command_with_generated_kcat_configuration(self) -> None:
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            config_path = Path(environment["KCAT_CONFIG"])
            observed["arguments"] = arguments
            observed["environment"] = environment
            observed["contents"] = config_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
            observed["session_directory"] = config_path.parent
            return subprocess.CompletedProcess(arguments, 23)

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            result = run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

        self.assertEqual(23, result)
        self.assertEqual(["kcat", "-L"], observed["arguments"])
        environment = observed["environment"]
        assert isinstance(environment, dict)
        self.assertEqual("local", environment["KANTRIP_PROFILE"])
        self.assertEqual("localhost:9092,localhost:9093", environment["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual(
            "bootstrap.servers=localhost:9092,localhost:9093\n" "security.protocol=PLAINTEXT\n",
            observed["contents"],
        )
        self.assertEqual(0o600, observed["mode"])
        session_directory = observed["session_directory"]
        assert isinstance(session_directory, Path)
        self.assertFalse(session_directory.exists())

    def test_removes_an_abandoned_stale_session_before_exec(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=0):
            abandoned = create_session_runtime(PROFILE_ID, 1, {})
        abandoned_path = abandoned.path
        abandoned._closed = True
        os.close(abandoned._lock_descriptor)
        os.close(abandoned._session_descriptor)
        os.close(abandoned._root_descriptor)

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kcat"], 0),
            ),
        ):
            result = run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

        self.assertEqual(0, result)
        self.assertFalse(abandoned_path.exists())

    def test_opens_configured_shell_when_command_is_omitted(self) -> None:
        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["/bin/bash"], 0),
            ) as run,
        ):
            run_profile_session("local", self.profile, [], environment={})

        self.assertEqual(["/bin/bash", "--rcfile"], run.call_args.args[0][:2])

    def test_rejects_a_nested_kantrip_session_before_launch(self) -> None:
        with (
            patch("kantrip.session.shutil.which") as which,
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, "session is already active"),
        ):
            run_profile_session(
                "local",
                self.profile,
                [],
                environment={
                    "KANTRIP_PROFILE": "development",
                    "KANTRIP_SESSION_ID": "existing-session",
                    "SHELL": sys.executable,
                },
            )

        which.assert_not_called()
        run.assert_not_called()

    def test_adapts_official_kafka_executable_names(self) -> None:
        adapters = {
            "kafka-console-consumer": "--consumer.config",
            "kafka-console-consumer.sh": "--consumer.config",
            "kafka-console-producer": "--producer.config",
            "kafka-console-producer.sh": "--producer.config",
            "kafka-topics": "--command-config",
            "kafka-topics.sh": "--command-config",
            "kafka-consumer-groups": "--command-config",
            "kafka-consumer-groups.sh": "--command-config",
            "kafka-configs": "--command-config",
            "kafka-configs.sh": "--command-config",
            "kafka-acls": "--command-config",
            "kafka-acls.sh": "--command-config",
            "kafka-broker-api-versions": "--command-config",
            "kafka-broker-api-versions.sh": "--command-config",
        }
        for executable, config_option in adapters.items():
            with self.subTest(executable=executable):
                observed: dict[str, object] = {}

                def inspect_run(
                    arguments: list[str],
                    *,
                    _observed: dict[str, object] = observed,
                    **options: object,
                ) -> subprocess.CompletedProcess:
                    environment = options["env"]
                    assert isinstance(environment, dict)
                    config_path = Path(environment["KAFKA_JAVA_CONFIG_FILE"])
                    _observed["arguments"] = arguments
                    _observed["contents"] = config_path.read_text(encoding="utf-8")
                    _observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
                    return subprocess.CompletedProcess(arguments, 0)

                with (
                    patch("kantrip.session.shutil.which", return_value=f"/opt/kafka/{executable}"),
                    patch("kantrip.session._run_child", side_effect=inspect_run),
                ):
                    run_profile_session(
                        "local", self.profile, [executable, "--list"], environment={}
                    )

                arguments = observed["arguments"]
                assert isinstance(arguments, list)
                self.assertEqual(
                    [
                        executable,
                        "--bootstrap-server",
                        "localhost:9092,localhost:9093",
                        config_option,
                    ],
                    arguments[:4],
                )
                self.assertEqual("kafka.properties", Path(arguments[4]).name)
                self.assertEqual(["--list"], arguments[5:])
                self.assertEqual(
                    "bootstrap.servers=localhost:9092,localhost:9093\n"
                    "security.protocol=PLAINTEXT\n",
                    observed["contents"],
                )
                self.assertEqual(0o600, observed["mode"])

    def test_tls_session_materializes_a_private_ca_and_verification_properties(self) -> None:
        self.profile["kafka"].update(
            {
                "bootstrapServers": ["broker.invalid:9093"],
                "transport": "tls",
                "tls": {"caCertificates": synthetic_pki().ca},
            }
        )
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            config_path = Path(environment["KCAT_CONFIG"])
            ca_path = config_path.parent / "kafka-ca.pem"
            java_path = Path(environment["KAFKA_JAVA_CONFIG_FILE"])
            observed["kcat"] = config_path.read_text(encoding="utf-8")
            observed["java"] = java_path.read_text(encoding="utf-8")
            observed["ca"] = ca_path.read_text(encoding="utf-8")
            observed["ca_mode"] = stat.S_IMODE(ca_path.stat().st_mode)
            observed["security_protocol"] = environment["KAFKA_SECURITY_PROTOCOL"]
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session("production", self.profile, ["kcat", "-L"], environment={})

        self.assertIn("security.protocol=SSL\n", observed["kcat"])
        self.assertIn("enable.ssl.certificate.verification=true\n", observed["kcat"])
        self.assertIn("ssl.endpoint.identification.algorithm=https\n", observed["kcat"])
        self.assertIn("ssl.ca.location=", observed["kcat"])
        self.assertIn("security.protocol=SSL\n", observed["java"])
        self.assertIn("ssl.endpoint.identification.algorithm=https\n", observed["java"])
        self.assertIn("ssl.truststore.type=PEM\n", observed["java"])
        self.assertEqual(synthetic_pki().ca, observed["ca"])
        self.assertEqual(0o600, observed["ca_mode"])
        self.assertEqual("SSL", observed["security_protocol"])

    def test_mtls_java_configuration_escapes_pem_and_keeps_private_files(self) -> None:
        pki = synthetic_pki()
        resolved = KafkaConnection(
            ("localhost:9095",),
            "tls",
            pki.ca,
            "mtls",
            client_certificate=pki.client_certificate,
            private_key=pki.client_key,
        )
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            session = Path(environment["KANTRIP_SESSION_DIR"])
            java_path = Path(environment["KAFKA_JAVA_CONFIG_FILE"])
            observed["java"] = java_path.read_text(encoding="utf-8")
            observed["certificate_mode"] = stat.S_IMODE(
                (session / "kafka-client.crt").stat().st_mode
            )
            observed["key_mode"] = stat.S_IMODE((session / "kafka-client.key").stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/kafka/kafka-topics"),
            patch("kantrip.adapters.require_java_pem_support"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "mtls",
                self.profile,
                ["kafka-topics", "--list"],
                environment={},
                resolved_kafka=resolved,
            )

        java = observed["java"]
        assert isinstance(java, str)
        self.assertIn("ssl.keystore.type=PEM\n", java)
        self.assertIn("ssl.keystore.certificate.chain=-----BEGIN CERTIFICATE-----\\n", java)
        self.assertIn("ssl.keystore.key=-----BEGIN PRIVATE KEY-----\\n", java)
        self.assertNotIn("\nMI", java)
        self.assertEqual(0o600, observed["certificate_mode"])
        self.assertEqual(0o600, observed["key_mode"])

    def test_custom_ca_rejects_java_clients_older_than_kafka_2_7_before_operation(
        self,
    ) -> None:
        self.profile["kafka"].update(
            {
                "transport": "tls",
                "tls": {"caCertificates": synthetic_pki().ca},
            }
        )
        for version in ("2.13-2.6.0", "6.0.4"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as root:
                client = Path(root) / "kafka-topics"
                client.write_text(
                    "#!/bin/sh\n"
                    f"if [ \"${{1-}}\" = --version ]; then printf '{version}\\n'; exit 0; fi\n"
                    "printf 'CLIENT_LAUNCHED\\n'\n",
                    encoding="utf-8",
                )
                client.chmod(0o700)
                with (
                    patch("kantrip.session._run_child") as run,
                    self.assertRaisesRegex(SessionError, "does not support PEM trust stores"),
                ):
                    run_profile_session(
                        "production",
                        self.profile,
                        ["kafka-topics", "--list"],
                        environment={"PATH": root},
                    )

                run.assert_not_called()

    def test_custom_ca_accepts_kafka_2_7_java_clients(self) -> None:
        self.profile["kafka"].update(
            {
                "transport": "tls",
                "tls": {"caCertificates": synthetic_pki().ca},
            }
        )
        for version in ("2.13-2.7.0", "4.3.0", "6.1.0", "8.3.1"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as root:
                client = Path(root) / "kafka-topics"
                client.write_text(
                    f"#!/bin/sh\nprintf '{version}\\n'\n",
                    encoding="utf-8",
                )
                client.chmod(0o700)
                with patch(
                    "kantrip.session._run_child",
                    return_value=subprocess.CompletedProcess([str(client)], 0),
                ) as run:
                    result = run_profile_session(
                        "production",
                        self.profile,
                        ["kafka-topics", "--list"],
                        environment={"PATH": root},
                    )

                self.assertEqual(0, result)
                run.assert_called_once()

    def test_custom_ca_subshell_shim_rejects_an_unverified_java_client(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            client = root_path / "kafka-topics"
            client.write_text(
                "#!/bin/sh\n"
                "if [ \"${1-}\" = --version ]; then printf 'unknown\\n'; exit 0; fi\n"
                "printf 'CLIENT_LAUNCHED\\n'\n",
                encoding="utf-8",
            )
            client.chmod(0o700)
            shim_directory = create_subshell_shims(
                root_path / "bin",
                bootstrap_servers="localhost:9093",
                java_config_path=root_path / "kafka.properties",
                kcat_config_path=root_path / "kcat.conf",
                kaskade_config_path=root_path / "kaskade.ini",
                kaskade_registry_config_path=root_path / "kaskade-registry.ini",
                environment={"PATH": root},
                require_java_pem=True,
            )

            result = subprocess.run(
                [shim_directory / "kafka-topics", "--list"],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(2, result.returncode)
        self.assertIn("could not verify", result.stderr)
        self.assertNotIn("CLIENT_LAUNCHED", result.stdout)

    def test_oauth_java_subshell_shim_preserves_only_owned_kafka_opts(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            client = root_path / "kafka-topics"
            client.write_text(
                "#!/bin/sh\n"
                "if [ \"${1-}\" = --version ]; then printf '4.0.0\\n'; exit 0; fi\n"
                'printf \'%s|%s\\n\' "${KAFKA_OPTS-unset}" "${JAVA_TOOL_OPTIONS-unset}"\n',
                encoding="utf-8",
            )
            client.chmod(0o700)
            shim_directory = create_subshell_shims(
                root_path / "bin",
                bootstrap_servers="localhost:9096",
                java_config_path=root_path / "kafka.properties",
                kcat_config_path=root_path / "kcat.conf",
                kaskade_config_path=root_path / "kaskade.ini",
                kaskade_registry_config_path=root_path / "kaskade-registry.ini",
                environment={"PATH": root},
                kafka_auth_type="oauth",
            )

            result = subprocess.run(
                [shim_directory / "kafka-topics", "--list"],
                env={
                    "KAFKA_OPTS": "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp",
                    "JAVA_TOOL_OPTIONS": "-Duntrusted=true",
                },
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(0, result.returncode)
        self.assertEqual(
            "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp|unset\n",
            result.stdout,
        )

    def test_official_kafka_commands_cannot_override_profile_connection_options(self) -> None:
        cases = (
            ("kafka-console-consumer", "--consumer.config=other.properties"),
            ("kafka-console-producer", "--producer.config=other.properties"),
            ("kafka-console-producer", "--broker-list=other:9092"),
            ("kafka-topics", "--bootstrap-server=other:9092"),
            ("kafka-topics", "--command-config=other.properties"),
            ("kafka-topics", "--zookeeper=other:2181"),
            ("kafka-consumer-groups", "--zookeeper=other:2181"),
            ("kafka-configs", "--zookeeper=other:2181"),
            ("kafka-configs", "--bootstrap-controller=other:9093"),
            ("kafka-acls", "--authorizer=example.Authorizer"),
            ("kafka-acls", "--authorizer-properties=zookeeper.connect=other:2181"),
            ("kafka-acls", "--bootstrap-controller=other:9093"),
            ("kafka-broker-api-versions", "--command-config=other.properties"),
        )
        for executable, option in cases:
            with (
                self.subTest(executable=executable, option=option),
                patch("kantrip.session.shutil.which", return_value=f"/opt/kafka/{executable}"),
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session("local", self.profile, [executable, option], environment={})

    def test_official_kafka_commands_keep_safe_runtime_and_resource_options(self) -> None:
        cases = (
            ("kafka-console-consumer", "--consumer-property", "group.id=manual-readers"),
            ("kafka-console-consumer", "--property", "print.key=true"),
            ("kafka-configs", "--add-config", "retention.ms=1000"),
        )
        for executable, option, value in cases:
            with (
                self.subTest(executable=executable, option=option),
                patch("kantrip.session.shutil.which", return_value=f"/opt/kafka/{executable}"),
                patch(
                    "kantrip.session._run_child",
                    return_value=subprocess.CompletedProcess([executable], 0),
                ) as run,
            ):
                run_profile_session(
                    "local", self.profile, [executable, option, value], environment={}
                )
                self.assertIn(value, run.call_args.args[0])

    def test_adapts_all_schema_registry_console_commands(self) -> None:
        adapters = {
            "kafka-avro-console-consumer": (
                "--consumer.config",
                "--formatter-config",
                "--formatter-property",
            ),
            "kafka-avro-console-producer": (
                "--producer.config",
                "--reader-config",
                "--reader-property",
            ),
            "kafka-json-schema-console-consumer": (
                "--consumer.config",
                "--formatter-config",
                "--formatter-property",
            ),
            "kafka-json-schema-console-producer": (
                "--producer.config",
                "--reader-config",
                "--reader-property",
            ),
            "kafka-protobuf-console-consumer": (
                "--consumer.config",
                "--formatter-config",
                "--formatter-property",
            ),
            "kafka-protobuf-console-producer": (
                "--producer.config",
                "--reader-config",
                "--reader-property",
            ),
        }
        for executable, options in adapters.items():
            with self.subTest(executable=executable):
                config_option, auxiliary_config, auxiliary_property = options
                observed: dict[str, object] = {}

                def inspect_run(
                    arguments: list[str],
                    *,
                    _observed: dict[str, object] = observed,
                    **options: object,
                ) -> subprocess.CompletedProcess:
                    environment = options["env"]
                    assert isinstance(environment, dict)
                    registry_config = Path(environment["SCHEMA_REGISTRY_CONFIG_FILE"])
                    java_registry_config = Path(arguments[4])
                    _observed["arguments"] = arguments
                    _observed["environment"] = environment
                    _observed["contents"] = registry_config.read_text(encoding="utf-8")
                    _observed["java_contents"] = java_registry_config.read_text(encoding="utf-8")
                    _observed["mode"] = stat.S_IMODE(registry_config.stat().st_mode)
                    return subprocess.CompletedProcess(arguments, 0)

                with (
                    patch(
                        "kantrip.session.shutil.which", return_value=f"/opt/confluent/{executable}"
                    ),
                    patch("kantrip.session._run_child", side_effect=inspect_run),
                ):
                    run_profile_session(
                        "local", self.profile, [executable, "--topic", "orders"], environment={}
                    )

                arguments = observed["arguments"]
                assert isinstance(arguments, list)
                self.assertEqual(
                    [
                        executable,
                        "--bootstrap-server",
                        "localhost:9092,localhost:9093",
                        config_option,
                    ],
                    arguments[:4],
                )
                self.assertEqual("schema-registry-kafka.properties", Path(arguments[4]).name)
                self.assertEqual(auxiliary_config, arguments[5])
                self.assertEqual("schema-registry-kafka.properties", Path(arguments[6]).name)
                self.assertEqual(auxiliary_property, arguments[7])
                self.assertEqual(
                    "schema.registry.url=http://registry.invalid:8081",
                    arguments[8],
                )
                self.assertEqual(["--topic", "orders"], arguments[9:])
                environment = observed["environment"]
                assert isinstance(environment, dict)
                self.assertEqual("http://registry.invalid:8081", environment["SCHEMA_REGISTRY_URL"])
                self.assertEqual(
                    "provider=confluent\nurl=http://registry.invalid:8081\n",
                    observed["contents"],
                )
                self.assertIn(
                    "schema.registry.url=http://registry.invalid:8081\n",
                    observed["java_contents"],
                )
                self.assertEqual(0o600, observed["mode"])

    def test_confluent_console_reads_basic_secret_only_from_private_config(self) -> None:
        password_reference = secret_reference(PROFILE_ID, "registry/password")
        self.profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.invalid:8081",
            "auth": {
                "type": "basic",
                "username": "registry-user",
                "passwordRef": password_reference,
            },
        }
        store = Mock()
        store.get.return_value = "registry-password"
        observed: dict[str, str] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            config_path = Path(arguments[4])
            observed["arguments"] = " ".join(arguments)
            observed["config"] = config_path.read_text(encoding="utf-8")
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch(
                "kantrip.session.shutil.which",
                return_value="/opt/confluent/kafka-avro-console-consumer",
            ),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kafka-avro-console-consumer", "--topic", "orders"],
                environment={},
                secret_store=store,
            )

        self.assertNotIn("registry-password", observed["arguments"])
        self.assertIn(
            "schema.registry.basic.auth.user.info=registry-user:registry-password\n",
            observed["config"],
        )
        self.assertIn(
            "schema.registry.basic.auth.credentials.source=USER_INFO\n",
            observed["config"],
        )

    def test_confluent_console_oauth_owns_java_allowlist_and_pem_trust(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/oauth/client-secret")
        ca = synthetic_pki().ca
        self.profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.invalid:8081",
            "tls": {"caCertificates": ca},
            "auth": {
                "type": "oauth",
                "tokenUrl": "https://idp.invalid/token",
                "clientId": "registry-client",
                "scopes": ["registry.read"],
                "clientSecretRef": reference,
                "caCertificates": ca,
                "logicalCluster": "lsrc-synthetic",
            },
        }
        store = Mock()
        store.get.return_value = "registry-client-secret"
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            config = Path(arguments[4]).read_text(encoding="utf-8")
            observed["arguments"] = arguments
            observed["environment"] = environment
            observed["config"] = config
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch(
                "kantrip.session.shutil.which",
                return_value="/opt/confluent/kafka-avro-console-consumer",
            ),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kafka-avro-console-consumer", "--topic", "orders"],
                environment={"SCHEMA_REGISTRY_OPTS": "-Duntrusted=true"},
                secret_store=store,
            )

        arguments = observed["arguments"]
        assert isinstance(arguments, list)
        self.assertNotIn("registry-client-secret", " ".join(arguments))
        self.assertIn(
            "schema.registry.url=https://registry.invalid:8081",
            arguments,
        )
        config = observed["config"]
        assert isinstance(config, str)
        self.assertIn("schema.registry.ssl.truststore.type=PEM\n", config)
        self.assertIn("schema.registry.ssl.truststore.location=", config)
        self.assertNotIn("schema.registry.ssl.ca.location=", config)
        environment = observed["environment"]
        assert isinstance(environment, dict)
        allowed = "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://idp.invalid/token"
        self.assertEqual(allowed, environment["KAFKA_OPTS"])
        self.assertEqual(allowed, environment["SCHEMA_REGISTRY_OPTS"])

    def test_kaskade_reads_native_apicurio_basic_from_private_ini(self) -> None:
        password_reference = secret_reference(PROFILE_ID, "registry/password")
        self.profile["registry"] = {
            "provider": "apicurio",
            "apicurio.registry.url": "https://registry.invalid/apis/registry/v3",
            "auth": {
                "type": "basic",
                "username": "registry-user",
                "passwordRef": password_reference,
            },
        }
        store = Mock()
        store.get.return_value = "registry-password"
        observed: dict[str, str] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            observed["arguments"] = " ".join(arguments)
            observed["config"] = Path(arguments[3]).read_text(encoding="utf-8")
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kaskade", "consumer", "--topic", "orders", "--value=registry"],
                environment={},
                secret_store=store,
            )

        self.assertNotIn("registry-password", observed["arguments"])
        self.assertIn("apicurio.registry.auth.password=registry-password\n", observed["config"])
        self.assertIn("apicurio.registry.auth.username=registry-user\n", observed["config"])

    def test_kaskade_uses_official_shared_apicurio_oauth_trust(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/oauth/client-secret")
        ca = synthetic_pki().ca
        self.profile["registry"] = {
            "provider": "apicurio",
            "apicurio.registry.url": "https://registry.invalid/apis/registry/v3",
            "tls": {"caCertificates": ca},
            "auth": {
                "type": "oauth",
                "tokenUrl": "https://idp.invalid/token",
                "clientId": "registry-client",
                "scopes": ["registry.read"],
                "clientSecretRef": reference,
                "caCertificates": ca,
            },
        }
        store = Mock()
        store.get.return_value = "registry-client-secret"
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            config_path = Path(arguments[3])
            properties = dict(
                line.split("=", 1)
                for line in config_path.read_text(encoding="utf-8").splitlines()
                if "=" in line
            )
            registry_ca = Path(properties["apicurio.registry.tls.certificates"])
            observed["arguments"] = arguments
            observed["properties"] = properties
            observed["ca_path"] = registry_ca
            observed["ca_mode"] = stat.S_IMODE(registry_ca.stat().st_mode)
            observed["ca_content"] = registry_ca.read_text(encoding="utf-8")
            return subprocess.CompletedProcess(arguments, 0)

        version = subprocess.CompletedProcess(
            ["kaskade", "--version"],
            0,
            stdout="kaskade, version 5.0.1\n",
            stderr="",
        )
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.adapters.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.adapters.subprocess.run", return_value=version),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kaskade", "consumer", "--topic", "orders", "-v", "registry"],
                environment={"PATH": "/opt/bin"},
                secret_store=store,
            )

        arguments = observed["arguments"]
        assert isinstance(arguments, list)
        self.assertNotIn("registry-client-secret", " ".join(arguments))
        properties = observed["properties"]
        assert isinstance(properties, dict)
        self.assertEqual(
            "registry.read",
            properties["apicurio.registry.auth.client.scope"],
        )
        registry_ca = observed["ca_path"]
        assert isinstance(registry_ca, Path)
        self.assertEqual(0o600, observed["ca_mode"])
        self.assertEqual(ca, observed["ca_content"])
        self.assertFalse(registry_ca.exists())

    def test_schema_registry_commands_reject_connection_property_overrides(self) -> None:
        cases = (
            ("--property", "schema.registry.url=http://other.invalid:8081"),
            ("--property", " schema.registry.url =http://other.invalid:8081"),
            ("--property=schema.registry.url=http://other.invalid:8081",),
            ("--formatter-property", "schema.registry.url=http://other.invalid:8081"),
            ("--reader-property=schema.registry.bearer.auth.token=other",),
            ("--reader-config=other.properties",),
            ("--command-property=bootstrap.servers=other.invalid:9092",),
            ("--producer-property", "bootstrap.servers=other.invalid:9092"),
            ("--consumer-property=bootstrap.servers=other.invalid:9092",),
            ("--producer.config=other.properties",),
            ("--command-config=other.properties",),
            ("--formatter-property", "sasl.jaas.config=other"),
            ("--reader-property", "ssl.truststore.location=other"),
        )
        for arguments in cases:
            with (
                self.subTest(arguments=arguments),
                patch("kantrip.session.shutil.which", return_value="/opt/confluent/client"),
                patch("kantrip.session._run_child") as run,
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    ["kafka-avro-console-producer", *arguments],
                    environment={},
                )
            run.assert_not_called()

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/confluent/client"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, "cannot override"),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kafka-avro-console-consumer", "--formatter-config=other.properties"],
                environment={},
            )
        run.assert_not_called()

    def test_schema_registry_consumer_keeps_runtime_group_id(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/confluent/client"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kafka-avro-console-consumer"], 0),
            ) as run,
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kafka-avro-console-consumer", "--consumer-property", "group.id=readers"],
                environment={},
            )
        self.assertIn("group.id=readers", run.call_args.args[0])

    def test_schema_registry_command_requires_registry_configuration(self) -> None:
        del self.profile["registry"]
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/confluent/client"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, "requires a registry section"),
        ):
            run_profile_session(
                "local", self.profile, ["kafka-avro-console-consumer"], environment={}
            )
        run.assert_not_called()

    def test_secure_registry_profile_uses_the_selected_client_before_launch(self) -> None:
        self.profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.invalid",
        }
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/confluent/client"),
            patch("kantrip.session._run_child") as run,
        ):
            run_profile_session(
                "local", self.profile, ["kafka-protobuf-console-consumer"], environment={}
            )
        run.assert_called_once()

    def test_unsupported_registry_mapping_blocks_only_the_selected_client(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/oauth/client-secret")
        oauth_ca = synthetic_pki().ca
        self.profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.invalid",
            "auth": {
                "type": "oauth",
                "tokenUrl": "https://idp.invalid/token",
                "clientId": "registry-client",
                "scopes": [],
                "clientSecretRef": reference,
                "caCertificates": oauth_ca,
                "logicalCluster": "lsrc-synthetic",
            },
        }
        store = Mock()
        store.get.return_value = "registry-client-secret"

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kcat", "-L"], 0),
            ) as run,
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kcat", "-L"],
                environment={},
                secret_store=store,
            )
        run.assert_called_once()

        observed: dict[str, object] = {}

        def inspect_kaskade(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            bundle_path = Path(environment["SSL_CERT_FILE"])
            observed["bundle_path"] = bundle_path
            observed["bundle"] = bundle_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(bundle_path.stat().st_mode)
            observed["ssl_cert_dir"] = environment.get("SSL_CERT_DIR")
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.session._run_child", side_effect=inspect_kaskade),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kaskade", "consumer", "--topic", "orders", "--value=registry"],
                environment={
                    "SSL_CERT_FILE": "/untrusted/inherited.pem",
                    "SSL_CERT_DIR": "/untrusted/certs",
                },
                secret_store=store,
            )

        self.assertIn(oauth_ca.strip(), observed["bundle"])
        self.assertEqual(0o600, observed["mode"])
        self.assertIsNone(observed["ssl_cert_dir"])
        bundle_path = observed["bundle_path"]
        assert isinstance(bundle_path, Path)
        self.assertFalse(bundle_path.exists())

        with (
            patch(
                "kantrip.session.shutil.which",
                return_value="/opt/confluent/kafka-avro-console-consumer",
            ),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, "independent CA bundles"),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kafka-avro-console-consumer", "--topic", "orders"],
                environment={},
                secret_store=store,
            )
        run.assert_not_called()

    def test_registry_shims_reject_missing_and_native_profiles_without_launching_clients(
        self,
    ) -> None:
        for registry, expected in (
            (None, "requires a registry section"),
            (
                RegistryConnection(
                    "apicurio",
                    "http://registry.invalid/apis/registry/v3",
                    "apicurio.registry.url",
                ),
                "supports only Confluent-compatible registry profiles",
            ),
        ):
            with self.subTest(registry=registry), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                client_path = root_path / "client"
                client_path.write_text("#!/bin/sh\nprintf CLIENT_LAUNCHED\n", encoding="utf-8")
                client_path.chmod(0o700)

                def find_executable(
                    name: str, *, _client_path: Path = client_path, **options: object
                ) -> str | None:
                    del options
                    return str(_client_path) if name in SCHEMA_REGISTRY_EXECUTABLES else None

                with patch("kantrip.adapters.shutil.which", side_effect=find_executable):
                    shim_directory = create_subshell_shims(
                        root_path / "bin",
                        bootstrap_servers="localhost:9092",
                        java_config_path=root_path / "kafka.properties",
                        kcat_config_path=root_path / "kcat.conf",
                        kaskade_config_path=root_path / "kaskade.ini",
                        kaskade_registry_config_path=root_path / "kaskade-registry.ini",
                        environment={"PATH": str(root_path)},
                        registry=registry,
                    )

                for executable in SCHEMA_REGISTRY_EXECUTABLES:
                    result = subprocess.run(
                        [shim_directory / executable], capture_output=True, text=True, check=False
                    )
                    self.assertEqual(2, result.returncode)
                    self.assertIn(expected, result.stderr)
                    self.assertNotIn("CLIENT_LAUNCHED", result.stdout)

    def test_missing_registry_does_not_inherit_registry_environment(self) -> None:
        del self.profile["registry"]
        observed: dict[str, str] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            observed.update(environment)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kcat", "-L"],
                environment={
                    "APICURIO_REGISTRY_URL": "http://apicurio.invalid",
                    "APICURIO_REGISTRY_CONFIG_FILE": "/tmp/apicurio.properties",
                    "SCHEMA_REGISTRY_URL": "http://other.invalid",
                    "SCHEMA_REGISTRY_CONFIG_FILE": "/tmp/other.properties",
                },
            )

        self.assertNotIn("SCHEMA_REGISTRY_URL", observed)
        self.assertNotIn("SCHEMA_REGISTRY_CONFIG_FILE", observed)
        self.assertNotIn("APICURIO_REGISTRY_URL", observed)
        self.assertNotIn("APICURIO_REGISTRY_CONFIG_FILE", observed)

    def test_adapts_kaskade_admin_and_consumer_with_a_private_ini_file(self) -> None:
        for command, command_arguments in (
            ("admin", []),
            ("consumer", ["--topic", "orders"]),
        ):
            with self.subTest(command=command):
                observed: dict[str, object] = {}

                def inspect_run(
                    arguments: list[str],
                    *,
                    _observed: dict[str, object] = observed,
                    **options: object,
                ) -> subprocess.CompletedProcess:
                    environment = options["env"]
                    assert isinstance(environment, dict)
                    config_path = Path(arguments[3])
                    _observed["arguments"] = arguments
                    _observed["contents"] = config_path.read_text(encoding="utf-8")
                    _observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
                    _observed["environment"] = environment
                    return subprocess.CompletedProcess(arguments, 0)

                with (
                    patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
                    patch("kantrip.session._run_child", side_effect=inspect_run),
                ):
                    run_profile_session(
                        "local",
                        self.profile,
                        ["kaskade", command, *command_arguments],
                        environment={},
                    )

                arguments = observed["arguments"]
                assert isinstance(arguments, list)
                self.assertEqual(["kaskade", command, "--config-file"], arguments[:3])
                self.assertEqual("kaskade.ini", Path(arguments[3]).name)
                self.assertEqual(command_arguments, arguments[4:])
                self.assertEqual(
                    "[kafka]\n"
                    "bootstrap.servers=localhost:9092,localhost:9093\n"
                    "security.protocol=PLAINTEXT\n",
                    observed["contents"],
                )
                self.assertEqual(0o600, observed["mode"])
                environment = observed["environment"]
                assert isinstance(environment, dict)
                self.assertNotIn("KASKADE_CLIENT_CONFIG", environment)

    def test_kaskade_registry_deserializer_uses_profile_registry_config(self) -> None:
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            config_path = Path(arguments[3])
            observed["arguments"] = arguments
            observed["contents"] = config_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kaskade", "consumer", "--topic", "orders", "-v", "registry"],
                environment={},
            )

        arguments = observed["arguments"]
        assert isinstance(arguments, list)
        self.assertEqual("kaskade-registry.ini", Path(arguments[3]).name)
        self.assertEqual(["--topic", "orders", "-v", "registry"], arguments[4:])
        self.assertEqual(
            "[kafka]\n"
            "bootstrap.servers=localhost:9092,localhost:9093\n"
            "security.protocol=PLAINTEXT\n"
            "\n[registry]\n"
            "provider=confluent\n"
            "url=http://registry.invalid:8081\n",
            observed["contents"],
        )
        self.assertEqual(0o600, observed["mode"])

    def test_kaskade_uses_native_apicurio_registry_configuration(self) -> None:
        self.profile["registry"] = {
            "provider": "apicurio",
            "apicurio.registry.url": "http://registry.invalid/apis/registry/v3",
        }
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            config_path = Path(arguments[3])
            registry_config_path = Path(environment["APICURIO_REGISTRY_CONFIG_FILE"])
            observed["contents"] = config_path.read_text(encoding="utf-8")
            observed["registry_contents"] = registry_config_path.read_text(encoding="utf-8")
            observed["environment"] = environment
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kaskade", "consumer", "--topic", "orders", "-v", "registry"],
                environment={"SCHEMA_REGISTRY_URL": "http://inherited.invalid"},
            )

        self.assertIn(
            "apicurio.registry.url=http://registry.invalid/apis/registry/v3\n"
            "provider=apicurio\n",
            observed["contents"],
        )
        self.assertEqual(
            "apicurio.registry.url=http://registry.invalid/apis/registry/v3\n"
            "provider=apicurio\n",
            observed["registry_contents"],
        )
        environment = observed["environment"]
        assert isinstance(environment, dict)
        self.assertEqual(
            "http://registry.invalid/apis/registry/v3",
            environment["APICURIO_REGISTRY_URL"],
        )
        self.assertNotIn("SCHEMA_REGISTRY_URL", environment)

    def test_confluent_only_adapters_reject_native_apicurio_profiles(self) -> None:
        self.profile["registry"] = {
            "provider": "apicurio",
            "apicurio.registry.url": "http://registry.invalid/apis/registry/v3",
        }
        for command in (
            ["kcat", "-C", "-s", "value=avro", "-t", "orders"],
            ["kafka-avro-console-consumer", "--topic", "orders"],
        ):
            with (
                self.subTest(command=command),
                patch("kantrip.session.shutil.which", return_value=f"/opt/bin/{command[0]}"),
                patch("kantrip.session._run_child") as run,
                self.assertRaisesRegex(
                    SessionError, "supports only Confluent-compatible registry profiles"
                ),
            ):
                run_profile_session("local", self.profile, command, environment={})
            run.assert_not_called()

    def test_registry_deserializers_require_a_configured_profile_registry(self) -> None:
        commands = (
            ["kcat", "-C", "-s", "value=avro", "-t", "orders"],
            ["kaskade", "consumer", "-t", "orders", "-v", "registry"],
        )
        for command in commands:
            for registry, expected in ((None, "requires a registry section"),):
                with (
                    self.subTest(command=command, registry=registry),
                    patch("kantrip.session.shutil.which", return_value=f"/opt/bin/{command[0]}"),
                    patch("kantrip.session._run_child") as run,
                    self.assertRaisesRegex(SessionError, expected),
                ):
                    if registry is None:
                        del self.profile["registry"]
                    else:
                        self.profile["registry"] = registry
                    run_profile_session("local", self.profile, command, environment={})
                run.assert_not_called()
                self.profile["registry"] = {
                    "provider": "confluent",
                    "schema.registry.url": "http://registry.invalid:8081",
                }

    def test_kaskade_cannot_override_profile_connection_options(self) -> None:
        for option in (
            "-bother:9092",
            "--bootstrap-servers=other:9092",
            "--config-file=other.ini",
            "--kafka=bootstrap.servers=other:9092",
            "--kafka=security.protocol=PLAINTEXT",
            "--kafka=broker.address.family=invalid",
            "--kafka=group.id=",
            "--registry=url=http://other.invalid:8081",
        ):
            with (
                self.subTest(option=option),
                patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session(
                    "local", self.profile, ["kaskade", "admin", option], environment={}
                )

    def test_kaskade_accepts_explicit_group_and_ip_family(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kaskade"], 0),
            ) as run,
        ):
            run_profile_session(
                "local",
                self.profile,
                [
                    "kaskade",
                    "consumer",
                    "--topic",
                    "orders",
                    "--kafka",
                    "group.id=orders-readers",
                    "--kafka=broker.address.family=v4",
                ],
                environment={},
            )

        arguments = run.call_args.args[0]
        self.assertIn("group.id=orders-readers", arguments)
        self.assertIn("--kafka=broker.address.family=v4", arguments)

    def test_kaskade_root_options_are_not_adapted(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kaskade", "--help"], 0),
            ) as run,
        ):
            run_profile_session("local", self.profile, ["kaskade", "--help"], environment={})

        self.assertEqual(["kaskade", "--help"], run.call_args.args[0])

    def test_interactive_shell_contains_official_kafka_shims(self) -> None:
        observed: dict[str, object] = {}

        adapters = {
            "kafka-console-consumer": "--consumer.config",
            "kafka-console-producer.sh": "--producer.config",
            "kafka-topics": "--command-config",
            "kafka-consumer-groups.sh": "--command-config",
            "kafka-configs": "--command-config",
            "kafka-acls.sh": "--command-config",
            "kafka-broker-api-versions": "--command-config",
        }

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            if executable in adapters:
                return f"/opt/kafka/bin/{executable}"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim_directory = Path(environment["PATH"].split(":", 1)[0])
            observed["directory"] = shim_directory
            observed["shims"] = {
                path.name: path.read_text(encoding="utf-8") for path in shim_directory.iterdir()
            }
            observed["modes"] = {
                path.name: stat.S_IMODE(path.stat().st_mode) for path in shim_directory.iterdir()
            }
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", self.profile, [], environment={"SHELL": "/bin/bash", "PATH": "/bin"}
            )

        shims = observed["shims"]
        assert isinstance(shims, dict)
        self.assertEqual(set(adapters), set(shims))
        for name, contents in shims.items():
            self.assertIn(f"exec /opt/kafka/bin/{name}", contents)
            self.assertIn("--bootstrap-server localhost:9092,localhost:9093", contents)
            self.assertIn(adapters[name], contents)
        self.assertEqual({name: 0o700 for name in adapters}, observed["modes"])
        directory = observed["directory"]
        assert isinstance(directory, Path)
        self.assertFalse(directory.exists())

    def test_interactive_shell_contains_a_kaskade_shim(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            if executable == "kaskade":
                return "/opt/bin/kaskade"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim_path = Path(environment["PATH"].split(":", 1)[0]) / "kaskade"
            observed["directory"] = shim_path.parent
            observed["contents"] = shim_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(shim_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", self.profile, [], environment={"SHELL": "/bin/bash", "PATH": "/bin"}
            )

        self.assertIn("exec /opt/bin/kaskade", observed["contents"])
        self.assertIn("--config-file", observed["contents"])
        self.assertEqual(0o700, observed["mode"])
        directory = observed["directory"]
        assert isinstance(directory, Path)
        self.assertFalse(directory.exists())

    def test_kaskade_shim_scopes_registry_oauth_trust_to_the_client(self) -> None:
        reference = secret_reference(PROFILE_ID, "registry/oauth/client-secret")
        self.profile["registry"] = {
            "provider": "confluent",
            "schema.registry.url": "https://registry.invalid",
            "auth": {
                "type": "oauth",
                "tokenUrl": "https://idp.invalid/token",
                "clientId": "registry-client",
                "scopes": [],
                "clientSecretRef": reference,
                "caCertificates": synthetic_pki().ca,
                "logicalCluster": "lsrc-synthetic",
            },
        }
        store = Mock()
        store.get.return_value = "registry-client-secret"
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == "/bin/bash":
                return executable
            if executable == "kaskade":
                return "/opt/bin/kaskade"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim_path = Path(environment["PATH"].split(":", 1)[0]) / "kaskade"
            observed["contents"] = shim_path.read_text(encoding="utf-8")
            observed["environment"] = environment
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch("kantrip.session.shutil.which", side_effect=find_executable),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                self.profile,
                [],
                environment={
                    "SHELL": "/bin/bash",
                    "PATH": "/bin",
                    "SSL_CERT_FILE": "/parent/custom.pem",
                },
                secret_store=store,
            )

        contents = observed["contents"]
        assert isinstance(contents, str)
        self.assertIn("unset SSL_CERT_FILE SSL_CERT_DIR", contents)
        self.assertIn("export SSL_CERT_FILE=", contents)
        self.assertNotIn("/parent/custom.pem", contents)
        environment = observed["environment"]
        assert isinstance(environment, dict)
        self.assertEqual("/parent/custom.pem", environment["SSL_CERT_FILE"])

    def test_zsh_restores_shims_after_loading_user_configuration(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == "/bin/zsh":
                return executable
            if executable == "kafka-topics":
                return "/opt/kafka/bin/kafka-topics"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            startup_path = Path(environment["ZDOTDIR"]) / ".zshrc"
            observed["arguments"] = arguments
            observed["environment"] = environment
            observed["contents"] = startup_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(startup_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with tempfile.TemporaryDirectory() as home:
            user_startup = Path(home) / ".zshrc"
            user_startup.write_text('export PATH="/opt/homebrew/bin:$PATH"\n', encoding="utf-8")
            with (
                patch("kantrip.session.shutil.which", side_effect=find_executable),
                patch("kantrip.adapters.shutil.which", side_effect=find_executable),
                patch("kantrip.session._run_child", side_effect=inspect_run),
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    [],
                    environment={"SHELL": "/bin/zsh", "PATH": "/bin", "HOME": home},
                )

        self.assertEqual(["/bin/zsh"], observed["arguments"])
        environment = observed["environment"]
        assert isinstance(environment, dict)
        self.assertEqual("1", environment["SHELL_SESSIONS_DISABLE"])
        contents = observed["contents"]
        assert isinstance(contents, str)
        history_position = contents.index(f"HISTFILE={Path(home) / '.zsh_history'}")
        source_position = contents.index(f"source {user_startup}")
        path_position = contents.index("export PATH=")
        self.assertLess(history_position, source_position)
        self.assertLess(source_position, path_position)
        self.assertIn("unset SHELL_SESSIONS_DISABLE", contents)
        self.assertNotIn("fc -R", contents)
        self.assertIn("/bin", contents)
        self.assertTrue(contents.endswith("rehash\n"))
        self.assertEqual(0o600, observed["mode"])

    def test_bash_uses_a_session_rcfile_that_restores_shims_last(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == "/bin/bash":
                return executable
            if executable == "kaskade":
                return "/opt/bin/kaskade"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            observed["arguments"] = arguments
            startup_path = Path(arguments[-1])
            observed["contents"] = startup_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(startup_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with tempfile.TemporaryDirectory() as home:
            user_startup = Path(home) / ".bashrc"
            user_startup.write_text('export PATH="/opt/homebrew/bin:$PATH"\n', encoding="utf-8")
            with (
                patch("kantrip.session.shutil.which", side_effect=find_executable),
                patch("kantrip.adapters.shutil.which", side_effect=find_executable),
                patch("kantrip.session._run_child", side_effect=inspect_run),
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    [],
                    environment={"SHELL": "/bin/bash", "PATH": "/bin", "HOME": home},
                )

        arguments = observed["arguments"]
        assert isinstance(arguments, list)
        self.assertEqual(["/bin/bash", "--rcfile"], arguments[:2])
        contents = observed["contents"]
        assert isinstance(contents, str)
        source_position = contents.index(f"source {user_startup}")
        path_position = contents.index("export PATH=")
        self.assertLess(source_position, path_position)
        self.assertTrue(contents.endswith("hash -r\n"))
        self.assertEqual(0o600, observed["mode"])

    def test_missing_command_is_an_actionable_error(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value=None),
            self.assertRaisesRegex(SessionError, "command 'kcat' was not found"),
        ):
            run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

    def test_kcat_cannot_override_generated_configuration(self) -> None:
        for arguments in (
            ("-F", "other.conf"),
            ("-r", "http://other.invalid:8081"),
            ("-X", "schema.registry.url=http://other.invalid:8081"),
            ("-b", "other.invalid:9092"),
            ("-bother.invalid:9092",),
            ("-X", "security.protocol=PLAINTEXT"),
            ("-Xsasl.username=other",),
            ("-X", "dump"),
            ("-LXdump",),
            ("-Lbother.invalid:9092",),
            ("-LFother.conf",),
            ("-Lrhttp://other.invalid:8081",),
            ("-L", "-Xdump"),
        ):
            with (
                self.subTest(arguments=arguments),
                patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session("local", self.profile, ["kcat", *arguments], environment={})

    def test_kcat_unknown_option_fails_closed(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            self.assertRaisesRegex(SessionError, "not supported by Kantrip"),
        ):
            run_profile_session("local", self.profile, ["kcat", "-Y"], environment={})

    def test_kcat_keeps_safe_group_and_address_family_properties(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kcat"], 0),
            ) as run,
        ):
            run_profile_session(
                "local",
                self.profile,
                ["kcat", "-C", "-X", "group.id=readers", "-Xbroker.address.family=v4"],
                environment={},
            )
        self.assertIn("group.id=readers", run.call_args.args[0])

    def test_kcat_parses_clusters_without_treating_values_as_options(self) -> None:
        for arguments in (
            ("-CqXgroup.id=readers",),
            ("-C", "-t", "topic-b-Xdump"),
            ("-Cf", "payload -b -Xdump"),
            ("-C", "--", "-Xdump"),
        ):
            with (
                self.subTest(arguments=arguments),
                patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
                patch(
                    "kantrip.session._run_child",
                    return_value=subprocess.CompletedProcess(["kcat"], 0),
                ) as run,
            ):
                run_profile_session("local", self.profile, ["kcat", *arguments], environment={})
                self.assertEqual(["kcat", *arguments], run.call_args.args[0])

    def test_kcat_avro_deserializer_uses_profile_registry_url(self) -> None:
        for arguments in (("-C", "-s", "value=avro"), ("-Csvalue=avro",)):
            with (
                self.subTest(arguments=arguments),
                patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
                patch(
                    "kantrip.session._run_child",
                    return_value=subprocess.CompletedProcess(["kcat"], 0),
                ) as run,
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    ["kcat", *arguments, "-t", "orders"],
                    environment={},
                )
                self.assertEqual(
                    ["kcat", "-r", "http://registry.invalid:8081", *arguments, "-t", "orders"],
                    run.call_args.args[0],
                )

    def test_authenticated_profile_resolves_once_and_launches(self) -> None:
        profile_id = "018f8f13-7c21-7cee-8000-000000000010"
        self.profile["id"] = profile_id
        self.profile["kafka"]["transport"] = "tls"
        self.profile["kafka"]["auth"] = {
            "type": "plain",
            "username": "synthetic-user",
            "passwordRef": secret_reference(profile_id, "kafka/password"),
        }
        store = Mock()
        store.get.return_value = "synthetic-password"
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch(
                "kantrip.session._run_child",
                return_value=subprocess.CompletedProcess(["kcat"], 0),
            ) as run,
        ):
            result = run_profile_session(
                "local",
                self.profile,
                ["kcat", "-L"],
                environment={},
                secret_store=store,
            )

        self.assertEqual(0, result)
        store.get.assert_called_once()
        run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
