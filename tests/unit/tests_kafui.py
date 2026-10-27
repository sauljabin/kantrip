"""The kafui adapter: kaf config, private home, Registry, argument policy, gate, and shim (#144)."""

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml

from kantrip.adapter_policy import AdapterError, check_kafui_arguments
from kantrip.adapters import KAFUI_ADAPTER, ClientVersion, require_minimum_version
from kantrip.go_build_info import go_build_info
from kantrip.kafka import KafkaConnection
from kantrip.oauth import OAuthConnection
from kantrip.registry import RegistryConnection
from kantrip.secret_value import Secret
from kantrip.session import SessionError, run_profile_session
from tests.unit.client_versions import (
    fake_go_executable,
    go_build_info_blob,
    use_supported_client_versions,
)
from tests.unit.pki import KEY_PASSWORD, synthetic_pki

PROFILE_ID = "018f8f13-7c21-7cee-8000-000000000144"
PROFILE: dict[str, Any] = {
    "id": PROFILE_ID,
    "kafka": {
        "bootstrapServers": ["localhost:9092"],
        "transport": "plaintext",
        "auth": {"type": "none"},
    },
}
KAFUI_MODULE = "path\tgithub.com/Benny93/kafui\nmod\tgithub.com/Benny93/kafui\t{version}\t\n"


class KafuiSessionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        runtime_directory = tempfile.TemporaryDirectory()
        self.addCleanup(runtime_directory.cleanup)
        runtime_patch = patch(
            "kantrip.runtime.tempfile.gettempdir", return_value=runtime_directory.name
        )
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        use_supported_client_versions(self)

    def run_kafui(
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
            home = Path(environment["HOME"])
            observed.update(
                arguments=command,
                environment=environment,
                umask=options["umask"],
                config=yaml.safe_load(config.read_text(encoding="utf-8")),
                mode=stat.S_IMODE(config.stat().st_mode),
                home=home,
                home_mode=stat.S_IMODE(home.stat().st_mode),
                home_entries=sorted(path.name for path in home.iterdir()),
                session=session,
                files={
                    path.name: path.read_text(encoding="utf-8")
                    for path in session.iterdir()
                    if path.is_file()
                },
            )
            return subprocess.CompletedProcess(command, 0)

        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kafui"),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            result = run_profile_session(
                "local",
                PROFILE,
                arguments,
                environment={"HOME": "/Users/someone", "XDG_CONFIG_HOME": "/Users/someone/.cfg"},
                resolved_kafka=resolved,
                resolved_registry=registry,
            )
        self.assertEqual(0, result)
        return observed

    def assert_refused(
        self,
        message: str,
        arguments: list[str] | None = None,
        resolved: KafkaConnection | None = None,
        registry: RegistryConnection | None = None,
    ) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kafui"),
            patch("kantrip.session._run_child") as run,
            self.assertRaisesRegex(SessionError, message),
        ):
            run_profile_session(
                "local",
                PROFILE,
                arguments or ["kafui", "health"],
                environment={},
                resolved_kafka=resolved,
                resolved_registry=registry,
            )
        run.assert_not_called()


class TestKafuiSession(KafuiSessionTestCase):
    def test_plaintext_profile_is_the_only_cluster_under_an_empty_private_home(self) -> None:
        observed = self.run_kafui(["kafui", "get", "brokers"])

        session = observed["session"]
        home = session / "kafui-home"
        self.assertEqual(
            ["kafui", "--config", str(session / "kaf.yaml"), "get", "brokers"],
            observed["arguments"],
        )
        self.assertEqual(
            {
                "current-cluster": "kantrip",
                "clusters": [{"name": "kantrip", "brokers": ["localhost:9092"]}],
            },
            observed["config"],
        )
        self.assertEqual(0o600, observed["mode"])
        # The user's kafui config, kaf config, and preferences are out of reach.
        self.assertEqual(home, observed["home"])
        self.assertEqual(str(home / ".config"), observed["environment"]["XDG_CONFIG_HOME"])
        self.assertEqual(0o700, observed["home_mode"])
        self.assertEqual([], observed["home_entries"])
        self.assertEqual(0o077, observed["umask"])
        self.assertFalse(session.exists())

    def test_scram_and_plain_use_sasl_ssl_with_the_private_ca(self) -> None:
        pki = synthetic_pki()
        for auth_type, mechanism in (
            ("plain", "PLAIN"),
            ("scram-sha-256", "SCRAM-SHA-256"),
            ("scram-sha-512", "SCRAM-SHA-512"),
        ):
            with self.subTest(auth_type=auth_type):
                resolved = KafkaConnection(
                    ("broker.invalid:9094",),
                    "tls",
                    pki.ca,
                    auth_type,
                    username="synthetic-user",
                    password=Secret("synthetic-password"),
                )

                observed = self.run_kafui(["kafui", "health"], resolved)

                cluster = observed["config"]["clusters"][0]
                ca_path = str(observed["session"] / "kafka-ca.pem")
                self.assertEqual("SASL_SSL", cluster["security-protocol"])
                self.assertEqual(
                    {
                        "mechanism": mechanism,
                        "username": "synthetic-user",
                        "password": "synthetic-password",
                    },
                    cluster["SASL"],
                )
                self.assertEqual({"insecure": False, "cafile": ca_path}, cluster["TLS"])
                self.assertEqual(pki.ca, observed["files"]["kafka-ca.pem"])
                self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))
                self.assertNotIn("synthetic-password", json.dumps(observed["environment"]))

    def test_system_trust_tls_keeps_verification_without_a_ca_file(self) -> None:
        observed = self.run_kafui(
            ["kafui", "health"], KafkaConnection(("broker.invalid:9093",), "tls")
        )

        cluster = observed["config"]["clusters"][0]
        self.assertEqual({"insecure": False}, cluster["TLS"])
        self.assertNotIn("security-protocol", cluster)

    def test_mtls_reads_a_decrypted_key_from_a_private_file(self) -> None:
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

        observed = self.run_kafui(["kafui", "health"], resolved)

        session = observed["session"]
        cluster = observed["config"]["clusters"][0]
        self.assertNotIn("security-protocol", cluster)
        self.assertEqual(str(session / "kafka-client.crt"), cluster["TLS"]["clientfile"])
        self.assertEqual(str(session / "client-unencrypted.key"), cluster["TLS"]["clientkeyfile"])
        self.assertEqual(pki.client_key, observed["files"]["client-unencrypted.key"])
        self.assertNotIn("kafka-client.key", observed["files"])

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
        self.assert_refused(
            "kafui does not support Kafka authentication 'oauth'", resolved=resolved
        )

    def test_connection_overrides_fail_before_launch(self) -> None:
        self.assert_refused(
            "cannot override the selected Kantrip profile", ["kafui", "-c", "other", "health"]
        )


class TestKafuiRegistry(KafuiSessionTestCase):
    def test_unauthenticated_registry_sets_only_the_url(self) -> None:
        registry = RegistryConnection("confluent", "http://registry.invalid:8081", "")

        observed = self.run_kafui(["kafui", "health"], registry=registry)

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

        observed = self.run_kafui(["kafui", "health"], registry=registry)

        cluster = observed["config"]["clusters"][0]
        self.assertEqual(
            {"username": "synthetic-user", "password": "synthetic-password"},
            cluster["schema-registry-credentials"],
        )
        self.assertNotIn("synthetic-password", " ".join(observed["arguments"]))

    def test_registries_kafui_cannot_reach_safely_fail_before_launch(self) -> None:
        pki = synthetic_pki()
        cases = (
            (
                RegistryConnection("apicurio", "https://registry.invalid/apis/registry/v3", ""),
                "kafui supports only Confluent-compatible registry profiles",
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
                "kafui trusts only the system CA store for the Registry",
            ),
            (
                RegistryConnection(
                    "confluent", "https://registry.invalid", "", "token", token=Secret("t")
                ),
                "kafui does not support Registry authentication 'token'",
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
                "kafui does not support Registry authentication 'mtls'",
            ),
        )
        for registry, message in cases:
            with self.subTest(message=message):
                self.assert_refused(message, registry=registry)


class TestKafuiUserSettings(unittest.TestCase):
    """A real child sees neither the user's kaf nor kafui config, and its files stay private."""

    def test_user_configs_stay_unread_and_unchanged_and_the_session_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            user_home = root / "home"
            user_configs = {
                user_home / ".kaf" / "config": "current-cluster: decoy\nclusters:\n- name: decoy\n",
                user_home
                / ".config"
                / "kafui"
                / "config.yaml": ("clusters:\n  decoy:\n    brokers: [decoy.invalid:9092]\n"),
            }
            for path, contents in user_configs.items():
                path.parent.mkdir(parents=True)
                path.write_text(contents, encoding="utf-8")
            before = {path: path.stat().st_mtime_ns for path in user_configs}
            clients = root / "clients"
            clients.mkdir()
            record = root / "record.json"
            _write_fake_kafui(clients / "kafui", record)
            runtime = root / "runtime"
            runtime.mkdir()

            with (
                patch("kantrip.runtime.tempfile.gettempdir", return_value=str(runtime)),
                patch(
                    "kantrip.adapters._version_output",
                    return_value="mod\tgithub.com/Benny93/kafui\tv0.1.38\t\n",
                ),
            ):
                result = run_profile_session(
                    "local",
                    PROFILE,
                    ["kafui", "health"],
                    environment={
                        "HOME": str(user_home),
                        "XDG_CONFIG_HOME": str(user_home / ".config"),
                        "PATH": f"{clients}{os.pathsep}{os.defpath}",
                    },
                )

            self.assertEqual(0, result)
            observed = json.loads(record.read_text(encoding="utf-8"))
            self.assertNotEqual(str(user_home), observed["home"])
            self.assertEqual([False, False], observed["user_configs_visible"])
            self.assertIn("current-cluster: kantrip", observed["config"])
            self.assertNotIn("decoy", observed["config"])
            # kafui writes its log as 0750/0640; the session mask makes it 0700/0600,
            # so cleanup removes the whole session.
            self.assertEqual([0o700, 0o600], observed["written_modes"])
            self.assertEqual([], list((runtime / f"kantrip-{os.getuid()}" / "sessions").iterdir()))
            self.assertEqual(before, {path: path.stat().st_mtime_ns for path in user_configs})


class TestKafuiShim(KafuiSessionTestCase):
    def test_interactive_shim_selects_the_config_under_the_private_home(self) -> None:
        observed: dict[str, Any] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            return "/opt/bin/kafui" if executable == "kafui" else None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim = Path(environment["PATH"].split(":", 1)[0]) / "kafui"
            observed["contents"] = shim.read_text(encoding="utf-8")
            observed["session"] = Path(environment["KANTRIP_SESSION_DIR"])
            observed["home_is_directory"] = (observed["session"] / "kafui-home").is_dir()
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", PROFILE, [], environment={"SHELL": "/bin/bash", "PATH": "/bin"}
            )

        session = observed["session"]
        home = session / "kafui-home"
        self.assertIn("umask 077\n", observed["contents"])
        self.assertIn(
            f"HOME={home} XDG_CONFIG_HOME={home}/.config exec /opt/bin/kafui "
            f'--config {session}/kaf.yaml "$@"',
            observed["contents"],
        )
        self.assertIn("kantrip._adapter_guard kafui", observed["contents"])
        self.assertTrue(observed["home_is_directory"])

    def test_shim_reports_the_capability_error_for_an_oauth_profile(self) -> None:
        profile = {
            "id": PROFILE_ID,
            "kafka": {
                "bootstrapServers": ["broker.invalid:9096"],
                "transport": "tls",
                "auth": {"type": "oauth"},
            },
        }
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
        contents: list[str] = []

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim = Path(environment["PATH"].split(":", 1)[0]) / "kafui"
            contents.append(shim.read_text(encoding="utf-8"))
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.resolve_interactive_shell", return_value="/bin/bash"),
            patch(
                "kantrip.adapters.shutil.which",
                side_effect=lambda name, **_: "/opt/bin/kafui" if name == "kafui" else None,
            ),
            patch("kantrip.session._run_child", side_effect=inspect_run),
        ):
            run_profile_session(
                "local",
                profile,
                [],
                environment={"SHELL": "/bin/bash", "PATH": "/bin"},
                resolved_kafka=resolved,
            )

        guard, _, launch = contents[0].partition("exit 2\n")
        self.assertIn("kafui does not support Kafka authentication", guard)
        self.assertIn("exec /opt/bin/kafui", launch)


class TestKafuiArgumentPolicy(unittest.TestCase):
    def test_connection_options_are_rejected_in_every_form(self) -> None:
        for arguments in (
            ["--config", "other.yaml"],
            ["--config=other.yaml", "health"],
            ["-b", "other:9092"],
            ["-bother:9092", "get", "brokers"],
            ["--brokers=other:9092"],
            ["-vb", "other:9092"],
            ["-c", "other"],
            ["-cother", "health"],
            ["--cluster", "other"],
            ["--schema-registry", "http://other.invalid"],
            ["health", "--schema-registry=http://other.invalid"],
            # An option-like value is checked anyway: a rejection beats a bypass.
            ["--topic", "-b", "other:9092"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(
                    AdapterError, "kafui option .* cannot override the selected Kantrip profile"
                ),
            ):
                check_kafui_arguments("kafui", arguments)

    def test_everyday_options_and_commands_are_accepted(self) -> None:
        for arguments in (
            [],
            ["--read-only"],
            ["--read-only", "--resource", "topics"],
            ["--topic", "orders"],
            ["--topic=config"],
            ["-v", "--resource", "consumer-groups"],
            ["--metrics-listen", "127.0.0.1:9090"],
            ["health"],
            ["get", "brokers", "--format", "csv"],
            ["--mock", "get", "brokers"],
            ["version"],
            ["--help"],
            ["health", "--", "-b"],
        ):
            with self.subTest(arguments=arguments):
                self.assertIsNone(check_kafui_arguments("kafui", arguments))


class TestKafuiReleaseGate(unittest.TestCase):
    """kafui's release comes from its Go build information, never from running it."""

    def test_module_releases_from_the_floor_on_are_accepted(self) -> None:
        for version, expected in (("v0.1.38", (0, 1, 38)), ("v0.2.0", (0, 2, 0))):
            with self.subTest(version=version):
                self.assertEqual(ClientVersion(expected), self._gate(version))

    def test_older_development_or_pseudo_versions_are_rejected(self) -> None:
        for version, message in (
            ("v0.1.37", "kafui 0.1.37 is not supported; install kafui 0.1.38 or newer"),
            ("(devel)", "could not verify the installed kafui version"),
            ("v0.1.39-0.20261020101010-abcdef123456", "kafui 0.1.39-0.2026"),
        ):
            with self.subTest(version=version), self.assertRaisesRegex(AdapterError, message):
                self._gate(version)

    def test_a_client_without_go_build_information_cannot_be_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = Path(directory) / "kafui"
            client.write_text("#!/bin/sh\necho 'kafui v0.1.38'\n", encoding="utf-8")
            client.chmod(0o700)
            with self.assertRaisesRegex(AdapterError, "could not verify the installed kafui"):
                require_minimum_version(
                    "kafui", KAFUI_ADAPTER.minimum_version, environment={"PATH": directory}
                )

    def _gate(self, version: str) -> ClientVersion:
        with tempfile.TemporaryDirectory() as directory:
            client = Path(directory) / "kafui"
            client.write_bytes(
                fake_go_executable("#!/bin/sh\nexit 1\n", KAFUI_MODULE.format(version=version))
            )
            client.chmod(0o700)
            return require_minimum_version(
                "kafui", KAFUI_ADAPTER.minimum_version, environment={"PATH": directory}
            )


class TestGoBuildInfo(unittest.TestCase):
    def test_reads_the_toolchain_and_module_lines(self) -> None:
        info = self._read(b"\0" * 64 + go_build_info_blob("mod\texample.invalid/tool\tv1.2.3\t\n"))

        self.assertEqual("go1.24.13\nmod\texample.invalid/tool\tv1.2.3\t\n", info)

    def test_rejects_unaligned_pointer_format_truncated_and_unframed_information(self) -> None:
        blob = go_build_info_blob("mod\texample.invalid/tool\tv1.2.3\t\n")
        pointer_format = blob[:15] + b"\0" + blob[16:]
        unframed = go_build_info_blob("no trailing newline")
        for contents in (
            b"\0" * 8 + blob,
            pointer_format,
            blob[:40],
            unframed,
            b"",
            b"#!/bin/sh\n",
        ):
            with self.subTest(contents=contents[:20]):
                self.assertIsNone(self._read(contents))

    def test_a_missing_file_has_no_information(self) -> None:
        self.assertIsNone(go_build_info("/definitely/not/a/kantrip-client"))

    def _read(self, contents: bytes) -> str | None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "client"
            path.write_bytes(contents)
            return go_build_info(str(path))


def _write_fake_kafui(path: Path, record: Path) -> None:
    """Write a kafui stand-in that records what it sees and writes as kafui v0.1.38 does."""
    path.write_text(
        f"""#!{sys.executable}
import json, os, stat, sys
from pathlib import Path

home = Path(os.environ["HOME"])
config = Path(sys.argv[sys.argv.index("--config") + 1])
logs = home / ".kafui"
os.makedirs(logs, 0o750)
log = logs / "kafui.log"
os.close(os.open(log, os.O_CREAT | os.O_WRONLY, 0o640))
Path({str(record)!r}).write_text(json.dumps({{
    "home": str(home),
    "user_configs_visible": [
        (home / ".kaf" / "config").exists(),
        (Path(os.environ["XDG_CONFIG_HOME"]) / "kafui" / "config.yaml").exists(),
    ],
    "config": config.read_text(encoding="utf-8"),
    "written_modes": [stat.S_IMODE(logs.stat().st_mode), stat.S_IMODE(log.stat().st_mode)],
}}))
""",
        encoding="utf-8",
    )
    path.chmod(0o700)


if __name__ == "__main__":
    unittest.main()
