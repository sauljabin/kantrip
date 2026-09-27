"""Every supported client's minimum version and feature checks (#93)."""

import subprocess
import threading
import unittest
from unittest.mock import patch

from kantrip.adapters import (
    CLIENT_ADAPTERS,
    JAVA_CLI_ADAPTER,
    KAF_ADAPTER,
    KASKADE_ADAPTER,
    KCAT_ADAPTER,
    KCL_ADAPTER,
    AdapterError,
    ClientVersion,
    ReleaseGate,
    VersionProbe,
    require_adapter_capability,
    require_minimum_version,
)
from tests.e2e.preconditions import _versions, kcat_pin, parse_pin

_KCAT_OUTPUT = (
    "kcat - Apache Kafka producer and consumer tool\n"
    "Version {version} (JSON, Avro, Transactions, IncrementalAssign, librdkafka {librdkafka} "
    "builtin.features=ssl,sasl,sasl_oauthbearer,oidc)\n"
)

_JAVA_FLOOR = "install Apache Kafka 2.6 or Confluent Platform 6.0 or newer"
_REGISTRY_FLOOR = "install Confluent Platform 5.5 or newer"


def _kcat(version: str, librdkafka: str = "2.15.1") -> str:
    return _KCAT_OUTPUT.format(version=version, librdkafka=librdkafka)


class TestClientFloors(unittest.TestCase):
    """Each family passes from its floor on and rejects older, suffixed, or unreadable builds."""

    # (executable, accepted outputs, (rejected output, message) pairs)
    CASES = (
        (
            "kcat",
            (_kcat("1.7.0", "2.3.0"), _kcat("1.7.1"), _kcat("2.0.0")),
            (
                (_kcat("1.6.0"), "kcat 1.6.0 is not supported; install kcat 1.7.0 or newer"),
                (_kcat("1.7.0-dirty"), "kcat 1.7.0-dirty is not supported"),
                ("kcat - Apache Kafka producer and consumer tool\n", "could not verify"),
            ),
        ),
        (
            "kafkacat",
            (_kcat("1.7.0"),),
            ((_kcat("1.5.0"), "kafkacat 1.5.0 is not supported; install kcat 1.7.0 or newer"),),
        ),
        (
            "kaskade",
            ("kaskade, version 5.0.1\n", "Kaskade, version 6.0.0\n"),
            (
                (
                    "kaskade, version 5.0.0\n",
                    "kaskade 5.0.0 is not supported; install Kaskade 5.0.1 or newer",
                ),
                ("kaskade, version 5.0.2.dev5\n", "kaskade 5.0.2.dev5 is not supported"),
                ("kaskade\n", "could not verify the installed kaskade version"),
            ),
        ),
        (
            "kaf",
            ("kaf version 0.2.14 (Homebrew)\n", "kaf version 0.3.0 (abc1234)\n"),
            (
                (
                    "kaf version 0.2.13 (Homebrew)\n",
                    "kaf 0.2.13 is not supported; install kaf 0.2.14 or newer",
                ),
                ("kaf version 0.2.15-rc1 (abc1234)\n", "kaf 0.2.15-rc1 is not supported"),
                ("kaf version dev (none)\n", "could not verify the installed kaf version"),
            ),
        ),
        (
            "kcl",
            ("kcl version v0.20.0\n",),
            (("kcl version v0.19.0\n", "install kcl 0.20.0 or newer"),),
        ),
        (
            "kafka-topics.sh",
            ("2.6.0 (Commit:62abe01bee039651)\n", "2.13-2.6.0\n", "4.3.1\n"),
            (
                (
                    "2.5.1 (Commit:0efa8fb0f4c73d92)\n",
                    f"kafka-topics.sh 2.5.1 is not supported; {_JAVA_FLOOR}",
                ),
                ("unknown\n", "could not verify the installed kafka-topics.sh version"),
            ),
        ),
        (
            "kafka-topics",
            ("6.0.0-ccs (Commit:abc)\n", "8.3.1-ce\n"),
            (
                (
                    "5.5.4-ccs\n",
                    f"kafka-topics 5.5.4-ccs is not supported; {_JAVA_FLOOR}",
                ),
            ),
        ),
        (
            "kafka-avro-console-consumer",
            ("5.5.0-ccs\n", "8.3.1-ccs\n"),
            (
                (
                    "5.4.1-ccs\n",
                    f"kafka-avro-console-consumer 5.4.1-ccs is not supported; {_REGISTRY_FLOOR}",
                ),
                ("3.9.0\n", _REGISTRY_FLOOR),
                ("OpenJDK 64-Bit Server VM warning\n", "could not verify"),
            ),
        ),
    )

    def test_every_family_declares_a_floor(self) -> None:
        for adapter in CLIENT_ADAPTERS:
            with self.subTest(adapter=adapter.name):
                self.assertIsNotNone(adapter.minimum_version)

    def test_releases_from_the_floor_on_are_accepted(self) -> None:
        for executable, accepted, _ in self.CASES:
            for output in accepted:
                with self.subTest(executable=executable, output=output):
                    self.assertIsInstance(self._check(executable, output), ClientVersion)

    def test_older_suffixed_or_unreadable_builds_are_rejected(self) -> None:
        for executable, _, rejected in self.CASES:
            for output, message in rejected:
                with (
                    self.subTest(executable=executable, output=output),
                    self.assertRaisesRegex(AdapterError, message),
                ):
                    self._check(executable, output)

    def test_a_failed_version_run_cannot_be_verified(self) -> None:
        failed = subprocess.CompletedProcess(["kaf", "--version"], 1, "kaf version 0.2.14\n", "")
        with (
            patch("kantrip.adapters.shutil.which", return_value="/opt/bin/kaf"),
            patch("kantrip.adapters.subprocess.run", return_value=failed),
            self.assertRaisesRegex(AdapterError, "could not verify the installed kaf version"),
        ):
            require_minimum_version(
                "kaf", KAF_ADAPTER.minimum_version, environment={"PATH": "/opt/bin"}
            )

    def _check(self, executable: str, output: str) -> ClientVersion:
        adapter = next(adapter for adapter in CLIENT_ADAPTERS if executable in adapter.executables)
        with (
            patch("kantrip.adapters.shutil.which", return_value=f"/opt/bin/{executable}"),
            patch("kantrip.adapters._version_output", return_value=output),
        ):
            return require_minimum_version(
                executable, adapter.minimum_version, environment={"PATH": "/opt/bin"}
            )


class TestKcatOAuthCertificateAuthority(unittest.TestCase):
    """kcat trusts a profile's OAuth token-endpoint CA only through librdkafka 2.11.0+."""

    def test_oauth_token_endpoint_ca_needs_librdkafka_2_11(self) -> None:
        for librdkafka, oauth_ca, expected in (
            ("2.11.0", True, None),
            ("2.15.1", True, None),
            ("2.10.1", True, "kcat links librdkafka 2.10.1, which cannot trust"),
            ("2.3.0", False, None),
        ):
            with self.subTest(librdkafka=librdkafka, oauth_ca=oauth_ca):
                error = self._decide(_kcat("1.7.0", librdkafka), oauth_ca=oauth_ca)
                if expected is None:
                    self.assertIsNone(error)
                else:
                    self.assertIn(expected, error or "")
                    self.assertIn("install librdkafka 2.11.0 or newer", error or "")

    def test_an_unidentified_librdkafka_cannot_take_an_oauth_ca(self) -> None:
        error = self._decide("kcat\nVersion 1.7.0 (JSON)\n", oauth_ca=True)
        self.assertIn("links an unidentified librdkafka", error or "")

    def _decide(self, output: str, *, oauth_ca: bool) -> str | None:
        with (
            patch("kantrip.adapters.shutil.which", return_value="/opt/bin/kcat"),
            patch("kantrip.adapters._version_output", return_value=output),
        ):
            try:
                require_adapter_capability(
                    "kcat",
                    auth_type="oauth",
                    custom_pem=False,
                    oauth_ca=oauth_ca,
                    environment={"PATH": "/opt/bin"},
                )
            except AdapterError as error:
                return str(error)
        return None


class TestVersionProbe(unittest.TestCase):
    """Each client is probed once per launch; Java install directories share a run."""

    def test_prefetch_runs_each_client_once_and_java_once_per_directory(self) -> None:
        calls: list[tuple[str, str]] = []
        lock = threading.Lock()

        def version_output(resolved: str, option: str, environment: object) -> str:
            del environment
            with lock:
                calls.append((resolved, option))
            return f"{resolved}\n"

        versions = VersionProbe({"PATH": "/opt"})
        java = JAVA_CLI_ADAPTER.minimum_version
        clients = [
            (KCAT_ADAPTER.minimum_version, "/opt/bin/kcat"),
            (KCAT_ADAPTER.minimum_version, "/opt/bin/kcat"),
            (java, "/opt/kafka/bin/kafka-topics.sh"),
            (java, "/opt/kafka/bin/kafka-configs.sh"),
            (java, "/opt/confluent/bin/kafka-topics"),
            (java, "/opt/confluent/bin/kafka-avro-console-consumer"),
        ]
        with patch("kantrip.adapters._version_output", side_effect=version_output):
            versions.prefetch(clients)
            for gate, resolved in clients:
                versions.output(gate, resolved)

        self.assertEqual(
            [
                ("/opt/bin/kcat", "-V"),
                ("/opt/confluent/bin/kafka-topics", "--version"),
                ("/opt/kafka/bin/kafka-topics.sh", "--version"),
            ],
            sorted(calls),
        )

    def test_output_is_none_when_the_client_cannot_run(self) -> None:
        versions = VersionProbe({})
        gate = KCL_ADAPTER.minimum_version
        with patch("kantrip.adapters.subprocess.run", side_effect=OSError("exec format error")):
            self.assertIsNone(versions.output(gate, "/opt/bin/kcl"))
        with patch(
            "kantrip.adapters.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["kcl", "--version"], 10),
        ):
            self.assertIsNone(versions.output(gate, "/opt/other/kcl"))


class TestEndToEndPins(unittest.TestCase):
    """The E2E suite never pins a client below the product floor."""

    def test_every_pin_meets_its_floor(self) -> None:
        versions = _versions()
        for adapter, name, pin in (
            (KASKADE_ADAPTER, "kaskade", versions["KASKADE_VERSION"]),
            (KAF_ADAPTER, "kaf", versions["KAF_VERSION"]),
            (KCL_ADAPTER, "kcl", versions["KCL_VERSION"]),
            (KCAT_ADAPTER, "kcat", versions["KCAT_VERSION"]),
            (KCAT_ADAPTER, "kcat", versions["KCAT_MACOS_VERSION"]),
            (KCAT_ADAPTER, "kcat", kcat_pin(versions)),
            (JAVA_CLI_ADAPTER, "kafka-topics.sh", versions["APACHE_KAFKA_VERSION"]),
            (JAVA_CLI_ADAPTER, "kafka-topics", versions["CONFLUENT_VERSION"]),
            (JAVA_CLI_ADAPTER, "kafka-avro-console-consumer", versions["CONFLUENT_VERSION"]),
        ):
            with self.subTest(name=name, pin=pin):
                self.assertTrue(
                    adapter.minimum_version.supports(name, ClientVersion(parse_pin(pin)))
                )

    def test_librdkafka_pin_can_trust_an_oauth_token_endpoint_ca(self) -> None:
        self.assertGreaterEqual(parse_pin(_versions()["LIBRDKAFKA_MIN_VERSION"]), (2, 11, 0))

    def test_release_gates_render_their_floor(self) -> None:
        gate = KASKADE_ADAPTER.minimum_version
        assert isinstance(gate, ReleaseGate)
        self.assertEqual("Kaskade 5.0.1 or newer", gate.requirement("kaskade"))
