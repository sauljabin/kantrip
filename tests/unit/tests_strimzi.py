import base64
import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from kantrip.cli import cli
from kantrip.profile_imports import MAX_IMPORT_BYTES, ProfileImportError, read_import_source
from kantrip.profile_storage import load_profiles
from kantrip.strimzi import parse_strimzi_secret
from tests.unit.pki import synthetic_pki
from tests.unit.tests_cli import _MemorySecretStore

SCRAM_PASSWORD = "synthetic-strimzi-password"


def _encoded(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def _secret(data: dict[str, str], *, name: str = "app-user", **labels: str) -> dict[str, Any]:
    """Build a Secret shaped like the Strimzi user operator's output."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "namespace": "kafka",
            "labels": {
                "app.kubernetes.io/instance": "app-user",
                "app.kubernetes.io/managed-by": "strimzi-user-operator",
                "strimzi.io/cluster": "synthetic",
                "strimzi.io/kind": "KafkaUser",
                **labels,
            },
        },
        "data": {key: _encoded(value) for key, value in data.items()},
    }


def _scram_secret() -> dict[str, Any]:
    return _secret(
        {
            "password": SCRAM_PASSWORD,
            "sasl.jaas.config": "synthetic generated JAAS",
        }
    )


def _tls_secret() -> dict[str, Any]:
    pki = synthetic_pki()
    return _secret(
        {
            "ca.crt": pki.wrong_ca,
            "user.crt": pki.client_certificate,
            "user.key": pki.client_key,
            "user.p12": "synthetic keystore",
            "user.password": "synthetic-keystore-password",
        }
    )


def _yaml(document: object) -> str:
    return str(yaml.safe_dump(document, sort_keys=False))


class TestStrimziSecretParsing(unittest.TestCase):
    def test_scram_secret_imports_scram_sha_512_over_tls(self) -> None:
        imported = parse_strimzi_secret(_yaml(_scram_secret()))

        self.assertEqual(("tls", "scram-sha-512"), (imported.transport, imported.auth_type))
        self.assertEqual("app-user", imported.username)
        self.assertIsNotNone(imported.password)
        self.assertEqual(SCRAM_PASSWORD, imported.password.reveal() if imported.password else "")
        self.assertIsNone(imported.client_certificate)

    def test_username_comes_from_the_instance_label_despite_a_secret_prefix(self) -> None:
        imported = parse_strimzi_secret(_yaml(_secret({"password": "p"}, name="team-app-user")))

        self.assertEqual("app-user", imported.username)

    def test_tls_secret_imports_mtls_and_ignores_the_clients_ca(self) -> None:
        pki = synthetic_pki()

        imported = parse_strimzi_secret(json.dumps(_tls_secret()))

        self.assertEqual(("tls", "mtls"), (imported.transport, imported.auth_type))
        self.assertEqual(
            pki.client_certificate.strip(), (imported.client_certificate or "").strip()
        )
        self.assertIsNotNone(imported.private_key)
        self.assertIsNone(imported.username)

    def test_rejects_unsupported_documents_without_echoing_values(self) -> None:
        pki = synthetic_pki()
        scram = _yaml(_scram_secret())
        cases: dict[str, tuple[str, str]] = {
            "two documents": (scram + "---\n" + scram, "exactly one shallow document"),
            "duplicate YAML key": (scram + "kind: Secret\n", "duplicate"),
            "duplicate JSON key": (
                json.dumps(_scram_secret())[:-1] + ', "kind": "Secret"}',
                "duplicate",
            ),
            "alias": (
                "apiVersion: v1\nkind: Secret\nmetadata: &m {name: x}\nother: *m\n",
                "anchors or aliases",
            ),
            "deep nesting": ("a: " + "[" * 40 + "]" * 40 + "\n", "shallow"),
            "invalid YAML": ("apiVersion: [v1\n", "not valid JSON or YAML"),
            "invalid JSON": ('{"apiVersion": ', "not valid JSON or YAML"),
            "not a mapping": ("- v1\n", "one Kubernetes Secret object"),
            "KafkaUser": (_yaml({**_scram_secret(), "kind": "KafkaUser"}), "not the KafkaUser"),
            "List": (_yaml({"apiVersion": "v1", "kind": "List", "items": []}), "kind Secret"),
            "stringData": (
                _yaml({**_scram_secret(), "stringData": {"password": SCRAM_PASSWORD}}),
                "stringData",
            ),
            "TLS type": (_yaml({**_scram_secret(), "type": "kubernetes.io/tls"}), "type Opaque"),
            "unlabelled": (_yaml(_without_kind_label(_scram_secret())), "KafkaUser label"),
            "foreign name": (_yaml(_secret({"password": "p"}, name="other")), "disagree"),
            "mixed": (
                _yaml(_secret({"password": SCRAM_PASSWORD, "user.crt": pki.client_certificate})),
                "mixes SCRAM and TLS",
            ),
            "certificate only": (
                _yaml(_secret({"user.crt": pki.client_certificate})),
                "both user.crt and user.key",
            ),
            "mismatched key": (
                _yaml(
                    _secret(
                        {"user.crt": pki.client_certificate, "user.key": pki.mismatched_client_key}
                    )
                ),
                "does not match its private key",
            ),
            "neither": (_yaml(_secret({"ca.crt": pki.ca})), "neither a SCRAM password"),
            "unknown key": (_yaml(_secret({"password": "p", "token": "t"})), "data key token"),
            "invalid base64": (_yaml(_with_data(_scram_secret(), password="%%%")), "base64"),
            "binary password": (
                _yaml(_with_data(_scram_secret(), password=base64.b64encode(b"\xff").decode())),
                "base64-encoded UTF-8",
            ),
            "control characters": (
                _yaml(_secret({"password": "synthetic\nline"})),
                "empty or contains controls",
            ),
            "number value": (_yaml(_with_data(_scram_secret(), password=12)), "strings"),
            "no data": (_yaml({**_scram_secret(), "data": {}}), "needs a data section"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), self.assertRaises(ProfileImportError) as raised:
                parse_strimzi_secret(text)

            self.assertIn(message, str(raised.exception))
            self.assertNotIn(SCRAM_PASSWORD, str(raised.exception))
            self.assertNotIn(_encoded(SCRAM_PASSWORD), str(raised.exception))


def _without_kind_label(document: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(document)
    del changed["metadata"]["labels"]["strimzi.io/kind"]
    return changed


def _with_data(document: dict[str, Any], **data: object) -> dict[str, Any]:
    changed = copy.deepcopy(document)
    changed["data"].update(data)
    return changed


class TestImportSource(unittest.TestCase):
    def test_reads_a_regular_file_or_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "secret.yaml"
            path.write_text("kind: Secret\n", encoding="utf-8")

            from_file = read_import_source(str(path), stdin=io.BytesIO(), label="Source")
            from_stdin = read_import_source("-", stdin=io.BytesIO(b"kind: Secret\n"), label="S")

        self.assertEqual("kind: Secret\n", from_file)
        self.assertEqual("kind: Secret\n", from_stdin)

    def test_rejects_unreadable_oversized_and_non_text_sources(self) -> None:
        oversized = b"#" * (MAX_IMPORT_BYTES + 1)
        with tempfile.TemporaryDirectory() as directory:
            large = Path(directory) / "large.yaml"
            large.write_bytes(oversized)
            cases = {
                "directory": (directory, b"", "regular file"),
                "missing": (str(Path(directory) / "missing"), b"", "could not be read"),
                "large file": (str(large), b"", "1 MiB"),
                "large stdin": ("-", oversized, "1 MiB"),
                "binary": ("-", b"\xff\xfe", "UTF-8"),
            }
            for name, (source, stdin, message) in cases.items():
                with self.subTest(name), self.assertRaises(ProfileImportError) as raised:
                    read_import_source(source, stdin=io.BytesIO(stdin), label="Source")

                self.assertIn(message, str(raised.exception))


class TestAddFromStrimzi(unittest.TestCase):
    """`add --from-strimzi` feeds the ordinary add path and merges explicit options."""

    def setUp(self) -> None:
        self.runner = CliRunner()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.database = self.directory / "profiles.db"
        self.store = _MemorySecretStore()
        for target, settings in (
            ("kantrip.profiles.load_secret_store", {"return_value": self.store}),
            ("kantrip.cli_inputs.secret_prompt", {"side_effect": AssertionError("prompted")}),
        ):
            patcher = patch(target, **settings)
            patcher.start()
            self.addCleanup(patcher.stop)

    def invoke(self, *arguments: str, source: str | None = None) -> Any:
        return self.runner.invoke(
            cli,
            ["add", *arguments],
            input=source,
            env={"KANTRIP_DATABASE": str(self.database)},
        )

    def write(self, document: object) -> Path:
        path = self.directory / "secret.yaml"
        path.write_text(_yaml(document), encoding="utf-8")
        return path

    def test_scram_file_creates_a_profile_with_its_password_only_in_the_vault(self) -> None:
        path = self.write(_scram_secret())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        result = self.invoke("prod", "--from-strimzi", str(path), "-b", "kafka.invalid:9093")

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            "Added profile 'prod': kafka.invalid:9093, tls, scram-sha-512\n", result.stdout
        )
        auth = load_profiles(self.database).profile("prod")["kafka"]["auth"]
        self.assertEqual("app-user", auth["username"])
        self.assertEqual(SCRAM_PASSWORD, self.store.values[auth["passwordRef"]])
        self.assertNotIn(SCRAM_PASSWORD, self.database.read_bytes().decode("latin-1"))
        self.assertNotIn(SCRAM_PASSWORD, result.output)
        self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())

    def test_tls_stdin_creates_an_mtls_profile_with_listener_trust_from_ca_file(self) -> None:
        pki = synthetic_pki()
        ca = self.directory / "cluster-ca.pem"
        ca.write_text(pki.ca, encoding="utf-8")

        result = self.invoke(
            "prod",
            "--from-strimzi",
            "-",
            "-b",
            "kafka.invalid:9093",
            "--ca-file",
            str(ca),
            source=json.dumps(_tls_secret()),
        )

        self.assertEqual(0, result.exit_code, result.output)
        kafka = load_profiles(self.database).profile("prod")["kafka"]
        self.assertEqual("mtls", kafka["auth"]["type"])
        self.assertEqual(pki.ca.strip(), kafka["tls"]["caCertificates"].strip())
        self.assertEqual({kafka["auth"]["privateKeyRef"]}, set(self.store.values))

    def test_matching_explicit_options_are_accepted(self) -> None:
        path = self.write(_scram_secret())

        result = self.invoke(
            "prod",
            "--from-strimzi",
            str(path),
            "-b",
            "kafka.invalid:9093",
            "--transport",
            "tls",
            "--auth",
            "scram-sha-512",
            "--username",
            "app-user",
        )

        self.assertEqual(0, result.exit_code, result.output)

    def test_conflicting_or_missing_options_fail_before_any_change(self) -> None:
        path = str(self.write(_scram_secret()))
        broker = ("-b", "kafka.invalid:9093")
        cases = {
            "no brokers": (("--from-strimzi", path), "requires --bootstrap-server"),
            "username": (
                ("--from-strimzi", path, *broker, "--username", "other"),
                "--username does not match",
            ),
            "auth": (("--from-strimzi", path, *broker, "--auth", "plain"), "--auth does not"),
            "transport": (
                ("--from-strimzi", path, *broker, "--transport", "plaintext"),
                "--transport does not",
            ),
            "credential": (
                ("--from-strimzi", path, *broker, "--oauth-client-id", "client"),
                "--oauth-client-id cannot be combined with --from-strimzi",
            ),
            "invalid Secret": (("--from-strimzi", "-", *broker), "must be one Kubernetes"),
        }
        for name, (arguments, message) in cases.items():
            with self.subTest(name):
                result = self.invoke("prod", *arguments, source="- not a Secret\n")

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn(message, result.output)
                self.assertFalse(self.database.exists())
                self.assertEqual({}, self.store.values)

    def test_edit_has_no_import_option(self) -> None:
        result = self.runner.invoke(cli, ["edit", "prod", "--from-strimzi", "-"])

        self.assertEqual(2, result.exit_code)
        self.assertIn("No such option", result.output)


if __name__ == "__main__":
    unittest.main()
