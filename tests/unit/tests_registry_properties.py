import hashlib
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner

from kantrip.cli import cli
from kantrip.client_properties import parse_client_properties
from kantrip.profile_imports import ImportedRegistry, ProfileImportError
from kantrip.profile_storage import load_profiles
from tests.unit.pki import KEY_PASSWORD, synthetic_pki
from tests.unit.tests_cli import _MemorySecretStore
from tests.unit.tests_client_properties import (
    CONFLUENT_JAVA_KAFKA,
    CONFLUENT_LIBRDKAFKA_KAFKA,
    PASSWORD,
)

REGISTRY_PASSWORD = "synthetic-registry-password"
REGISTRY_SECRET = "synthetic-registry-client-secret"
REGISTRY_TOKEN = "synthetic-registry-token"
CONFLUENT_URL = "https://psrc-00000.synthetic.confluent.invalid"
APICURIO_URL = "https://apicurio.invalid/apis/registry/v3"
TOKEN_URL = "https://idp.invalid/realms/synthetic/protocol/openid-connect/token"
# `confluent kafka client-config create java --schema-registry-api-key …` output.
CONFLUENT_JAVA_WITH_REGISTRY = CONFLUENT_JAVA_KAFKA + f"""
# Required connection configs for Confluent Cloud Schema Registry
schema.registry.url={CONFLUENT_URL}
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=SYNTHETICSRKEY:{REGISTRY_PASSWORD}
"""


CONFLUENT_REGISTRY_LINES = f"""
# Required connection configs for Confluent Cloud Schema Registry
schema.registry.url={CONFLUENT_URL}
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=SYNTHETICSRKEY:{REGISTRY_PASSWORD}
"""


def _properties(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def _java_pem(pem: str) -> str:
    return "\\n\\\n    ".join(pem.strip().splitlines())


def _registry(text: str, base: Path | None = None) -> ImportedRegistry:
    registry = parse_client_properties(text, base_directory=base).registry
    assert registry is not None
    return registry


def _revealed(secret: Any) -> str:
    return str(secret.reveal()) if secret is not None else ""


class TestConfluentRegistry(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.pki = synthetic_pki()
        (self.directory / "registry-ca.pem").write_text(self.pki.ca, encoding="utf-8")

    def test_confluent_generated_files_import_their_registry(self) -> None:
        for name, text in {
            "java": CONFLUENT_JAVA_WITH_REGISTRY,
            "python": CONFLUENT_LIBRDKAFKA_KAFKA + CONFLUENT_REGISTRY_LINES,
        }.items():
            with self.subTest(name):
                imported = parse_client_properties(text, base_directory=None)

                self.assertEqual("plain", imported.auth_type)
                registry = imported.registry
                assert registry is not None and registry.auth is not None
                self.assertEqual(("confluent", CONFLUENT_URL), (registry.provider, registry.url))
                self.assertEqual(
                    ("basic", "SYNTHETICSRKEY"), (registry.auth.auth_type, registry.auth.username)
                )
                self.assertEqual(REGISTRY_PASSWORD, _revealed(registry.auth.password))
                self.assertIsNone(registry.auth.ca_certificates)

    def test_basic_token_and_oauth_with_either_spelling(self) -> None:
        trust = (
            "schema.registry.ssl.truststore.type=PEM",
            "schema.registry.ssl.truststore.location=registry-ca.pem",
        )
        cases = {
            "basic": (
                (
                    "basic.auth.credentials.source=USER_INFO",
                    f"basic.auth.user.info=user:{REGISTRY_PASSWORD}",
                ),
                "basic",
            ),
            "prefixed basic": (
                (
                    "schema.registry.basic.auth.credentials.source=USER_INFO",
                    f"schema.registry.basic.auth.user.info=user:{REGISTRY_PASSWORD}",
                ),
                "basic",
            ),
            "token": (
                (
                    "bearer.auth.credentials.source=STATIC_TOKEN",
                    f"bearer.auth.token={REGISTRY_TOKEN}",
                ),
                "token",
            ),
            "oauth": (
                (
                    "bearer.auth.credentials.source=OAUTHBEARER",
                    f"bearer.auth.issuer.endpoint.url={TOKEN_URL}",
                    "bearer.auth.client.id=registry-client",
                    f"bearer.auth.client.secret={REGISTRY_SECRET}",
                    "bearer.auth.scope=registry.read openid",
                    "bearer.auth.logical.cluster=lsrc-00000",
                    "bearer.auth.identity.pool.id=pool-00000",
                ),
                "oauth",
            ),
        }
        for name, (lines, auth_type) in cases.items():
            with self.subTest(name):
                registry = _registry(
                    _properties(f"schema.registry.url={CONFLUENT_URL}", *trust, *lines),
                    self.directory,
                )

                auth = registry.auth
                assert auth is not None
                self.assertEqual(auth_type, auth.auth_type)
                self.assertEqual(self.pki.ca.strip(), (auth.ca_certificates or "").strip())
                self.assertTrue(registry.has_secrets)

        oauth = _registry(
            _properties(f"schema.registry.url={CONFLUENT_URL}", *trust, *cases["oauth"][0]),
            self.directory,
        ).auth
        assert oauth is not None
        self.assertEqual(
            (TOKEN_URL, "registry-client", ("registry.read", "openid"), "lsrc-00000", "pool-00000"),
            (
                oauth.oauth_token_url,
                oauth.oauth_client_id,
                oauth.oauth_scopes,
                oauth.oauth_logical_cluster,
                oauth.oauth_identity_pool_id,
            ),
        )
        self.assertEqual(REGISTRY_SECRET, _revealed(oauth.oauth_client_secret))
        # Confluent's client uses its one ssl.* trust for the token endpoint too.
        self.assertEqual(oauth.ca_certificates, oauth.oauth_ca_certificates)

    def test_pem_mtls_through_the_registry_namespace(self) -> None:
        registry = _registry(
            _properties(
                f"schema.registry.url={CONFLUENT_URL}",
                "schema.registry.ssl.keystore.type=PEM",
                f"schema.registry.ssl.keystore.certificate.chain={_java_pem(self.pki.client_certificate)}",
                f"schema.registry.ssl.keystore.key={_java_pem(self.pki.encrypted_client_key)}",
                f"schema.registry.ssl.key.password={KEY_PASSWORD}",
                "schema.registry.ssl.endpoint.identification.algorithm=https",
            )
        )

        auth = registry.auth
        assert auth is not None
        self.assertEqual("mtls", auth.auth_type)
        self.assertEqual(KEY_PASSWORD, _revealed(auth.private_key_password))
        self.assertIsNone(auth.ca_certificates)

    def test_bare_ssl_keys_stay_with_kafka(self) -> None:
        imported = parse_client_properties(
            _properties(
                "security.protocol=SSL",
                "ssl.truststore.type=PEM",
                "ssl.truststore.location=registry-ca.pem",
                f"schema.registry.url={CONFLUENT_URL}",
            ),
            base_directory=self.directory,
        )

        self.assertIsNotNone(imported.ca_certificates)
        self.assertEqual(ImportedRegistry("confluent", CONFLUENT_URL), imported.registry)

    def test_plain_http_registry_without_security_imports_alone(self) -> None:
        imported = parse_client_properties(
            "schema.registry.url=http://registry.invalid:8081\nauto.register.schemas=false\n",
            base_directory=None,
        )

        self.assertIsNone(imported.transport)
        self.assertEqual(
            ImportedRegistry("confluent", "http://registry.invalid:8081"), imported.registry
        )
        self.assertEqual(("auto.register.schemas",), imported.ignored_keys)
        self.assertFalse(imported.has_secrets)


class TestApicurioRegistry(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.pki = synthetic_pki()
        for name, contents in (
            ("ca.pem", self.pki.ca),
            ("other-ca.pem", self.pki.wrong_ca),
            ("client.crt", self.pki.client_certificate),
            ("client.key", self.pki.client_key),
        ):
            (self.directory / name).write_text(contents, encoding="utf-8")

    def test_basic_and_oauth_share_the_registry_trust(self) -> None:
        url = f"apicurio.registry.url={APICURIO_URL}"
        cases = {
            "basic": (
                "apicurio.registry.tls.certificates=ca.pem",
                "apicurio.registry.auth.username=registry-user",
                f"apicurio.registry.auth.password={REGISTRY_PASSWORD}",
            ),
            "oauth": (
                "apicurio.registry.tls.truststore.type=PEM",
                "apicurio.registry.tls.truststore.location=ca.pem",
                f"apicurio.registry.auth.service.token.endpoint={TOKEN_URL}",
                "apicurio.registry.auth.client.id=registry-client",
                f"apicurio.registry.auth.client.secret={REGISTRY_SECRET}",
                "apicurio.registry.auth.client.scope=openid",
                "apicurio.registry.url.version=3",
                "apicurio.registry.tls.verify-host=true",
                "apicurio.registry.tls.trust-all=false",
                "apicurio.registry.auto-register=true",
            ),
        }
        for auth_type, lines in cases.items():
            with self.subTest(auth_type):
                imported = parse_client_properties(
                    _properties(url, *lines), base_directory=self.directory
                )

                registry = imported.registry
                assert registry is not None and registry.auth is not None
                self.assertEqual(("apicurio", APICURIO_URL), (registry.provider, registry.url))
                self.assertEqual(auth_type, registry.auth.auth_type)
                self.assertEqual(self.pki.ca.strip(), (registry.auth.ca_certificates or "").strip())
                if auth_type == "oauth":
                    self.assertEqual(("openid",), registry.auth.oauth_scopes)
                    self.assertEqual(
                        registry.auth.ca_certificates, registry.auth.oauth_ca_certificates
                    )
                    self.assertEqual(("apicurio.registry.auto-register",), imported.ignored_keys)

    def test_certificates_as_inline_pem_or_several_paths(self) -> None:
        for name, value in {
            "inline": " ".join(self.pki.ca.split()),
            "paths": "ca.pem, other-ca.pem",
        }.items():
            with self.subTest(name):
                registry = _registry(
                    _properties(
                        f"apicurio.registry.url={APICURIO_URL}",
                        f"apicurio.registry.tls.certificates={value}",
                    ),
                    self.directory,
                )

                assert registry.auth is not None
                self.assertEqual("none", registry.auth.auth_type)
                self.assertIn("BEGIN CERTIFICATE", registry.auth.ca_certificates or "")

    def test_pem_mtls_from_paths_or_inline_content(self) -> None:
        for name, (certificate, key) in {
            "paths": ("client.crt", f"{self.directory / 'client.key'}"),
            "inline": (_java_pem(self.pki.client_certificate), _java_pem(self.pki.client_key)),
        }.items():
            with self.subTest(name):
                registry = _registry(
                    _properties(
                        f"apicurio.registry.url={APICURIO_URL}",
                        f"apicurio.registry.tls.client-certificate={certificate}",
                        f"apicurio.registry.tls.client-key={key}",
                    ),
                    self.directory,
                )

                assert registry.auth is not None
                self.assertEqual("mtls", registry.auth.auth_type)
                self.assertIsNone(registry.auth.private_key_password)


class TestRejectedRegistries(unittest.TestCase):
    """Unsupported Registries fail with messages that never repeat a value."""

    def assert_rejected(self, text: str, message: str) -> None:
        with self.assertRaises(ProfileImportError) as raised:
            parse_client_properties(text, base_directory=None)
        error = str(raised.exception)
        self.assertIn(message, error)
        for value in (REGISTRY_PASSWORD, REGISTRY_SECRET, REGISTRY_TOKEN, "synthetic-value"):
            self.assertNotIn(value, error)

    def test_confluent_registries_kantrip_cannot_accept(self) -> None:
        url = f"schema.registry.url={CONFLUENT_URL}"
        basic = f"basic.auth.user.info=user:{REGISTRY_PASSWORD}"
        cases = {
            "mixed providers": (
                _properties(url, f"apicurio.registry.url={APICURIO_URL}"),
                (
                    "mix Confluent and Apicurio Registry keys, such as schema.registry.url "
                    "and apicurio.registry.url"
                ),
            ),
            "both spellings": (
                _properties(
                    url,
                    "basic.auth.credentials.source=USER_INFO",
                    "schema.registry.basic.auth.credentials.source=USER_INFO",
                    basic,
                ),
                (
                    "set both basic.auth.credentials.source and "
                    "schema.registry.basic.auth.credentials.source"
                ),
            ),
            "URL source": (
                _properties(url, "basic.auth.credentials.source=URL"),
                "credentials in URLs",
            ),
            "SASL_INHERIT": (
                _properties(url, "basic.auth.credentials.source=SASL_INHERIT"),
                "SASL_INHERIT",
            ),
            "OAuth inheritance": (
                _properties(url, "bearer.auth.credentials.source=SASL_OAUTHBEARER_INHERIT"),
                "SASL_OAUTHBEARER_INHERIT",
            ),
            "custom provider": (
                _properties(url, "bearer.auth.credentials.source=CUSTOM"),
                "custom credential providers",
            ),
            "user info without source": (_properties(url, basic), "client would ignore it"),
            "malformed user info": (
                _properties(
                    url,
                    "basic.auth.credentials.source=USER_INFO",
                    "basic.auth.user.info=synthetic-value",
                ),
                "must be USER:PASSWORD",
            ),
            "plain HTTP auth": (
                _properties(
                    "schema.registry.url=http://registry.invalid",
                    "basic.auth.credentials.source=USER_INFO",
                    basic,
                ),
                "must use https:// for Registry authentication",
            ),
            "credentials in URL": (
                "schema.registry.url=https://user:synthetic-value@registry.invalid\n",
                "Kantrip does not import credentials in URLs",
            ),
            "several URLs": (
                f"schema.registry.url={CONFLUENT_URL},https://other.invalid\n",
                "must be one http:// or https:// URL",
            ),
            "HTTP token endpoint": (
                _properties(
                    url,
                    "bearer.auth.credentials.source=OAUTHBEARER",
                    "bearer.auth.issuer.endpoint.url=http://idp.invalid/token",
                    "bearer.auth.client.id=registry-client",
                    f"bearer.auth.client.secret={REGISTRY_SECRET}",
                ),
                "bearer.auth.issuer.endpoint.url must use https://",
            ),
            "unknown bearer key": (
                _properties(
                    url,
                    "bearer.auth.credentials.source=STATIC_TOKEN",
                    f"bearer.auth.token={REGISTRY_TOKEN}",
                    "bearer.auth.custom.provider.class=x.Y",
                ),
                "set bearer.auth.custom.provider.class, which",
            ),
            "proxy": (
                _properties(url, "schema.registry.proxy.host=proxy.invalid"),
                "schema.registry.proxy.host",
            ),
            "no URL": (
                _properties("bearer.auth.credentials.source=STATIC_TOKEN"),
                "need schema.registry.url",
            ),
            "JKS trust": (
                _properties(url, "schema.registry.ssl.truststore.location=/synthetic/trust.jks"),
                "schema.registry.ssl.truststore.type must be PEM",
            ),
            "disabled hostname verification": (
                _properties(url, "schema.registry.ssl.endpoint.identification.algorithm="),
                "disabled hostname verification",
            ),
            "placeholder": (
                CONFLUENT_JAVA_KAFKA.replace(PASSWORD, "p")
                + _properties(
                    url,
                    "basic.auth.credentials.source=USER_INFO",
                    "basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}",
                ),
                "basic.auth.user.info has an unfilled {{ … }} placeholder",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)

    def test_apicurio_registries_kantrip_cannot_accept(self) -> None:
        pki = synthetic_pki()
        url = f"apicurio.registry.url={APICURIO_URL}"
        cases = {
            "OAuth and Basic": (
                _properties(
                    url,
                    f"apicurio.registry.auth.service.token.endpoint={TOKEN_URL}",
                    "apicurio.registry.auth.username=registry-user",
                ),
                "Apicurio would use only OAuth",
            ),
            "trust all": (_properties(url, "apicurio.registry.tls.trust-all=true"), "trust-all"),
            "no host verification": (
                _properties(url, "apicurio.registry.tls.verify-host=false"),
                "disabled hostname verification",
            ),
            "JKS default": (
                _properties(url, "apicurio.registry.tls.truststore.location=/synthetic/trust.jks"),
                "truststore.type must be PEM",
            ),
            "both trust forms": (
                _properties(
                    url,
                    "apicurio.registry.tls.truststore.type=PEM",
                    "apicurio.registry.tls.truststore.location=/synthetic/ca.pem",
                    "apicurio.registry.tls.certificates=/synthetic/ca.pem",
                ),
                "Apicurio would ignore the certificates",
            ),
            "keystore": (
                _properties(url, "apicurio.registry.tls.keystore.location=/synthetic/key.p12"),
                "apicurio.registry.tls.keystore.location",
            ),
            "proxy": (_properties(url, "apicurio.registry.proxy.host=proxy.invalid"), "proxy.host"),
            "librdkafka dialect": (
                _properties(url, "sasl.username=user", f"sasl.password={PASSWORD}"),
                "mix Java and librdkafka keys, such as apicurio.registry.url and sasl.password",
            ),
            "API v2": (
                _properties(url, "apicurio.registry.url.version=2"),
                "url.version must be 3",
            ),
            "mTLS and Basic": (
                _properties(
                    url,
                    f"apicurio.registry.tls.client-certificate={_java_pem(pki.client_certificate)}",
                    f"apicurio.registry.tls.client-key={_java_pem(pki.client_key)}",
                    "apicurio.registry.auth.username=registry-user",
                    f"apicurio.registry.auth.password={REGISTRY_PASSWORD}",
                ),
                "one Registry authentication",
            ),
            "encrypted key": (
                _properties(
                    url,
                    f"apicurio.registry.tls.client-certificate={_java_pem(pki.client_certificate)}",
                    f"apicurio.registry.tls.client-key={_java_pem(pki.encrypted_client_key)}",
                ),
                "client identity is invalid",
            ),
            "missing secret": (
                _properties(
                    url,
                    f"apicurio.registry.auth.service.token.endpoint={TOKEN_URL}",
                    "apicurio.registry.auth.client.id=registry-client",
                ),
                "need apicurio.registry.auth.client.secret",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)


class TestAddRegistryFromProperties(unittest.TestCase):
    """`add --from-properties` stores an imported Registry and merges `--registry-*` options."""

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

    def write(self, text: str, name: str = "client.properties") -> Path:
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_registry_secrets_go_only_to_the_vault(self) -> None:
        path = self.write(CONFLUENT_JAVA_WITH_REGISTRY)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        result = self.invoke("cloud", "--from-properties", str(path))

        self.assertEqual(0, result.exit_code, result.output)
        registry = load_profiles(self.database).profile("cloud")["registry"]
        self.assertEqual(
            ("confluent", CONFLUENT_URL), (registry["provider"], registry["schema.registry.url"])
        )
        self.assertEqual(
            ("basic", "SYNTHETICSRKEY"), (registry["auth"]["type"], registry["auth"]["username"])
        )
        self.assertEqual(REGISTRY_PASSWORD, self.store.values[registry["auth"]["passwordRef"]])
        stored = self.database.read_bytes().decode("latin-1")
        self.assertNotIn(REGISTRY_PASSWORD, stored)
        self.assertNotIn(REGISTRY_PASSWORD, result.output)
        self.assertIn("still holds the imported credentials", result.stderr)
        self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())

    def test_registry_only_file_with_options_for_kafka_and_registry_trust(self) -> None:
        pki = synthetic_pki()
        ca = self.write(pki.ca, "registry-ca.pem")
        path = self.write(
            _properties(
                f"apicurio.registry.url={APICURIO_URL}",
                f"apicurio.registry.auth.service.token.endpoint={TOKEN_URL}",
                "apicurio.registry.auth.client.id=registry-client",
                f"apicurio.registry.auth.client.secret={REGISTRY_SECRET}",
            )
        )

        result = self.invoke(
            "apicurio",
            "--from-properties",
            str(path),
            "-b",
            "kafka.invalid:9092",
            "--registry-provider",
            "apicurio",
            "--registry-auth",
            "oauth",
            "--registry-ca-file",
            str(ca),
        )

        self.assertEqual(0, result.exit_code, result.output)
        profile = load_profiles(self.database).profile("apicurio")
        self.assertEqual(["kafka.invalid:9092"], profile["kafka"]["bootstrapServers"])
        registry = profile["registry"]
        self.assertEqual(pki.ca.strip(), registry["tls"]["caCertificates"].strip())
        self.assertEqual(pki.ca.strip(), registry["auth"]["caCertificates"].strip())
        self.assertEqual({registry["auth"]["clientSecretRef"]}, set(self.store.values))
        self.assertIn(f"{path} still holds the imported credentials", result.stderr)

    def test_conflicting_registry_options_fail_before_any_change(self) -> None:
        path = str(self.write(CONFLUENT_JAVA_WITH_REGISTRY))
        cases = {
            "URL": (("--registry-url", "https://other.invalid"), "--registry-url does not match"),
            "provider": (("--registry-provider", "apicurio"), "--registry-provider does not match"),
            "auth": (("--registry-auth", "oauth"), "--registry-auth does not match"),
            "username": (("--registry-username", "other"), "--registry-username does not match"),
            "credential": (
                ("--registry-oauth-client-id", "registry-client"),
                "--registry-oauth-client-id cannot be combined with --from-properties",
            ),
        }
        for name, (arguments, message) in cases.items():
            with self.subTest(name):
                result = self.invoke("cloud", "--from-properties", path, *arguments)

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn(message, result.output)
                self.assertFalse(self.database.exists())
                self.assertEqual({}, self.store.values)

    def test_an_unsupported_registry_creates_nothing(self) -> None:
        result = self.invoke(
            "cloud",
            "--from-properties",
            "-",
            source=CONFLUENT_JAVA_WITH_REGISTRY.replace("USER_INFO", "SASL_INHERIT"),
        )

        self.assertEqual(2, result.exit_code, result.output)
        self.assertNotIn(REGISTRY_PASSWORD, result.output)
        self.assertNotIn(PASSWORD, result.output)
        self.assertFalse(self.database.exists())


if __name__ == "__main__":
    unittest.main()
