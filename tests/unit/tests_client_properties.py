import hashlib
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner

from kantrip.cli import cli
from kantrip.client_properties import parse_client_properties
from kantrip.profile_imports import ImportedConnection, ProfileImportError
from kantrip.profile_storage import load_profiles
from kantrip.properties_syntax import (
    MAX_PROPERTIES,
    PropertiesSyntaxError,
    parse_jaas_entries,
    parse_java_properties,
    parse_librdkafka_properties,
)
from tests.unit.pki import KEY_PASSWORD, synthetic_pki
from tests.unit.tests_cli import _MemorySecretStore

PASSWORD = "synthetic-properties-password"
CLIENT_SECRET = "synthetic-oauth-client-secret"
TOKEN_URL = "https://idp.invalid/realms/synthetic/protocol/openid-connect/token"
PLAIN_JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="app-user" password="{PASSWORD}";'
)
SCRAM_JAAS = PLAIN_JAAS.replace("plain.PlainLoginModule", "scram.ScramLoginModule")
OAUTH_HANDLER = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
OAUTH_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
# Shaped like `confluent kafka client-config create java` output, with synthetic values.
CONFLUENT_JAVA = f"""\
# Required connection configs for Kafka producer, consumer, and admin
bootstrap.servers=pkc-00000.synthetic.confluent.invalid:9092
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
username='SYNTHETICAPIKEY' password='{PASSWORD}';
sasl.mechanism=PLAIN
# Required for correctness in Apache Kafka clients prior to 2.6
client.dns.lookup=use_all_dns_ips

# Best practice for higher availability in Apache Kafka clients prior to 3.0
session.timeout.ms=45000

# Best practice for Kafka producer to prevent data loss
acks=all
"""
CONFLUENT_REGISTRY = """
# Required connection configs for Confluent Cloud Schema Registry
schema.registry.url=https://psrc-00000.synthetic.confluent.invalid
basic.auth.credentials.source=USER_INFO
basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}
"""


def _java_pem(pem: str) -> str:
    """Write multi-line PEM as one Java property value with escaped newlines."""
    return "\\n\\\n    ".join(pem.strip().splitlines())


def _properties(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def _parse(text: str, base: Path | None = None) -> ImportedConnection:
    return parse_client_properties(text, base_directory=base)


def _revealed(secret: Any) -> str:
    return str(secret.reveal()) if secret is not None else ""


class TestJavaPropertiesSyntax(unittest.TestCase):
    def test_reads_escapes_separators_comments_and_continuations(self) -> None:
        text = (
            "# comment\n"
            "  ! another comment \\\n"
            "plain=value\n"
            "colon: value\n"
            "spaced   value with spaces\n"
            "equals==x\n"
            "escaped\\=key\\ name = a\\tb\\nc\\\\d\\e\n"
            "unicode=na\\u00efve \\uD83D\\uDE00\n"
            "continued=first, \\\n"
            "      second\\\n"
            "\n"
            "crlf=one\r\nempty\n"
            "   \n"
        )

        self.assertEqual(
            {
                "plain": "value",
                "colon": "value",
                "spaced": "value with spaces",
                "equals": "=x",
                "escaped=key name": "a\tb\nc\\de",
                "unicode": "naïve 😀",
                "continued": "first, second",
                "crlf": "one",
                "empty": "",
            },
            parse_java_properties(text),
        )

    def test_rejects_malformed_documents_by_line(self) -> None:
        cases = {
            "duplicate": ("a=1\nb=2\na=3\n", "line 3 repeats key a"),
            "short escape": ("a=\\u12\n", "line 1 has a malformed \\uXXXX escape"),
            "bad escape": ("a=\\uZZZZ\n", "malformed"),
            "lone surrogate": ("a=\\uD83D\n", "unpaired"),
            "dangling continuation": ("a=b\\", "line 1 ends with a continuation"),
            "non-ASCII": ("a=naïve\n", "\\uXXXX escape"),
            "empty key": ("=value\n", "empty key"),
            "long key": ("k" * 300 + "=v\n", "longer than"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), self.assertRaises(PropertiesSyntaxError) as raised:
                parse_java_properties(text)

            self.assertIn(message, str(raised.exception))

    def test_bounds_the_property_count(self) -> None:
        text = "".join(f"k{index}=v\n" for index in range(MAX_PROPERTIES + 1))

        with self.assertRaises(PropertiesSyntaxError):
            parse_java_properties(text)


class TestLibrdkafkaPropertiesSyntax(unittest.TestCase):
    def test_reads_verbatim_key_value_lines(self) -> None:
        text = "# comment\n  sasl.username=user\\name\r\nsasl.password= a=b \n\n"

        self.assertEqual(
            {"sasl.username": "user\\name", "sasl.password": " a=b "},
            parse_librdkafka_properties(text),
        )

    def test_rejects_malformed_lines(self) -> None:
        cases = {
            "no separator": ("bootstrap.servers\n", "line 1 is not a key=value line"),
            "spaced key": ("a =b\n", "not a key=value"),
            "control": ("a=b\x00\n", "control character"),
            "duplicate": ("a=1\na=2\n", "line 2 repeats key a"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), self.assertRaises(PropertiesSyntaxError) as raised:
                parse_librdkafka_properties(text)

            self.assertIn(message, str(raised.exception))


class TestJaasSyntax(unittest.TestCase):
    def test_tokenizes_quotes_escapes_words_and_comments(self) -> None:
        entries = parse_jaas_entries(
            'org.example.Module required /* block */ a="x\\"y\\\\z\\101" '
            "b='single \"quoted\"' c=word.with-dots // trailing\n"
            'd=""; other.Module optional;'
        )

        self.assertEqual(2, len(entries))
        self.assertEqual(
            ("org.example.Module", "required"),
            (entries[0].login_module, entries[0].control_flag),
        )
        self.assertEqual(
            {"a": 'x"y\\zA', "b": 'single "quoted"', "c": "word.with-dots", "d": ""},
            entries[0].options,
        )

    def test_rejects_malformed_entries(self) -> None:
        cases = {
            "no flag": ("org.example.Module;", "login module and a control flag"),
            "no semicolon": ('org.example.Module required a="b"', "semicolon"),
            "number": ("org.example.Module required a=123;", "option a needs a quoted value"),
            "no value": ("org.example.Module required a;", "not name=value"),
            "duplicate": ('org.example.Module required a="1" a="2";', "repeats option a"),
            "unterminated": ('org.example.Module required a="open;', "unterminated"),
            "slash value": (
                "org.example.Module required a=/tmp/x;",
                "option a needs a quoted value",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), self.assertRaises(PropertiesSyntaxError) as raised:
                parse_jaas_entries(text)

            self.assertIn(message, str(raised.exception))


class TestKafkaMechanisms(unittest.TestCase):
    """Every Kafka mechanism a profile supports imports from both dialects."""

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.pki = synthetic_pki()
        for name, contents in (
            ("ca.pem", self.pki.ca),
            ("client.crt", self.pki.client_certificate),
            ("client.key", self.pki.encrypted_client_key),
        ):
            (self.directory / name).write_text(contents, encoding="utf-8")

    def test_plaintext_and_tls_in_both_dialects(self) -> None:
        brokers = "bootstrap.servers=a.invalid:9092, b.invalid:9092"
        cases = {
            "plaintext": (_properties(brokers, "security.protocol=PLAINTEXT"), "plaintext", None),
            "Java TLS file": (
                _properties(
                    brokers,
                    "security.protocol=SSL",
                    "ssl.truststore.type=PEM",
                    "ssl.truststore.location=ca.pem",
                    "ssl.endpoint.identification.algorithm=HTTPS",
                ),
                "tls",
                self.pki.ca,
            ),
            "Java TLS inline": (
                _properties(
                    brokers,
                    "security.protocol=SSL",
                    "ssl.truststore.type=PEM",
                    f"ssl.truststore.certificates={_java_pem(self.pki.ca)}",
                ),
                "tls",
                self.pki.ca,
            ),
            "Java TLS flattened": (
                _properties(
                    brokers,
                    "security.protocol=ssl",
                    "ssl.truststore.type=PEM",
                    "ssl.truststore.certificates=" + " ".join(self.pki.ca.split()),
                ),
                "tls",
                self.pki.ca,
            ),
            "librdkafka TLS": (
                _properties(
                    brokers,
                    "security.protocol=SSL",
                    "ssl.ca.location=ca.pem",
                    "enable.ssl.certificate.verification=true",
                    "ssl.endpoint.identification.algorithm=https",
                ),
                "tls",
                self.pki.ca,
            ),
            "system trust": (_properties(brokers, "security.protocol=SSL"), "tls", None),
        }
        for name, (text, transport, ca) in cases.items():
            with self.subTest(name):
                imported = _parse(text, self.directory)

                self.assertEqual(("a.invalid:9092", "b.invalid:9092"), imported.bootstrap_servers)
                self.assertEqual((transport, "none"), (imported.transport, imported.auth_type))
                self.assertEqual(
                    ca.strip() if ca else None, (imported.ca_certificates or "").strip() or None
                )
                self.assertFalse(imported.has_secrets)

    def test_password_mechanisms_in_both_dialects(self) -> None:
        for mechanism, auth_type in (
            ("PLAIN", "plain"),
            ("SCRAM-SHA-256", "scram-sha-256"),
            ("SCRAM-SHA-512", "scram-sha-512"),
        ):
            jaas = PLAIN_JAAS if mechanism == "PLAIN" else SCRAM_JAAS
            java = _properties(
                "security.protocol=SASL_SSL",
                f"sasl.mechanism={mechanism}",
                f"sasl.jaas.config={jaas}",
            )
            librdkafka = _properties(
                "security.protocol=SASL_SSL",
                f"sasl.mechanisms={mechanism}",
                "sasl.username=app-user",
                f"sasl.password={PASSWORD}",
            )
            for dialect, text in (("Java", java), ("librdkafka", librdkafka)):
                with self.subTest(mechanism=mechanism, dialect=dialect):
                    imported = _parse(text)

                    self.assertEqual(("tls", auth_type), (imported.transport, imported.auth_type))
                    self.assertEqual("app-user", imported.username)
                    self.assertEqual(PASSWORD, _revealed(imported.password))
                    self.assertTrue(imported.has_secrets)

    def test_confluent_generated_java_file(self) -> None:
        imported = _parse(CONFLUENT_JAVA)

        self.assertEqual(
            ("pkc-00000.synthetic.confluent.invalid:9092",), imported.bootstrap_servers
        )
        self.assertEqual(("plain", "SYNTHETICAPIKEY"), (imported.auth_type, imported.username))
        self.assertEqual(PASSWORD, _revealed(imported.password))
        self.assertEqual(("client.dns.lookup", "session.timeout.ms", "acks"), imported.ignored_keys)

    def test_pem_mtls_with_a_key_password_in_both_dialects(self) -> None:
        java = _properties(
            "security.protocol=SSL",
            "ssl.keystore.type=PEM",
            f"ssl.keystore.certificate.chain={_java_pem(self.pki.client_certificate)}",
            f"ssl.keystore.key={_java_pem(self.pki.encrypted_client_key)}",
            f"ssl.key.password={KEY_PASSWORD}",
        )
        librdkafka = _properties(
            "security.protocol=SSL",
            "ssl.certificate.location=client.crt",
            f"ssl.key.location={self.directory / 'client.key'}",
            f"ssl.key.password={KEY_PASSWORD}",
        )
        for dialect, text in (("Java", java), ("librdkafka", librdkafka)):
            with self.subTest(dialect):
                imported = _parse(text, self.directory)

                self.assertEqual("mtls", imported.auth_type)
                self.assertEqual(
                    self.pki.client_certificate.strip(), (imported.client_certificate or "").strip()
                )
                self.assertEqual(KEY_PASSWORD, _revealed(imported.private_key_password))
                self.assertIn("ENCRYPTED PRIVATE KEY", _revealed(imported.private_key))

    def test_unencrypted_inline_librdkafka_identity(self) -> None:
        certificate = " ".join(self.pki.client_certificate.split())
        key = " ".join(self.pki.client_key.split())
        imported = _parse(
            _properties(
                "security.protocol=SSL",
                f"ssl.certificate.pem={certificate}",
                f"ssl.key.pem={key}",
            )
        )

        self.assertEqual("mtls", imported.auth_type)
        self.assertIsNone(imported.private_key_password)

    def test_java_oauth_in_properties_and_jaas_forms(self) -> None:
        common = (
            "security.protocol=SASL_SSL",
            "sasl.mechanism=OAUTHBEARER",
            f"sasl.login.callback.handler.class={OAUTH_HANDLER}",
            f"sasl.oauthbearer.token.endpoint.url={TOKEN_URL}",
        )
        trust = f'ssl.truststore.type="PEM" ssl.truststore.location="{self.directory}/ca.pem"'
        cases = {
            "properties": _properties(
                *common,
                f"sasl.jaas.config={OAUTH_MODULE} required {trust};",
                "sasl.oauthbearer.client.credentials.client.id=synthetic-client",
                f"sasl.oauthbearer.client.credentials.client.secret={CLIENT_SECRET}",
                "sasl.oauthbearer.scope=kafka openid",
            ),
            "JAAS": _properties(
                *common,
                f"sasl.jaas.config={OAUTH_MODULE} required clientId='synthetic-client' "
                f"clientSecret='{CLIENT_SECRET}' scope='kafka openid' {trust};",
            ),
        }
        for name, text in cases.items():
            with self.subTest(name):
                imported = _parse(text)

                self.assertEqual("oauth", imported.auth_type)
                self.assertEqual(TOKEN_URL, imported.oauth_token_url)
                self.assertEqual("synthetic-client", imported.oauth_client_id)
                self.assertEqual(("kafka", "openid"), imported.oauth_scopes)
                self.assertEqual(CLIENT_SECRET, _revealed(imported.oauth_client_secret))
                self.assertEqual(
                    self.pki.ca.strip(), (imported.oauth_ca_certificates or "").strip()
                )
                self.assertIsNone(imported.ca_certificates)

    def test_librdkafka_oidc(self) -> None:
        imported = _parse(
            _properties(
                "security.protocol=SASL_SSL",
                "sasl.mechanism=OAUTHBEARER",
                "sasl.oauthbearer.method=oidc",
                f"sasl.oauthbearer.token.endpoint.url={TOKEN_URL}",
                "sasl.oauthbearer.client.id=synthetic-client",
                f"sasl.oauthbearer.client.secret={CLIENT_SECRET}",
                "https.ca.location=ca.pem",
                "ssl.ca.location=ca.pem",
            ),
            self.directory,
        )

        self.assertEqual(("oauth", ()), (imported.auth_type, imported.oauth_scopes))
        self.assertEqual(CLIENT_SECRET, _revealed(imported.oauth_client_secret))
        self.assertIsNotNone(imported.oauth_ca_certificates)
        self.assertIsNotNone(imported.ca_certificates)

    def test_shared_keys_import_when_both_dialects_read_them_alike(self) -> None:
        imported = _parse(_properties("bootstrap.servers=kafka.invalid:9093", "acks=all"))

        self.assertEqual(("kafka.invalid:9093",), imported.bootstrap_servers)
        self.assertIsNone(imported.transport)
        self.assertEqual(("acks",), imported.ignored_keys)


class TestRejectedProperties(unittest.TestCase):
    """Unsupported and hostile input fails with messages that never repeat a value."""

    def assert_rejected(self, text: str, message: str, base: Path | None = None) -> None:
        with self.assertRaises(ProfileImportError) as raised:
            _parse(text, base)
        error = str(raised.exception)
        self.assertIn(message, error)
        for value in (PASSWORD, CLIENT_SECRET, "synthetic-value"):
            self.assertNotIn(value, error)

    def test_dialects_must_be_decidable(self) -> None:
        cases = {
            "mixed": (
                _properties(f"sasl.jaas.config={PLAIN_JAAS}", f"sasl.password={PASSWORD}"),
                "mix Java and librdkafka keys, such as sasl.jaas.config and sasl.password",
            ),
            "undecidable escape": (
                "bootstrap.servers=kafka.invalid:9093\\\n",
                "the two read the file differently",
            ),
            "undecidable separator": (
                "bootstrap.servers = kafka.invalid:9093\n",
                "the two read the file differently",
            ),
            "Java syntax": (
                f"sasl.jaas.config={PLAIN_JAAS}\\uZZ\n",
                "are not valid Java properties: line 1 has a malformed",
            ),
            "librdkafka syntax": (
                f"sasl.password={PASSWORD}\nnot a line\n",
                "are not valid librdkafka properties: line 2",
            ),
            "duplicate": (
                f"sasl.password={PASSWORD}\nsasl.password={PASSWORD}\n",
                "repeats key sasl.password",
            ),
            "empty": ("# nothing\n", "set no Kafka connection key"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)

    def test_unsupported_security_settings_fail_by_name(self) -> None:
        sasl = ("security.protocol=SASL_SSL", "sasl.mechanism=PLAIN")
        java_ssl = ("security.protocol=SSL",)
        cases = {
            "SASL_PLAINTEXT": (
                _properties("security.protocol=SASL_PLAINTEXT"),
                "SASL_PLAINTEXT",
            ),
            "GSSAPI": (_properties("security.protocol=SASL_SSL"), "Kerberos"),
            "unknown mechanism": (
                _properties("security.protocol=SASL_SSL", "sasl.mechanism=AWS_MSK_IAM"),
                "sasl.mechanism must be PLAIN",
            ),
            "JKS trust": (
                _properties(*java_ssl, "ssl.truststore.location=/synthetic/trust.jks"),
                "ssl.truststore.type must be PEM",
            ),
            "PKCS12 keystore": (
                _properties(*java_ssl, "ssl.keystore.type=PKCS12"),
                "JKS and PKCS12",
            ),
            "keystore password": (
                _properties(*java_ssl, "ssl.keystore.password=synthetic-value"),
                "ssl.keystore.password",
            ),
            "PEM keystore file": (
                _properties(*java_ssl, "ssl.keystore.location=/synthetic/keystore.pem"),
                "inline PEM",
            ),
            "Java hostname verification": (
                _properties(*java_ssl, "ssl.endpoint.identification.algorithm="),
                "disabled hostname verification",
            ),
            "librdkafka verification": (
                _properties(*java_ssl, "enable.ssl.certificate.verification=false"),
                "disabled certificate verification",
            ),
            "custom login": (
                _properties(*sasl, f"sasl.jaas.config={PLAIN_JAAS}", "sasl.login.class=x.Y"),
                "custom login classes",
            ),
            "other login module": (
                _properties(*sasl, f"sasl.jaas.config={SCRAM_JAAS}"),
                "must use PlainLoginModule",
            ),
            "extra JAAS option": (
                _properties(*sasl, f'sasl.jaas.config={PLAIN_JAAS[:-1]} tokenauth="true";'),
                "sasl.jaas.config option tokenauth",
            ),
            "optional flag": (
                _properties(
                    *sasl, f"sasl.jaas.config={PLAIN_JAAS.replace('required', 'optional')}"
                ),
                "required control flag",
            ),
            "unused key": (
                _properties(*java_ssl, "sasl.mechanism=PLAIN"),
                "set sasl.mechanism, which security.protocol SSL does not use",
            ),
            "unknown security key": (
                _properties(*java_ssl, "ssl.protocol=TLSv1.3"),
                "ssl.protocol",
            ),
            "Registry key": (CONFLUENT_JAVA + CONFLUENT_REGISTRY, "kantrip edit PROFILE"),
            "placeholder": (
                CONFLUENT_JAVA.replace("SYNTHETICAPIKEY", "{{ CLUSTER_API_KEY }}"),
                "sasl.jaas.config has an unfilled {{ … }} placeholder",
            ),
            "bad brokers": (_properties("bootstrap.servers=kafka.invalid"), "host:port"),
            "stdin relative path": (
                _properties("security.protocol=SSL", "ssl.ca.location=ca.pem"),
                "read from stdin need an absolute path in ssl.ca.location",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)

    def test_oauth_requires_the_official_client_credentials_shapes(self) -> None:
        java = (
            "security.protocol=SASL_SSL",
            "sasl.mechanism=OAUTHBEARER",
            f"sasl.oauthbearer.token.endpoint.url={TOKEN_URL}",
        )
        credentials = (
            "sasl.oauthbearer.client.credentials.client.id=synthetic-client",
            f"sasl.oauthbearer.client.credentials.client.secret={CLIENT_SECRET}",
        )
        handler = f"sasl.login.callback.handler.class={OAUTH_HANDLER}"
        module = f"sasl.jaas.config={OAUTH_MODULE} required;"
        cases = {
            "unsecured Java": (_properties(*java, module, *credentials), "unsecured JWTs"),
            "other callback": (
                _properties(*java, module, *credentials, "sasl.login.callback.handler.class=x.Y"),
                "other callback classes",
            ),
            "unsecured JAAS option": (
                _properties(
                    *java,
                    handler,
                    *credentials,
                    f'sasl.jaas.config={OAUTH_MODULE} required unsecuredLoginStringClaim_sub="u";',
                ),
                "option unsecuredLoginStringClaim_sub",
            ),
            "both forms": (
                _properties(
                    *java,
                    handler,
                    *credentials,
                    f'sasl.jaas.config={OAUTH_MODULE} required clientId="synthetic-client";',
                ),
                "keep one form",
            ),
            "HTTP endpoint": (
                _properties(*java, handler, module, *credentials).replace("https://", "http://"),
                "must use https://",
            ),
            "unsecured librdkafka": (
                _properties(
                    "security.protocol=SASL_SSL",
                    "sasl.mechanism=OAUTHBEARER",
                    f"sasl.oauthbearer.client.secret={CLIENT_SECRET}",
                ),
                "sasl.oauthbearer.method oidc",
            ),
            "missing secret": (
                _properties(*java, handler, module, credentials[0]),
                "need sasl.oauthbearer.client.credentials.client.secret",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)

    def test_mtls_material_must_be_complete_and_valid(self) -> None:
        pki = synthetic_pki()
        ssl = ("security.protocol=SSL", "ssl.keystore.type=PEM")
        cases = {
            "certificate only": (
                _properties(
                    *ssl, f"ssl.keystore.certificate.chain={_java_pem(pki.client_certificate)}"
                ),
                "need both a client certificate and a private key",
            ),
            "mismatched key": (
                _properties(
                    *ssl,
                    f"ssl.keystore.certificate.chain={_java_pem(pki.client_certificate)}",
                    f"ssl.keystore.key={_java_pem(pki.mismatched_client_key)}",
                ),
                "does not match its private key",
            ),
            "wrong key password": (
                _properties(
                    *ssl,
                    f"ssl.keystore.certificate.chain={_java_pem(pki.client_certificate)}",
                    f"ssl.keystore.key={_java_pem(pki.encrypted_client_key)}",
                    "ssl.key.password=synthetic-value",
                ),
                "client identity is invalid",
            ),
            "password without key": (
                _properties("security.protocol=SSL", "ssl.key.password=synthetic-value"),
                "set ssl.key.password, which",
            ),
            "invalid CA": (
                _properties("security.protocol=SSL", "ssl.ca.pem=synthetic-value"),
                "ssl.ca.pem is invalid",
            ),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name):
                self.assert_rejected(text, message)


class TestAddFromProperties(unittest.TestCase):
    """`add --from-properties` feeds the ordinary add path and merges explicit options."""

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

    def test_file_import_keeps_secrets_in_the_vault_and_hints_about_the_file(self) -> None:
        path = self.write(CONFLUENT_JAVA)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()

        result = self.invoke("cloud", "--from-properties", str(path))

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            "Added profile 'cloud': pkc-00000.synthetic.confluent.invalid:9092, tls, plain\n",
            result.stdout,
        )
        self.assertIn(
            "Ignored 3 application settings a profile doesn't keep: "
            "client.dns.lookup, session.timeout.ms, acks",
            result.stderr,
        )
        self.assertIn(f"{path} still holds the imported credentials", result.stderr)
        auth = load_profiles(self.database).profile("cloud")["kafka"]["auth"]
        self.assertEqual(PASSWORD, self.store.values[auth["passwordRef"]])
        self.assertNotIn(PASSWORD, self.database.read_bytes().decode("latin-1"))
        self.assertNotIn(PASSWORD, result.output)
        self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())

    def test_stdin_and_secret_free_imports_print_no_file_hint(self) -> None:
        pki = synthetic_pki()
        ca = self.write(pki.ca, "ca.pem")
        tls = _properties(
            "bootstrap.servers=kafka.invalid:9093", "security.protocol=SSL", f"ssl.ca.location={ca}"
        )
        cases = {
            "stdin with secrets": (("--from-properties", "-"), CONFLUENT_JAVA),
            "file without secrets": (("--from-properties", str(self.write(tls))), None),
        }
        for index, (name, (arguments, source)) in enumerate(cases.items()):
            with self.subTest(name):
                result = self.invoke(f"profile-{index}", *arguments, source=source)

                self.assertEqual(0, result.exit_code, result.output)
                self.assertNotIn("still holds", result.stderr)

    def test_relative_pem_paths_resolve_against_the_file(self) -> None:
        pki = synthetic_pki()
        nested = self.directory / "config"
        nested.mkdir()
        (nested / "ca.pem").write_text(pki.ca, encoding="utf-8")
        path = self.write(
            _properties("security.protocol=SSL", "ssl.ca.location=ca.pem"), "config/c.properties"
        )

        result = self.invoke("tls", "--from-properties", str(path), "-b", "kafka.invalid:9093")

        self.assertEqual(0, result.exit_code, result.output)
        kafka = load_profiles(self.database).profile("tls")["kafka"]
        self.assertEqual(pki.ca.strip(), kafka["tls"]["caCertificates"].strip())
        self.assertEqual(["kafka.invalid:9093"], kafka["bootstrapServers"])

    def test_options_fill_absent_fields_and_must_match_set_ones(self) -> None:
        path = str(self.write(CONFLUENT_JAVA))
        matching = (
            "-b",
            "pkc-00000.synthetic.confluent.invalid:9092",
            "--transport",
            "tls",
            "--auth",
            "plain",
            "--username",
            "SYNTHETICAPIKEY",
            "--description",
            "Filled by an option",
        )

        result = self.invoke("cloud", "--from-properties", path, *matching)

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(
            "Filled by an option", load_profiles(self.database).profile("cloud")["description"]
        )

    def test_conflicting_options_fail_before_any_change(self) -> None:
        path = str(self.write(CONFLUENT_JAVA))
        pki = synthetic_pki()
        ca = str(self.write(pki.ca, "ca.pem"))
        cases = {
            "brokers": (("-b", "other.invalid:9092"), "--bootstrap-server does not match"),
            "auth": (("--auth", "scram-sha-512"), "--auth does not match the Kafka properties"),
            "username": (("--username", "other"), "--username does not match"),
            "transport": (("--transport", "plaintext"), "--transport does not match"),
            "credential": (
                ("--client-key-file", ca),
                "--client-key-file cannot be combined with --from-properties",
            ),
            "Strimzi": (("--from-strimzi", "-"), "mutually exclusive"),
        }
        for name, (arguments, message) in cases.items():
            with self.subTest(name):
                result = self.invoke("cloud", "--from-properties", path, *arguments)

                self.assertEqual(2, result.exit_code, result.output)
                self.assertIn(message, result.output)
                self.assertFalse(self.database.exists())
                self.assertEqual({}, self.store.values)

    def test_ca_file_must_match_imported_trust(self) -> None:
        pki = synthetic_pki()
        ca = self.write(pki.ca, "ca.pem")
        wrong = self.write(pki.wrong_ca, "wrong.pem")
        path = self.write(_properties("security.protocol=SSL", f"ssl.ca.location={ca}"))

        result = self.invoke("tls", "--from-properties", str(path), "--ca-file", str(wrong))

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("--ca-file does not match the Kafka properties", result.output)

    def test_invalid_import_exits_2_without_values(self) -> None:
        result = self.invoke(
            "cloud",
            "--from-properties",
            "-",
            source=CONFLUENT_JAVA.replace("SASL_SSL", "SASL_PLAINTEXT"),
        )

        self.assertEqual(2, result.exit_code, result.output)
        self.assertIn("SASL_PLAINTEXT", result.output)
        self.assertNotIn(PASSWORD, result.output)
        self.assertFalse(self.database.exists())

    def test_edit_has_no_import_option(self) -> None:
        result = self.runner.invoke(cli, ["edit", "prod", "--from-properties", "-"])

        self.assertEqual(2, result.exit_code)
        self.assertIn("No such option", result.output)


if __name__ == "__main__":
    unittest.main()
