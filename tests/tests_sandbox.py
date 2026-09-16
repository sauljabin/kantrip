import tempfile
import unittest
from pathlib import Path

import yaml

from sandbox.__main__ import (
    SECRET_FIELDS,
    load_credentials,
    load_or_create_credentials,
    load_versions,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SANDBOX_ROOT = PROJECT_ROOT / "sandbox"
VERSIONS_FILE = SANDBOX_ROOT / "versions.env"
KIND_CONFIG = SANDBOX_ROOT / "kind.yaml"
MANIFEST_ROOT = SANDBOX_ROOT / "kubernetes"


def documents(name: str) -> list[dict[str, object]]:
    content = (MANIFEST_ROOT / name).read_text(encoding="utf-8")
    return [document for document in yaml.safe_load_all(content) if document]


def resource(name: str, kind: str, resource_name: str) -> dict[str, object]:
    for document in documents(name):
        metadata = document.get("metadata", {})
        if document.get("kind") == kind and metadata.get("name") == resource_name:
            return document
    raise AssertionError(f"{kind}/{resource_name} is missing from {name}")


class TestSandbox(unittest.TestCase):
    def test_pins_current_components(self) -> None:
        versions = load_versions(VERSIONS_FILE)

        self.assertEqual("1.2.0", versions["STRIMZI_VERSION"])
        self.assertEqual("v1.21.1", versions["CERT_MANAGER_VERSION"])
        self.assertEqual("26.7.0", versions["KEYCLOAK_VERSION"])
        self.assertEqual("4.3.1", versions["KAFKA_VERSION"])
        self.assertEqual("3.3.3", versions["APICURIO_VERSION"])
        self.assertEqual("8.3.1", versions["SCHEMA_REGISTRY_VERSION"])

    def test_kind_binds_every_endpoint_to_loopback(self) -> None:
        config = yaml.safe_load(KIND_CONFIG.read_text(encoding="utf-8"))
        mappings = config["nodes"][0]["extraPortMappings"]

        self.assertTrue(mappings)
        self.assertTrue(all(mapping["listenAddress"] == "127.0.0.1" for mapping in mappings))
        self.assertEqual(
            {9092, 9093, 9094, 9095, 9096, 9097, 9098, 8081, 8082, 8083, 8084, 8085, 8443},
            {mapping["hostPort"] for mapping in mappings},
        )

    def test_auxiliary_kafka_covers_plain_scram_256_and_no_acl_ping(self) -> None:
        deployment = resource("23-auth-kafka.yaml", "Deployment", "auth-kafka")
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        script = container["args"][0]

        self.assertIn("SASL_SSL", script)
        self.assertIn("PLAIN", script)
        self.assertIn("SCRAM-SHA-256", script)
        self.assertIn("StandardAuthorizer", script)
        self.assertIn("allow.everyone.if.no.acl.found=false", script)
        super_users = next(line for line in script.splitlines() if line.startswith("super.users="))
        self.assertIn("User:$PLAIN_USERNAME", super_users)
        self.assertIn("User:$SCRAM_USERNAME", super_users)
        self.assertNotIn("NO_ACL", super_users)

    def test_one_kafka_cluster_exposes_the_supported_listener_matrix(self) -> None:
        kafka = resource("20-kafka.yaml", "Kafka", "kantrip")
        listeners = kafka["spec"]["kafka"]["listeners"]
        by_name = {listener["name"]: listener for listener in listeners}

        self.assertEqual({"plaintext", "tls", "scram", "mtls", "oauth", "registry"}, set(by_name))
        self.assertFalse(by_name["plaintext"]["tls"])
        self.assertNotIn("authentication", by_name["plaintext"])
        self.assertTrue(by_name["tls"]["tls"])
        self.assertNotIn("authentication", by_name["tls"])
        self.assertEqual("scram-sha-512", by_name["scram"]["authentication"]["type"])
        self.assertEqual("tls", by_name["mtls"]["authentication"]["type"])
        self.assertEqual("custom", by_name["oauth"]["authentication"]["type"])
        self.assertTrue(by_name["oauth"]["authentication"]["sasl"])
        self.assertEqual("internal", by_name["registry"]["type"])
        self.assertFalse(by_name["registry"]["tls"])
        self.assertFalse(kafka["spec"]["kafka"]["config"]["auto.create.topics.enable"])
        for listener in listeners:
            if listener["name"] == "registry":
                continue
            broker = listener["configuration"]["brokers"][0]
            self.assertEqual("localhost", broker["advertisedHost"])
            self.assertEqual(listener["port"], broker["advertisedPort"])

    def test_kafka_storage_survives_pod_restarts(self) -> None:
        pool = resource("20-kafka.yaml", "KafkaNodePool", "dual-role")

        self.assertEqual("persistent-claim", pool["spec"]["storage"]["type"])
        self.assertEqual("2Gi", pool["spec"]["storage"]["size"])
        self.assertTrue(pool["spec"]["storage"]["deleteClaim"])

    def test_strimzi_provisions_only_native_user_types(self) -> None:
        users = documents("21-kafka-users.yaml")
        authentication = {
            user["metadata"]["name"]: user["spec"]["authentication"]["type"] for user in users
        }

        self.assertEqual(
            {"kantrip-scram": "scram-sha-512", "kantrip-mtls": "tls"},
            authentication,
        )

    def test_cert_manager_issues_one_local_ca_and_all_service_certificates(self) -> None:
        pki = documents("00-pki.yaml")
        certificates = {
            item["metadata"]["name"]: item for item in pki if item["kind"] == "Certificate"
        }

        self.assertTrue(certificates["sandbox-root-ca"]["spec"]["isCA"])
        self.assertEqual(
            {"sandbox-root-ca", "keycloak-tls", "kafka-listeners-tls", "registries-tls"},
            set(certificates),
        )
        for name in ("keycloak-tls", "kafka-listeners-tls", "registries-tls"):
            self.assertIn("localhost", certificates[name]["spec"]["dnsNames"])

    def test_plain_and_secure_registry_variants_are_explicit(self) -> None:
        deployments = {
            item["metadata"]["name"]: item
            for item in documents("30-registries.yaml")
            if item["kind"] == "Deployment"
        }

        self.assertEqual(
            {
                "schema-registry",
                "schema-registry-secure",
                "schema-registry-oauth",
                "apicurio",
                "apicurio-secure",
            },
            set(deployments),
        )
        secure_apicurio_env = _environment(deployments["apicurio-secure"])
        plain_apicurio_env = _environment(deployments["apicurio"])
        self.assertEqual("kafkasql", plain_apicurio_env["APICURIO_STORAGE_KIND"])
        self.assertEqual("apicurio-journal", plain_apicurio_env["APICURIO_KAFKASQL_TOPIC"])
        self.assertEqual(
            {"port": "management", "path": "/health/ready"},
            deployments["apicurio"]["spec"]["template"]["spec"]["containers"][0]["readinessProbe"][
                "httpGet"
            ],
        )
        self.assertEqual("kafkasql", secure_apicurio_env["APICURIO_STORAGE_KIND"])
        self.assertEqual("apicurio-secure-journal", secure_apicurio_env["APICURIO_KAFKASQL_TOPIC"])
        self.assertEqual("true", secure_apicurio_env["QUARKUS_OIDC_TENANT_ENABLED"])
        self.assertEqual(
            "true", secure_apicurio_env["APICURIO_AUTHN_BASIC_CLIENT_CREDENTIALS_ENABLED"]
        )
        secure_schema_env = _environment(deployments["schema-registry-secure"])
        self.assertEqual("BASIC", secure_schema_env["SCHEMA_REGISTRY_AUTHENTICATION_METHOD"])
        self.assertNotIn("SCHEMA_REGISTRY_OAUTHBEARER_JWKS_ENDPOINT_URL", secure_schema_env)
        oauth_schema_env = _environment(deployments["schema-registry-oauth"])
        self.assertIn("SCHEMA_REGISTRY_OAUTHBEARER_JWKS_ENDPOINT_URL", oauth_schema_env)
        self.assertNotIn("SCHEMA_REGISTRY_AUTHENTICATION_METHOD", oauth_schema_env)

    def test_managed_topics_have_explicit_storage_policies(self) -> None:
        topics = documents("22-apicurio-topics.yaml")

        by_resource = {topic["metadata"]["name"]: topic for topic in topics}
        self.assertEqual(
            {
                "apicurio-journal",
                "apicurio-snapshots",
                "apicurio-secure-journal",
                "apicurio-secure-snapshots",
                "schema-registry",
                "schema-registry-secure",
                "schema-registry-oauth",
            },
            set(by_resource),
        )
        for topic in topics:
            config = topic["spec"]["config"]
            if topic["metadata"]["name"].startswith("schema-registry"):
                self.assertEqual("compact", config["cleanup.policy"])
                self.assertNotIn("topicName", topic["spec"])
            else:
                self.assertEqual("delete", config["cleanup.policy"])
                self.assertEqual("-1", config["retention.ms"])
                self.assertEqual("-1", config["retention.bytes"])

    def test_runtime_credentials_are_random_private_and_reused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state" / "credentials.env"

            first = load_or_create_credentials(path)
            second = load_or_create_credentials(path)

            self.assertEqual(first, second)
            self.assertEqual(set(SECRET_FIELDS), set(first))
            self.assertEqual(0o700, path.parent.stat().st_mode & 0o777)
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            self.assertEqual(first, load_credentials(path))
            for key, value in first.items():
                self.assertTrue(key.startswith("KANTRIP_SANDBOX_"), key)
                if key.endswith(("PASSWORD", "SECRET")):
                    self.assertGreaterEqual(len(value), 32)


def _environment(deployment: dict[str, object]) -> dict[str, object]:
    entries = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    return {entry["name"]: entry.get("value", entry.get("valueFrom")) for entry in entries}


if __name__ == "__main__":
    unittest.main()
