import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import yaml

from sandbox.__main__ import (
    SECRET_FIELDS,
    SandboxFailure,
    _reject_legacy_topology,
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
    def test_manifests_define_exactly_one_kafka_and_one_node_pool(self) -> None:
        manifests = [
            document
            for path in MANIFEST_ROOT.glob("*.yaml")
            for document in yaml.safe_load_all(path.read_text(encoding="utf-8"))
            if document
        ]

        self.assertEqual(
            ["kantrip"],
            [item["metadata"]["name"] for item in manifests if item["kind"] == "Kafka"],
        )
        self.assertEqual(
            ["dual-role"],
            [item["metadata"]["name"] for item in manifests if item["kind"] == "KafkaNodePool"],
        )

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

    def test_one_operator_managed_kafka_exposes_the_complete_listener_matrix(self) -> None:
        kafka = resource("20-kafka.yaml", "Kafka", "kantrip")
        listeners = {listener["name"]: listener for listener in kafka["spec"]["kafka"]["listeners"]}
        self.assertEqual(
            {"plaintext", "tls", "scram", "mtls", "oauth", "plain", "scram256", "services"},
            set(listeners),
        )
        self.assertEqual(
            "PLAIN",
            listeners["plain"]["authentication"]["listenerConfig"]["sasl.enabled.mechanisms"],
        )
        self.assertEqual(
            "SCRAM-SHA-256",
            listeners["scram256"]["authentication"]["listenerConfig"]["sasl.enabled.mechanisms"],
        )
        self.assertEqual("simple", kafka["spec"]["kafka"]["authorization"]["type"])
        super_users = set(kafka["spec"]["kafka"]["authorization"]["superUsers"])
        self.assertEqual({"sandbox-admin"}, super_users)
        self.assertEqual("internal", listeners["services"]["type"])
        self.assertTrue(listeners["services"]["tls"])
        self.assertEqual("scram-sha-512", listeners["services"]["authentication"]["type"])
        self.assertEqual(
            "org.apache.kafka.common.config.provider.FileConfigProvider",
            kafka["spec"]["kafka"]["config"]["config.providers.file.class"],
        )

        pool = resource("20-kafka.yaml", "KafkaNodePool", "dual-role")
        self.assertEqual("persistent-claim", pool["spec"]["storage"]["type"])
        self.assertEqual("2Gi", pool["spec"]["storage"]["size"])
        self.assertTrue(pool["spec"]["storage"]["deleteClaim"])

        job = resource("23-kafka-provisioning.yaml", "Job", "kafka-provisioning")
        script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
        self.assertIn("SCRAM-SHA-256", script)
        self.assertIn("--add-config-file /tmp/scram-256.properties", script)
        self.assertIn("> /tmp/kafka-configs.log 2>&1", script)
        self.assertNotIn('--add-config "', script)
        self.assertIn("kantrip-kafka-bootstrap:9099", script)
        self.assertIn("ANONYMOUS", script)
        self.assertNotIn("service-account-kantrip-kafka", script)
        self.assertNotIn("kantrip-auth-", script)
        volumes = job["spec"]["template"]["spec"]["volumes"]
        self.assertIn("sandbox-admin", {item["secret"]["secretName"] for item in volumes})
        self.assertIn("kafka-custom-users", {item["secret"]["secretName"] for item in volumes})

        user_operator_env = kafka["spec"]["entityOperator"]["template"]["userOperatorContainer"][
            "env"
        ]
        self.assertIn(
            {"name": "STRIMZI_IGNORED_USERS_PATTERN", "value": "^ANONYMOUS$"},
            user_operator_env,
        )

        by_name = listeners
        self.assertFalse(by_name["plaintext"]["tls"])
        self.assertNotIn("authentication", by_name["plaintext"])
        self.assertTrue(by_name["tls"]["tls"])
        self.assertNotIn("authentication", by_name["tls"])
        self.assertEqual("scram-sha-512", by_name["scram"]["authentication"]["type"])
        self.assertEqual("tls", by_name["mtls"]["authentication"]["type"])
        self.assertEqual("custom", by_name["oauth"]["authentication"]["type"])
        self.assertTrue(by_name["oauth"]["authentication"]["sasl"])
        self.assertFalse(kafka["spec"]["kafka"]["config"]["auto.create.topics.enable"])
        for listener in kafka["spec"]["kafka"]["listeners"]:
            if listener["name"] == "services":
                continue
            broker = listener["configuration"]["brokers"][0]
            self.assertEqual("localhost", broker["advertisedHost"])
            self.assertEqual(listener["port"], broker["advertisedPort"])

    def test_kafka_storage_survives_pod_restarts(self) -> None:
        pool = resource("20-kafka.yaml", "KafkaNodePool", "dual-role")

        self.assertEqual("persistent-claim", pool["spec"]["storage"]["type"])
        self.assertEqual("2Gi", pool["spec"]["storage"]["size"])
        self.assertTrue(pool["spec"]["storage"]["deleteClaim"])

    def test_strimzi_users_separate_admin_behavior_and_registry_identities(self) -> None:
        users = documents("21-kafka-users.yaml")
        authentication = {
            user["metadata"]["name"]: user["spec"]["authentication"]["type"] for user in users
        }

        self.assertEqual("scram-sha-512", authentication["sandbox-admin"])
        self.assertEqual("scram-sha-512", authentication["kantrip-scram"])
        self.assertEqual("scram-sha-512", authentication["kantrip-scram-no-acl"])
        self.assertEqual("tls", authentication["kantrip-mtls"])
        self.assertEqual("tls", authentication["kantrip-mtls-no-acl"])
        self.assertEqual("scram-sha-512", authentication["kantrip-plain"])
        self.assertEqual("scram-sha-512", authentication["kantrip-scram-256"])
        self.assertEqual("scram-sha-512", authentication["service-account-kantrip-kafka"])
        self.assertEqual(13, len(authentication))
        by_name = {user["metadata"]["name"]: user for user in users}
        self.assertNotIn("authorization", by_name["sandbox-admin"]["spec"])
        self.assertNotIn("authorization", by_name["kantrip-scram-no-acl"]["spec"])
        self.assertNotIn("authorization", by_name["kantrip-mtls-no-acl"]["spec"])
        for name in ("kantrip-plain", "kantrip-scram-256", "service-account-kantrip-kafka"):
            self.assertEqual("simple", by_name[name]["spec"]["authorization"]["type"])
        for name in (
            "schema-registry-kafka",
            "schema-registry-secure-kafka",
            "schema-registry-oauth-kafka",
            "apicurio-kafka",
            "apicurio-secure-kafka",
        ):
            self.assertEqual("simple", by_name[name]["spec"]["authorization"]["type"])

    def test_legacy_two_cluster_topology_requires_explicit_recreation(self) -> None:
        found = CompletedProcess(("kubectl",), 0, "kafka.kafka.strimzi.io/auth-kantrip\n", "")
        with (
            patch("sandbox.__main__._run", return_value=found),
            self.assertRaisesRegex(SandboxFailure, "retired two-Kafka topology"),
        ):
            _reject_legacy_topology()

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
        self.assertNotIn("ipAddresses", certificates["kafka-listeners-tls"]["spec"])

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
        self.assertEqual("SASL_SSL", plain_apicurio_env["APICURIO_KAFKA_COMMON_SECURITY_PROTOCOL"])
        self.assertEqual(
            {"secretKeyRef": {"name": "apicurio-kafka", "key": "sasl.jaas.config"}},
            plain_apicurio_env["APICURIO_KAFKA_COMMON_SASL_JAAS_CONFIG"],
        )
        self.assertEqual(
            {"port": "management", "path": "/health/ready"},
            deployments["apicurio"]["spec"]["template"]["spec"]["containers"][0]["readinessProbe"][
                "httpGet"
            ],
        )
        self.assertEqual("kafkasql", secure_apicurio_env["APICURIO_STORAGE_KIND"])
        self.assertEqual("apicurio-secure-journal", secure_apicurio_env["APICURIO_KAFKASQL_TOPIC"])
        self.assertEqual("SASL_SSL", secure_apicurio_env["APICURIO_KAFKA_COMMON_SECURITY_PROTOCOL"])
        self.assertEqual("true", secure_apicurio_env["QUARKUS_OIDC_TENANT_ENABLED"])
        self.assertEqual(
            "true", secure_apicurio_env["APICURIO_AUTHN_BASIC_CLIENT_CREDENTIALS_ENABLED"]
        )
        secure_schema_env = _environment(deployments["schema-registry-secure"])
        plain_schema_env = _environment(deployments["schema-registry"])
        self.assertEqual(
            "SASL_SSL", plain_schema_env["SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL"]
        )
        self.assertEqual(
            "SASL_SSL", secure_schema_env["SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL"]
        )
        self.assertEqual("BASIC", secure_schema_env["SCHEMA_REGISTRY_AUTHENTICATION_METHOD"])
        self.assertNotIn("SCHEMA_REGISTRY_OAUTHBEARER_JWKS_ENDPOINT_URL", secure_schema_env)
        oauth_schema_env = _environment(deployments["schema-registry-oauth"])
        self.assertEqual(
            "SASL_SSL", oauth_schema_env["SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL"]
        )
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
                "registry-events",
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
