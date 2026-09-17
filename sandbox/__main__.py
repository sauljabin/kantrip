"""Create and manage Kantrip's local Kubernetes integration sandbox."""

from __future__ import annotations

import base64
import json
import os
import re
import secrets
import shlex
import shutil
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path

import click
import cloup
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SANDBOX_ROOT = PROJECT_ROOT / "sandbox"
MANIFEST_ROOT = SANDBOX_ROOT / "kubernetes"
STATE_ROOT = SANDBOX_ROOT / ".state"
VERSIONS_FILE = SANDBOX_ROOT / "versions.env"
KIND_CONFIG = SANDBOX_ROOT / "kind.yaml"
CLUSTER_NAME = "kantrip-sandbox"
NAMESPACE = "kantrip-sandbox"
KUBECTL_CONTEXT = f"kind-{CLUSTER_NAME}"
STATE_FILE = STATE_ROOT / "credentials.env"
CA_FILE = STATE_ROOT / "ca.crt"

SECRET_FIELDS = (
    "KANTRIP_SANDBOX_KEYCLOAK_ADMIN_USERNAME",
    "KANTRIP_SANDBOX_KEYCLOAK_ADMIN_PASSWORD",
    "KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID",
    "KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_SECRET",
    "KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME",
    "KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD",
    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME",
    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_PASSWORD",
    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_USERNAME",
    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_PASSWORD",
    "KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME",
    "KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD",
    "KANTRIP_SANDBOX_APICURIO_CLIENT_ID",
    "KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET",
    "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID",
    "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET",
    "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME",
    "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_PASSWORD",
)


class SandboxFailure(RuntimeError):
    """Raised when a sandbox lifecycle operation cannot complete."""


@cloup.group(invoke_without_command=True)
@cloup.pass_context
def main(context: cloup.Context) -> None:
    """Manage the isolated Kind, Strimzi, Registry, and Keycloak sandbox."""
    if context.invoked_subcommand is None:
        click.echo(context.get_help())


@main.command()
def up() -> None:
    """Create the cluster and reconcile every sandbox service."""
    try:
        _require_commands(("docker", "helm", "kind", "kubectl"))
        versions = load_versions(VERSIONS_FILE)
        credentials = load_or_create_credentials(STATE_FILE)
        if not cluster_exists():
            _run(
                ("kind", "create", "cluster", "--name", CLUSTER_NAME, "--config", str(KIND_CONFIG))
            )
        _install_operators(versions)
        _reject_legacy_topology()
        _apply_manifest("00-pki.yaml", versions)
        _wait_for_certificates()
        _apply_runtime_secrets(credentials)
        _apply_manifest("10-keycloak.yaml", versions)
        _wait_for_deployment("keycloak", timeout="5m")
        _apply_manifest("20-kafka.yaml", versions)
        _apply_manifest("21-kafka-users.yaml", versions)
        _wait_for_kafka()
        _delete_job("kafka-provisioning")
        _apply_manifest("23-kafka-provisioning.yaml", versions)
        _wait_for_job("kafka-provisioning", timeout="5m")
        _apply_manifest("22-apicurio-topics.yaml", versions)
        _wait_for_apicurio_topics()
        _apply_manifest("30-registries.yaml", versions)
        _wait_for_registries()
        _export_credentials(credentials)
    except SandboxFailure as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Sandbox is ready. Private client material: {STATE_ROOT}")


@main.command()
def down() -> None:
    """Delete the Kind cluster while retaining private generated credentials."""
    try:
        _require_commands(("kind",))
        if cluster_exists():
            _run(("kind", "delete", "cluster", "--name", CLUSTER_NAME))
        else:
            click.echo(f"Kind cluster '{CLUSTER_NAME}' does not exist.")
    except SandboxFailure as error:
        raise click.ClickException(str(error)) from error


@main.command()
def status() -> None:
    """Show the cluster and workload status without displaying credentials."""
    try:
        _require_commands(("kind", "kubectl"))
        if not cluster_exists():
            raise SandboxFailure(f"Kind cluster '{CLUSTER_NAME}' does not exist")
        _run(("kubectl", "--context", KUBECTL_CONTEXT, "get", "pods,services", "-n", NAMESPACE))
    except SandboxFailure as error:
        raise click.ClickException(str(error)) from error


@main.command(name="credentials")
def credentials_command() -> None:
    """Show where generated credentials are stored, without revealing values."""
    if not STATE_FILE.exists():
        raise click.ClickException("sandbox credentials do not exist; run 'python -m sandbox up'")
    fields = sorted(load_credentials(STATE_FILE))
    click.echo(f"Private environment file: {STATE_FILE}")
    click.echo(f"Trusted CA bundle: {CA_FILE}")
    click.echo("Available variables:")
    for field in fields:
        click.echo(f"  {field}")


@main.command(name="oauth-session")
@cloup.argument("service", type=cloup.Choice(("apicurio", "schema-registry")))
def oauth_session(service: str) -> None:
    """Obtain a short-lived token and write a private HTTPie bearer session."""
    try:
        credentials = load_credentials(STATE_FILE)
        client_prefix = (
            "KANTRIP_SANDBOX_APICURIO"
            if service == "apicurio"
            else "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH"
        )
        token = _request_access_token(
            credentials[f"{client_prefix}_CLIENT_ID"],
            credentials[f"{client_prefix}_CLIENT_SECRET"],
        )
        path = STATE_ROOT / f"{service}-oauth.json"
        _write_httpie_session(path, auth_type="bearer", raw_auth=token)
    except SandboxFailure as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Private HTTPie session: {path}")


def load_versions(path: Path) -> dict[str, str]:
    """Read the pinned sandbox component versions."""
    versions = _read_assignment_file(path)
    required = {
        "APICURIO_VERSION",
        "CERT_MANAGER_VERSION",
        "KEYCLOAK_VERSION",
        "KAFKA_VERSION",
        "SCHEMA_REGISTRY_VERSION",
        "STRIMZI_VERSION",
    }
    missing = sorted(required.difference(versions))
    if missing:
        raise SandboxFailure(f"missing sandbox versions: {', '.join(missing)}")
    return versions


def load_credentials(path: Path) -> dict[str, str]:
    """Read the private generated sandbox credential file."""
    credentials = _read_assignment_file(path)
    missing = sorted(set(SECRET_FIELDS).difference(credentials))
    if missing:
        raise SandboxFailure(f"credential file is incomplete: {', '.join(missing)}")
    return credentials


def load_or_create_credentials(path: Path) -> dict[str, str]:
    """Reuse credentials or create a complete private set atomically."""
    if path.exists():
        credentials = _read_assignment_file(path)
        generated = _new_credentials()
        missing = set(SECRET_FIELDS).difference(credentials)
        if not missing:
            return credentials
        credentials.update({key: generated[key] for key in missing})
        content = "".join(f"{key}={shlex.quote(value)}\n" for key, value in credentials.items())
        _write_private_text(path, content)
        return credentials
    credentials = _new_credentials()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    content = "".join(f"{key}={shlex.quote(value)}\n" for key, value in credentials.items())
    raw_descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    descriptor = os.fdopen(raw_descriptor, "w", encoding="utf-8")
    try:
        descriptor.write(content)
    finally:
        descriptor.close()
    return credentials


def _new_credentials() -> dict[str, str]:
    """Create a complete set of fresh sandbox credential inputs."""
    return {
        "KANTRIP_SANDBOX_KEYCLOAK_ADMIN_USERNAME": "sandbox-admin",
        "KANTRIP_SANDBOX_KEYCLOAK_ADMIN_PASSWORD": _password(),
        "KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID": "kantrip-kafka",
        "KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_SECRET": _password(),
        "KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME": "kantrip-plain",
        "KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD": _password(),
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME": "kantrip-scram-256",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_PASSWORD": _password(),
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_USERNAME": "kantrip-scram-256-no-acl",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_PASSWORD": _password(),
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME": "kantrip-no-acl",
        "KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD": _password(),
        "KANTRIP_SANDBOX_APICURIO_CLIENT_ID": "kantrip-apicurio",
        "KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET": _password(),
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID": "kantrip-schema-registry",
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET": _password(),
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME": "sandbox-schema",
        "KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_PASSWORD": _password(),
    }


def cluster_exists() -> bool:
    """Return whether the managed Kind cluster exists."""
    result = subprocess.run(
        ("kind", "get", "clusters"), capture_output=True, text=True, check=False
    )
    return CLUSTER_NAME in result.stdout.splitlines()


def _install_operators(versions: Mapping[str, str]) -> None:
    _run(
        (
            "helm",
            "upgrade",
            "--install",
            "cert-manager",
            "oci://quay.io/jetstack/charts/cert-manager",
            "--version",
            versions["CERT_MANAGER_VERSION"],
            "--namespace",
            "cert-manager",
            "--create-namespace",
            "--set",
            "crds.enabled=true",
            "--wait",
            "--timeout",
            "5m",
        )
    )
    _run(
        (
            "helm",
            "upgrade",
            "--install",
            "strimzi",
            "oci://quay.io/strimzi-helm/strimzi-kafka-operator",
            "--version",
            versions["STRIMZI_VERSION"],
            "--namespace",
            NAMESPACE,
            "--create-namespace",
            "--set",
            f"watchNamespaces={{{NAMESPACE}}}",
            "--wait",
            "--timeout",
            "5m",
        )
    )


def _reject_legacy_topology() -> None:
    result = _run(
        (
            "kubectl",
            "--context",
            KUBECTL_CONTEXT,
            "-n",
            NAMESPACE,
            "get",
            "kafka/auth-kantrip",
            "--ignore-not-found",
            "-o",
            "name",
        ),
        capture_output=True,
    )
    if result.stdout.strip():
        raise SandboxFailure(
            "the sandbox uses the retired two-Kafka topology; run "
            "'python -m sandbox down' and then 'python -m sandbox up' to recreate "
            "only the disposable kantrip-sandbox Kind cluster"
        )


def _apply_runtime_secrets(credentials: Mapping[str, str]) -> None:
    realm = {
        "realm": "kantrip",
        "enabled": True,
        "sslRequired": "external",
        "clients": [
            _keycloak_client(
                credentials["KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID"],
                credentials["KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_SECRET"],
            ),
            _keycloak_client(
                credentials["KANTRIP_SANDBOX_APICURIO_CLIENT_ID"],
                credentials["KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET"],
            ),
            _keycloak_client(
                credentials["KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID"],
                credentials["KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_SECRET"],
            ),
        ],
    }
    password_line = (
        f"{credentials['KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME']}: "
        f"{credentials['KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_PASSWORD']},developer\n"
    )
    plain_jaas = (
        "plain-jaas-config="
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"user_{credentials['KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME']}="
        f'"{credentials["KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD"]}" '
        f"user_{credentials['KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME']}="
        f'"{credentials["KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD"]}";\n'
    )
    documents = (
        _secret(
            "keycloak-admin",
            {
                "username": credentials["KANTRIP_SANDBOX_KEYCLOAK_ADMIN_USERNAME"],
                "password": credentials["KANTRIP_SANDBOX_KEYCLOAK_ADMIN_PASSWORD"],
            },
        ),
        _secret("keycloak-realm", {"realm.json": json.dumps(realm, indent=2)}),
        _secret(
            "registry-clients",
            {
                "apicurio-client-id": credentials["KANTRIP_SANDBOX_APICURIO_CLIENT_ID"],
                "apicurio-client-secret": credentials["KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET"],
            },
        ),
        _secret(
            "schema-registry-auth",
            {
                "password.properties": password_line,
                "jaas.conf": (
                    "SchemaRegistry-Props {\n"
                    "  org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule required\n"
                    '  file="/etc/schema-registry-auth/password.properties"\n'
                    '  debug="true";\n'
                    "};\n"
                ),
            },
        ),
        _secret(
            "kafka-custom-users",
            {
                "plain-username": credentials["KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME"],
                "plain-password": credentials["KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD"],
                "scram-256-username": credentials["KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME"],
                "scram-256-password": credentials["KANTRIP_SANDBOX_KAFKA_SCRAM_256_PASSWORD"],
                "scram-256-no-acl-username": credentials[
                    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_USERNAME"
                ],
                "scram-256-no-acl-password": credentials[
                    "KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_PASSWORD"
                ],
                "no-acl-username": credentials["KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME"],
                "no-acl-password": credentials["KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD"],
                "plain-jaas.properties": plain_jaas,
            },
        ),
    )
    content = "---\n".join(yaml.safe_dump(document, sort_keys=False) for document in documents)
    _run(("kubectl", "--context", KUBECTL_CONTEXT, "apply", "-f", "-"), input_text=content)


def _apply_manifest(name: str, versions: Mapping[str, str]) -> None:
    path = MANIFEST_ROOT / name
    content = path.read_text(encoding="utf-8")
    for key, value in versions.items():
        content = content.replace(f"${{{key}}}", value)
    unresolved = sorted(set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", content)))
    if unresolved:
        raise SandboxFailure(f"unresolved version placeholders in {path}: {', '.join(unresolved)}")
    _run(("kubectl", "--context", KUBECTL_CONTEXT, "apply", "-f", "-"), input_text=content)


def _keycloak_client(client_id: str, client_secret: str) -> dict[str, object]:
    return {
        "clientId": client_id,
        "secret": client_secret,
        "enabled": True,
        "publicClient": False,
        "serviceAccountsEnabled": True,
        "standardFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "protocol": "openid-connect",
    }


def _secret(name: str, string_data: Mapping[str, str]) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "type": "Opaque",
        "stringData": dict(string_data),
    }


def _wait_for_certificates() -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    for certificate in ("sandbox-root-ca", "keycloak-tls", "kafka-listeners-tls", "registries-tls"):
        _run((*base, "wait", f"certificate/{certificate}", "--for=condition=Ready", "--timeout=3m"))


def _wait_for_deployment(name: str, *, timeout: str) -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    _run((*base, "rollout", "status", f"deployment/{name}", "--timeout", timeout))


def _wait_for_kafka() -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    _run((*base, "wait", "kafka/kantrip", "--for=condition=Ready", "--timeout=10m"))
    for user in (
        "sandbox-admin",
        "kantrip-plain",
        "kantrip-scram-256",
        "kantrip-scram",
        "kantrip-scram-no-acl",
        "kantrip-mtls",
        "kantrip-mtls-no-acl",
        "schema-registry-kafka",
        "schema-registry-secure-kafka",
        "schema-registry-oauth-kafka",
        "apicurio-kafka",
        "apicurio-secure-kafka",
        "service-account-kantrip-kafka",
    ):
        _run((*base, "wait", f"kafkauser/{user}", "--for=condition=Ready", "--timeout=5m"))


def _delete_job(name: str) -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    _run((*base, "delete", f"job/{name}", "--ignore-not-found"))


def _wait_for_job(name: str, *, timeout: str) -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    _run((*base, "wait", f"job/{name}", "--for=condition=Complete", "--timeout", timeout))


def _wait_for_apicurio_topics() -> None:
    base = ("kubectl", "--context", KUBECTL_CONTEXT, "-n", NAMESPACE)
    for topic in (
        "apicurio-journal",
        "apicurio-snapshots",
        "apicurio-secure-journal",
        "apicurio-secure-snapshots",
        "registry-events",
        "schema-registry",
        "schema-registry-secure",
        "schema-registry-oauth",
    ):
        _run((*base, "wait", f"kafkatopic/{topic}", "--for=condition=Ready", "--timeout=3m"))


def _wait_for_registries() -> None:
    for deployment in (
        "apicurio",
        "apicurio-secure",
        "schema-registry",
        "schema-registry-secure",
        "schema-registry-oauth",
    ):
        _wait_for_deployment(deployment, timeout="10m")


def _export_credentials(credentials: Mapping[str, str]) -> None:
    _write_private_bytes(CA_FILE, _secret_value("sandbox-root-ca", "ca.crt"))
    generated = {
        "KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME": "kantrip-scram",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD": _secret_value("kantrip-scram", "password").decode(),
        "KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_USERNAME": "kantrip-scram-no-acl",
        "KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_PASSWORD": (
            _secret_value("kantrip-scram-no-acl", "password").decode()
        ),
    }
    for key, secret_name, secret_key, filename in (
        ("KANTRIP_SANDBOX_KAFKA_MTLS_CERTIFICATE", "kantrip-mtls", "user.crt", "user.crt"),
        ("KANTRIP_SANDBOX_KAFKA_MTLS_KEY", "kantrip-mtls", "user.key", "user.key"),
        ("KANTRIP_SANDBOX_KAFKA_MTLS_KEYSTORE", "kantrip-mtls", "user.p12", "user.p12"),
        (
            "KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_CERTIFICATE",
            "kantrip-mtls-no-acl",
            "user.crt",
            "no-acl-user.crt",
        ),
        (
            "KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEY",
            "kantrip-mtls-no-acl",
            "user.key",
            "no-acl-user.key",
        ),
        (
            "KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEYSTORE",
            "kantrip-mtls-no-acl",
            "user.p12",
            "no-acl-user.p12",
        ),
    ):
        target = STATE_ROOT / filename
        _write_private_bytes(target, _secret_value(secret_name, secret_key))
        generated[key] = str(target)
    generated["KANTRIP_SANDBOX_KAFKA_MTLS_KEYSTORE_PASSWORD"] = _secret_value(
        "kantrip-mtls", "user.password"
    ).decode()
    generated["KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEYSTORE_PASSWORD"] = _secret_value(
        "kantrip-mtls-no-acl", "user.password"
    ).decode()
    wrong_ca = STATE_ROOT / "wrong-kafka-ca.crt"
    _write_private_bytes(wrong_ca, _secret_value("kantrip-cluster-ca-cert", "ca.crt"))
    generated["KANTRIP_SANDBOX_WRONG_KAFKA_CA"] = str(wrong_ca)
    combined = {**credentials, **generated, "KANTRIP_SANDBOX_CA": str(CA_FILE)}
    content = "".join(f"{key}={shlex.quote(value)}\n" for key, value in combined.items())
    _write_private_text(STATE_FILE, content)
    _write_client_properties(combined)


def _write_client_properties(values: Mapping[str, str]) -> None:
    ca = values["KANTRIP_SANDBOX_CA"]
    common_tls = f"ssl.truststore.type=PEM\nssl.truststore.location={ca}\n"
    properties = {
        "kafka-tls.properties": "security.protocol=SSL\n" + common_tls,
        "kafka-scram.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=SCRAM-SHA-512\n"
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD"]}";\n' + common_tls
        ),
        "kafka-plain.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=PLAIN\n"
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_PLAIN_PASSWORD"]}";\n' + common_tls
        ),
        "kafka-scram-256.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=SCRAM-SHA-256\n"
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_256_PASSWORD"]}";\n' + common_tls
        ),
        "kafka-no-acl.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=PLAIN\n"
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_NO_ACL_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_NO_ACL_PASSWORD"]}";\n' + common_tls
        ),
        "kafka-scram-256-no-acl.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=SCRAM-SHA-256\n"
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_256_NO_ACL_PASSWORD"]}";\n'
            + common_tls
        ),
        "kafka-scram-no-acl.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=SCRAM-SHA-512\n"
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_USERNAME"]}" '
            f'password="{values["KANTRIP_SANDBOX_KAFKA_SCRAM_NO_ACL_PASSWORD"]}";\n' + common_tls
        ),
        "kafka-mtls.properties": (
            "security.protocol=SSL\n"
            + common_tls
            + "ssl.keystore.type=PKCS12\n"
            + f'ssl.keystore.location={values["KANTRIP_SANDBOX_KAFKA_MTLS_KEYSTORE"]}\n'
            + "ssl.keystore.password="
            + f'{values["KANTRIP_SANDBOX_KAFKA_MTLS_KEYSTORE_PASSWORD"]}\n'
        ),
        "kafka-mtls-no-acl.properties": (
            "security.protocol=SSL\n"
            + common_tls
            + "ssl.keystore.type=PKCS12\n"
            + f'ssl.keystore.location={values["KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEYSTORE"]}\n'
            + "ssl.keystore.password="
            + f'{values["KANTRIP_SANDBOX_KAFKA_MTLS_NO_ACL_KEYSTORE_PASSWORD"]}\n'
        ),
        "kafka-oauth.properties": (
            "security.protocol=SASL_SSL\n"
            "sasl.mechanism=OAUTHBEARER\n"
            "sasl.login.callback.handler.class="
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler\n"
            "sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            f'required ssl.truststore.type="PEM" ssl.truststore.location="{ca}";\n'
            "sasl.oauthbearer.client.credentials.client.id="
            f'{values["KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID"]}\n'
            "sasl.oauthbearer.client.credentials.client.secret="
            f'{values["KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_SECRET"]}\n'
            "sasl.oauthbearer.token.endpoint.url="
            "https://localhost:8443/realms/kantrip/protocol/openid-connect/token\n" + common_tls
        ),
    }
    for filename, content in properties.items():
        _write_private_text(STATE_ROOT / filename, content)
    _write_httpie_session(
        STATE_ROOT / "apicurio-basic.json",
        auth_type="basic",
        raw_auth=(
            f'{values["KANTRIP_SANDBOX_APICURIO_CLIENT_ID"]}:'
            f'{values["KANTRIP_SANDBOX_APICURIO_CLIENT_SECRET"]}'
        ),
    )
    _write_httpie_session(
        STATE_ROOT / "schema-registry-basic.json",
        auth_type="basic",
        raw_auth=(
            f'{values["KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME"]}:'
            f'{values["KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_PASSWORD"]}'
        ),
    )


def _request_access_token(client_id: str, client_secret: str) -> str:
    if not CA_FILE.is_file():
        raise SandboxFailure("sandbox CA is unavailable; run 'python -m sandbox up'")
    encoded_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    request = urllib.request.Request(
        "https://localhost:8443/realms/kantrip/protocol/openid-connect/token",
        data=urllib.parse.urlencode({"grant_type": "client_credentials"}).encode(),
        headers={
            "Authorization": f"Basic {encoded_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            request,
            context=ssl.create_default_context(cafile=str(CA_FILE)),
            timeout=10,
        ) as response:
            payload = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise SandboxFailure("could not obtain an OAuth token from the sandbox") from error
    token = payload.get("access_token")
    if not isinstance(token, str) or not token:
        raise SandboxFailure("Keycloak returned no OAuth access token")
    return token


def _write_httpie_session(path: Path, *, auth_type: str, raw_auth: str) -> None:
    session = {
        "__meta__": {
            "about": "HTTPie session file",
            "help": "https://httpie.io/docs#sessions",
            "httpie": "3",
        },
        "auth": {"raw_auth": raw_auth, "type": auth_type},
        "cookies": [],
        "headers": [{"name": "Accept", "value": "application/json"}],
    }
    _write_private_text(path, json.dumps(session, indent=4) + "\n")


def _write_private_text(path: Path, content: str) -> None:
    _write_private_bytes(path, content.encode())


def _write_private_bytes(path: Path, content: bytes) -> None:
    raw_descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(raw_descriptor, 0o600)
    with os.fdopen(raw_descriptor, "wb") as descriptor:
        descriptor.write(content)


def _secret_value(name: str, key: str) -> bytes:
    escaped_key = key.replace(".", r"\.")
    result = _run(
        (
            "kubectl",
            "--context",
            KUBECTL_CONTEXT,
            "-n",
            NAMESPACE,
            "get",
            "secret",
            name,
            "-o",
            f"jsonpath={{.data.{escaped_key}}}",
        ),
        capture_output=True,
    )
    return base64.b64decode(result.stdout)


def _read_assignment_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise SandboxFailure(f"required file does not exist: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, raw_value = line.partition("=")
        if not separator or not key.isidentifier():
            raise SandboxFailure(f"invalid assignment in {path}: {line!r}")
        parsed = shlex.split(raw_value)
        if len(parsed) != 1:
            raise SandboxFailure(f"invalid value for {key} in {path}")
        values[key] = parsed[0]
    return values


def _password() -> str:
    return secrets.token_urlsafe(32)


def _require_commands(commands: Sequence[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise SandboxFailure(f"required commands were not found on PATH: {', '.join(missing)}")


def _run(
    command: Sequence[str], *, input_text: str | None = None, capture_output: bool = False
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if result.returncode:
        details = result.stderr.strip() if capture_output else ""
        suffix = f": {details}" if details else ""
        raise SandboxFailure(f"command failed ({command[0]} exited {result.returncode}){suffix}")
    return result


if __name__ == "__main__":
    main()
