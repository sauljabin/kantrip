"""Create profile-scoped child-process sessions."""

from __future__ import annotations

import os
import shutil
import ssl
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from kantrip._files import write_exclusive_text
from kantrip.adapters import (
    KCAT_EXECUTABLES,
    AdapterError,
    ClientConfiguration,
    client_adapter,
    create_subshell_shims,
    missing_command_message,
    prepare_command,
    prepare_command_environment,
    require_adapter_capability,
    resolve_client_command,
)
from kantrip.kafka import (
    CA_BUNDLE_FILENAME,
    CLIENT_CERTIFICATE_FILENAME,
    CLIENT_KEY_FILENAME,
    OAUTH_CA_BUNDLE_FILENAME,
    KafkaConnection,
    KafkaProfileError,
    java_properties,
    kaf_cluster,
    kafka_connection,
    kcl_config,
    librdkafka_properties,
    resolve_kafka_connection,
    unencrypted_pem_key,
)
from kantrip.registry import (
    APICURIO_PROVIDER,
    CONFLUENT_PROVIDER,
    REGISTRY_CA_BUNDLE_FILENAME,
    REGISTRY_CLIENT_CERTIFICATE_FILENAME,
    REGISTRY_CLIENT_KEY_FILENAME,
    REGISTRY_OAUTH_CA_BUNDLE_FILENAME,
    RegistryConnection,
    RegistryProfileError,
    _UnresolvedRegistry,
    confluent_console_properties,
    kaf_registry_fields,
    kaskade_registry_properties,
    kcl_registry_config,
    registry_connection,
    resolve_registry_connection,
)
from kantrip.runtime import (
    SessionRuntime,
    SessionRuntimeError,
    cleanup_abandoned_sessions,
    create_session_runtime,
)
from kantrip.secret_store import SecretStore, SecretStoreError, load_secret_store
from kantrip.secret_value import Secret
from kantrip.shells import ShellError, prepare_interactive_shell, resolve_interactive_shell
from kantrip.supervisor import SupervisorError, run_supervised_process

KAF_CLUSTER_NAME = "kantrip"
KAF_CONFIG_FILENAME = "kaf.yaml"
KCL_CONFIG_FILENAME = "kcl.toml"
# Go clients (kaf, kcl) cannot decrypt a PEM key; they read these unencrypted copies.
UNENCRYPTED_CLIENT_KEY_FILENAME = "client-unencrypted.key"
UNENCRYPTED_REGISTRY_CLIENT_KEY_FILENAME = "registry-client-unencrypted.key"
_SCRUBBED_PREFIXES = ("KAFKA_", "SCHEMA_REGISTRY_", "APICURIO_", "KANTRIP_SANDBOX_")
_SCRUBBED_JAVA_VARIABLES = frozenset(
    {"KAFKA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"}
)
_PLATFORM_CA_BUNDLE_CANDIDATES = (
    Path("/etc/ssl/cert.pem"),
    Path("/etc/ssl/certs/ca-certificates.crt"),
    Path("/etc/pki/tls/certs/ca-bundle.crt"),
    Path("/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"),
    Path("/etc/ssl/ca-bundle.pem"),
)


class SessionError(RuntimeError):
    """Raised when a profile session cannot be prepared or started."""


def ensure_session_available(environment: Mapping[str, str] | None = None) -> None:
    """Reject attempts to create a session below an existing Kantrip session."""
    env = os.environ if environment is None else environment
    if env.get("KANTRIP_SESSION_ID"):
        raise SessionError("a Kantrip session is already active; exit it before starting another")


def run_profile_session(
    profile_name: str,
    profile: Mapping[str, Any],
    command: Sequence[str],
    *,
    environment: Mapping[str, str] | None = None,
    profile_revision: int = 1,
    resolved_kafka: KafkaConnection | None = None,
    resolved_registry: RegistryConnection | None | _UnresolvedRegistry = _UnresolvedRegistry.VALUE,
    secret_store: SecretStore | None = None,
) -> int:
    """Run a command or interactive shell in a temporary profile session."""
    env = dict(os.environ if environment is None else environment)
    ensure_session_available(env)

    try:
        executable = command[0] if command else resolve_interactive_shell(env)
    except ShellError as error:
        raise SessionError(str(error)) from error
    arguments = list(command) if command else [executable]

    if command:
        _validate_executable(executable, env)
        executable = arguments[0] = resolve_client_command(executable, env.get("PATH"))
    _validate_kcat_arguments(arguments)
    try:
        kafka = resolved_kafka or kafka_connection(profile)
        selected_store = secret_store
        if kafka.requires_secrets and resolved_kafka is None:
            selected_store = selected_store or load_secret_store()
            kafka = resolve_kafka_connection(kafka, selected_store)
    except (KafkaProfileError, SecretStoreError) as error:
        raise SessionError(str(error)) from error
    registry = (
        _profile_registry(profile, selected_store)
        if isinstance(resolved_registry, _UnresolvedRegistry)
        else resolved_registry
    )

    try:
        cleanup_abandoned_sessions(env)
        profile_id = profile.get("id")
        if not isinstance(profile_id, str):
            raise SessionError("profile identity is missing")
        runtime = create_session_runtime(profile_id, profile_revision, env)
    except SessionRuntimeError as error:
        raise SessionError(str(error)) from error
    try:
        return _run_in_runtime(
            runtime,
            profile_name,
            executable,
            arguments,
            bool(command),
            env,
            kafka,
            registry,
        )
    except (SessionRuntimeError, SupervisorError) as error:
        raise SessionError(str(error)) from error
    finally:
        try:
            runtime.close()
        except SessionRuntimeError as error:
            raise SessionError(str(error)) from error


class _SessionFiles:
    """Private session files, planned up front and written only when selected.

    Each planned file names the files its contents refer to, so selecting a
    client configuration also writes the keys and CA bundles it points at.
    Private keys and derived bundles render only when written.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._planned: dict[Path, tuple[Callable[[], str], tuple[Path, ...]]] = {}
        self._written: set[Path] = set()

    def plan(self, name: str, render: Callable[[], str], *requires: Path | None) -> Path:
        path = self.directory / name
        self._planned[path] = (render, tuple(item for item in requires if item is not None))
        return path

    def write_all(self) -> None:
        self._write(tuple(self._planned))

    def write_referenced(self, values: Iterable[str]) -> None:
        """Write the planned files named exactly by an argument or variable value."""
        references = set(values)
        self._write(tuple(path for path in self._planned if str(path) in references))

    def _write(self, paths: Iterable[Path]) -> None:
        for path in paths:
            if path in self._written:
                continue
            render, requires = self._planned[path]
            self._write(requires)
            _write_private(path, render())
            self._written.add(path)


@dataclass(frozen=True)
class _KafkaMaterial:
    """Private Kafka trust and identity files planned for one session."""

    ca: Path | None = None
    certificate: Path | None = None
    private_key: Path | None = None
    oauth_ca: Path | None = None
    unencrypted_private_key: Path | None = None


@dataclass(frozen=True)
class _RegistryMaterial:
    """Private Registry trust and identity files planned for one session."""

    ca: Path | None = None
    oauth_ca: Path | None = None
    certificate: Path | None = None
    private_key: Path | None = None
    unencrypted_private_key: Path | None = None


@dataclass(frozen=True)
class _ClientFiles:
    """Rendered client configuration shared by adapters and the child environment."""

    configuration: ClientConfiguration
    kcat_properties: Mapping[str, str]
    variables: Mapping[str, Path]


def _run_in_runtime(
    runtime: SessionRuntime,
    profile_name: str,
    executable: str,
    arguments: list[str],
    has_command: bool,
    environment: Mapping[str, str],
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> int:
    files = _SessionFiles(runtime.path)
    kafka_material = _plan_kafka_material(files, kafka)
    registry_material = _plan_registry_material(files, registry)
    clients = _plan_client_files(files, kafka, kafka_material, registry, registry_material)
    child_environment = _child_environment(
        runtime,
        profile_name,
        environment,
        clients,
        kafka,
        registry,
    )
    if not has_command or client_adapter(Path(executable).name) is None:
        # Shells and custom commands read every documented file variable.
        files.write_all()
        child_environment.update({name: str(path) for name, path in clients.variables.items()})
    try:
        if has_command:
            arguments = _prepare_command(
                arguments,
                clients.configuration,
                environment,
                kafka,
                registry,
            )
        else:
            arguments = _prepare_subshell(
                executable,
                runtime.path,
                environment,
                child_environment,
                clients.configuration,
                kafka,
                registry,
            )
    except (AdapterError, ShellError) as error:
        raise SessionError(str(error)) from error
    execution_environment = _registry_client_environment(
        arguments,
        child_environment,
        registry,
        registry_material.oauth_ca,
    )
    if has_command:
        try:
            execution_environment = prepare_command_environment(
                arguments, execution_environment, clients.configuration
            )
        except AdapterError as error:
            raise SessionError(str(error)) from error
        # A supported client gets only the files its adapter passes to it.
        files.write_referenced([*arguments, *execution_environment.values()])
    runtime.mark_running()
    result = _run_child(
        arguments,
        env=execution_environment,
        check=False,
        interactive=not has_command,
    )
    return result.returncode


def _plan_kafka_material(files: _SessionFiles, kafka: KafkaConnection) -> _KafkaMaterial:
    ca_path: Path | None = None
    if kafka.ca_certificates is not None:
        ca_path = files.plan(CA_BUNDLE_FILENAME, _text(kafka.ca_certificates))
    certificate_path: Path | None = None
    private_key_path: Path | None = None
    unencrypted_key_path: Path | None = None
    if kafka.auth_type == "mtls":
        if kafka.client_certificate is None or kafka.private_key is None:
            raise SessionError("Kafka mTLS credentials are not resolved")
        private_key = kafka.private_key
        certificate_path = files.plan(CLIENT_CERTIFICATE_FILENAME, _text(kafka.client_certificate))
        private_key_path = files.plan(CLIENT_KEY_FILENAME, lambda: private_key.reveal())
        unencrypted_key_path = _plan_unencrypted_key(
            files,
            UNENCRYPTED_CLIENT_KEY_FILENAME,
            private_key,
            kafka.private_key_password,
            private_key_path,
        )
    oauth_ca_path: Path | None = None
    if kafka.oauth is not None and kafka.oauth.ca_certificates is not None:
        oauth_ca_path = files.plan(OAUTH_CA_BUNDLE_FILENAME, _text(kafka.oauth.ca_certificates))
    return _KafkaMaterial(
        ca_path, certificate_path, private_key_path, oauth_ca_path, unencrypted_key_path
    )


def _plan_registry_material(
    files: _SessionFiles, registry: RegistryConnection | None
) -> _RegistryMaterial:
    if registry is None:
        return _RegistryMaterial()
    ca_path: Path | None = None
    if registry.ca_certificates is not None:
        ca_path = files.plan(REGISTRY_CA_BUNDLE_FILENAME, _text(registry.ca_certificates))
    oauth_ca_path: Path | None = None
    if (
        registry.provider == CONFLUENT_PROVIDER
        and registry.oauth is not None
        and registry.oauth.ca_certificates is not None
    ):
        oauth_ca = registry.oauth.ca_certificates
        oauth_ca_path = files.plan(
            REGISTRY_OAUTH_CA_BUNDLE_FILENAME, lambda: _oauth_trust_bundle(oauth_ca)
        )
    certificate_path: Path | None = None
    private_key_path: Path | None = None
    unencrypted_key_path: Path | None = None
    if registry.auth_type == "mtls":
        if registry.client_certificate is None or registry.private_key is None:
            raise SessionError("Registry mTLS credentials are not resolved")
        private_key = registry.private_key
        certificate_path = files.plan(
            REGISTRY_CLIENT_CERTIFICATE_FILENAME, _text(registry.client_certificate)
        )
        private_key_path = files.plan(REGISTRY_CLIENT_KEY_FILENAME, lambda: private_key.reveal())
        unencrypted_key_path = _plan_unencrypted_key(
            files,
            UNENCRYPTED_REGISTRY_CLIENT_KEY_FILENAME,
            private_key,
            registry.private_key_password,
            private_key_path,
        )
    return _RegistryMaterial(
        ca_path, oauth_ca_path, certificate_path, private_key_path, unencrypted_key_path
    )


def _plan_unencrypted_key(
    files: _SessionFiles,
    name: str,
    private_key: Secret,
    password: Secret | None,
    plain_key_path: Path,
) -> Path:
    """Reuse an unencrypted key file, or plan a decrypted copy for Go clients."""
    if password is None:
        return plain_key_path

    def render() -> str:
        try:
            return unencrypted_pem_key(private_key, password).reveal()
        except KafkaProfileError as error:
            raise SessionError(str(error)) from error

    return files.plan(name, render)


def _text(contents: str) -> Callable[[], str]:
    return lambda: contents


def _plan_client_files(
    files: _SessionFiles,
    kafka: KafkaConnection,
    kafka_material: _KafkaMaterial,
    registry: RegistryConnection | None,
    registry_material: _RegistryMaterial,
) -> _ClientFiles:
    try:
        kcat_properties = librdkafka_properties(
            kafka,
            ca_location=kafka_material.ca,
            client_certificate_location=kafka_material.certificate,
            private_key_location=kafka_material.private_key,
            oauth_ca_location=kafka_material.oauth_ca,
        )
        java_config = java_properties(
            kafka,
            ca_location=kafka_material.ca,
            oauth_ca_location=kafka_material.oauth_ca,
        )
    except KafkaProfileError as error:
        raise SessionError(str(error)) from error
    librdkafka_files = (
        kafka_material.ca,
        kafka_material.certificate,
        kafka_material.private_key,
        kafka_material.oauth_ca,
    )
    java_files = (kafka_material.ca, kafka_material.oauth_ca)
    registry_files = (
        registry_material.ca,
        registry_material.certificate,
        registry_material.private_key,
    )
    rendered_kafka = _render_properties(kcat_properties)
    kcat_config = files.plan("kcat.conf", _text(rendered_kafka), *librdkafka_files)
    java_config_path = files.plan(
        "kafka.properties", _text(_render_java_properties(java_config)), *java_files
    )
    kaskade_config = files.plan(
        "kaskade.ini", _text(f"[kafka]\n{rendered_kafka}"), *librdkafka_files
    )
    kaskade_registry_config = files.directory / "kaskade-registry.ini"
    schema_registry_java_config = files.directory / "schema-registry-kafka.properties"
    variables = {
        "KAFKA_JAVA_CONFIG_FILE": java_config_path,
        "KAFKA_LIBRDKAFKA_CONFIG_FILE": kcat_config,
        "KCAT_CONFIG": kcat_config,
    }
    if registry is not None:
        kaskade_registry = _render_properties(
            _registry_properties(kaskade_registry_properties, registry, registry_material)
        )
        console_registry = (
            _registry_properties(confluent_console_properties, registry, registry_material)
            if registry.provider != APICURIO_PROVIDER
            else {}
        )
        files.plan(
            kaskade_registry_config.name,
            _text(f"[kafka]\n{rendered_kafka}\n[registry]\n{kaskade_registry}"),
            *librdkafka_files,
            *registry_files,
        )
        # Confluent's Java serializers carry the Registry client key inline.
        files.plan(
            schema_registry_java_config.name,
            _text(_render_java_properties(java_config | console_registry)),
            *java_files,
            registry_material.ca,
        )
        prefix = "APICURIO" if registry.provider == APICURIO_PROVIDER else "SCHEMA"
        variables[f"{prefix}_REGISTRY_CONFIG_FILE"] = files.plan(
            "registry.properties", _text(kaskade_registry), *registry_files
        )
        variables["SCHEMA_REGISTRY_KAFKA_CONFIG_FILE"] = schema_registry_java_config
    configuration = ClientConfiguration(
        bootstrap_servers=kcat_properties["bootstrap.servers"],
        java_config=java_config_path,
        kcat_config=kcat_config,
        kaskade_config=kaskade_config,
        kaskade_registry_config=kaskade_registry_config,
        schema_registry_java_config=schema_registry_java_config,
        registry_oauth_ssl_cert_file=registry_material.oauth_ca,
        kaf_config=_plan_kaf_config(files, kafka, kafka_material, registry),
        kcl_config=_plan_kcl_config(files, kafka, kafka_material, registry, registry_material),
    )
    return _ClientFiles(configuration, kcat_properties, variables)


def _plan_kaf_config(
    files: _SessionFiles,
    kafka: KafkaConnection,
    material: _KafkaMaterial,
    registry: RegistryConnection | None,
) -> Path | None:
    """Plan kaf's one-cluster config; a profile kaf cannot map gets none.

    Kafka OAuth and a Registry beyond Confluent with system trust and Basic at
    most have no kaf mapping; the capability gate refuses kaf before launch.
    """
    if kafka.auth_type == "oauth":
        return None
    try:
        registry_fields = kaf_registry_fields(registry) if registry is not None else {}
    except RegistryProfileError:
        return None
    try:
        cluster = kaf_cluster(
            kafka,
            ca_location=material.ca,
            client_certificate_location=material.certificate,
            private_key_location=material.unencrypted_private_key,
        )
    except KafkaProfileError as error:
        raise SessionError(str(error)) from error
    document = {
        "current-cluster": KAF_CLUSTER_NAME,
        "clusters": [{"name": KAF_CLUSTER_NAME, **cluster, **registry_fields}],
    }
    return files.plan(
        KAF_CONFIG_FILENAME,
        _text(yaml.safe_dump(document, sort_keys=False)),
        material.ca,
        material.certificate,
        material.unencrypted_private_key,
    )


def _plan_kcl_config(
    files: _SessionFiles,
    kafka: KafkaConnection,
    kafka_material: _KafkaMaterial,
    registry: RegistryConnection | None,
    registry_material: _RegistryMaterial,
) -> Path | None:
    """Plan kcl's flat TOML config; a profile kcl cannot map gets none.

    Kafka OAuth, Registry OAuth, and native Apicurio have no kcl mapping; the
    capability gate refuses kcl before launch.
    """
    if kafka.auth_type == "oauth":
        return None
    try:
        registry_config = (
            {
                "registry": kcl_registry_config(
                    registry,
                    ca_location=registry_material.ca,
                    client_certificate_location=registry_material.certificate,
                    private_key_location=registry_material.unencrypted_private_key,
                )
            }
            if registry is not None
            else {}
        )
    except RegistryProfileError:
        return None
    try:
        config = kcl_config(
            kafka,
            ca_location=kafka_material.ca,
            client_certificate_location=kafka_material.certificate,
            private_key_location=kafka_material.unencrypted_private_key,
        )
    except KafkaProfileError as error:
        raise SessionError(str(error)) from error
    return files.plan(
        KCL_CONFIG_FILENAME,
        _text(_render_kcl_toml(config | registry_config)),
        kafka_material.ca,
        kafka_material.certificate,
        kafka_material.unencrypted_private_key,
        registry_material.ca,
        registry_material.certificate,
        registry_material.unencrypted_private_key,
    )


def _registry_properties(
    render: Callable[..., dict[str, str]],
    registry: RegistryConnection,
    material: _RegistryMaterial,
) -> dict[str, str]:
    try:
        return render(
            registry,
            ca_location=material.ca,
            client_certificate_location=material.certificate,
            private_key_location=material.private_key,
        )
    except RegistryProfileError:
        return {}


def _write_private(path: Path, contents: str) -> None:
    write_exclusive_text(path, contents, mode=0o600)


def _uses_custom_pem(kafka: KafkaConnection) -> bool:
    return kafka.ca_certificates is not None or kafka.auth_type == "mtls"


def _uses_oauth_ca(kafka: KafkaConnection) -> bool:
    return kafka.oauth is not None and kafka.oauth.ca_certificates is not None


def _prepare_command(
    arguments: Sequence[str],
    configuration: ClientConfiguration,
    environment: Mapping[str, str],
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> list[str]:
    # The capability decision comes first, as in the shims, so an unsupported
    # mechanism is reported before any argument or configuration problem.
    require_adapter_capability(
        arguments[0],
        auth_type=kafka.auth_type,
        custom_pem=_uses_custom_pem(kafka),
        environment=environment,
        registry=registry,
        oauth_ca=_uses_oauth_ca(kafka),
    )
    return prepare_command(arguments, configuration, registry=registry)


def _profile_registry(
    profile: Mapping[str, Any], secret_store: SecretStore | None
) -> RegistryConnection | None:
    try:
        registry = registry_connection(profile)
    except RegistryProfileError as error:
        raise SessionError(str(error)) from error
    if registry is not None and registry.requires_secrets:
        try:
            registry = resolve_registry_connection(registry, secret_store or load_secret_store())
        except (RegistryProfileError, SecretStoreError) as error:
            raise SessionError(str(error)) from error
    return registry


def _run_child(
    arguments: Sequence[str],
    *,
    env: Mapping[str, str],
    check: bool,
    interactive: bool,
) -> subprocess.CompletedProcess[str]:
    del check
    return_code = run_supervised_process(
        arguments,
        environment=env,
        interactive=interactive,
    )
    return subprocess.CompletedProcess(arguments, return_code)


def _child_environment(
    runtime: SessionRuntime,
    profile_name: str,
    environment: Mapping[str, str],
    clients: _ClientFiles,
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> dict[str, str]:
    child_environment = {
        name: value
        for name, value in environment.items()
        if not name.startswith(_SCRUBBED_PREFIXES) and name not in _SCRUBBED_JAVA_VARIABLES
    }
    child_environment.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": clients.kcat_properties["bootstrap.servers"],
            "KAFKA_SECURITY_PROTOCOL": clients.kcat_properties["security.protocol"],
            "KANTRIP_PROFILE": profile_name,
            "KANTRIP_SESSION_DIR": str(runtime.path),
            "KANTRIP_SESSION_ID": runtime.session_id,
        }
    )
    if registry is not None:
        prefix = "APICURIO" if registry.provider == APICURIO_PROVIDER else "SCHEMA"
        child_environment[f"{prefix}_REGISTRY_URL"] = registry.url
    allowed_oauth_urls: list[str] = []
    if kafka.auth_type == "oauth":
        assert kafka.oauth is not None
        allowed_oauth_urls.append(kafka.oauth.token_url)
    if (
        registry is not None
        and registry.provider == CONFLUENT_PROVIDER
        and registry.auth_type == "oauth"
    ):
        assert registry.oauth is not None
        allowed_oauth_urls.append(registry.oauth.token_url)
    if allowed_oauth_urls:
        allowed_urls_property = "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=" + ",".join(
            dict.fromkeys(allowed_oauth_urls)
        )
        child_environment["KAFKA_OPTS"] = allowed_urls_property
        child_environment["SCHEMA_REGISTRY_OPTS"] = allowed_urls_property
    return child_environment


def _prepare_subshell(
    executable: str,
    session_directory: Path,
    environment: Mapping[str, str],
    child_environment: dict[str, str],
    configuration: ClientConfiguration,
    kafka: KafkaConnection,
    registry: RegistryConnection | None,
) -> list[str]:
    shim_directory = create_subshell_shims(
        session_directory / "bin",
        bootstrap_servers=configuration.bootstrap_servers,
        java_config_path=configuration.java_config,
        schema_registry_java_config_path=configuration.schema_registry_java_config,
        kcat_config_path=configuration.kcat_config,
        kaskade_config_path=configuration.kaskade_config,
        kaskade_registry_config_path=configuration.kaskade_registry_config,
        kaf_config_path=configuration.kaf_config,
        kcl_config_path=configuration.kcl_config,
        environment=environment,
        registry=registry,
        registry_oauth_ssl_cert_file=configuration.registry_oauth_ssl_cert_file,
        require_java_pem=_uses_custom_pem(kafka),
        kafka_auth_type=kafka.auth_type,
        kafka_oauth_ca=_uses_oauth_ca(kafka),
    )
    child_environment["PATH"] = f"{shim_directory}{os.pathsep}{environment.get('PATH', os.defpath)}"
    plan = prepare_interactive_shell(
        executable,
        session_directory,
        shim_directory,
        environment,
        owned_environment={
            name: value
            for name, value in child_environment.items()
            if name.startswith(("KAFKA_", "SCHEMA_REGISTRY_", "APICURIO_", "KANTRIP_"))
            or name == "KCAT_CONFIG"
        },
    )
    child_environment.update(plan.environment_overrides)
    return list(plan.arguments)


def _oauth_trust_bundle(profile_ca: str) -> str:
    default_roots = _platform_default_ca_bundle()
    return f"{default_roots.rstrip()}\n{profile_ca.strip()}\n"


def _platform_default_ca_bundle() -> str:
    compiled_path = ssl.get_default_verify_paths().openssl_cafile
    candidates = (
        *((Path(compiled_path),) if compiled_path is not None else ()),
        *_PLATFORM_CA_BUNDLE_CANDIDATES,
    )
    visited: set[Path] = set()
    for path in candidates:
        if path in visited:
            continue
        visited.add(path)
        try:
            contents = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if contents.strip():
            return contents
    raise SessionError("the platform default CA bundle could not be located or read")


def _registry_client_environment(
    arguments: Sequence[str],
    environment: Mapping[str, str],
    registry: RegistryConnection | None,
    oauth_ca_path: Path | None,
) -> dict[str, str]:
    result = dict(environment)
    if not _is_kaskade_registry_oauth(arguments, registry):
        return result
    result.pop("SSL_CERT_FILE", None)
    result.pop("SSL_CERT_DIR", None)
    if oauth_ca_path is not None:
        result["SSL_CERT_FILE"] = str(oauth_ca_path)
    return result


def _is_kaskade_registry_oauth(
    arguments: Sequence[str], registry: RegistryConnection | None
) -> bool:
    if (
        registry is None
        or registry.auth_type != "oauth"
        or not arguments
        or Path(arguments[0]).name != "kaskade"
        or len(arguments) < 2
        or arguments[1] != "consumer"
    ):
        return False
    return any(
        value.lower() == "registry"
        or value.lower().endswith("=registry")
        or value.lower() in {"-kregistry", "-vregistry"}
        for value in arguments[2:]
    )


def _validate_executable(executable: str, environment: Mapping[str, str]) -> None:
    path = environment.get("PATH")
    if shutil.which(executable, path=path) is not None:
        return
    executable_name = Path(executable).name
    adapter = client_adapter(executable_name)
    if adapter is None:
        raise SessionError(missing_command_message(executable))
    raise SessionError(adapter.missing_command(executable_name))


def _validate_kcat_arguments(arguments: Sequence[str]) -> None:
    if Path(arguments[0]).name not in KCAT_EXECUTABLES:
        return
    if any(argument == "-F" or argument.startswith("-F") for argument in arguments[1:]):
        raise SessionError("kcat's -F option cannot override the selected Kantrip profile")


def _render_properties(properties: Mapping[str, str]) -> str:
    for key, value in properties.items():
        if "=" in key or "\n" in key or "\r" in key:
            raise SessionError("kcat property names cannot contain '=' or line breaks")
        if "\n" in value or "\r" in value:
            raise SessionError("client property values cannot contain line breaks")
    return "".join(f"{key}={value}\n" for key, value in sorted(properties.items()))


def _render_kcl_toml(table: Mapping[str, Any], header: str = "") -> str:
    """Serialize kcl's string, boolean, and string-list keys, then its nested tables."""
    lines = [f"[{header}]\n"] if header else []
    nested: list[str] = []
    for key, value in table.items():
        if isinstance(value, Mapping):
            nested.append(_render_kcl_toml(value, f"{header}.{key}" if header else key))
        elif isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}\n")
        elif isinstance(value, list):
            items = ", ".join(_kcl_toml_string(item) for item in value)
            lines.append(f"{key} = [{items}]\n")
        else:
            lines.append(f"{key} = {_kcl_toml_string(value)}\n")
    return "\n".join(["".join(lines), *nested]) if nested else "".join(lines)


def _kcl_toml_string(value: str) -> str:
    """Quote a TOML basic string that kcl reads literally.

    kcl expands `${NAME}` from the environment in every string it loads and reads
    `$${` as a literal `${`, so each `${` is escaped first.
    """
    escaped: list[str] = []
    for character in value.replace("${", "$${"):
        if character in {'"', "\\"}:
            escaped.append(f"\\{character}")
        elif ord(character) < 0x20 or ord(character) == 0x7F:
            escaped.append(f"\\u{ord(character):04X}")
        else:
            escaped.append(character)
    return f'"{"".join(escaped)}"'


def _render_java_properties(properties: Mapping[str, str]) -> str:
    """Serialize Java properties without losing PEM or credential characters."""
    rendered: list[str] = []
    for key, value in sorted(properties.items()):
        if any(character in key for character in ("=", ":", "\n", "\r")):
            raise SessionError("Java property names contain unsupported characters")
        escaped = (
            value.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\f", "\\f")
        )
        if escaped.startswith(" "):
            escaped = f"\\{escaped}"
        rendered.append(f"{key}={escaped}\n")
    return "".join(rendered)


__all__ = ["SessionError", "ensure_session_available", "run_profile_session"]
