"""Validate external tools and sandbox readiness before E2E assertions."""

from __future__ import annotations

import re
import shlex
import shutil
import socket
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path

from kantrip.adapters import ADAPTER_EXECUTABLES
from kantrip.secret_store import load_secret_store, secret_reference
from sandbox.__main__ import CA_FILE, STATE_FILE, STATE_ROOT

VERSIONS_FILE = Path(__file__).with_name("versions.env")
HOST_PORTS = (8081, 8082, 8083, 8084, 8085, 8086, 8443, 9092, 9093, 9094, 9095, 9096, 9097, 9098)


class E2ESetupError(RuntimeError):
    """Raised when the explicitly provisioned E2E environment is incomplete."""


def check_preconditions(environment: Mapping[str, str]) -> None:
    """Fail with setup diagnostics before any product assertion runs."""
    versions = _versions()
    _require_commands(("kantrip", "kubectl", "helm", "kind", *sorted(ADAPTER_EXECUTABLES)))
    _require_exact_version(
        "Apache Kafka",
        ("kafka-topics.sh", "--version"),
        versions["APACHE_KAFKA_VERSION"],
    )
    _require_exact_version(
        "Confluent Platform",
        ("kafka-topics", "--version"),
        versions["CONFLUENT_VERSION"],
    )
    _require_exact_version("kaskade", ("kaskade", "--version"), versions["KASKADE_VERSION"])
    kcat_version = (
        versions["KCAT_MACOS_VERSION"] if sys.platform == "darwin" else versions["KCAT_VERSION"]
    )
    _require_exact_version("kcat", ("kcat", "-V"), kcat_version)
    _require_librdkafka_version(versions["LIBRDKAFKA_MIN_VERSION"])
    _require_sandbox_files()
    _wait_for_workloads()
    _require_host_endpoints()
    _exercise_native_secret_store()
    candidate = environment.get("KANTRIP_E2E_KANTRIP")
    if not candidate or not Path(candidate).is_file():
        raise E2ESetupError("KANTRIP_E2E_KANTRIP must name an installed candidate wheel executable")


def _versions() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in VERSIONS_FILE.read_text(encoding="utf-8").splitlines():
        name, separator, raw_value = line.partition("=")
        if not separator or not name.isidentifier():
            raise E2ESetupError(f"invalid E2E version assignment: {line!r}")
        parsed = shlex.split(raw_value)
        if len(parsed) != 1:
            raise E2ESetupError(f"invalid E2E version value for {name}")
        values[name] = parsed[0]
    return values


def _require_commands(commands: Sequence[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise E2ESetupError("required released E2E executables are missing: " + ", ".join(missing))


def _require_exact_version(name: str, command: Sequence[str], expected: str) -> None:
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=15)
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0 or re.search(rf"(?<!\d){re.escape(expected)}(?!\d)", output) is None:
        raise E2ESetupError(f"{name} {expected} is required by {VERSIONS_FILE}")


def _require_librdkafka_version(minimum: str) -> None:
    result = subprocess.run(("kcat", "-V"), capture_output=True, text=True, check=False, timeout=15)
    output = f"{result.stdout}\n{result.stderr}"
    match = re.search(r"librdkafka (\d+)\.(\d+)\.(\d+)", output)
    if (
        result.returncode != 0
        or match is None
        or tuple(map(int, match.groups())) < tuple(map(int, minimum.split(".")))
    ):
        raise E2ESetupError(f"kcat must link librdkafka >= {minimum}; found: {output.strip()}")


def _require_sandbox_files() -> None:
    missing = [str(path) for path in (CA_FILE, STATE_FILE) if not path.is_file()]
    if missing:
        raise E2ESetupError(
            "sandbox private material is missing; run `python -m sandbox up`: " + ", ".join(missing)
        )
    for path in (STATE_ROOT, CA_FILE, STATE_FILE):
        expected = 0o700 if path.is_dir() else 0o600
        actual = path.stat().st_mode & 0o777
        if actual != expected:
            raise E2ESetupError(f"sandbox path {path} has mode {actual:o}, expected {expected:o}")


def _wait_for_workloads() -> None:
    commands = (
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "wait",
            "--for=condition=Ready",
            "kafka/kantrip",
            "-n",
            "kantrip-sandbox",
            "--timeout=90s",
        ),
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "wait",
            "--for=condition=Ready",
            "certificate",
            "--all",
            "-n",
            "kantrip-sandbox",
            "--timeout=90s",
        ),
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "wait",
            "--for=condition=Ready",
            "kafkatopic",
            "--all",
            "-n",
            "kantrip-sandbox",
            "--timeout=90s",
        ),
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "wait",
            "--for=condition=complete",
            "job/kafka-provisioning",
            "-n",
            "kantrip-sandbox",
            "--timeout=90s",
        ),
        (
            "kubectl",
            "--context",
            "kind-kantrip-sandbox",
            "wait",
            "--for=condition=Available",
            "deployment/keycloak",
            "deployment/schema-registry",
            "deployment/schema-registry-secure",
            "deployment/schema-registry-mtls",
            "deployment/schema-registry-oauth",
            "deployment/apicurio",
            "deployment/apicurio-secure",
            "-n",
            "kantrip-sandbox",
            "--timeout=90s",
        ),
    )
    for command in commands:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=100)
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise E2ESetupError(f"sandbox readiness failed: {detail}")


def _require_host_endpoints() -> None:
    unavailable: list[int] = []
    for port in HOST_PORTS:
        try:
            with socket.create_connection(("localhost", port), timeout=3):
                pass
        except OSError:
            unavailable.append(port)
    if unavailable:
        raise E2ESetupError(
            "sandbox host endpoints are unavailable: "
            + ", ".join(str(port) for port in unavailable)
        )


def _exercise_native_secret_store() -> None:
    store = load_secret_store()
    reference = secret_reference(
        str(uuid.uuid4()),
        "kafka/password",
        credential_id=str(uuid.uuid4()),
    )
    value = f"e2e-precondition-{uuid.uuid4()}"
    try:
        store.set(reference, value)
        if store.get(reference) != value:
            raise E2ESetupError("native credential store returned a different test value")
    finally:
        store.delete(reference)


__all__ = ["E2ESetupError", "check_preconditions"]
