"""What Kantrip's version floors allow the pinned Java releases to do."""

from __future__ import annotations

from kantrip.adapters import JAVA_CLI_ADAPTER, ClientVersion, KafkaNeeds
from tests.e2e.preconditions import parse_pin, pinned_versions


def java_refusal(executable: str, needs: KafkaNeeds) -> str | None:
    """Return how Kantrip refuses the pinned release of `executable`, or None if it runs.

    The text leaves out the executable and its version, which the installed
    release prints with a suffix such as `-ccs`.
    """
    versions = pinned_versions()
    pin = versions["APACHE_KAFKA_VERSION" if executable.endswith(".sh") else "CONFLUENT_VERSION"]
    version = ClientVersion(parse_pin(pin))
    gate = JAVA_CLI_ADAPTER.minimum_version
    if not gate.supports(executable, version):
        return f"is not supported; install {gate.requirement(executable)}"
    prefix = f"{executable} {version} "
    for check in JAVA_CLI_ADAPTER.feature_checks:
        if (error := check(executable, version, "", needs)) is not None:
            return error.removeprefix(prefix)
    return None


def refused(output: str, refusal: str) -> bool:
    """Return whether Kantrip's output, however it wrapped, carries `refusal`."""
    return " ".join(refusal.split()) in " ".join(output.split())
