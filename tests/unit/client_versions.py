"""Supported client releases for tests that launch clients without installing them."""

from collections.abc import Mapping
from typing import Any
from unittest import TestCase
from unittest.mock import patch

# What each family's version option prints for a release at or above its floor.
SUPPORTED_VERSION_OUTPUTS = {
    "kcat": (
        "kcat - Apache Kafka producer and consumer tool\n"
        "Version 1.7.0 (JSON, Avro, Transactions, IncrementalAssign, librdkafka 2.15.1 "
        "builtin.features=ssl,sasl,sasl_scram,sasl_oauthbearer,oidc)\n"
    ),
    "Kaskade": "kaskade, version 5.0.1\n",
    "kaf": "kaf version 0.2.14 (Homebrew)\n",
    "kcl": "kcl version v0.20.0\n",
    # Last, because the Java gate reads the last release-like line.
    "Apache/Confluent Java CLI": "8.3.1-ccs\n",
}
# Tests often resolve every client to one fake path, so a probe answers with
# every family's line and each gate reads its own.
SUPPORTED_VERSION_OUTPUT = "".join(SUPPORTED_VERSION_OUTPUTS.values())


def supported_version_output(resolved: str, option: str, environment: Mapping[str, str]) -> str:
    """Answer a version probe as a supported release of any client."""
    del resolved, option, environment
    return SUPPORTED_VERSION_OUTPUT


def use_supported_client_versions(test: TestCase) -> Any:
    """Report a supported release for every client a test launches.

    A test that probes its own fake client stops the returned patch first.
    """
    patcher = patch("kantrip.adapters._version_output", side_effect=supported_version_output)
    patcher.start()
    test.addCleanup(patcher.stop)
    return patcher
