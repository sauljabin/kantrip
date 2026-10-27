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
    # kafui's gate reads the module line of its embedded Go build information.
    "kafui": "mod\tgithub.com/Benny93/kafui\tv0.1.38\t\n",
    # Last, because the Java gate reads the last release-like line.
    "Apache/Confluent Java CLI": "8.3.1-ccs\n",
}
# Tests often resolve every client to one fake path, so a probe answers with
# every family's line and each gate reads its own.
SUPPORTED_VERSION_OUTPUT = "".join(SUPPORTED_VERSION_OUTPUTS.values())


# The sentinels `cmd/go` frames module information with.
_MODULE_INFO_START = bytes.fromhex("3077af0c9274080241e1c107e6d618e6")
_MODULE_INFO_END = bytes.fromhex("f932433186182072008242104116d8f2")


def go_build_info_blob(module_info: str, go_version: str = "go1.24.13") -> bytes:
    """Return Go build information as a Go 1.18+ toolchain embeds it in an executable."""
    module = _MODULE_INFO_START + module_info.encode() + _MODULE_INFO_END
    header = b"\xff Go buildinf:" + bytes((8, 0x2))
    return (
        header.ljust(32, b"\0")
        + _uvarint(len(go_version))
        + go_version.encode()
        + _uvarint(len(module))
        + module
    )


def fake_go_executable(script: str, module_info: str) -> bytes:
    """Return a runnable script that also carries Go build information at an aligned offset.

    The shell stops reading at the script's `exec` line, so the trailing
    build information never runs.
    """
    contents = script.encode()
    return contents.ljust(-(-len(contents) // 16) * 16, b"\n") + go_build_info_blob(module_info)


def _uvarint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append(value & 0x7F | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


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
