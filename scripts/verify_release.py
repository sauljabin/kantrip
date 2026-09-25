"""Verify Kantrip wheel and source-distribution contents."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from email.parser import BytesParser
from pathlib import Path

WHEEL_REQUIRED = {
    "kantrip/kafka.py",
    "kantrip/migrations.py",
    "kantrip/profiles.py",
    "kantrip/reconciliation.py",
    "kantrip/schemas/profile.schema.json",
    "kantrip/secret_store.py",
}

SDIST_REQUIRED = {
    ".github/actions/python-uv/action.yml",
    ".github/actions/released-e2e-tools/action.yml",
    ".github/workflows/e2e.yml",
    ".github/workflows/main.yml",
    ".github/workflows/release.yml",
    "AGENT.md",
    "ARCHITECTURE.md",
    "COMPATIBILITY.md",
    "DEVELOPMENT.md",
    "LICENSE",
    "MANUAL_TESTING.md",
    "README.md",
    "RELEASE_CHECKLIST.md",
    "SECURITY.md",
    "THREAT_MODEL.md",
    "USAGE.md",
    "examples/profile.json",
    "examples/tls-profile.json",
    "images/banner.svg",
    "images/data-flow.svg",
    "images/database-migration.svg",
    "images/exec-sequence.svg",
    "images/littlehorse-badge.svg",
    "images/nf-md-apache-kafka.svg",
    "images/nf-md-magic-staff.svg",
    "images/rich-badge.svg",
    "images/session-cleanup-decision.svg",
    "images/session-lifecycle.svg",
    "kantrip/maintenance.py",
    "kantrip/migrations.py",
    "kantrip/profiles.py",
    "kantrip/reconciliation.py",
    "kantrip/secret_store.py",
    "pyproject.toml",
    "sandbox/kubernetes/23-kafka-provisioning.yaml",
    "schemas/profile.schema.json",
    "scripts/tests.py",
    "tests/e2e/test_end_to_end.py",
    "tests/e2e/versions.env",
    "tests/unit/pki.py",
    "tests/unit/mutation_worker.py",
    "tests/unit/tests_mutation_crash.py",
    "tests/unit/tests_pki.py",
}
_FORBIDDEN_TEST_CREDENTIAL_SUFFIXES = (".crt", ".key", ".p12", ".pem", ".pfx")
# Bundled tests that read repository metadata; they must pass from the extracted sdist.
SDIST_SELF_CONTAINED_TESTS = ("tests.unit.tests_e2e_selection",)


def one_artifact(dist: Path, pattern: str) -> Path:
    artifacts = list(dist.glob(pattern))
    if len(artifacts) != 1:
        raise ValueError(f"expected one {pattern} artifact, found {len(artifacts)}")
    return artifacts[0]


def verify_wheel(wheel: Path, expected_version: str | None = None) -> str:
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = BytesParser().parsebytes(archive.read(metadata_name))
        version = metadata["Version"]

        if version is None:
            raise ValueError("wheel metadata does not contain a version")
        if expected_version is not None and version != expected_version:
            raise ValueError(f"wheel version is {version}, expected {expected_version}")
        if not wheel.name.startswith(f"kantrip-{version}-"):
            raise ValueError(f"wheel filename does not match metadata version {version}")
        missing = WHEEL_REQUIRED.difference(names)
        if missing:
            raise ValueError(f"wheel is missing: {', '.join(sorted(missing))}")
        if not any(name.endswith(".dist-info/entry_points.txt") for name in names):
            raise ValueError("wheel does not contain console entry points")

    return version


def verify_sdist(sdist: Path, version: str) -> None:
    if sdist.name != f"kantrip-{version}.tar.gz":
        raise ValueError(f"source distribution filename does not match wheel version {version}")
    expected_root = f"kantrip-{version}/"
    with tarfile.open(sdist, "r:gz") as archive:
        names = set(archive.getnames())

    missing = {path for path in SDIST_REQUIRED if f"{expected_root}{path}" not in names}
    if missing:
        raise ValueError(f"source distribution is missing: {', '.join(sorted(missing))}")
    test_names = {
        name.removeprefix(expected_root)
        for name in names
        if name.startswith(f"{expected_root}tests/")
    }
    forbidden = sorted(
        name
        for name in test_names
        if name.startswith("tests/fixtures/")
        or name.lower().endswith(_FORBIDDEN_TEST_CREDENTIAL_SUFFIXES)
    )
    if forbidden:
        raise ValueError(
            "source distribution contains committed test credential material: "
            + ", ".join(forbidden)
        )
    verify_sdist_self_contained(sdist, expected_root.rstrip("/"))


def verify_sdist_self_contained(sdist: Path, root_name: str) -> None:
    """Run bundled checks that read repository metadata from the extracted sdist."""
    with tempfile.TemporaryDirectory(prefix="kantrip-sdist-") as directory:
        with tarfile.open(sdist, "r:gz") as archive:
            _extract_safely(archive, Path(directory))
        result = subprocess.run(
            [sys.executable, "-m", "unittest", *SDIST_SELF_CONTAINED_TESTS],
            cwd=Path(directory) / root_name,
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
    if result.returncode != 0:
        tail = "\n".join(result.stderr.strip().splitlines()[-15:])
        raise ValueError(f"bundled checks fail from the extracted source distribution:\n{tail}")


def _extract_safely(archive: tarfile.TarFile, destination: Path) -> None:
    if hasattr(tarfile, "data_filter"):
        archive.extractall(destination, filter="data")
        return
    root = destination.resolve()
    for member in archive.getmembers():
        target = (destination / member.name).resolve()
        if not (member.isfile() or member.isdir()) or not target.is_relative_to(root):
            raise ValueError(f"unsafe source distribution member: {member.name}")
    archive.extractall(destination)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify release distribution metadata.")
    parser.add_argument("dist", type=Path)
    parser.add_argument("--expected-version")
    args = parser.parse_args()

    wheel = one_artifact(args.dist, "kantrip-*.whl")
    sdist = one_artifact(args.dist, "kantrip-*.tar.gz")
    version = verify_wheel(wheel, args.expected_version)
    verify_sdist(sdist, version)


if __name__ == "__main__":
    main()
