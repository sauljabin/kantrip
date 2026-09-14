"""Verify Kantrip wheel and source-distribution contents."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path

WHEEL_REQUIRED = {
    "kantrip/migrations.py",
    "kantrip/profiles.py",
    "kantrip/reconciliation.py",
    "kantrip/schemas/profile.schema.json",
    "kantrip/secret_store.py",
}

SDIST_REQUIRED = {
    "AGENT.md",
    "ARCHITECTURE.md",
    "COMPATIBILITY.md",
    "DEVELOPMENT.md",
    "LICENSE",
    "MANUAL_TESTING.md",
    "MVP.md",
    "README.md",
    "RELEASE_CHECKLIST.md",
    "SECURITY.md",
    "THREAT_MODEL.md",
    "USAGE.md",
    "examples/profile.json",
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
    "schemas/profile.schema.json",
    "scripts/manual-environment.sh",
}


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
