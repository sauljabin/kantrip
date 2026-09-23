"""Run the full E2E suite against a wheel built from the staged index."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def _run(*command: str, cwd: Path, environment: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def main() -> None:
    repository = Path(
        subprocess.check_output(("git", "rev-parse", "--show-toplevel"), text=True).strip()
    )
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="kantrip-staged-e2e-") as temporary:
        root = Path(temporary)
        source = root / "source"
        source.mkdir()
        _run("git", "checkout-index", "--all", f"--prefix={source}/", cwd=repository)
        state = repository / "sandbox" / ".state"
        if state.is_dir():
            (source / "sandbox" / ".state").symlink_to(state, target_is_directory=True)

        distribution = root / "dist"
        _run("uv", "build", "--wheel", "--out-dir", str(distribution), cwd=source)
        wheels = list(distribution.glob("kantrip-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one staged candidate wheel, found {len(wheels)}")

        environment_path = root / "candidate-venv"
        _run("uv", "venv", "--python", sys.executable, str(environment_path), cwd=source)
        candidate_python = environment_path / "bin" / "python"
        _run(
            "uv",
            "pip",
            "install",
            "--python",
            str(candidate_python),
            str(wheels[0]),
            cwd=source,
        )
        environment = dict(os.environ)
        environment["KANTRIP_E2E_KANTRIP"] = str(environment_path / "bin" / "kantrip")
        _run(
            "uv",
            "run",
            "--locked",
            "python",
            "-m",
            "scripts.tests",
            "--suite",
            "e2e",
            cwd=source,
            environment=environment,
        )
    print(f"Staged candidate E2E passed in {time.monotonic() - started:.1f}s")


if __name__ == "__main__":
    main()
