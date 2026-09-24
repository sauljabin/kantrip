"""Keep runner-owned Zsh startup files out of interactive E2E sessions."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from kantrip._files import write_exclusive_text


@contextmanager
def isolated_zsh_environment(environment: Mapping[str, str]) -> Iterator[dict[str, str]]:
    """Run installed Zsh without global RC while retaining Kantrip's session RC."""
    installed_zsh = shutil.which("zsh", path=environment.get("PATH"))
    if installed_zsh is None:
        raise RuntimeError("the released Zsh executable is unavailable")
    with tempfile.TemporaryDirectory(prefix="kantrip-e2e-zsh-") as directory:
        root = Path(directory)
        wrapper_directory = root / "bin"
        wrapper_directory.mkdir(mode=0o700)
        write_exclusive_text(
            wrapper_directory / "zsh",
            f'#!/bin/sh\nexec {shlex.quote(installed_zsh)} -d "$@"\n',
            mode=0o700,
        )
        selected = dict(environment)
        selected["ZDOTDIR"] = directory
        selected["PATH"] = f"{wrapper_directory}:{environment.get('PATH', '')}"
        _verify_zsh_startup(wrapper_directory / "zsh", root, selected)
        yield selected


def _verify_zsh_startup(wrapper: Path, root: Path, environment: Mapping[str, str]) -> None:
    probe_directory = root / "probe"
    probe_directory.mkdir(mode=0o700)
    write_exclusive_text(
        probe_directory / ".zshrc", "print -r -- KANTRIP_E2E_RC_LOADED\n", mode=0o600
    )
    probe_environment = dict(environment)
    probe_environment["ZDOTDIR"] = str(probe_directory)
    result = subprocess.run(
        (str(wrapper), "-ic", "print -r -- KANTRIP_E2E_GLOBAL_RCS=$options[globalrcs]"),
        env=probe_environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    output = result.stdout + result.stderr
    if (
        result.returncode != 0
        or "KANTRIP_E2E_RC_LOADED" not in output
        or "KANTRIP_E2E_GLOBAL_RCS=off" not in output
        or "insecure directories" in output
    ):
        raise RuntimeError(
            "E2E Zsh startup did not isolate global RC while loading the session RC "
            f"(exit={result.returncode}, "
            f"session_rc={'KANTRIP_E2E_RC_LOADED' in output}, "
            f"global_rc_disabled={'KANTRIP_E2E_GLOBAL_RCS=off' in output}, "
            f"completion_prompt={'insecure directories' in output})"
        )


__all__ = ["isolated_zsh_environment"]


if __name__ == "__main__":
    with isolated_zsh_environment(os.environ):
        print("E2E Zsh startup preflight passed")
