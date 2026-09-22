"""Keep runner-owned Zsh startup files out of interactive E2E sessions."""

from __future__ import annotations

import shlex
import shutil
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
            f'#!/bin/sh\nexec {shlex.quote(installed_zsh)} +d "$@"\n',
            mode=0o700,
        )
        selected = dict(environment)
        selected["ZDOTDIR"] = directory
        selected["PATH"] = f"{wrapper_directory}:{environment.get('PATH', '')}"
        yield selected


__all__ = ["isolated_zsh_environment"]
