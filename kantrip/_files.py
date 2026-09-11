"""Internal helpers for securely materializing session files."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TextIO


def write_exclusive_text(path: Path, contents: str, *, mode: int) -> None:
    """Create a text file with explicit permissions without replacing another file."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    stream: TextIO
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents)


__all__ = ["write_exclusive_text"]
