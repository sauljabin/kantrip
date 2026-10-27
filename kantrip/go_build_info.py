"""Read the build information a Go toolchain embeds in an executable.

Some Go clients' version commands report a placeholder in release builds, but
the module version recorded by the toolchain is always the tagged release, so
an adapter can read the installed release without running the client. This
follows `debug/buildinfo` for executables built by Go 1.18 or newer, whose
header carries the version and module strings inline.
"""

from __future__ import annotations

import mmap

_MAGIC = b"\xff Go buildinf:"
_HEADER_SIZE = 32
_FLAGS_OFFSET = 15
_INLINE_STRINGS = 0x2
_ALIGNMENT = 16
# The module information is framed by two 16-byte sentinels.
_SENTINEL_SIZE = 16
_STRING_LIMIT = 1 << 20


def go_build_info(path: str) -> str | None:
    """Return the Go version line and module information of an executable, if any.

    The result reads like `go version -m`: the toolchain release, then lines
    such as `mod<TAB>example.com/tool<TAB>v1.2.3`. None means the file could
    not be read or carries no inline Go build information.
    """
    try:
        with (
            open(path, "rb") as stream,
            mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as data,
        ):
            return _search(data)
    except (OSError, ValueError):
        return None


def _search(data: mmap.mmap) -> str | None:
    position = data.find(_MAGIC)
    while position != -1:
        if position % _ALIGNMENT == 0:
            info = _decode(data, position)
            if info is not None:
                return info
        position = data.find(_MAGIC, position + 1)
    return None


def _decode(data: mmap.mmap, position: int) -> str | None:
    if position + _HEADER_SIZE > len(data):
        return None
    if not data[position + _FLAGS_OFFSET] & _INLINE_STRINGS:
        return None
    version = _string(data, position + _HEADER_SIZE)
    if version is None or not version[0]:
        return None
    module = _string(data, version[1])
    if module is None:
        return None
    framed = module[0]
    if len(framed) <= 2 * _SENTINEL_SIZE or framed[-_SENTINEL_SIZE - 1] != ord("\n"):
        return None
    contents = framed[_SENTINEL_SIZE:-_SENTINEL_SIZE]
    return f"{version[0].decode(errors='replace')}\n{contents.decode(errors='replace')}"


def _string(data: mmap.mmap, position: int) -> tuple[bytes, int] | None:
    """Decode one unsigned-varint length-prefixed string and the position after it."""
    length = 0
    shift = 0
    while position < len(data) and shift < 64:
        byte = data[position]
        position += 1
        length |= (byte & 0x7F) << shift
        if byte < 0x80:
            end = position + length
            if length > _STRING_LIMIT or end > len(data):
                return None
            return data[position:end], end
        shift += 7
    return None


__all__ = ["go_build_info"]
