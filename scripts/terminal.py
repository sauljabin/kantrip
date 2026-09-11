"""Small PTY runner shared by shell contract and manual smoke workflows."""

from __future__ import annotations

import errno
import os
import pty
import select
import signal
import termios
import time
from collections import deque
from collections.abc import Mapping, Sequence


class TerminalTimeout(TimeoutError):
    """Raised when an interactive child does not exit before its deadline."""


def run_terminal(
    arguments: Sequence[str],
    commands: Sequence[str],
    *,
    environment: Mapping[str, str],
    timeout: float = 30,
) -> tuple[int, str]:
    """Run newline-delimited commands in a real terminal and capture decoded output."""
    ready_reader, ready_writer = os.pipe()
    child, master = pty.fork()
    if child == 0:
        os.close(ready_reader)
        attributes = termios.tcgetattr(0)
        attributes[3] &= ~termios.ECHO
        termios.tcsetattr(0, termios.TCSANOW, attributes)
        os.write(ready_writer, b"1")
        os.close(ready_writer)
        try:
            os.execvpe(arguments[0], list(arguments), dict(environment))
        except OSError:
            os._exit(127)
    os.close(ready_writer)
    os.read(ready_reader, 1)
    os.close(ready_reader)
    os.set_blocking(master, False)
    output = bytearray()
    pending = deque(f"{command}\n".encode() for command in commands)
    current = bytearray()
    next_write = time.monotonic()
    deadline = time.monotonic() + timeout
    child_status: int | None = None
    try:
        while child_status is None:
            waited, status = os.waitpid(child, os.WNOHANG)
            if waited:
                child_status = status
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                os.kill(child, signal.SIGTERM)
                raise TerminalTimeout(f"terminal process exceeded {timeout:g} seconds")
            if not current and pending:
                current.extend(pending.popleft())
            can_write = bool(current) and time.monotonic() >= next_write
            readable, writable, _ = select.select(
                (master,), (master,) if can_write else (), (), min(remaining, 0.05)
            )
            if readable:
                _read_available(master, output)
            if writable:
                written = os.write(master, current)
                del current[:written]
                if not current:
                    next_write = time.monotonic() + 0.05
        _drain(master, output)
        return os.waitstatus_to_exitcode(child_status), output.decode(errors="replace")
    finally:
        os.close(master)
        if child_status is None:
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child, 0)


def _drain(master: int, output: bytearray) -> None:
    while True:
        ready, _, _ = select.select((master,), (), (), 0)
        if not ready or not _read_available(master, output):
            return


def _read_available(master: int, output: bytearray) -> bool:
    try:
        contents = os.read(master, 65536)
    except BlockingIOError:
        return True
    except OSError as error:
        if error.errno == errno.EIO:
            return False
        raise
    output.extend(contents)
    return bool(contents)


__all__ = ["TerminalTimeout", "run_terminal"]
