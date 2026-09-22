"""Bounded PTY controller with terminal-state parsing for interactive E2E clients."""

from __future__ import annotations

import codecs
import errno
import fcntl
import os
import pty
import select
import signal
import struct
import termios
import time
from collections.abc import Mapping, Sequence

import pyte


class TerminalProcessError(RuntimeError):
    """Raised when an interactive process misses an observable terminal state."""


class TerminalProcess:
    """Run one real executable and expose its rendered terminal state."""

    def __init__(self, command: Sequence[str], environment: Mapping[str, str]) -> None:
        self.command = tuple(command)
        self.screen = pyte.Screen(160, 48)
        self.stream = pyte.Stream(self.screen)
        self.decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self.child, self.master = pty.fork()
        if self.child == 0:
            try:
                os.execvpe(command[0], list(command), dict(environment))
            except OSError:
                os._exit(127)
        fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", 48, 160, 0, 0))
        os.kill(self.child, signal.SIGWINCH)
        os.set_blocking(self.master, False)
        self.status: int | None = None

    def wait_for(self, expected: Sequence[str], *, timeout: float) -> str:
        """Wait until every marker is present in the rendered screen."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._poll(min(0.2, deadline - time.monotonic()))
            rendered = self.rendered()
            if all(marker in rendered for marker in expected):
                return rendered
            if self.status is not None:
                raise TerminalProcessError(
                    f"interactive process exited before rendering {tuple(expected)!r}:\n{rendered}"
                )
        raise TerminalProcessError(
            f"interactive process did not render {tuple(expected)!r} before the deadline:\n"
            f"{self.rendered()}"
        )

    def write(self, value: str) -> None:
        os.write(self.master, value.encode())

    def close(self) -> None:
        """Ask the application to quit, then terminate only if it remains alive."""
        if self.status is not None:
            os.close(self.master)
            return
        self.write("q")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and self.status is None:
            self._poll(0.1)
        if self.status is None:
            os.kill(self.child, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self.status is None:
                self._poll(0.1)
        if self.status is None:
            os.kill(self.child, signal.SIGKILL)
            _, self.status = os.waitpid(self.child, 0)
        os.close(self.master)

    def rendered(self) -> str:
        return "\n".join(line.rstrip() for line in self.screen.display if line.rstrip())

    def _poll(self, timeout: float) -> None:
        waited, status = os.waitpid(self.child, os.WNOHANG)
        if waited:
            self.status = status
        readable, _, _ = select.select((self.master,), (), (), max(0, timeout))
        if not readable:
            return
        try:
            contents = os.read(self.master, 65536)
        except BlockingIOError:
            return
        except OSError as error:
            if error.errno == errno.EIO:
                return
            raise
        if contents:
            self.stream.feed(self.decoder.decode(contents))


__all__ = ["TerminalProcess", "TerminalProcessError"]
