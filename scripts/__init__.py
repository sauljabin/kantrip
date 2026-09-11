"""Shared process, terminal, and rendering helpers for repository scripts."""

from __future__ import annotations

import errno
import os
import pty
import re
import select
import shlex
import signal
import subprocess
import termios
import time
from collections import deque
from collections.abc import Mapping, Sequence

from rich.console import Console

SVG_VIEWBOX = re.compile(r'(<svg\b)(?![^>]*\bwidth=)(?=[^>]*\bviewBox="0 0 ([\d.]+) ([\d.]+)")')


def normalize_svg(svg: str) -> str:
    """Add intrinsic dimensions and remove trailing whitespace from an SVG."""
    svg = SVG_VIEWBOX.sub(
        lambda match: f'{match.group(1)} width="{match.group(2)}" height="{match.group(3)}"',
        svg,
        count=1,
    )
    return "\n".join(line.rstrip() for line in svg.splitlines()) + "\n"


class CommandProcessor:
    """Run named commands in order and render consistent diagnostics."""

    def __init__(
        self,
        commands: Mapping[str, str],
    ) -> None:
        self.commands = commands
        self.console = Console()

    def run(self) -> None:
        for name, command in self.commands.items():
            result = self.execute_command(name, command)
            if result.returncode:
                self._report_failure(name, command, result)
                raise SystemExit(result.returncode)

    def _report_failure(
        self,
        name: str,
        command: str,
        result: subprocess.CompletedProcess[str],
    ) -> None:
        self.console.print(
            "\n[bold red]Error[/] when executing "
            f'[bold blue]"{name}" ([bold yellow]{command}[/])[/]:\n'
            f"[red]{result.stdout}{result.stderr}[/]\n"
        )

    def execute_command(self, name: str, command: str) -> subprocess.CompletedProcess[str]:
        self.console.print()
        self.console.print(f"[bold blue]{name.lower()}:")
        self.console.print(f"[bold yellow]{command}[/]")
        return subprocess.run(shlex.split(command), capture_output=True, text=True, check=False)


class TerminalTimeout(TimeoutError):
    """Raised when an interactive child does not exit before its deadline."""


def run_terminal(
    arguments: Sequence[str],
    commands: Sequence[str],
    *,
    environment: Mapping[str, str],
    ready_text: str | None = None,
    timeout: float = 30,
) -> tuple[int, str]:
    """Run newline-delimited commands in a real terminal and capture decoded output."""
    child, master = _spawn_terminal(arguments, environment)
    child_status: int | None = None
    try:
        child_status, output = _communicate(master, child, commands, ready_text, timeout)
        return os.waitstatus_to_exitcode(child_status), output.decode(errors="replace")
    finally:
        os.close(master)
        if child_status is None:
            _terminate_child(child)


def _spawn_terminal(arguments: Sequence[str], environment: Mapping[str, str]) -> tuple[int, int]:
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
    return child, master


def _communicate(
    master: int,
    child: int,
    commands: Sequence[str],
    ready_text: str | None,
    timeout: float,
) -> tuple[int, bytearray]:
    output = bytearray()
    pending = deque(f"{command}\n".encode() for command in commands)
    current = bytearray()
    next_write = time.monotonic()
    deadline = time.monotonic() + timeout
    ready = ready_text is None
    ready_search_start = 0
    ready_marker = ready_text.encode() if ready_text is not None else None
    while True:
        waited, status = os.waitpid(child, os.WNOHANG)
        if waited:
            _drain(master, output)
            return status, output
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            os.kill(child, signal.SIGTERM)
            raise TerminalTimeout(f"terminal process exceeded {timeout:g} seconds")
        if not current and pending:
            current.extend(pending.popleft())
        can_write = ready and bool(current) and time.monotonic() >= next_write
        readable, writable, _ = select.select(
            (master,), (master,) if can_write else (), (), min(remaining, 0.05)
        )
        if readable:
            _read_available(master, output)
            if (
                not ready
                and ready_marker is not None
                and ready_marker in output[ready_search_start:]
            ):
                ready = True
        if writable:
            written = os.write(master, current)
            del current[:written]
            if not current:
                next_write = time.monotonic() + 0.05
                if ready_marker is not None:
                    ready = False
                    ready_search_start = max(0, len(output) - len(ready_marker) + 1)


def _terminate_child(child: int) -> None:
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


__all__ = ["CommandProcessor", "TerminalTimeout", "normalize_svg", "run_terminal"]
