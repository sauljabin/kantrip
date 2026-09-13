"""Supervise one-off process groups and interactive PTY sessions."""

from __future__ import annotations

import errno
import fcntl
import os
import pty
import select
import signal
import subprocess
import sys
import termios
import time
import tty
from collections.abc import Callable, Mapping, Sequence
from types import FrameType
from typing import Any

SHUTDOWN_GRACE_SECONDS = 5.0
_POLL_SECONDS = 0.05
_FORWARDED_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


class SupervisorError(RuntimeError):
    """Raised when a supervised child cannot be started or managed."""


class _SignalForwarder:
    def __init__(self, sender: Callable[[int], None], grace_seconds: float) -> None:
        self._sender = sender
        self._grace_seconds = grace_seconds
        self._previous: dict[int, Any] = {}
        self.deadline: float | None = None
        self.escalated = False

    def install(self) -> None:
        try:
            for number in _FORWARDED_SIGNALS:
                self._previous[number] = signal.getsignal(number)
                signal.signal(number, self._handle)
        except BaseException:
            self.restore()
            raise

    def restore(self) -> None:
        for number, handler in self._previous.items():
            signal.signal(number, handler)

    def escalate_if_due(self) -> None:
        if self.deadline is None or self.escalated or time.monotonic() < self.deadline:
            return
        self._send(signal.SIGKILL)
        self.escalated = True

    def _handle(self, number: int, frame: FrameType | None) -> None:
        del frame
        if self.deadline is None:
            self._send(number)
            self.deadline = time.monotonic() + self._grace_seconds
            return
        if not self.escalated:
            self._send(signal.SIGKILL)
            self.escalated = True

    def _send(self, number: int) -> None:
        try:
            self._sender(number)
        except ProcessLookupError:
            pass


class _ForegroundTerminal:
    def __init__(self, child_group: int) -> None:
        self._descriptor: int | None = None
        self._attributes: Any = None
        self._foreground_group: int | None = None
        try:
            descriptor = sys.stdin.fileno()
        except (AttributeError, OSError):
            return
        if not os.isatty(descriptor):
            return
        self._descriptor = descriptor
        self._attributes = termios.tcgetattr(descriptor)
        self._foreground_group = os.tcgetpgrp(descriptor)
        _set_foreground_group(descriptor, child_group)

    def restore(self) -> None:
        if self._descriptor is None:
            return
        if self._foreground_group is not None:
            _set_foreground_group(self._descriptor, self._foreground_group)
        if self._attributes is not None:
            termios.tcsetattr(self._descriptor, termios.TCSADRAIN, self._attributes)


class _RawTerminal:
    def __init__(self) -> None:
        try:
            self.input_descriptor = sys.stdin.fileno()
            self.output_descriptor = sys.stdout.fileno()
        except (AttributeError, OSError) as error:
            raise SupervisorError("interactive shell requires terminal file descriptors") from error
        self._attributes: Any = None
        self._foreground_group: int | None = None
        if os.isatty(self.input_descriptor):
            self._attributes = termios.tcgetattr(self.input_descriptor)
            self._foreground_group = os.tcgetpgrp(self.input_descriptor)

    def activate(self) -> None:
        """Switch the parent side to raw mode after the child PTY is created."""
        if self._attributes is not None:
            tty.setraw(self.input_descriptor, when=termios.TCSANOW)

    def restore(self) -> None:
        if self._foreground_group is not None:
            _set_foreground_group(self.input_descriptor, self._foreground_group)
        if self._attributes is not None:
            termios.tcsetattr(self.input_descriptor, termios.TCSADRAIN, self._attributes)


def run_supervised_process(
    arguments: Sequence[str],
    *,
    environment: Mapping[str, str],
    interactive: bool = False,
    grace_seconds: float = SHUTDOWN_GRACE_SECONDS,
) -> int:
    """Run a child inside the appropriate bounded POSIX process boundary."""
    if interactive:
        return _run_interactive(arguments, environment, grace_seconds)
    return _run_one_off(arguments, environment, grace_seconds)


def _run_one_off(
    arguments: Sequence[str], environment: Mapping[str, str], grace_seconds: float
) -> int:
    try:
        process = subprocess.Popen(
            list(arguments),
            env=dict(environment),
            start_new_session=True,
        )
    except OSError as error:
        raise SupervisorError(f"command could not be started: {arguments[0]}") from error
    forwarder = _SignalForwarder(lambda number: os.killpg(process.pid, number), grace_seconds)
    terminal: _ForegroundTerminal | None = None
    handlers_installed = False
    try:
        terminal = _ForegroundTerminal(process.pid)
        forwarder.install()
        handlers_installed = True
        return_code = _wait_for_process(process, forwarder)
    finally:
        try:
            if handlers_installed:
                forwarder.restore()
        finally:
            try:
                if terminal is not None:
                    terminal.restore()
            finally:
                if process.poll() is None:
                    _terminate_remaining_group(
                        process.pid,
                        grace_seconds,
                        deadline=forwarder.deadline,
                    )
                    process.wait()
                else:
                    _terminate_remaining_group(
                        process.pid,
                        grace_seconds,
                        deadline=forwarder.deadline,
                    )
    return _shell_exit_code(return_code)


def _wait_for_process(process: subprocess.Popen[bytes], forwarder: _SignalForwarder) -> int:
    while True:
        try:
            return process.wait(timeout=_POLL_SECONDS)
        except subprocess.TimeoutExpired:
            forwarder.escalate_if_due()


def _run_interactive(
    arguments: Sequence[str], environment: Mapping[str, str], grace_seconds: float
) -> int:
    terminal = _RawTerminal()
    child, master = pty.fork()
    if child == 0:
        _exec_pty_child(arguments, environment)
    forwarder = _SignalForwarder(
        lambda number: _signal_pty_child(master, child, number), grace_seconds
    )
    resize_requested = [False]
    previous_resize: Any = None
    resize_installed = False
    handlers_installed = False

    def request_resize(number: int, frame: FrameType | None) -> None:
        del number, frame
        resize_requested[0] = True

    status: int | None = None
    try:
        terminal.activate()
        os.set_blocking(master, False)
        _copy_window_size(terminal.input_descriptor, master)
        previous_resize = signal.getsignal(signal.SIGWINCH)
        signal.signal(signal.SIGWINCH, request_resize)
        resize_installed = True
        forwarder.install()
        handlers_installed = True
        status = _bridge_pty(master, child, terminal, forwarder, resize_requested)
    finally:
        try:
            if handlers_installed:
                forwarder.restore()
        finally:
            try:
                if resize_installed:
                    signal.signal(signal.SIGWINCH, previous_resize)
            finally:
                try:
                    terminal.restore()
                finally:
                    os.close(master)
                    if status is None:
                        _terminate_and_reap(child, grace_seconds)
    return _shell_exit_code(os.waitstatus_to_exitcode(status))


def _exec_pty_child(arguments: Sequence[str], environment: Mapping[str, str]) -> None:
    try:
        os.execvpe(arguments[0], list(arguments), dict(environment))
    except OSError:
        os._exit(127)


def _bridge_pty(
    master: int,
    child: int,
    terminal: _RawTerminal,
    forwarder: _SignalForwarder,
    resize_requested: list[bool],
) -> int:
    input_open = True
    output_open = True
    while True:
        waited, status = os.waitpid(child, os.WNOHANG)
        if waited:
            if output_open:
                _drain_pty(master, terminal.output_descriptor)
            return status
        forwarder.escalate_if_due()
        if resize_requested[0]:
            _copy_window_size(terminal.input_descriptor, master)
            resize_requested[0] = False
        readers = [master] if output_open else []
        if input_open:
            readers.append(terminal.input_descriptor)
        ready, _, _ = select.select(readers, (), (), _POLL_SECONDS)
        if master in ready:
            output_open = _copy_pty_output(master, terminal.output_descriptor)
        if terminal.input_descriptor in ready:
            input_open = _copy_terminal_input(terminal.input_descriptor, master)


def _copy_terminal_input(input_descriptor: int, master: int) -> bool:
    contents = os.read(input_descriptor, 65536)
    if not contents:
        try:
            os.write(master, b"\x04")
        except OSError:
            pass
        return False
    _write_bytes(master, contents)
    return True


def _copy_pty_output(master: int, output_descriptor: int) -> bool:
    try:
        contents = os.read(master, 65536)
    except BlockingIOError:
        return True
    except OSError as error:
        if error.errno == errno.EIO:
            return False
        raise
    if not contents:
        return False
    _write_bytes(output_descriptor, contents)
    return True


def _drain_pty(master: int, output_descriptor: int) -> None:
    while True:
        ready, _, _ = select.select((master,), (), (), 0)
        if not ready or not _copy_pty_output(master, output_descriptor):
            return


def _write_bytes(descriptor: int, contents: bytes) -> None:
    remaining = memoryview(contents)
    while remaining:
        written = os.write(descriptor, remaining)
        if not written:
            raise OSError(errno.EIO, "terminal write made no progress")
        remaining = remaining[written:]


def _copy_window_size(source: int, target: int) -> None:
    if not os.isatty(source):
        return
    try:
        size = fcntl.ioctl(source, termios.TIOCGWINSZ, b"\0" * 8)
        fcntl.ioctl(target, termios.TIOCSWINSZ, size)
    except OSError:
        pass


def _signal_pty_child(master: int, child: int, number: int) -> None:
    groups = {child}
    try:
        groups.add(os.tcgetpgrp(master))
    except OSError:
        pass
    for group in groups:
        try:
            os.killpg(group, number)
        except ProcessLookupError:
            pass


def _terminate_remaining_group(
    group: int,
    grace_seconds: float,
    *,
    deadline: float | None = None,
) -> None:
    if not _group_exists(group):
        return
    try:
        os.killpg(group, signal.SIGTERM)
    except ProcessLookupError:
        return
    if deadline is None:
        deadline = time.monotonic() + grace_seconds
    while _group_exists(group) and time.monotonic() < deadline:
        time.sleep(_POLL_SECONDS)
    if _group_exists(group):
        try:
            os.killpg(group, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _terminate_and_reap(child: int, grace_seconds: float) -> None:
    try:
        os.killpg(child, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            waited, _ = os.waitpid(child, os.WNOHANG)
        except ChildProcessError:
            return
        if waited:
            return
        time.sleep(_POLL_SECONDS)
    try:
        os.killpg(child, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(child, 0)
    except ChildProcessError:
        pass


def _group_exists(group: int) -> bool:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _set_foreground_group(descriptor: int, group: int) -> None:
    previous = signal.getsignal(signal.SIGTTOU)
    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    try:
        os.tcsetpgrp(descriptor, group)
    except OSError:
        pass
    finally:
        signal.signal(signal.SIGTTOU, previous)


def _shell_exit_code(return_code: int) -> int:
    return 128 - return_code if return_code < 0 else return_code


__all__ = ["SHUTDOWN_GRACE_SECONDS", "SupervisorError", "run_supervised_process"]
