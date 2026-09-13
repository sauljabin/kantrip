import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from kantrip.supervisor import SupervisorError, run_supervised_process
from scripts import run_terminal


class TestProcessSupervisor(unittest.TestCase):
    def test_preserves_normal_exit_status(self) -> None:
        result = run_supervised_process(
            [sys.executable, "-c", "raise SystemExit(17)"],
            environment=os.environ,
        )

        self.assertEqual(17, result)

    def test_maps_forwarded_signals_to_shell_exit_status(self) -> None:
        for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signal=number), tempfile.TemporaryDirectory() as directory:
                ready = Path(directory) / "ready"
                child_script = (
                    "import signal; from pathlib import Path; "
                    f"signal.signal({number}, signal.SIG_DFL); "
                    f"Path({str(ready)!r}).write_text('ready'); "
                    "signal.pause()"
                )
                supervisor = _start_supervisor(child_script)
                try:
                    _wait_for_path(ready)
                    os.kill(supervisor.pid, number)
                    output, _ = supervisor.communicate(timeout=3)
                finally:
                    _stop_process(supervisor)

                self.assertEqual(0, supervisor.returncode)
                self.assertIn(f"EXIT={128 + number}", output)

    def test_shutdown_timeout_escalates_to_sigkill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / "ready"
            child_script = (
                "import signal; from pathlib import Path; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"Path({str(ready)!r}).write_text('ready'); "
                "signal.pause()"
            )
            supervisor = _start_supervisor(child_script, grace_seconds=0.1)
            try:
                _wait_for_path(ready)
                os.kill(supervisor.pid, signal.SIGTERM)
                output, _ = supervisor.communicate(timeout=3)
            finally:
                _stop_process(supervisor)

        self.assertIn("EXIT=137", output)

    def test_repeated_signal_escalates_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / "ready"
            child_script = (
                "import signal; from pathlib import Path; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"Path({str(ready)!r}).write_text('ready'); "
                "signal.pause()"
            )
            supervisor = _start_supervisor(child_script, grace_seconds=5)
            try:
                _wait_for_path(ready)
                os.kill(supervisor.pid, signal.SIGTERM)
                time.sleep(0.05)
                os.kill(supervisor.pid, signal.SIGTERM)
                output, _ = supervisor.communicate(timeout=3)
            finally:
                _stop_process(supervisor)

        self.assertIn("EXIT=137", output)

    def test_signal_reaches_grandchild_in_managed_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            grandchild_ready = root / "grandchild-ready"
            events = root / "events"
            grandchild_script = _signal_recording_script(grandchild_ready, events, "grandchild")
            child_script = (
                "import signal, subprocess, sys, time\n"
                "from pathlib import Path\n"
                f"events = Path({str(events)!r})\n"
                "def stop(*_):\n"
                " events.open('a').write('leader\\n'); raise SystemExit(0)\n"
                "signal.signal(signal.SIGTERM, stop)\n"
                f"subprocess.Popen([sys.executable, '-c', {grandchild_script!r}])\n"
                f"grandchild_ready = Path({str(grandchild_ready)!r})\n"
                "while not grandchild_ready.exists(): time.sleep(0.01)\n"
                f"Path({str(ready)!r}).write_text('ready')\n"
                "signal.pause()"
            )
            supervisor = _start_supervisor(child_script)
            try:
                _wait_for_path(ready)
                os.kill(supervisor.pid, signal.SIGTERM)
                supervisor.communicate(timeout=3)
            finally:
                _stop_process(supervisor)

            recorded = events.read_text(encoding="utf-8").splitlines()

        self.assertCountEqual(("leader", "grandchild"), recorded)

    def test_process_that_creates_a_new_session_is_outside_the_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "detached-pid"
            detached_script = "import time; time.sleep(30)"
            child_script = (
                "import subprocess, sys; from pathlib import Path; "
                f"child = subprocess.Popen([sys.executable, '-c', {detached_script!r}], "
                "start_new_session=True); "
                f"Path({str(pid_file)!r}).write_text(str(child.pid))"
            )

            code = run_supervised_process(
                [sys.executable, "-c", child_script],
                environment=os.environ,
                grace_seconds=0.1,
            )
            _wait_for_path(pid_file)
            detached_pid = int(pid_file.read_text(encoding="utf-8"))
            try:
                os.kill(detached_pid, 0)
            finally:
                os.kill(detached_pid, signal.SIGKILL)

        self.assertEqual(0, code)

    def test_start_failure_is_actionable(self) -> None:
        with self.assertRaisesRegex(SupervisorError, "command could not be started"):
            run_supervised_process(
                ["/definitely/not/a/kantrip-command"],
                environment=os.environ,
            )

    def test_one_off_command_can_read_from_the_terminal(self) -> None:
        child_script = "value = input('READY>'); print('VALUE=' + value, flush=True)"
        arguments = [sys.executable, "-c", child_script]
        wrapper = (
            "import os; from kantrip.supervisor import run_supervised_process; "
            f"code = run_supervised_process({arguments!r}, environment=os.environ); "
            "print(f'EXIT={code}', flush=True)"
        )

        status, output = run_terminal(
            [sys.executable, "-c", wrapper],
            ["hello"],
            environment=os.environ,
            ready_text="READY>",
            timeout=3,
        )

        self.assertEqual(0, status, output)
        self.assertIn("VALUE=hello", output)
        self.assertIn("EXIT=0", output)


class TestInteractiveSupervisor(unittest.TestCase):
    def test_preserves_input_queued_before_raw_mode(self) -> None:
        child_script = "value = input(); print('EARLY=' + value, flush=True)"
        wrapper = _interactive_wrapper(child_script)

        status, output = run_terminal(
            [sys.executable, "-c", wrapper],
            ["queued"],
            environment=os.environ,
            timeout=3,
        )

        self.assertEqual(0, status, output)
        self.assertIn("EARLY=queued", output)
        self.assertIn("EXIT=0", output)

    def test_bridges_input_and_restores_terminal(self) -> None:
        child_script = "value = input('READY>'); print('ECHO=' + value, flush=True)"
        wrapper = _interactive_wrapper(child_script, check_terminal=True)

        status, output = run_terminal(
            [sys.executable, "-c", wrapper],
            ["hello"],
            environment=os.environ,
            ready_text="READY>",
        )

        self.assertEqual(0, status, output)
        self.assertIn("ECHO=hello", output)
        self.assertIn("RESTORED=True", output)
        self.assertIn("EXIT=0", output)

    def test_ctrl_c_reaches_interactive_child(self) -> None:
        child_script = (
            "import signal; signal.signal(signal.SIGINT, signal.SIG_DFL); "
            "print('READY', flush=True); signal.pause()"
        )
        wrapper = _interactive_wrapper(child_script)

        status, output = run_terminal(
            [sys.executable, "-c", wrapper],
            ["\x03"],
            environment=os.environ,
            ready_text="READY",
        )

        self.assertEqual(0, status, output)
        self.assertIn("EXIT=130", output)

    def test_sigwinch_is_applied_to_the_child_pty(self) -> None:
        child_script = (
            "import signal\n"
            "def resized(*_):\n"
            " print('RESIZED', flush=True); raise SystemExit(0)\n"
            "signal.signal(signal.SIGWINCH, resized)\n"
            "signal.pause()"
        )
        arguments = [sys.executable, "-c", child_script]
        wrapper = (
            "import fcntl, os, signal, struct, termios, threading, time\n"
            "from kantrip.supervisor import run_supervised_process\n"
            "def resize():\n"
            " time.sleep(0.2)\n"
            " fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack('HHHH', 37, 111, 0, 0))\n"
            " os.kill(os.getpid(), signal.SIGWINCH)\n"
            "threading.Thread(target=resize, daemon=True).start()\n"
            f"code = run_supervised_process({arguments!r}, environment=os.environ, "
            "interactive=True)\n"
            "print(f'EXIT={code}', flush=True)"
        )

        status, output = run_terminal(
            [sys.executable, "-c", wrapper],
            [],
            environment=os.environ,
            timeout=3,
        )

        self.assertEqual(0, status, output)
        self.assertIn("RESIZED", output)
        self.assertIn("EXIT=0", output)


def _start_supervisor(child_script: str, *, grace_seconds: float = 0.5) -> subprocess.Popen[str]:
    arguments = [sys.executable, "-c", child_script]
    wrapper = (
        "import os; from kantrip.supervisor import run_supervised_process; "
        f"code = run_supervised_process({arguments!r}, environment=os.environ, "
        f"grace_seconds={grace_seconds!r}); "
        "print(f'EXIT={code}', flush=True)"
    )
    return subprocess.Popen(
        [sys.executable, "-c", wrapper],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ,
    )


def _interactive_wrapper(child_script: str, *, check_terminal: bool = False) -> str:
    arguments = [sys.executable, "-c", child_script]
    before = "before = termios.tcgetattr(0); " if check_terminal else ""
    after = (
        "after = termios.tcgetattr(0); dynamic = getattr(termios, 'PENDIN', 0); "
        "restored = before[:3] == after[:3] and "
        "(before[3] & ~dynamic) == (after[3] & ~dynamic) and before[4:] == after[4:]; "
        "print(f'RESTORED={restored}', flush=True); "
        if check_terminal
        else ""
    )
    return (
        "import os, termios; from kantrip.supervisor import run_supervised_process; "
        f"{before}"
        f"code = run_supervised_process({arguments!r}, environment=os.environ, interactive=True); "
        f"{after}"
        "print(f'EXIT={code}', flush=True)"
    )


def _signal_recording_script(ready: Path, events: Path, label: str) -> str:
    return (
        "import signal\n"
        "from pathlib import Path\n"
        f"events = Path({str(events)!r})\n"
        "def stop(*_):\n"
        f" events.open('a').write({label!r} + '\\n'); raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        f"Path({str(ready)!r}).write_text('ready')\n"
        "signal.pause()"
    )


def _wait_for_path(path: Path, timeout: float = 3) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {path.name}")
        time.sleep(0.01)


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
