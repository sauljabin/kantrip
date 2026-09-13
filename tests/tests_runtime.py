import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from kantrip.runtime import (
    LOCK_FILENAME,
    MARKER_FILENAME,
    SESSION_STALE_SECONDS,
    SessionRuntime,
    SessionRuntimeError,
    create_session_runtime,
    resolve_runtime_root,
    scan_sessions,
)


class TestSessionRuntime(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.runtime_base = Path(self.temporary_directory.name)
        self.runtime_base.chmod(0o700)
        self.environment = {"XDG_RUNTIME_DIR": str(self.runtime_base)}
        self.addCleanup(self.temporary_directory.cleanup)

    def test_creates_private_locked_session_and_removes_it_normally(self) -> None:
        runtime = create_session_runtime(self.environment)
        marker = json.loads((runtime.path / MARKER_FILENAME).read_text(encoding="utf-8"))

        self.assertEqual(0o700, stat.S_IMODE(runtime.path.stat().st_mode))
        self.assertEqual(0o600, stat.S_IMODE((runtime.path / LOCK_FILENAME).stat().st_mode))
        self.assertEqual(0o600, stat.S_IMODE((runtime.path / MARKER_FILENAME).stat().st_mode))
        self.assertEqual(
            {"createdAt", "ownerUid", "sessionId", "state", "supervisorPid"},
            set(marker),
        )
        self.assertEqual("preparing", marker["state"])
        created_at = marker["createdAt"]
        self.assertEqual(1, scan_sessions(self.environment).active)

        runtime.mark_running()
        marker = json.loads((runtime.path / MARKER_FILENAME).read_text(encoding="utf-8"))
        self.assertEqual("running", marker["state"])
        self.assertEqual(created_at, marker["createdAt"])
        runtime.close()

        self.assertFalse(runtime.path.exists())
        self.assertEqual(0, scan_sessions(self.environment).active)

    def test_uses_fallback_when_xdg_runtime_directory_is_unsafe(self) -> None:
        self.runtime_base.chmod(0o755)
        with (
            tempfile.TemporaryDirectory() as fallback,
            patch("kantrip.runtime.tempfile.gettempdir", return_value=fallback),
        ):
            expected = Path(fallback) / f"kantrip-{os.getuid()}" / "sessions"

            self.assertEqual(expected, resolve_runtime_root(self.environment))

    def test_rejects_an_unsafe_existing_runtime_root(self) -> None:
        root = resolve_runtime_root(self.environment)
        root.parent.mkdir(mode=0o700)
        root.mkdir(mode=0o700)
        root.chmod(0o755)

        with self.assertRaisesRegex(SessionRuntimeError, "not private"):
            scan_sessions(self.environment)

    def test_rejects_a_dangling_runtime_root_symlink(self) -> None:
        root = resolve_runtime_root(self.environment)
        root.parent.mkdir(mode=0o700)
        root.symlink_to(self.runtime_base / "missing")

        with self.assertRaisesRegex(SessionRuntimeError, "opened safely"):
            scan_sessions(self.environment)

    def test_classifies_recent_and_stale_sessions_by_lock_and_age(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=1000):
            runtime = create_session_runtime(self.environment)
        path = runtime.path
        _abandon(runtime)

        recent = scan_sessions(self.environment, now=1000 + SESSION_STALE_SECONDS - 1)
        stale = scan_sessions(self.environment, now=1000 + SESSION_STALE_SECONDS)

        self.assertEqual(1, recent.recent)
        self.assertEqual(1, stale.stale)
        self.assertTrue(path.exists())

        removed = scan_sessions(
            self.environment,
            remove=True,
            now=1000 + SESSION_STALE_SECONDS,
        )
        self.assertEqual(1, removed.removed)
        self.assertFalse(path.exists())

    def test_pid_does_not_make_an_unlocked_old_session_active(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=1000):
            runtime = create_session_runtime(self.environment)
        _abandon(runtime)

        report = scan_sessions(
            self.environment,
            remove=True,
            now=1000 + SESSION_STALE_SECONDS,
        )

        self.assertEqual(1, report.removed)

    def test_rejects_symlinks_without_removing_the_session(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=1000):
            runtime = create_session_runtime(self.environment)
        path = runtime.path
        outside = self.runtime_base / "outside"
        outside.write_text("keep", encoding="utf-8")
        (path / "escape").symlink_to(outside)
        _abandon(runtime)

        report = scan_sessions(
            self.environment,
            remove=True,
            now=1000 + SESSION_STALE_SECONDS,
        )

        self.assertEqual(1, report.invalid)
        self.assertTrue(path.exists())
        self.assertEqual("keep", outside.read_text(encoding="utf-8"))

    def test_rejects_malformed_and_unexpected_entries(self) -> None:
        runtime = create_session_runtime(self.environment)
        path = runtime.path
        _abandon(runtime)
        (path / MARKER_FILENAME).write_text("not json", encoding="utf-8")
        unexpected = path.parent / "../sessions/not-a-session"
        unexpected.mkdir()

        report = scan_sessions(self.environment, now=time.time() + SESSION_STALE_SECONDS)

        self.assertEqual(2, report.invalid)

    def test_rejects_a_marker_whose_id_does_not_match_its_directory(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=1000):
            runtime = create_session_runtime(self.environment)
        path = runtime.path
        marker_path = path / MARKER_FILENAME
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        marker["sessionId"] = "../outside"
        marker_path.write_text(json.dumps(marker), encoding="utf-8")
        marker_path.chmod(0o600)
        _abandon(runtime)

        report = scan_sessions(
            self.environment,
            remove=True,
            now=1000 + SESSION_STALE_SECONDS,
        )

        self.assertEqual(1, report.invalid)
        self.assertTrue(path.exists())

    def test_bounded_scan_reports_truncation_and_makes_progress(self) -> None:
        runtimes = []
        with patch("kantrip.runtime.time.time", return_value=1000):
            for _ in range(2):
                runtimes.append(create_session_runtime(self.environment))
        for runtime in runtimes:
            _abandon(runtime)

        first = scan_sessions(
            self.environment,
            remove=True,
            limit=1,
            now=1000 + SESSION_STALE_SECONDS,
        )
        second = scan_sessions(
            self.environment,
            remove=True,
            limit=1,
            now=1000 + SESSION_STALE_SECONDS,
        )

        self.assertTrue(first.truncated)
        self.assertEqual(1, first.removed)
        self.assertFalse(second.truncated)
        self.assertEqual(1, second.removed)

    def test_concurrent_cleanup_removes_a_stale_session_once(self) -> None:
        with patch("kantrip.runtime.time.time", return_value=1000):
            runtime = create_session_runtime(self.environment)
        _abandon(runtime)

        def cleanup() -> object:
            return scan_sessions(
                self.environment,
                remove=True,
                now=1000 + SESSION_STALE_SECONDS,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            reports = tuple(executor.map(lambda _: cleanup(), range(2)))

        self.assertEqual(1, sum(report.removed for report in reports))
        self.assertEqual(0, sum(report.invalid + report.failed for report in reports))

    def test_process_crash_leaves_a_recoverable_session(self) -> None:
        script = (
            "import os; "
            "from kantrip.runtime import create_session_runtime; "
            f"runtime = create_session_runtime({self.environment!r}); "
            "os._exit(0)"
        )

        result = subprocess.run([sys.executable, "-c", script], check=False)
        report = scan_sessions(
            self.environment,
            remove=True,
            now=time.time() + SESSION_STALE_SECONDS,
        )

        self.assertEqual(0, result.returncode)
        self.assertEqual(1, report.removed)


def _abandon(runtime: SessionRuntime) -> None:
    runtime._closed = True
    os.close(runtime._lock_descriptor)
    os.close(runtime._session_descriptor)
    os.close(runtime._root_descriptor)


if __name__ == "__main__":
    unittest.main()
