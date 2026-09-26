"""Own, inspect, and safely remove Kantrip session runtime directories."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import secrets
import stat
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

SESSION_STALE_SECONDS = 300
AUTOMATIC_SCAN_LIMIT = 256
SESSION_DIRECTORY_PATTERN = re.compile(r"session-([0-9a-f]{32})\Z")
MARKER_FILENAME = "session.json"
LOCK_FILENAME = "session.lock"
_MARKER_LIMIT = 4096
_MARKER_KEYS = frozenset(
    {
        "sessionId",
        "ownerUid",
        "supervisorPid",
        "createdAt",
        "state",
        "profileId",
        "profileRevision",
    }
)
_MARKER_STATES = frozenset({"preparing", "running"})
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class SessionRuntimeError(RuntimeError):
    """Raised when the session runtime cannot be used safely."""


class _InvalidSessionError(RuntimeError):
    pass


@dataclass(frozen=True)
class SessionScan:
    """Aggregate state from one bounded or complete runtime scan."""

    root: Path
    exists: bool = False
    active: int = 0
    recent: int = 0
    stale: int = 0
    removed: int = 0
    invalid: int = 0
    failed: int = 0
    truncated: bool = False
    observations: tuple[SessionObservation, ...] = ()
    invalid_paths: tuple[Path, ...] = ()

    @property
    def has_errors(self) -> bool:
        """Return whether the scan could not validate or complete all work."""
        return bool(self.invalid or self.failed or self.truncated)


@dataclass(frozen=True)
class _RuntimeLocation:
    root: Path
    private_parent: Path


@dataclass(frozen=True)
class SessionObservation:
    """One validated session marker and its descriptor-proven state."""

    session_id: str
    profile_id: str
    profile_revision: int
    supervisor_pid: int
    created_at: int
    state: str
    path: Path


@dataclass
class SessionRuntime:
    """One private session directory with held directory and lock descriptors."""

    session_id: str
    path: Path
    _root_descriptor: int
    _session_descriptor: int
    _lock_descriptor: int
    _owner_uid: int
    _created_at: int
    _profile_id: str
    _profile_revision: int
    _closed: bool = False

    def mark_running(self) -> None:
        """Record that preparation completed and the child is ready to start."""
        _write_marker(
            self._session_descriptor,
            _marker(
                self.session_id,
                self._owner_uid,
                "running",
                created_at=self._created_at,
                profile_id=self._profile_id,
                profile_revision=self._profile_revision,
            ),
        )

    def close(self) -> None:
        """Remove the owned directory while retaining the liveness lock."""
        if self._closed:
            return
        self._closed = True
        name = self.path.name
        error: OSError | None = None
        try:
            _delete_open_session(
                self._root_descriptor,
                name,
                self._session_descriptor,
                self._owner_uid,
            )
        except OSError as caught:
            error = caught
        finally:
            os.close(self._lock_descriptor)
            os.close(self._session_descriptor)
            os.close(self._root_descriptor)
        if error is not None:
            raise SessionRuntimeError("session directory could not be removed safely") from error


def resolve_runtime_root(environment: Mapping[str, str] | None = None) -> Path:
    """Resolve the preferred safe runtime root without creating it."""
    return _runtime_location(environment).root


def create_session_runtime(
    profile_id: str,
    profile_revision: int,
    environment: Mapping[str, str] | None = None,
) -> SessionRuntime:
    """Create and lock one private session runtime directory."""
    location = _runtime_location(environment)
    _validate_profile_generation(profile_id, profile_revision)
    owner_uid = os.getuid()
    _ensure_private_directory(location.private_parent, owner_uid)
    _ensure_private_directory(location.root, owner_uid)
    root_descriptor = _open_private_directory(location.root, owner_uid)
    session_id = secrets.token_hex(16)
    name = f"session-{session_id}"
    directory_created = False
    session_descriptor: int | None = None
    lock_descriptor: int | None = None
    try:
        os.mkdir(name, 0o700, dir_fd=root_descriptor)
        directory_created = True
        session_descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=root_descriptor)
        os.fchmod(session_descriptor, 0o700)
        lock_descriptor = os.open(
            LOCK_FILENAME,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=session_descriptor,
        )
        os.fchmod(lock_descriptor, 0o600)
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        created_at = int(time.time())
        _write_marker(
            session_descriptor,
            _marker(
                session_id,
                owner_uid,
                "preparing",
                created_at=created_at,
                profile_id=profile_id,
                profile_revision=profile_revision,
            ),
        )
        return SessionRuntime(
            session_id,
            location.root / name,
            root_descriptor,
            session_descriptor,
            lock_descriptor,
            owner_uid,
            created_at,
            profile_id,
            profile_revision,
        )
    except (OSError, ValueError) as error:
        if lock_descriptor is not None:
            os.close(lock_descriptor)
        if session_descriptor is not None:
            _discard_incomplete_session(root_descriptor, name, session_descriptor)
            os.close(session_descriptor)
        elif directory_created:
            try:
                os.rmdir(name, dir_fd=root_descriptor)
            except OSError:
                pass
        os.close(root_descriptor)
        raise SessionRuntimeError("session runtime could not be created safely") from error


def scan_sessions(
    environment: Mapping[str, str] | None = None,
    *,
    remove: bool = False,
    limit: int | None = None,
    now: float | None = None,
) -> SessionScan:
    """Inspect sessions and optionally remove validated stale entries."""
    location = _runtime_location(environment)
    owner_uid = os.getuid()
    root_descriptor = _open_runtime_for_scan(location, owner_uid)
    if root_descriptor is None:
        return SessionScan(location.root)
    try:
        try:
            fcntl.flock(
                root_descriptor,
                fcntl.LOCK_EX if remove else fcntl.LOCK_SH,
            )
            return _scan_open_root(
                location.root,
                root_descriptor,
                owner_uid,
                remove=remove,
                limit=limit,
                now=time.time() if now is None else now,
            )
        except OSError as error:
            raise SessionRuntimeError("session runtime could not be scanned safely") from error
    finally:
        os.close(root_descriptor)


def cleanup_abandoned_sessions(environment: Mapping[str, str] | None = None) -> SessionScan:
    """Run the bounded best-effort janitor used before a new session."""
    return scan_sessions(environment, remove=True, limit=AUTOMATIC_SCAN_LIMIT)


def _runtime_location(environment: Mapping[str, str] | None) -> _RuntimeLocation:
    env = os.environ if environment is None else environment
    owner_uid = os.getuid()
    configured = env.get("XDG_RUNTIME_DIR")
    if configured:
        base = Path(configured)
        if base.is_absolute() and _is_private_directory(base, owner_uid):
            private_parent = base / "kantrip"
            return _RuntimeLocation(private_parent / "sessions", private_parent)
    private_parent = Path(tempfile.gettempdir()) / f"kantrip-{owner_uid}"
    return _RuntimeLocation(private_parent / "sessions", private_parent)


def _is_private_directory(path: Path, owner_uid: int) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == owner_uid
        and stat.S_IMODE(metadata.st_mode) == 0o700
    )


def _ensure_private_directory(path: Path, owner_uid: int) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except FileNotFoundError as error:
        raise SessionRuntimeError(f"runtime parent does not exist: {path.parent}") from error
    if not _is_private_directory(path, owner_uid):
        raise SessionRuntimeError(f"runtime directory is not private and user-owned: {path}")


def _open_private_directory(path: Path, owner_uid: int) -> int:
    try:
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as error:
        raise SessionRuntimeError(
            f"runtime directory could not be opened safely: {path}"
        ) from error
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        os.close(descriptor)
        raise SessionRuntimeError(f"runtime directory is not private and user-owned: {path}")
    return descriptor


def _open_runtime_for_scan(location: _RuntimeLocation, owner_uid: int) -> int | None:
    parent_exists = _path_entry_exists(location.private_parent)
    root_exists = _path_entry_exists(location.root)
    if not parent_exists and not root_exists:
        return None
    if not _is_private_directory(location.private_parent, owner_uid):
        raise SessionRuntimeError(
            f"runtime directory is not private and user-owned: {location.private_parent}"
        )
    if not root_exists:
        return None
    return _open_private_directory(location.root, owner_uid)


def _path_entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def _marker(
    session_id: str,
    owner_uid: int,
    state: str,
    *,
    created_at: int,
    profile_id: str,
    profile_revision: int,
) -> dict[str, Any]:
    return {
        "sessionId": session_id,
        "ownerUid": owner_uid,
        "supervisorPid": os.getpid(),
        "createdAt": created_at,
        "state": state,
        "profileId": profile_id,
        "profileRevision": profile_revision,
    }


def _write_marker(session_descriptor: int, marker: Mapping[str, Any]) -> None:
    contents = (json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary_name = ".session.json.tmp"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=session_descriptor,
        )
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, contents)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.replace(
            temporary_name,
            MARKER_FILENAME,
            src_dir_fd=session_descriptor,
            dst_dir_fd=session_descriptor,
        )
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=session_descriptor)
        except FileNotFoundError:
            pass


def _write_all(descriptor: int, contents: bytes) -> None:
    remaining = memoryview(contents)
    while remaining:
        written = os.write(descriptor, remaining)
        if not written:
            raise OSError(errno.EIO, "session metadata write made no progress")
        remaining = remaining[written:]


def _scan_open_root(
    root: Path,
    root_descriptor: int,
    owner_uid: int,
    *,
    remove: bool,
    limit: int | None,
    now: float,
) -> SessionScan:
    counts = {
        "active": 0,
        "recent": 0,
        "stale": 0,
        "removed": 0,
        "invalid": 0,
        "failed": 0,
    }
    truncated = False
    observations: list[SessionObservation] = []
    invalid_paths: list[Path] = []
    with os.scandir(root_descriptor) as entries:
        for index, entry in enumerate(entries):
            if limit is not None and index >= limit:
                truncated = True
                break
            result = _inspect_entry(
                root,
                root_descriptor,
                entry,
                owner_uid,
                remove=remove,
                now=now,
            )
            if isinstance(result, SessionObservation):
                counts[result.state] += 1
                observations.append(result)
            elif result is not None:
                counts[result] += 1
                if result == "invalid":
                    invalid_paths.append(root / entry.name)
    return SessionScan(
        root,
        True,
        **counts,
        truncated=truncated,
        observations=tuple(observations),
        invalid_paths=tuple(invalid_paths),
    )


def _inspect_entry(
    root: Path,
    root_descriptor: int,
    entry: os.DirEntry[str],
    owner_uid: int,
    *,
    remove: bool,
    now: float,
) -> str | SessionObservation | None:
    match = SESSION_DIRECTORY_PATTERN.fullmatch(entry.name)
    if match is None:
        return "invalid"
    try:
        session_descriptor = _open_valid_session(root_descriptor, entry, owner_uid)
    except FileNotFoundError:
        return None
    except _InvalidSessionError:
        return "invalid"
    try:
        return _inspect_open_session(
            root,
            root_descriptor,
            entry.name,
            match.group(1),
            session_descriptor,
            owner_uid,
            remove=remove,
            now=now,
        )
    finally:
        os.close(session_descriptor)


def _inspect_open_session(
    root: Path,
    root_descriptor: int,
    name: str,
    session_id: str,
    session_descriptor: int,
    owner_uid: int,
    *,
    remove: bool,
    now: float,
) -> str | SessionObservation | None:
    lock_descriptor = _open_valid_lock(session_descriptor, owner_uid)
    if lock_descriptor is None:
        return _invalid_or_vanished(root_descriptor, name, session_descriptor)
    try:
        marker = _read_valid_marker(session_descriptor, session_id, owner_uid)
        if marker is None:
            return _invalid_or_vanished(root_descriptor, name, session_descriptor)
        try:
            _validate_tree(session_descriptor, owner_uid)
        except OSError:
            return _invalid_or_vanished(root_descriptor, name, session_descriptor)
        if not _try_lock(lock_descriptor):
            return _observation(root, name, marker, "active")
        return _inspect_unlocked_session(
            root,
            root_descriptor,
            name,
            session_id,
            session_descriptor,
            owner_uid,
            marker,
            remove=remove,
            now=now,
        )
    finally:
        os.close(lock_descriptor)


def _inspect_unlocked_session(
    root: Path,
    root_descriptor: int,
    name: str,
    session_id: str,
    session_descriptor: int,
    owner_uid: int,
    marker: Mapping[str, Any],
    *,
    remove: bool,
    now: float,
) -> str | SessionObservation | None:
    del session_id
    age = now - marker["createdAt"]
    if age < SESSION_STALE_SECONDS:
        return _observation(root, name, marker, "recent")
    if not remove:
        return _observation(root, name, marker, "stale")
    try:
        _delete_open_session(root_descriptor, name, session_descriptor, owner_uid)
    except OSError:
        return "failed"
    return "removed"


def _observation(
    root: Path,
    name: str,
    marker: Mapping[str, Any],
    state: str,
) -> SessionObservation:
    return SessionObservation(
        session_id=cast(str, marker["sessionId"]),
        profile_id=cast(str, marker["profileId"]),
        profile_revision=cast(int, marker["profileRevision"]),
        supervisor_pid=cast(int, marker["supervisorPid"]),
        created_at=cast(int, marker["createdAt"]),
        state=state,
        path=root / name,
    )


def _invalid_or_vanished(
    root_descriptor: int,
    name: str,
    session_descriptor: int,
) -> str | None:
    if _session_is_linked(root_descriptor, name, session_descriptor):
        return "invalid"
    return None


def _session_is_linked(
    root_descriptor: int,
    name: str,
    session_descriptor: int,
) -> bool:
    try:
        linked = os.stat(name, dir_fd=root_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    opened = os.fstat(session_descriptor)
    return (linked.st_dev, linked.st_ino) == (opened.st_dev, opened.st_ino)


def _open_valid_session(root_descriptor: int, entry: os.DirEntry[str], owner_uid: int) -> int:
    try:
        listed = entry.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(listed.st_mode)
            or listed.st_uid != owner_uid
            or stat.S_IMODE(listed.st_mode) != 0o700
        ):
            raise _InvalidSessionError
        descriptor = os.open(entry.name, _DIRECTORY_FLAGS, dir_fd=root_descriptor)
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (listed.st_dev, listed.st_ino):
            os.close(descriptor)
            raise _InvalidSessionError
        return descriptor
    except FileNotFoundError:
        raise
    except OSError as error:
        raise _InvalidSessionError from error


def _read_valid_marker(
    session_descriptor: int, session_id: str, owner_uid: int
) -> dict[str, Any] | None:
    descriptor = _open_valid_file(session_descriptor, MARKER_FILENAME, owner_uid)
    if descriptor is None:
        return None
    try:
        contents = os.read(descriptor, _MARKER_LIMIT + 1)
    except OSError:
        return None
    finally:
        os.close(descriptor)
    if len(contents) > _MARKER_LIMIT:
        return None
    try:
        marker = json.loads(contents)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not _valid_marker(marker, session_id, owner_uid):
        return None
    return cast(dict[str, Any], marker)


def _valid_marker(marker: object, session_id: str, owner_uid: int) -> bool:
    if not isinstance(marker, dict) or frozenset(marker) != _MARKER_KEYS:
        return False
    return (
        marker.get("sessionId") == session_id
        and marker.get("ownerUid") == owner_uid
        and type(marker.get("supervisorPid")) is int
        and marker["supervisorPid"] > 0
        and type(marker.get("createdAt")) is int
        and marker["createdAt"] >= 0
        and marker.get("state") in _MARKER_STATES
        and _valid_profile_generation(marker.get("profileId"), marker.get("profileRevision"))
    )


def _validate_profile_generation(profile_id: str, profile_revision: int) -> None:
    if not _valid_profile_generation(profile_id, profile_revision):
        raise SessionRuntimeError("profile generation is invalid")


def _valid_profile_generation(profile_id: object, profile_revision: object) -> bool:
    if not isinstance(profile_id, str) or type(profile_revision) is not int:
        return False
    try:
        import uuid

        parsed = uuid.UUID(profile_id)
    except ValueError:
        return False
    return str(parsed) == profile_id and profile_revision > 0


def _open_valid_lock(session_descriptor: int, owner_uid: int) -> int | None:
    try:
        descriptor = os.open(
            LOCK_FILENAME, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), dir_fd=session_descriptor
        )
    except OSError:
        return None
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        return None
    return descriptor


def _open_valid_file(session_descriptor: int, name: str, owner_uid: int) -> int | None:
    try:
        descriptor = os.open(name, _FILE_FLAGS, dir_fd=session_descriptor)
    except OSError:
        return None
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        return None
    return descriptor


def _try_lock(descriptor: int) -> bool:
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _delete_open_session(
    root_descriptor: int,
    name: str,
    session_descriptor: int,
    owner_uid: int,
) -> None:
    _validate_tree(session_descriptor, owner_uid)
    _delete_tree_contents(session_descriptor, owner_uid)
    os.rmdir(name, dir_fd=root_descriptor)


def _validate_tree(directory_descriptor: int, owner_uid: int) -> None:
    with os.scandir(directory_descriptor) as entries:
        for entry in entries:
            metadata = entry.stat(follow_symlinks=False)
            if metadata.st_uid != owner_uid or stat.S_ISLNK(metadata.st_mode):
                raise OSError(errno.EPERM, "unsafe session entry")
            if stat.S_ISREG(metadata.st_mode):
                if stat.S_IMODE(metadata.st_mode) & 0o077:
                    raise OSError(errno.EPERM, "session file permissions are unsafe")
                continue
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o700:
                raise OSError(errno.EPERM, "unsupported session entry")
            child_descriptor = os.open(entry.name, _DIRECTORY_FLAGS, dir_fd=directory_descriptor)
            try:
                opened = os.fstat(child_descriptor)
                if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                    raise OSError(errno.EAGAIN, "session entry changed during validation")
                _validate_tree(child_descriptor, owner_uid)
            finally:
                os.close(child_descriptor)


def _delete_tree_contents(directory_descriptor: int, owner_uid: int) -> None:
    with os.scandir(directory_descriptor) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        metadata = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
        if metadata.st_uid != owner_uid or stat.S_ISLNK(metadata.st_mode):
            raise OSError(errno.EPERM, "unsafe session entry")
        if stat.S_ISDIR(metadata.st_mode):
            child_descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_descriptor)
            try:
                _delete_tree_contents(child_descriptor, owner_uid)
            finally:
                os.close(child_descriptor)
            os.rmdir(name, dir_fd=directory_descriptor)
        else:
            os.unlink(name, dir_fd=directory_descriptor)


def _discard_incomplete_session(root_descriptor: int, name: str, session_descriptor: int) -> None:
    try:
        _delete_tree_contents(session_descriptor, os.getuid())
        os.rmdir(name, dir_fd=root_descriptor)
    except OSError:
        pass


__all__ = [
    "AUTOMATIC_SCAN_LIMIT",
    "SESSION_STALE_SECONDS",
    "SessionObservation",
    "SessionRuntime",
    "SessionRuntimeError",
    "SessionScan",
    "cleanup_abandoned_sessions",
    "create_session_runtime",
    "resolve_runtime_root",
    "scan_sessions",
]
