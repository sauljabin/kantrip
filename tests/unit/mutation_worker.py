"""Kill-point worker and persistent test-only secret store for lifecycle tests."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

from kantrip.profile_auth import KafkaAuthInput
from kantrip.profiles import add_profile, edit_profile
from kantrip.secret_store import SecretNotFoundError
from kantrip.secret_value import Secret

FIRST_SECRET = "synthetic-first-secret"
SECOND_SECRET = "synthetic-second-secret"


class FileSecretStore:
    """Minimal process-persistent store used only by crash tests."""

    def __init__(self, root: Path, barrier: str | None = None) -> None:
        self.root = root
        self.barrier = barrier
        root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def get(self, reference: str) -> str:
        path = self._path(reference)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, KeyError) as error:
            raise SecretNotFoundError("test secret is unavailable") from error
        value = document.get("value")
        if document.get("reference") != reference or not isinstance(value, str):
            raise SecretNotFoundError("test secret is unavailable")
        if self.barrier == "after-store-readback":
            _barrier(self.barrier)
        return value

    def set(self, reference: str, value: str) -> None:
        if self.barrier == "before-store-write":
            _barrier(self.barrier)
        path = self._path(reference)
        temporary = path.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            payload = json.dumps(
                {"reference": reference, "value": value},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        _sync_directory(self.root)

    def delete(self, reference: str) -> None:
        try:
            self._path(reference).unlink()
        except FileNotFoundError:
            pass
        _sync_directory(self.root)
        if self.barrier == "after-store-delete":
            _barrier(self.barrier)

    def references(self) -> tuple[str, ...]:
        references: list[str] = []
        for path in self.root.glob("*.json"):
            document = json.loads(path.read_text(encoding="utf-8"))
            references.append(str(document["reference"]))
        return tuple(sorted(references))

    def _path(self, reference: str) -> Path:
        digest = hashlib.sha256(reference.encode()).hexdigest()
        return self.root / f"{digest}.json"


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _barrier(name: str) -> None:
    print(f"BARRIER {name}", flush=True)
    sys.stdin.readline()
    raise RuntimeError("crash-test barrier was released unexpectedly")


def _run(scenario: str, database: Path, store_root: Path) -> None:
    auth = KafkaAuthInput("plain", username="alice", password=Secret(FIRST_SECRET))
    if scenario == "before-store-write":
        add_profile(
            "local",
            database,
            transport="tls",
            auth=auth,
            secret_store=FileSecretStore(store_root, scenario),
        )
        return
    if scenario == "after-store-readback":
        add_profile(
            "local",
            database,
            transport="tls",
            auth=auth,
            secret_store=FileSecretStore(store_root, scenario),
        )
        return
    if scenario == "after-profile-commit":
        import kantrip.profile_storage as storage_module

        def stop_after_commit(path: Path, connection: object) -> object:
            del path, connection
            _barrier(scenario)

        storage_module.load_profile_collection = stop_after_commit
        add_profile(
            "local",
            database,
            transport="tls",
            auth=auth,
            secret_store=FileSecretStore(store_root),
        )
        return
    if scenario == "after-store-delete":
        store = FileSecretStore(store_root)
        add_profile(
            "local",
            database,
            transport="tls",
            auth=auth,
            secret_store=store,
        )
        edit_profile(
            "local",
            database,
            auth=KafkaAuthInput("plain", username="alice", password=Secret(SECOND_SECRET)),
            secret_store=FileSecretStore(store_root, scenario),
        )
        return
    raise ValueError("unknown crash-test scenario")


if __name__ == "__main__":
    _run(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]))
