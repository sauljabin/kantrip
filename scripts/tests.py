"""Run test suites and select or validate their local and CI E2E gates."""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
import time
from pathlib import Path, PurePosixPath

_NON_E2E_FILES = {"LICENSE", "LICENSE.txt"}
_IMAGE_SUFFIXES = frozenset({".gif", ".ico", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
_SITE_SUFFIXES = _IMAGE_SUFFIXES | {".css", ".html", ".js"}
_TEMPLATE_SUFFIXES = frozenset({".md", ".yaml", ".yml"})


def requires_e2e(paths: tuple[str, ...]) -> bool:
    """Unknown paths and either side of a runtime rename require E2E."""
    return any(_path_requires_e2e(path) for path in paths)


def _path_requires_e2e(path: str) -> bool:
    pure = PurePosixPath(path)
    if not path or pure.is_absolute() or ".." in pure.parts:
        return True
    if path.endswith(".md") or path in _NON_E2E_FILES:
        return False
    if path.startswith("tests/unit/"):
        return False
    if path.startswith("images/"):
        return pure.suffix.lower() not in _IMAGE_SUFFIXES
    if path.startswith("site/"):
        return pure.suffix.lower() not in _SITE_SUFFIXES
    if path.startswith((".github/ISSUE_TEMPLATE/", ".github/PULL_REQUEST_TEMPLATE/")):
        return pure.suffix.lower() not in _TEMPLATE_SUFFIXES
    return True


def changed_paths(diff: bytes) -> tuple[str, ...]:
    """Parse Git's NUL-delimited name/status format, including both rename sides."""
    if not diff:
        return ()
    fields = diff.split(b"\0")
    if fields.pop() != b"":
        raise ValueError("unterminated Git diff output")
    paths: list[str] = []
    index = 0
    while index < len(fields):
        status = fields[index].decode("ascii")
        index += 1
        count = 2 if status.startswith(("R", "C")) else 1
        if not status or status[0] not in "ACDMRTUXB" or index + count > len(fields):
            raise ValueError("invalid Git diff status")
        for field in fields[index : index + count]:
            paths.append(field.decode("utf-8", errors="surrogateescape"))
        index += count
    return tuple(paths)


def staged_requires_e2e() -> bool:
    diff = subprocess.check_output(
        ("git", "diff", "--cached", "--name-status", "-z", "--find-renames")
    )
    return requires_e2e(changed_paths(diff))


def push_requires_e2e(before: str, after: str) -> bool:
    """Fail safe when a new branch, shallow checkout, or invalid base hides paths."""
    if not before or set(before) == {"0"}:
        return True
    try:
        diff = subprocess.check_output(
            ("git", "diff", "--name-status", "-z", "--find-renames", before, after),
            stderr=subprocess.DEVNULL,
        )
        return requires_e2e(changed_paths(diff))
    except (OSError, subprocess.CalledProcessError, ValueError):
        return True


def event_requires_e2e(
    event: str,
    *,
    action: str = "",
    label: str = "",
    labels: tuple[str, ...] = (),
    before: str = "",
    after: str = "",
) -> bool:
    """PR E2E is a one-shot label event; dispatch and relevant main pushes run.

    A PR opened with the label counts as the label being applied: the separate
    ``opened`` and ``labeled`` runs race and cancel each other, so both select E2E.
    """
    if event == "workflow_dispatch":
        return True
    if event == "pull_request":
        return (action == "labeled" and label == "run-e2e") or (
            action == "opened" and "run-e2e" in labels
        )
    if event == "push":
        return push_requires_e2e(before, after)
    raise ValueError(f"unsupported CI event: {event}")


def valid_e2e_result(selected: str, result: str) -> bool:
    """Only a deliberate skip or a successful selected run satisfies CI."""
    return (selected == "true" and result == "success") or (
        selected == "false" and result == "skipped"
    )


def _run(*command: str, cwd: Path, environment: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _run_suite(suite: str) -> None:
    # Keep project imports out of the stdlib-only selection path.
    from scripts import CommandProcessor

    CommandProcessor(
        {f"executing {suite} tests": f"python -m unittest discover -v -s tests/{suite} -t ."}
    ).run()


def _run_staged_wheel() -> None:
    try:
        selected = os.environ.get("KANTRIP_E2E_FORCE") == "1" or staged_requires_e2e()
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        raise RuntimeError("cannot classify staged paths; refusing to skip E2E") from error
    if not selected:
        print("Staged E2E skipped: only non-runtime paths changed")
        return

    repository = Path(
        subprocess.check_output(("git", "rev-parse", "--show-toplevel"), text=True).strip()
    )
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="kantrip-staged-e2e-") as temporary:
        root = Path(temporary)
        source = root / "source"
        source.mkdir()
        _run("git", "checkout-index", "--all", f"--prefix={source}/", cwd=repository)
        state = repository / "sandbox" / ".state"
        if state.is_dir():
            (source / "sandbox" / ".state").symlink_to(state, target_is_directory=True)

        distribution = root / "dist"
        _run("uv", "build", "--wheel", "--out-dir", str(distribution), cwd=source)
        wheels = list(distribution.glob("kantrip-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one staged candidate wheel, found {len(wheels)}")

        # Resolve the project's locked Python, not the system bootstrap interpreter.
        project_python = subprocess.check_output(
            ("uv", "run", "--locked", "python", "-c", "import sys; print(sys.executable)"),
            cwd=source,
            text=True,
        ).strip()
        environment_path = root / "candidate-venv"
        _run("uv", "venv", "--python", project_python, str(environment_path), cwd=source)
        candidate_python = environment_path / "bin" / "python"
        _run("uv", "pip", "install", "--python", str(candidate_python), str(wheels[0]), cwd=source)
        environment = dict(os.environ)
        environment["KANTRIP_E2E_KANTRIP"] = str(environment_path / "bin" / "kantrip")
        _run(
            "uv",
            "run",
            "--locked",
            "python",
            "-m",
            "scripts.tests",
            "--suite",
            "e2e",
            cwd=source,
            environment=environment,
        )
    print(f"Staged candidate E2E passed in {time.monotonic() - started:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--suite", choices=("unit", "e2e"))
    mode.add_argument("--staged-wheel", action="store_true")
    mode.add_argument("--ci-event", choices=("workflow_dispatch", "pull_request", "push"))
    mode.add_argument("--verify-e2e-result", nargs=2, metavar=("SELECTED", "RESULT"))
    parser.add_argument("--action", default="")
    parser.add_argument("--label", default="")
    parser.add_argument("--pr-labels", default="", help="comma-separated current PR labels")
    parser.add_argument("--before", default="")
    parser.add_argument("--after", default="")
    arguments = parser.parse_args()

    if arguments.verify_e2e_result is not None:
        if not valid_e2e_result(*arguments.verify_e2e_result):
            parser.exit(1, "E2E result does not match selection\n")
    elif arguments.ci_event is not None:
        selected = os.environ.get("KANTRIP_E2E_FORCE") == "1" or event_requires_e2e(
            arguments.ci_event,
            action=arguments.action,
            label=arguments.label,
            labels=tuple(filter(None, arguments.pr_labels.split(","))),
            before=arguments.before,
            after=arguments.after,
        )
        print("true" if selected else "false")
    elif arguments.staged_wheel:
        _run_staged_wheel()
    else:
        _run_suite(arguments.suite or "unit")


if __name__ == "__main__":
    main()
