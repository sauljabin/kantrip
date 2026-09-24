"""Run the repository's non-mutating quality checks."""

import subprocess
from pathlib import Path

import yaml

from scripts import CommandProcessor


def _validate_action_step(path: Path, step: object) -> None:
    if not isinstance(step, dict) or ("run" in step) == ("uses" in step):
        raise ValueError(f"{path}: each step needs exactly one of run or uses")
    if "run" in step:
        if step.get("shell") != "bash" or not isinstance(step["run"], str):
            raise ValueError(f"{path}: run steps require an explicit bash shell")
        subprocess.run(("bash", "-n"), input=step["run"], text=True, check=True)
    elif not isinstance(step["uses"], str) or "@" not in step["uses"]:
        raise ValueError(f"{path}: external uses must specify a version")


def validate_local_actions() -> None:
    """Validate composite metadata and shell bodies that actionlint cannot lint."""
    actions = sorted(Path(".github/actions").glob("*/action.yml"))
    if not actions:
        raise ValueError("no local composite actions found")
    for path in actions:
        metadata = yaml.safe_load(path.read_text(encoding="utf-8"))
        if (
            not isinstance(metadata, dict)
            or not metadata.get("name")
            or not metadata.get("description")
        ):
            raise ValueError(f"{path}: name and description are required")
        runs = metadata.get("runs")
        if not isinstance(runs, dict) or runs.get("using") != "composite":
            raise ValueError(f"{path}: expected a composite action")
        steps = runs.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError(f"{path}: expected action steps")
        for step in steps:
            _validate_action_step(path, step)


def main() -> None:
    validate_local_actions()
    commands = {
        "checking types": "mypy kantrip/ sandbox/ scripts/ tests/e2e/",
        "black": "black --check .",
        "ruff": "ruff check .",
        "typos": "typos --format brief",
        "github actions": "actionlint",
    }
    CommandProcessor(commands).run()


if __name__ == "__main__":
    main()
