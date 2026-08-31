"""Run the repository's non-mutating quality checks."""

from scripts import CommandProcessor


def main() -> None:
    commands = {
        "checking types": "mypy kantrip/ sandbox/ scripts/",
        "black": "black --check .",
        "ruff": "ruff check .",
        "typos": "typos --format brief",
        "github actions": "actionlint",
    }
    CommandProcessor(commands).run()


if __name__ == "__main__":
    main()
