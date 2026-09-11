"""Run the unit test suite."""

import cloup

from scripts import CommandProcessor


@cloup.command()
def main() -> None:
    commands = {
        "executing tests": "python -m unittest discover -v -s tests/unit -t .",
    }
    CommandProcessor(commands).run()


if __name__ == "__main__":
    main()
