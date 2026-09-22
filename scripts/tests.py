"""Run Kantrip's unit or end-to-end test suite."""

import cloup

from scripts import CommandProcessor


@cloup.command()
@cloup.option(
    "--suite",
    type=cloup.Choice(("unit", "e2e")),
    default="unit",
    show_default=True,
    help="Test suite to execute.",
)
def main(suite: str) -> None:
    commands = {
        f"executing {suite} tests": (f"python -m unittest discover -v -s tests/{suite} -t ."),
    }
    CommandProcessor(commands).run()


if __name__ == "__main__":
    main()
