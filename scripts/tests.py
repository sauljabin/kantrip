"""Run the unit or end-to-end unittest suite."""

import cloup

from scripts import CommandProcessor


@cloup.command()
@cloup.option("--e2e", is_flag=True, help="Run end-to-end tests.")
def main(e2e: bool) -> None:
    module = "tests/e2e" if e2e else "tests/unit"
    commands = {
        "executing tests": f"python -m unittest discover -v -s {module} -t .",
    }
    CommandProcessor(commands).run()


if __name__ == "__main__":
    main()
