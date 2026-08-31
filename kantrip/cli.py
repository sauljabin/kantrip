"""Kantrip command-line entry point."""

from __future__ import annotations

from typing import Any

import cloup

from kantrip import APP_VERSION
from kantrip.console import Consoles, create_consoles

EPILOG = "More information at https://github.com/sauljabin/kantrip."


@cloup.group(
    epilog=EPILOG,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@cloup.version_option(APP_VERSION)
@cloup.option(
    "--no-color",
    is_flag=True,
    help="Disable styled terminal output.",
)
@cloup.pass_context
def cli(context: cloup.Context, no_color: bool) -> None:
    """Kantrip securely manages local Kafka profiles for command-line tools and compatible applications."""
    context.ensure_object(dict)
    context.obj["consoles"] = create_consoles(no_color=no_color)


def consoles_from_context(context: cloup.Context) -> Consoles:
    """Return the consoles initialized for the current invocation."""
    obj: dict[str, Any] = context.ensure_object(dict)
    consoles = obj.get("consoles")
    if not isinstance(consoles, Consoles):
        raise TypeError("Kantrip consoles have not been initialized")
    return consoles


def main() -> None:
    """Run the CLI using its installed program name."""
    cli(prog_name="kantrip")


if __name__ == "__main__":
    main()
