"""Generate the README screenshots from Kantrip's own output, without Kafka."""

from __future__ import annotations

import io
import os
import random
import tempfile
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from rich.console import Console
from rich.text import Text

from kantrip import cli
from kantrip.console import ARCANA_THEME
from scripts import normalize_svg
from scripts.banner import ARCANA_TERMINAL_THEME, IMAGES_DIRECTORY

SCREENSHOT_WIDTH = 120
# Profiles without secrets never open the credential vault, so the commands run
# against a temporary data directory and leave the developer's profiles alone.
SCREENSHOTS: dict[str, tuple[tuple[str, ...], ...]] = {
    "add": (
        (
            "add",
            "local",
            "-b",
            "localhost:9092",
            "--registry-url",
            "http://localhost:8081",
            "--description",
            "Local sandbox",
            "--label",
            "env=dev",
        ),
        (
            "add",
            "staging",
            "-b",
            "stg.kafka.acme.io:9093",
            "--transport",
            "tls",
            "--registry-url",
            "https://registry.acme.io",
            "--description",
            "Staging cluster",
            "--label",
            "env=staging",
            "--label",
            "team=payments",
        ),
        (
            "add",
            "production",
            "-b",
            "kafka.acme.io:9093",
            "--transport",
            "tls",
            "--description",
            "Production cluster",
            "--label",
            "env=prod",
            "--label",
            "team=payments",
        ),
    ),
    "list": (("list",),),
    "describe": (("describe", "staging"),),
    "help": (("--help",),),
}


def _profile_ids() -> Iterator[uuid.UUID]:
    """Stable, random-looking version 4 profile IDs, so regenerating changes nothing."""
    generator = random.Random(2026)
    while True:
        yield uuid.UUID(int=generator.getrandbits(128), version=4)


@contextmanager
def _isolated_kantrip(console: Console) -> Iterator[None]:
    with (
        tempfile.TemporaryDirectory() as directory,
        patch.dict(
            os.environ,
            {
                "XDG_DATA_HOME": str(Path(directory) / "data"),
                "XDG_RUNTIME_DIR": str(Path(directory) / "run"),
                "COLUMNS": str(SCREENSHOT_WIDTH),
            },
        ),
        patch.object(cli, "create_console", return_value=console),
        patch("kantrip.profiles.uuid.uuid4", side_effect=_profile_ids()),
    ):
        (Path(directory) / "run").mkdir(mode=0o700)
        yield


def _invoke(arguments: Sequence[str]) -> str:
    """Run one command; Rich output goes to the recording console, Click's to the result."""
    output = io.StringIO()
    with redirect_stdout(output):
        cli.cli.main(args=list(arguments), prog_name="kantrip", standalone_mode=False)
    return output.getvalue().rstrip("\n")


def _run(console: Console, arguments: Sequence[str]) -> None:
    console.print(Text.assemble(("❯ ", "secondary"), ("kantrip " + " ".join(arguments))))
    if output := _invoke(arguments):
        console.print(Text(output))


def _record(name: str, commands: Sequence[Sequence[str]]) -> Console:
    console = Console(
        file=io.StringIO(),
        record=True,
        width=SCREENSHOT_WIDTH,
        color_system="truecolor",
        force_terminal=True,
        theme=ARCANA_THEME,
        highlight=False,
    )
    with _isolated_kantrip(console):
        # Later screenshots show the profiles the add screenshot creates.
        for setup in () if name == "add" else SCREENSHOTS["add"]:
            _invoke(setup)
        console.export_text(clear=True)
        for arguments in commands:
            _run(console, arguments)
    return console


def render_screenshots() -> dict[str, str]:
    """Run each screenshot's commands in a fresh data directory and render SVG."""
    consoles = {name: _record(name, commands) for name, commands in SCREENSHOTS.items()}
    # The README shows the screenshots in a grid, so they share the tallest height.
    height = max(
        len(console.export_text(clear=False).splitlines()) for console in consoles.values()
    )
    screenshots = {}
    for name, console in consoles.items():
        if missing := height - len(console.export_text(clear=False).splitlines()):
            console.print("\n" * missing, end="")
        screenshots[name] = normalize_svg(
            console.export_svg(
                title=f"kantrip {name}",
                theme=ARCANA_TERMINAL_THEME,
                unique_id=f"kantrip-{name}",
            )
        )
    return screenshots


def generate_screenshots(directory: Path = IMAGES_DIRECTORY) -> list[Path]:
    """Write the README screenshots to images/."""
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, svg in render_screenshots().items():
        path = directory / f"screenshot-{name}.svg"
        path.write_text(svg, encoding="utf-8")
        paths.append(path)
    return paths


def main() -> None:
    for path in generate_screenshots():
        print(path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path)


if __name__ == "__main__":
    main()
