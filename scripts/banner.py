"""Generate the deterministic Rich SVG banner used by the README."""

from __future__ import annotations

import io
from pathlib import Path

from rich.box import Box
from rich.console import Console, Group
from rich.panel import Panel
from rich.terminal_theme import TerminalTheme
from rich.text import Text

from kantrip import APP_BANNER
from kantrip.console import ARCANA_COLORS, ARCANA_THEME
from scripts import normalize_svg

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IMAGES_DIRECTORY = PROJECT_ROOT / "images"
BANNER_PATH = IMAGES_DIRECTORY / "banner.svg"
BANNER_WIDTH = 44
BANNER_FONT_ASPECT_RATIO = 0.61
BANNER_SLOGAN = " -* switch kafka profiles like |_| magic"
WAND_HANDLE_COLOR = "#A16207"
# Rich's SVG export metrics: 20px glyphs, 1.22 line height, 1.5px line offset.
SVG_CHAR_HEIGHT = 20
SVG_LINE_HEIGHT = SVG_CHAR_HEIGHT * 1.22
SVG_LINE_OFFSET = 1.5
SVG_FRAME_STROKE = 2
# Box-drawing glyphs fall back to fonts of other widths and heights on Linux
# and Android, so the panel keeps blank border cells and the frame is drawn
# as an SVG rectangle through their centers instead.
BLANK_BOX = Box("    \n" * 8)

ARCANA_TERMINAL_THEME = TerminalTheme(
    background=(7, 20, 38),
    foreground=(239, 246, 255),
    normal=[
        (7, 20, 38),
        (251, 113, 133),
        (52, 211, 153),
        (251, 191, 36),
        (59, 130, 246),
        (96, 165, 250),
        (34, 211, 238),
        (239, 246, 255),
    ],
    bright=[
        (30, 58, 95),
        (251, 113, 133),
        (52, 211, 153),
        (251, 191, 36),
        (96, 165, 250),
        (147, 197, 253),
        (103, 232, 249),
        (255, 255, 255),
    ],
)


def render_banner_svg() -> str:
    """Render the Kantrip name and slogan to deterministic SVG text."""
    console = Console(
        file=io.StringIO(),
        record=True,
        width=BANNER_WIDTH,
        color_system="truecolor",
        theme=ARCANA_THEME,
    )
    title = Text(APP_BANNER[:-3].rstrip(), style="heading")
    slogan = Text(
        BANNER_SLOGAN,
        style="secondary",
        justify="left",
        no_wrap=True,
    )
    wand_start = BANNER_SLOGAN.index("-*")
    slogan.stylize(WAND_HANDLE_COLOR, wand_start, wand_start + 1)
    slogan.stylize("warning", wand_start + 1, wand_start + 2)
    descender_start = BANNER_SLOGAN.index("|_|")
    slogan.stylize("heading", descender_start, descender_start + 3)
    console.print(
        Panel(
            Group(title, slogan),
            box=BLANK_BOX,
            padding=(0, 0),
            width=BANNER_WIDTH,
        )
    )
    line_count = len(console.export_text(clear=False).splitlines())
    svg = console.export_svg(
        title="Kantrip",
        theme=ARCANA_TERMINAL_THEME,
        font_aspect_ratio=BANNER_FONT_ASPECT_RATIO,
        unique_id="kantrip-banner",
    )
    return normalize_svg(_add_frame(svg, line_count))


def _add_frame(svg: str, line_count: int) -> str:
    """Draw the panel border through the centers of its blank border cells."""
    char_width = SVG_CHAR_HEIGHT * BANNER_FONT_ASPECT_RATIO
    frame = (
        f'<rect fill="none" stroke="{ARCANA_COLORS["accent"]}" '
        f'stroke-width="{SVG_FRAME_STROKE}" '
        f'x="{char_width / 2:g}" '
        f'y="{SVG_LINE_OFFSET + SVG_LINE_HEIGHT / 2:g}" '
        f'width="{char_width * (BANNER_WIDTH - 1):g}" '
        f'height="{SVG_LINE_HEIGHT * (line_count - 1):g}" '
        f'rx="{char_width / 2:g}"/>\n    '
    )
    matrix = '<g class="kantrip-banner-matrix">'
    return svg.replace(matrix, frame + matrix, 1)


def generate_banner(path: Path = BANNER_PATH) -> Path:
    """Write the README banner and return its path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_banner_svg(), encoding="utf-8")
    return path


def main() -> None:
    path = generate_banner()
    print(f"Generated {path.relative_to(PROJECT_ROOT)} using {ARCANA_COLORS['primary']}")


if __name__ == "__main__":
    main()
