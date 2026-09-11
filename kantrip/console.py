"""Rich console construction and Kantrip's Arcana presentation theme."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import IO, Any

from rich import box
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from rich.theme import Theme

ARCANA_COLORS = {
    "background": "#071426",
    "surface": "#0B2340",
    "primary": "#3B82F6",
    "secondary": "#22D3EE",
    "accent": "#60A5FA",
    "success": "#34D399",
    "warning": "#FBBF24",
    "error": "#FB7185",
    "foreground": "#EFF6FF",
}

ARCANA_THEME = Theme(
    {
        "background": f"on {ARCANA_COLORS['background']}",
        "surface": f"on {ARCANA_COLORS['surface']}",
        "primary": ARCANA_COLORS["primary"],
        "secondary": ARCANA_COLORS["secondary"],
        "accent": ARCANA_COLORS["accent"],
        "success": ARCANA_COLORS["success"],
        "warning": ARCANA_COLORS["warning"],
        "error": ARCANA_COLORS["error"],
        "foreground": ARCANA_COLORS["foreground"],
        "heading": f"bold {ARCANA_COLORS['primary']}",
        "muted": "bright_black",
    }
)


@dataclass(frozen=True)
class Consoles:
    """Normal-output and diagnostic consoles with identical color policy."""

    out: Console
    err: Console


def colors_enabled(
    stream: IO[str],
    *,
    no_color: bool = False,
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Return whether styling is safe and requested for a stream."""
    env = os.environ if environment is None else environment
    if no_color or "NO_COLOR" in env or env.get("TERM", "").lower() == "dumb":
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


def create_console(
    *,
    stream: IO[str] | None = None,
    stderr: bool = False,
    no_color: bool = False,
    environment: Mapping[str, str] | None = None,
) -> Console:
    """Create a Rich console without modifying global terminal state."""
    target = stream if stream is not None else (sys.stderr if stderr else sys.stdout)
    color = colors_enabled(target, no_color=no_color, environment=environment)
    return Console(
        file=target,
        theme=ARCANA_THEME,
        color_system="truecolor" if color else None,
        force_terminal=color,
        no_color=not color,
        highlight=False,
    )


def create_consoles(*, no_color: bool = False) -> Consoles:
    """Create stdout and stderr consoles for one CLI invocation."""
    return Consoles(
        out=create_console(no_color=no_color),
        err=create_console(stderr=True, no_color=no_color),
    )


def create_profile_table(profiles: Mapping[str, Mapping[str, Any]]) -> Table:
    """Create the styled profile-list table."""
    table = Table(
        box=box.HORIZONTALS,
        border_style="muted",
        expand=True,
        header_style="heading",
        show_edge=False,
        show_lines=True,
    )
    table.add_column("Profile", style="secondary", no_wrap=True)
    table.add_column("Description", style="foreground")
    for name, profile in profiles.items():
        table.add_row(name, str(profile.get("description") or "-"))
    return table


def create_yaml_syntax(contents: str) -> Syntax:
    """Create syntax-colored YAML without a forced background."""
    return Syntax(contents, "yaml", theme="ansi_dark", background_color="default")


__all__ = [
    "ARCANA_COLORS",
    "ARCANA_THEME",
    "Consoles",
    "colors_enabled",
    "create_console",
    "create_consoles",
    "create_profile_table",
    "create_yaml_syntax",
]
