"""Rich console construction and Kantrip's Arcana presentation theme."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from typing import IO, Any, Literal

from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from kantrip.schema_registry import display_schema_registry_url

ARCANA_COLORS = {
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

StatusKind = Literal["progress", "success", "cleanup", "warning", "error"]
STATUS_PRESENTATION: dict[StatusKind, tuple[str, str, str]] = {
    "progress": ("primary", "🧪", "running"),
    "success": ("success", "✅", "passed"),
    "cleanup": ("muted", "🧹", "cleanup"),
    "warning": ("warning", "⚠️", "warning"),
    "error": ("error", "❌", "failed"),
}


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
    no_color: bool = False,
    environment: Mapping[str, str] | None = None,
) -> Console:
    """Create a Rich console without modifying global terminal state."""
    target = stream if stream is not None else sys.stdout
    color = colors_enabled(target, no_color=no_color, environment=environment)
    return Console(
        file=target,
        theme=ARCANA_THEME,
        color_system="truecolor" if color else None,
        force_terminal=color,
        no_color=not color,
        highlight=False,
    )


def create_profile_table(profiles: Mapping[str, Mapping[str, Any]]) -> Table:
    """Create the styled profile-list table."""
    table = Table(
        box=None,
        header_style="heading",
    )
    table.add_column("Profile", style="secondary", no_wrap=True, min_width=12)
    table.add_column("Description", style="foreground", min_width=12)
    table.add_column("Kafka", style="foreground", overflow="fold")
    table.add_column("Schema Registry", style="foreground", overflow="fold")
    for name, profile in profiles.items():
        kafka = profile.get("kafka", {})
        bootstrap_servers = kafka.get("bootstrapServers", ()) if isinstance(kafka, Mapping) else ()
        table.add_row(
            name,
            str(profile.get("description") or "-"),
            ",".join(str(server) for server in bootstrap_servers),
            display_schema_registry_url(profile),
        )
    return table


def create_yaml_syntax(contents: str) -> Syntax:
    """Create syntax-colored YAML without a forced background."""
    return Syntax(contents, "yaml", theme="ansi_dark", background_color="default")


def create_status_text(console: Console, status: StatusKind, message: str) -> Text:
    """Create a styled status line with a text marker for plain output."""
    style, emoji, label = STATUS_PRESENTATION[status]
    marker = emoji if console.color_system is not None else f"[{label}]"
    return Text(f"{marker} {message}", style=style)


__all__ = [
    "ARCANA_COLORS",
    "ARCANA_THEME",
    "colors_enabled",
    "create_console",
    "create_profile_table",
    "create_status_text",
    "create_yaml_syntax",
]
