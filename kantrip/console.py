"""Rich console construction and Kantrip's Arcana presentation theme."""

from __future__ import annotations

import hashlib
import os
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import IO, Any, Literal

from rich.console import Console, Group
from rich.padding import Padding
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from kantrip.registry import display_registry

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
LABEL_COLORS = (
    "#60A5FA",
    "#22D3EE",
    "#34D399",
    "#A78BFA",
    "#F472B6",
    "#FBBF24",
)


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
    table.add_column("Registry", style="foreground", overflow="fold")
    table.add_column("Labels", overflow="fold")
    for name, profile in profiles.items():
        kafka = profile.get("kafka", {})
        bootstrap_servers = kafka.get("bootstrapServers", ()) if isinstance(kafka, Mapping) else ()
        table.add_row(
            Text(name),
            Text(str(profile.get("description") or "-")),
            Text(",".join(str(server) for server in bootstrap_servers)),
            Text(display_registry(profile)),
            create_labels_text(profile.get("labels")),
        )
    return table


def create_profile_description(observation: Mapping[str, Any]) -> Group:
    """Create a sectioned human representation of one safe observation."""
    kafka = observation.get("kafka", {})
    registry = observation.get("registry")
    labels = observation.get("labels")
    sections = [
        _details_section(
            "Profile",
            (
                ("Name", observation.get("name")),
                ("ID", observation.get("id")),
                ("Revision", observation.get("revision")),
                ("Description", observation.get("description") or "-"),
            ),
        ),
        _details_section(
            "Kafka",
            (
                ("Bootstrap servers", _bootstrap_servers(kafka)),
                ("Transport", _mapping_value(kafka, "transport")),
                ("TLS trust", _tls_trust(kafka)),
                ("Authentication", _auth_type(kafka)),
                ("Credentials", _credential_states(kafka)),
            ),
        ),
        _details_section("Registry", _registry_details(registry)),
        _details_section("Labels", (("Values", create_labels_text(labels)),)),
    ]
    renderables: list[Any] = []
    for index, section in enumerate(sections):
        if index:
            renderables.append(Text())
        renderables.append(section)
    return Group(*renderables)


def create_labels_text(labels: object) -> Text:
    """Render labels with deterministic presentation-only colors."""
    if not isinstance(labels, Mapping) or not labels:
        return Text("-", style="muted")
    result = Text()
    for index, (key, value) in enumerate(sorted(labels.items())):
        if index:
            result.append(", ", style="muted")
        label = f"{key}={value}"
        result.append(label, style=_label_color(str(key)))
    return result


def _details_section(title: str, values: tuple[tuple[str, object], ...]) -> Group:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="muted", no_wrap=True)
    table.add_column(style="foreground")
    for label, value in values:
        table.add_row(Text(label), value if isinstance(value, Text) else Text(str(value)))
    return Group(Text(title, style="heading"), Padding(table, (0, 0, 0, 2)))


def _bootstrap_servers(kafka: object) -> str:
    servers = kafka.get("bootstrapServers", ()) if isinstance(kafka, Mapping) else ()
    return ",".join(str(server) for server in servers)


def _mapping_value(value: object, key: str) -> object:
    return value.get(key, "-") if isinstance(value, Mapping) else "-"


def _auth_type(kafka: object) -> object:
    auth = kafka.get("auth", {}) if isinstance(kafka, Mapping) else {}
    return _mapping_value(auth, "type")


def _credential_states(kafka: object) -> object:
    auth = kafka.get("auth", {}) if isinstance(kafka, Mapping) else {}
    credentials = auth.get("credentials", {}) if isinstance(auth, Mapping) else {}
    if not isinstance(credentials, Mapping) or not credentials:
        return "-"
    return ", ".join(f"{field}: {state}" for field, state in sorted(credentials.items()))


def _tls_trust(kafka: object) -> object:
    if not isinstance(kafka, Mapping):
        return "-"
    tls = kafka.get("tls")
    if not isinstance(tls, Mapping):
        return "-"
    return _mapping_value(tls, "trust")


def _registry_details(registry: object) -> tuple[tuple[str, object], ...]:
    if not isinstance(registry, Mapping):
        return (("Status", "Not configured"),)
    return (("Provider", registry.get("provider")), ("URL", registry.get("url")))


def _label_color(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return LABEL_COLORS[int.from_bytes(digest[:2], "big") % len(LABEL_COLORS)]


def create_structured_syntax(contents: str, language: Literal["json", "yaml"]) -> Syntax:
    """Create syntax-highlighted structured output without a forced background."""
    return Syntax(
        contents.removesuffix("\n"),
        language,
        theme="ansi_dark",
        background_color="default",
        word_wrap=False,
    )


def create_status_text(console: Console, status: StatusKind, message: str) -> Text:
    """Create a styled status line with a text marker for plain output."""
    style, emoji, label = STATUS_PRESENTATION[status]
    marker = emoji if console.color_system is not None else f"[{label}]"
    return Text(f"{marker} {message}", style=style)


@contextmanager
def show_progress(console: Console, message: str) -> Iterator[None]:
    """Show an animated status on colored terminals and a stable line otherwise."""
    if console.color_system is None:
        console.print(create_status_text(console, "progress", message))
        yield
        return
    with console.status(message, spinner="moon", spinner_style="primary"):
        yield


__all__ = [
    "ARCANA_COLORS",
    "ARCANA_THEME",
    "StatusKind",
    "colors_enabled",
    "create_console",
    "create_labels_text",
    "create_profile_description",
    "create_profile_table",
    "create_status_text",
    "create_structured_syntax",
    "show_progress",
]
