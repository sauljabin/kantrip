"""Shared helpers for repository workflow scripts."""

from __future__ import annotations

import re
import shlex
import subprocess
from collections.abc import Mapping

from rich.console import Console

SVG_VIEWBOX = re.compile(r'(<svg\b)(?![^>]*\bwidth=)(?=[^>]*\bviewBox="0 0 ([\d.]+) ([\d.]+)")')


def normalize_svg(svg: str) -> str:
    """Add intrinsic dimensions and remove trailing whitespace from an SVG."""
    svg = SVG_VIEWBOX.sub(
        lambda match: f'{match.group(1)} width="{match.group(2)}" height="{match.group(3)}"',
        svg,
        count=1,
    )
    return "\n".join(line.rstrip() for line in svg.splitlines()) + "\n"


class CommandProcessor:
    """Run named commands in order and render consistent diagnostics."""

    def __init__(
        self,
        commands: Mapping[str, str],
        rollback: Mapping[str, str] | None = None,
    ) -> None:
        self.commands = commands
        self.rollback = {} if rollback is None else rollback
        self.console = Console()

    def run(self) -> str:
        output = ""
        for name, command in self.commands.items():
            result = self.execute_command(name, command)
            if result.returncode:
                self._report_failure(name, command, result)
                self._run_rollback()
                raise SystemExit(result.returncode)
            output += result.stdout
        return output

    def _report_failure(
        self,
        name: str,
        command: str,
        result: subprocess.CompletedProcess[str],
    ) -> None:
        self.console.print(
            "\n[bold red]Error[/] when executing "
            f'[bold blue]"{name}" ([bold yellow]{command}[/])[/]:\n'
            f"[red]{result.stdout}{result.stderr}[/]\n"
        )

    def _run_rollback(self) -> None:
        if not self.rollback:
            return
        self.console.print("[bold yellow]Rolling back:[/]")
        for name, command in self.rollback.items():
            self.execute_command(name, command)

    def execute_command(self, name: str, command: str) -> subprocess.CompletedProcess[str]:
        self.console.print()
        self.console.print(f"[bold blue]{name.lower()}:")
        self.console.print(f"[bold yellow]{command}[/]")
        return subprocess.run(shlex.split(command), capture_output=True, text=True, check=False)


__all__ = ["CommandProcessor", "normalize_svg"]
