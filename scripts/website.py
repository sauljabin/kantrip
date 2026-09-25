"""Render the static demo transcript and validate the GitHub Pages site."""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SITE_ROOT = PROJECT_ROOT / "site"
DEMO_PATH = SITE_ROOT / "demo.json"
INDEX_PATH = SITE_ROOT / "index.html"
TRANSCRIPT_START = "<!-- demo-transcript:start -->"
TRANSCRIPT_END = "<!-- demo-transcript:end -->"
REPOSITORY_URL = "https://github.com/sauljabin/kantrip"
LINK_HOSTS = frozenset({"github.com", "pypi.org"})
OUTPUT_STYLES = frozenset(
    {"primary", "secondary", "accent", "success", "warning", "error", "muted"}
)
# Capture leftovers that must be made generic before the demo is committed.
DEMO_FORBIDDEN = ("localhost", "127.0.0.1", "/Users/", "sandbox", "kantrip-scram", "site-demo")
ASSET_BUDGET_BYTES = 30 * 1024
REQUEST_TAGS = {"script": "src", "img": "src", "source": "src", "iframe": "src"}
REQUEST_LINK_RELS = frozenset({"stylesheet", "icon", "preload", "modulepreload", "manifest"})
HELP_OPTION = re.compile(r"^\s{1,8}(-{1,2}[A-Za-z][\w-]*)(?:,\s*(-{1,2}[A-Za-z][\w-]*))?")

HelpRunner = Callable[[tuple[str, ...]], "str | None"]


class SiteError(ValueError):
    """The site or its demo data is invalid."""


class _Page(HTMLParser):
    """Collect ids and outgoing references from one HTML document."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: set[str] = set()
        self.references: list[tuple[str, str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name: value or "" for name, value in attrs}
        if "id" in values:
            self.ids.add(values["id"])
        if tag == "a" and "href" in values:
            self.references.append(("link", tag, values["href"]))
        elif tag == "link" and "href" in values:
            rels = set(values.get("rel", "").lower().split())
            kind = "request" if rels & REQUEST_LINK_RELS else "link"
            self.references.append((kind, tag, values["href"]))
        elif tag in REQUEST_TAGS and REQUEST_TAGS[tag] in values:
            self.references.append(("request", tag, values[REQUEST_TAGS[tag]]))


def load_demo(path: Path = DEMO_PATH) -> dict[str, Any]:
    """Read and structurally validate the demo data file."""
    demo = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(demo, dict) or not isinstance(demo.get("capture"), str):
        raise SiteError(f"{path}: expected an object with a 'capture' note")
    steps = demo.get("steps")
    if not isinstance(steps, list) or not steps:
        raise SiteError(f"{path}: expected a non-empty 'steps' list")
    for index, step in enumerate(steps):
        _validate_step(f"{path}: step {index + 1}", step)
    return demo


def _validate_step(where: str, step: object) -> None:
    if not isinstance(step, dict) or set(step) - {"prompt", "command", "spinner", "output"}:
        raise SiteError(f"{where}: expected prompt, command, optional spinner, and output")
    if not all(isinstance(step.get(key), str) for key in ("prompt", "command")):
        raise SiteError(f"{where}: prompt and command must be strings")
    if "spinner" in step and not isinstance(step["spinner"], str):
        raise SiteError(f"{where}: spinner must be a string")
    output = step.get("output")
    if not isinstance(output, list):
        raise SiteError(f"{where}: output must be a list")
    for line in output:
        if (
            not isinstance(line, dict)
            or not isinstance(line.get("text"), str)
            or set(line) - {"text", "style", "wait"}
            or line.get("style", "primary") not in OUTPUT_STYLES
            or not isinstance(line.get("wait", 0), int)
        ):
            raise SiteError(f"{where}: invalid output line {line!r}")


def render_transcript(demo: Mapping[str, Any]) -> str:
    """Render the demo as static, escaped transcript lines for index.html."""
    lines: list[str] = []
    for step in demo["steps"]:
        spinner = step.get("spinner")
        data = f' data-spinner="{html.escape(spinner)}"' if spinner else ""
        lines.append(
            f'<span class="ln cmd"{data}><span class="t-prompt">'
            f'{html.escape(step["prompt"])}</span>{html.escape(step["command"])}</span>'
        )
        for line in step["output"]:
            classes = "ln out" + (f' t-{line["style"]}' if "style" in line else "")
            wait = f' data-wait="{line["wait"]}"' if line.get("wait") else ""
            lines.append(f'<span class="{classes}"{wait}>{html.escape(line["text"])}</span>')
    return "\n".join(lines)


def _split_index(text: str) -> tuple[str, str, str]:
    start = text.find(TRANSCRIPT_START)
    end = text.find(TRANSCRIPT_END)
    if start < 0 or end < start:
        raise SiteError(f"{INDEX_PATH.name}: missing demo transcript markers")
    start += len(TRANSCRIPT_START)
    return text[:start], text[start:end], text[end:]


def render_index(demo: Mapping[str, Any], text: str) -> str:
    """Replace the generated transcript block, keeping the markers on their own lines."""
    head, _, tail = _split_index(text)
    return f"{head}{render_transcript(demo)}{tail}"


def check_transcript(demo: Mapping[str, Any], index_text: str) -> list[str]:
    if render_index(demo, index_text) != index_text:
        return ["site/index.html: demo transcript is stale; run python -m scripts.website render"]
    return []


def check_demo_content(demo: Mapping[str, Any]) -> list[str]:
    """Reject sandbox leftovers that would reveal private capture details."""
    text = json.dumps(demo["steps"], ensure_ascii=False)
    return [
        f"site/demo.json: replace capture-specific value {fragment!r} with a generic one"
        for fragment in DEMO_FORBIDDEN
        if fragment in text
    ]


def kantrip_invocations(demo: Mapping[str, Any]) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """Return (command path, option names) for every kantrip command in the demo."""
    invocations = []
    for step in demo["steps"]:
        tokens = shlex.split(step["command"])
        if not tokens or tokens[0] != "kantrip":
            continue
        path: list[str] = []
        options: list[str] = []
        for token in tokens[1:]:
            if token == "--":
                break
            if token.startswith("-"):
                options.append(token.split("=", 1)[0])
            elif not path:
                path.append(token)
        invocations.append((tuple(path), tuple(options)))
    return invocations


def help_options(help_text: str) -> set[str]:
    options: set[str] = set()
    for line in help_text.splitlines():
        match = HELP_OPTION.match(line)
        if match:
            options.update(name for name in match.groups() if name)
    return options


def run_kantrip_help(command: tuple[str, ...]) -> str | None:
    """Run `kantrip COMMAND --help`; return None when the command is unknown."""
    executable = shutil.which("kantrip")
    if executable is None:
        raise SiteError("kantrip is not on PATH; run through 'uv run --locked'")
    environment = {**os.environ, "NO_COLOR": "1", "COLUMNS": "200"}
    result = subprocess.run(
        (executable, *command, "--help"),
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        timeout=60,
    )
    return result.stdout if result.returncode == 0 else None


def check_demo_commands(
    demo: Mapping[str, Any], help_runner: HelpRunner = run_kantrip_help
) -> list[str]:
    """Fail when the demo uses a kantrip command or option the CLI does not define."""
    errors: list[str] = []
    for command, options in kantrip_invocations(demo):
        label = " ".join(("kantrip", *command))
        help_text = help_runner(command)
        if help_text is None:
            errors.append(f"site/demo.json: '{label}' is not a kantrip command")
            continue
        known = help_options(help_text)
        errors.extend(
            f"site/demo.json: '{label}' has no option {option}"
            for option in options
            if option not in known
        )
    return errors


def check_pages(site_root: Path, repository_root: Path = PROJECT_ROOT) -> list[str]:
    """Check local links, same-page anchors, repository links, and third-party requests."""
    pages = {path: _parse_page(path) for path in sorted(site_root.rglob("*.html"))}
    errors: list[str] = []
    for path, page in pages.items():
        where = path.relative_to(site_root.parent).as_posix()
        for kind, tag, reference in page.references:
            error = _check_reference(site_root, repository_root, pages, path, kind, reference)
            if error:
                errors.append(f"{where}: <{tag}> {reference!r} {error}")
    for path in sorted(site_root.rglob("*.css")):
        for reference in re.findall(r"url\(\s*['\"]?([^'\")]+)", path.read_text(encoding="utf-8")):
            if not reference.startswith(("#", "data:")):
                errors.append(
                    f"{path.relative_to(site_root.parent)}: url({reference}) is a request"
                )
    return errors


def _parse_page(path: Path) -> _Page:
    page = _Page()
    page.feed(path.read_text(encoding="utf-8"))
    page.close()
    return page


def _check_reference(
    site_root: Path,
    repository_root: Path,
    pages: Mapping[Path, _Page],
    path: Path,
    kind: str,
    reference: str,
) -> str | None:
    parts = urlsplit(reference)
    if parts.scheme or parts.netloc:
        if kind == "request":
            return "is a third-party request"
        if parts.scheme != "https" or parts.netloc not in LINK_HOSTS:
            return f"must use https on {', '.join(sorted(LINK_HOSTS))}"
        return _check_repository_link(repository_root, reference)
    if reference.startswith("/"):
        return "must be relative to work under the /kantrip/ path"
    if not parts.path:
        return None if parts.fragment and parts.fragment in pages[path].ids else "has no target"
    target = (path.parent / parts.path).resolve()
    if target.is_dir():
        target = target / "index.html"
    if not target.is_relative_to(site_root.resolve()) or not target.is_file():
        return "does not exist in site/"
    if parts.fragment and target in pages and parts.fragment not in pages[target].ids:
        return "points to a missing anchor"
    return None


def _check_repository_link(repository_root: Path, reference: str) -> str | None:
    prefix = f"{REPOSITORY_URL}/blob/main/"
    if not reference.startswith(prefix):
        return None
    relative = urlsplit(reference).path.removeprefix(urlsplit(prefix).path)
    if not (repository_root / relative).is_file():
        return "points to a repository file that does not exist"
    return None


def check_asset_budget(site_root: Path) -> list[str]:
    assets: Iterable[Path] = (*site_root.rglob("*.js"), *site_root.rglob("*.css"))
    total = sum(path.stat().st_size for path in assets)
    if total > ASSET_BUDGET_BYTES:
        return [f"site/: JavaScript and CSS use {total} bytes; the budget is {ASSET_BUDGET_BYTES}"]
    return []


def check_site(help_runner: HelpRunner = run_kantrip_help) -> list[str]:
    demo = load_demo()
    return [
        *check_transcript(demo, INDEX_PATH.read_text(encoding="utf-8")),
        *check_demo_content(demo),
        *check_pages(SITE_ROOT),
        *check_asset_budget(SITE_ROOT),
        *check_demo_commands(demo, help_runner),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("check", "render"),
        nargs="?",
        default="check",
        help="check the site (default) or rewrite the transcript in index.html",
    )
    args = parser.parse_args()
    if args.action == "render":
        demo = load_demo()
        INDEX_PATH.write_text(
            render_index(demo, INDEX_PATH.read_text(encoding="utf-8")), encoding="utf-8"
        )
        print(f"Rendered the demo transcript into {INDEX_PATH.relative_to(PROJECT_ROOT)}")
        return
    errors = check_site()
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        raise SystemExit(1)
    print("Site checks passed")


if __name__ == "__main__":
    main()
