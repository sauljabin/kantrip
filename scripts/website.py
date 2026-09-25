"""Capture, render, and validate the GitHub Pages site and its terminal demo."""

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
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pyte

from kantrip import APP_VERSION
from kantrip.console import ARCANA_COLORS
from scripts import TerminalTimeout, run_terminal

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
DEMO_FORBIDDEN = (
    "localhost",
    "127.0.0.1",
    "/Users/",
    "/var/",
    "/tmp/",
    "sandbox",
    "kantrip-scram",
    "kantrip-auth-",
    "site-demo",
)
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


# Demo capture: run the generic demo commands against the sandbox's
# SCRAM-SHA-512 listener in a real terminal, then map the output back to the
# generic values. The lab password is typed into the no-echo prompt and never
# printed, written, or accepted in captured output.
GENERIC_PROFILE = "prod"
GENERIC_BOOTSTRAP = "kafka.example.com:9093"
GENERIC_CA_FILE = "./ca.pem"
GENERIC_USERNAME = "app"
GENERIC_DATABASE = "/home/demo/.local/share/kantrip/profiles.db"
CAPTURE_PROFILE = "kantrip-site-demo"
CAPTURE_TOPICS = {
    "kantrip-auth-site-demo-orders": "orders",
    "kantrip-auth-site-demo-payments": "payments",
}
SANDBOX_BOOTSTRAP = "localhost:9094"
SANDBOX_USERNAME = "KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME"
SANDBOX_PASSWORD = "KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD"
SECRET_PROMPT = "Kafka password"
SECRET_PROMPT_WAIT_MS = 1400
TERMINAL_COLUMNS = 400
STYLE_BY_COLOR = {
    **{value.lstrip("#").lower(): name for name, value in ARCANA_COLORS.items()},
    "brightblack": "muted",
}
DEFAULT_STYLES = frozenset({"default", "foreground"})
SESSION_MARKER = "__KANTRIP_SITE_DEMO_{}__"
# Private session paths differ per run and platform; show a generic Linux one.
SESSION_PATH = re.compile(r"/\S*?/kantrip(?:-\d+)?/sessions/session-[0-9a-f]+/")
GENERIC_SESSION_PATH = "/run/user/1000/kantrip/sessions/session-5f0c2a9d4b7e41c8a3d6e9f1b2c4a7d0/"
# The sandbox binds IPv4 loopback only, so librdkafka's first try of localhost's
# IPv6 address fails; a real broker host does not produce these lines.
SANDBOX_NOISE = re.compile(r"^%\d\|[\d.]+\|FAIL\|.*Connect to ipv6#\[::1\]:")

TerminalRunner = Callable[..., tuple[int, str]]
CommandRunner = Callable[[Sequence[str], Mapping[str, str]], tuple[int, str]]


class CaptureError(RuntimeError):
    """The sandbox capture could not produce a trustworthy transcript."""


@dataclass(frozen=True)
class CaptureTarget:
    """Sandbox values substituted for the demo's generic ones, and their inverse."""

    kantrip: str
    ca_file: Path
    username: str
    password: str
    database: Path

    def arguments(self, command: str) -> list[str]:
        values = {
            "kantrip": self.kantrip,
            GENERIC_PROFILE: CAPTURE_PROFILE,
            GENERIC_BOOTSTRAP: SANDBOX_BOOTSTRAP,
            GENERIC_CA_FILE: str(self.ca_file),
            GENERIC_USERNAME: self.username,
        }
        tokens = shlex.split(command)
        return [values.get(token, token) if token != "--" else token for token in tokens]

    def generic(self, text: str) -> str:
        if self.password in text:
            raise CaptureError("captured output contains the sandbox password; nothing was written")
        replacements = {
            str(self.database.resolve()): GENERIC_DATABASE,
            str(self.database): GENERIC_DATABASE,
            str(self.ca_file): GENERIC_CA_FILE,
            SANDBOX_BOOTSTRAP: GENERIC_BOOTSTRAP,
            self.username: GENERIC_USERNAME,
            **CAPTURE_TOPICS,
            CAPTURE_PROFILE: GENERIC_PROFILE,
        }
        text = SESSION_PATH.sub(GENERIC_SESSION_PATH, text)
        for sandbox_value in sorted(replacements, key=len, reverse=True):
            text = text.replace(sandbox_value, replacements[sandbox_value])
        return text


def render_screen(output: str) -> pyte.Screen:
    """Replay terminal output on a virtual screen, as a user would see it."""
    screen = pyte.Screen(TERMINAL_COLUMNS, output.count("\n") + 2)
    pyte.Stream(screen).feed(output.replace("\r\n", "\n").replace("\n", "\r\n"))
    return screen


def screen_lines(
    output: str | pyte.Screen, target: CaptureTarget, rows: range | None = None
) -> list[dict[str, Any]]:
    """Map screen rows to generic demo lines, trimming surrounding blank rows."""
    screen = render_screen(output) if isinstance(output, str) else output
    selected = range(screen.lines) if rows is None else rows
    lines = [
        _screen_line(screen, row, target)
        for row in selected
        if not SANDBOX_NOISE.match(_row_text(screen, row))
    ]
    while lines and not lines[-1]["text"]:
        lines.pop()
    while lines and not lines[0]["text"]:
        lines.pop(0)
    return lines


def _row_text(screen: pyte.Screen, row: int) -> str:
    return "".join(screen.buffer[row][column].data for column in range(screen.columns)).rstrip()


def _screen_line(screen: pyte.Screen, row: int, target: CaptureTarget) -> dict[str, Any]:
    cells = [screen.buffer[row][column] for column in range(screen.columns)]
    text = target.generic(_row_text(screen, row))
    colors = {cell.fg for cell in cells if cell.data.strip()} - DEFAULT_STYLES
    unknown = colors - STYLE_BY_COLOR.keys()
    if unknown:
        raise CaptureError(f"unknown terminal colors {sorted(unknown)} in {text!r}")
    styles = {STYLE_BY_COLOR[color] for color in colors} - DEFAULT_STYLES
    if len(styles) > 1:
        raise CaptureError(f"mixed styles {sorted(styles)} in {text!r} are not supported")
    line: dict[str, Any] = {"text": text}
    if styles:
        line["style"] = styles.pop()
    if text.startswith(f"{SECRET_PROMPT}:"):
        line.update(text=f"{SECRET_PROMPT}: ", wait=SECRET_PROMPT_WAIT_MS)
    return line


def capture_environment(database: Path) -> dict[str, str]:
    """A colored terminal environment with an isolated profile database."""
    bash = shutil.which("bash")
    if bash is None:
        raise CaptureError("bash is required for the interactive demo session")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("KANTRIP_", "KAFKA_")) and key != "NO_COLOR"
    }
    environment.update(
        KANTRIP_DATABASE=str(database),
        TERM="xterm-256color",
        COLUMNS=str(TERMINAL_COLUMNS),
        LINES="50",
        SHELL=bash,
    )
    return environment


def run_quiet(arguments: Sequence[str], environment: Mapping[str, str]) -> tuple[int, str]:
    result = subprocess.run(
        tuple(arguments),
        env=dict(environment),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    return result.returncode, result.stdout + result.stderr


def _session_range(steps: Sequence[Mapping[str, Any]], start: int) -> int:
    """Return the index of the 'exit' step that closes an interactive session."""
    for index in range(start + 1, len(steps)):
        if steps[index]["command"].strip() == "exit":
            return index
    raise CaptureError(f"step {start + 1} opens a session without a later 'exit' step")


def _opens_session(command: str) -> bool:
    tokens = shlex.split(command)
    return tokens[:2] == ["kantrip", "exec"] and len(tokens) == 3


@dataclass
class DemoCapture:
    """Run each demo step once and collect its generic output lines."""

    target: CaptureTarget
    environment: Mapping[str, str]
    terminal: TerminalRunner = run_terminal
    command: CommandRunner = run_quiet

    def run(self, steps: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
        if shlex.split(steps[0]["command"])[:3] != ["kantrip", "add", GENERIC_PROFILE]:
            raise CaptureError(f"the first demo step must be 'kantrip add {GENERIC_PROFILE}'")
        try:
            outputs = self._run_steps(steps)
        except BaseException:
            # The profile and its keychain entry may exist even when add failed.
            for error in self._cleanup():
                print(f"cleanup: {error}", file=sys.stderr)
            raise
        errors = self._cleanup()
        if errors:
            raise CaptureError("\n".join(errors))
        return outputs

    def _run_steps(self, steps: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
        outputs = [self._step(steps[0]["command"], secret=True)]
        self._topics("--create", "--partitions", "1", "--if-not-exists")
        index = 1
        while index < len(steps):
            if _opens_session(steps[index]["command"]):
                end = _session_range(steps, index)
                outputs.extend(self._session(steps[index : end + 1]))
                index = end + 1
            else:
                outputs.append(self._step(steps[index]["command"]))
                index += 1
        return outputs

    def _terminal(self, arguments: Sequence[str], inputs: Sequence[str], ready: str | None) -> str:
        try:
            status, output = self.terminal(
                arguments, inputs, environment=self.environment, ready_text=ready, timeout=120
            )
        except TerminalTimeout as error:
            raise CaptureError(f"{shlex.join(arguments[1:])} timed out") from error
        generic = self.target.generic(output)
        if status:
            raise CaptureError(f"a demo command exited with {status}:\n{generic}")
        return output

    def _step(self, command: str, *, secret: bool = False) -> list[dict[str, Any]]:
        arguments = self.target.arguments(command)
        inputs = (self.target.password,) if secret else ()
        output = self._terminal(arguments, inputs, SECRET_PROMPT if secret else None)
        return screen_lines(output, self.target)

    def _session(self, steps: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
        """Run the commands between 'kantrip exec PROFILE' and 'exit' in its subshell."""
        commands = [step["command"] for step in steps[1:-1]]
        with tempfile.TemporaryDirectory(prefix="kantrip-demo-session-") as directory:
            driver = Path(directory) / "commands"
            lines = []
            for index, command in enumerate(commands):
                lines.append(f"printf '%s\\n' {SESSION_MARKER.format(index)}")
                lines.append(shlex.join(self.target.arguments(command)))
            lines.append(f"printf '%s\\n' {SESSION_MARKER.format('end')}")
            driver.write_text("\n".join(lines) + "\n", encoding="utf-8")
            driver.chmod(0o600)
            output = self._terminal(
                self.target.arguments(steps[0]["command"]),
                (f". {shlex.quote(str(driver))} < /dev/null; exit $?",),
                None,
            )
        # Only rows between markers are demo output; the user's prompt is not.
        screen = render_screen(output)
        texts = [_row_text(screen, row).strip() for row in range(screen.lines)]
        markers = [SESSION_MARKER.format(index) for index in range(len(commands))]
        markers.append(SESSION_MARKER.format("end"))
        if any(marker not in texts for marker in markers):
            raise CaptureError("the interactive session did not run every demo command")
        positions = [texts.index(marker) for marker in markers]
        inner = [
            screen_lines(screen, self.target, range(start + 1, end))
            for start, end in pairwise(positions)
        ]
        return [[], *inner, []]

    def _topics(self, action: str, *options: str) -> None:
        for topic in CAPTURE_TOPICS:
            arguments = [self.target.kantrip, "exec", CAPTURE_PROFILE, "--", "kafka-topics"]
            status, output = self.command(
                [*arguments, action, "--topic", topic, *options], self.environment
            )
            if status:
                generic = self.target.generic(output)
                raise CaptureError(f"kafka-topics {action} {topic} failed:\n{generic}")

    def _cleanup(self) -> list[str]:
        errors: list[str] = []
        try:
            self._topics("--delete", "--if-exists")
        except CaptureError as error:
            errors.append(str(error))
        status, output = self.command(
            [self.target.kantrip, "remove", CAPTURE_PROFILE, "--force"], self.environment
        )
        if status:
            errors.append(
                f"kantrip remove {CAPTURE_PROFILE} failed; check the credential store:\n"
                + self.target.generic(output)
            )
        return errors


@contextmanager
def sandbox_target(state_dir: Path) -> Iterator[CaptureTarget]:
    """Load the sandbox SCRAM identity without printing it, in a private database."""
    from sandbox.__main__ import load_credentials

    kantrip = shutil.which("kantrip")
    if kantrip is None:
        raise CaptureError("kantrip is not on PATH; run through 'uv run --locked'")
    for tool in ("kcat", "kafka-topics"):
        if shutil.which(tool) is None:
            raise CaptureError(f"{tool} is required on PATH for the demo capture")
    ca_file = state_dir / "ca.crt"
    if not ca_file.is_file():
        raise CaptureError(f"{ca_file} is missing; run 'python -m sandbox up' first")
    credentials = load_credentials(state_dir / "credentials.env")
    with tempfile.TemporaryDirectory(prefix="kantrip-demo-db-") as directory:
        yield CaptureTarget(
            kantrip=kantrip,
            ca_file=ca_file,
            username=credentials[SANDBOX_USERNAME],
            password=credentials[SANDBOX_PASSWORD],
            database=Path(directory) / "profiles.db",
        )


def capture_demo(demo: Mapping[str, Any], capture: DemoCapture) -> dict[str, Any]:
    """Replace every step's output with a fresh, generic capture."""
    outputs = capture.run(demo["steps"])
    steps = [
        {**step, "output": output} for step, output in zip(demo["steps"], outputs, strict=True)
    ]
    note = (
        f"Captured on {datetime.now(timezone.utc).date().isoformat()} with kantrip {APP_VERSION} by "
        "'python -m scripts.website capture' against the local sandbox's SCRAM-SHA-512 "
        "TLS listener; hosts, names, paths, and topics are made generic."
    )
    captured = {"capture": note, "steps": steps}
    errors = check_demo_content(captured)
    if errors:
        raise CaptureError("\n".join(("the capture was not written:", *errors)))
    return captured


def write_demo(demo: Mapping[str, Any]) -> None:
    DEMO_PATH.write_text(json.dumps(demo, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    INDEX_PATH.write_text(
        render_index(demo, INDEX_PATH.read_text(encoding="utf-8")), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("check", "render", "capture"),
        nargs="?",
        default="check",
        help="check the site (default), rewrite the transcript in index.html, "
        "or recapture the demo from the running sandbox",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=PROJECT_ROOT / "sandbox" / ".state",
        help="private sandbox state directory used by capture",
    )
    args = parser.parse_args()
    if args.action == "capture":
        with sandbox_target(args.state_dir) as target:
            capture = DemoCapture(target, capture_environment(target.database))
            demo = capture_demo(load_demo(), capture)
        write_demo(demo)
        print(f"Captured the demo into {DEMO_PATH.relative_to(PROJECT_ROOT)}")
    elif args.action == "render":
        write_demo(load_demo())
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
