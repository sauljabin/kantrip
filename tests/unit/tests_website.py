"""GitHub Pages site rendering and validation."""

from __future__ import annotations

import copy
import shlex
import tempfile
import unittest
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner

from kantrip.cli import cli
from scripts.website import (
    CaptureError,
    CaptureTarget,
    DemoCapture,
    SiteError,
    capture_demo,
    capture_environment,
    check_demo_commands,
    check_demo_content,
    check_pages,
    check_site,
    check_social_preview,
    check_transcript,
    help_options,
    kantrip_invocations,
    load_demo,
    page_commands,
    render_index,
    render_transcript,
    screen_lines,
    sort_topic_blocks,
)

PROMPT = [{"text": "❯ ", "style": "secondary"}]
DEMO: dict[str, Any] = {
    "capture": "synthetic",
    "steps": [
        {
            "prompt": PROMPT,
            "command": "kantrip add prod -b kafka.example.com:9093 --transport=tls --auth plain",
            "output": [{"text": "Kafka password: ", "wait": 900}, {"text": "Added <prod>"}],
        },
        {
            "prompt": PROMPT,
            "command": "kantrip ping prod",
            "spinner": "Checking profile 'prod'",
            "output": [{"text": "ok", "style": "success"}],
        },
        {"prompt": PROMPT, "command": "kantrip exec prod -- kcat -L -b other", "output": []},
        {"prompt": PROMPT, "command": "kafka-topics --list --bogus", "output": []},
    ],
}


def cli_help(command: tuple[str, ...]) -> str | None:
    """Run the in-process CLI help, mirroring `kantrip COMMAND --help`."""
    result = CliRunner().invoke(cli, [*command, "--help"], env={"NO_COLOR": "1"})
    return result.output if result.exit_code == 0 else None


class TestDemoCommands(unittest.TestCase):
    def test_invocations_stop_at_the_child_command(self) -> None:
        self.assertEqual(
            kantrip_invocations(DEMO),
            [
                (("add",), ("-b", "--transport", "--auth")),
                (("ping",), ()),
                (("exec",), ()),
            ],
        )

    def test_known_commands_and_options_pass(self) -> None:
        self.assertEqual(check_demo_commands(DEMO, cli_help), [])

    def test_unknown_option_fails(self) -> None:
        demo = copy.deepcopy(DEMO)
        demo["steps"][0]["command"] += " --bootstrap-servers kafka.example.com:9093"
        self.assertEqual(
            check_demo_commands(demo, cli_help),
            ["site/demo.json: 'kantrip add' has no option --bootstrap-servers"],
        )

    def test_unknown_command_fails(self) -> None:
        demo = copy.deepcopy(DEMO)
        demo["steps"][1]["command"] = "kantrip import prod"
        self.assertEqual(
            check_demo_commands(demo, cli_help),
            ["site/demo.json: 'kantrip import' is not a kantrip command"],
        )

    def test_card_commands_are_checked_too(self) -> None:
        page = (
            '<p class="card-command" aria-hidden="true">kantrip doctor prod --sessions</p>'
            '<p class="card-command">kantrip doctor prod --session</p>'
        )
        self.assertEqual(
            check_demo_commands(page_commands(page), cli_help, "site/index.html"),
            ["site/index.html: 'kantrip doctor' has no option --session"],
        )

    def test_help_options_read_only_option_rows(self) -> None:
        text = (
            "Usage: kantrip add [OPTIONS] PROFILE\n\n"
            "Options:\n"
            "  -b, --bootstrap-servers TEXT  Broker addresses.\n"
            "  --registry-provider [confluent|apicurio]\n"
            "                                  defaults when --registry-url is supplied.\n"
        )
        self.assertEqual(help_options(text), {"-b", "--bootstrap-servers", "--registry-provider"})

    def test_capture_leftovers_are_rejected(self) -> None:
        demo = copy.deepcopy(DEMO)
        demo["steps"][0]["command"] = "kantrip add site-demo -b localhost:9094"
        self.assertEqual(len(check_demo_content(demo)), 2)
        self.assertEqual(check_demo_content(DEMO), [])


class TestTranscript(unittest.TestCase):
    def test_render_escapes_text_and_keeps_replay_metadata(self) -> None:
        html = render_transcript(DEMO)
        self.assertIn("Added &lt;prod&gt;", html)
        self.assertIn('data-wait="900"', html)
        self.assertIn('data-spinner="Checking profile &#x27;prod&#x27;"', html)
        self.assertIn('<span class="ln out t-success">ok</span>', html)
        self.assertIn(
            '<span class="ln cmd"><span class="t-prompt"><span class="t-secondary">❯ </span>'
            "</span>kantrip exec prod -- kcat -L -b other</span>",
            html,
        )
        self.assertEqual(html.count("\n"), 6)

    def test_nerd_font_glyphs_become_inline_icons(self) -> None:
        demo = copy.deepcopy(DEMO)
        demo["steps"][0]["prompt"] = [{"text": "\U000f100f prod ", "style": "accent"}]
        html = render_transcript(demo)
        self.assertIn(
            '<span class="t-accent"><svg class="glyph" aria-hidden="true" focusable="false">'
            '<use href="#kafka"/></svg> prod </span>',
            html,
        )
        self.assertNotIn("\U000f100f", html)

    def test_stale_transcript_is_reported(self) -> None:
        page = "<pre><!-- demo-transcript:start -->old<!-- demo-transcript:end --></pre>"
        self.assertEqual(len(check_transcript(DEMO, page)), 1)
        self.assertEqual(check_transcript(DEMO, render_index(DEMO, page)), [])

    def test_invalid_demo_data_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "demo.json"
            path.write_text(
                '{"capture": "x", "steps": [{"prompt": [{"text": "$ "}], "command": "kantrip list",'
                ' "output": [{"text": "x", "style": "blink"}]}]}',
                encoding="utf-8",
            )
            with self.assertRaises(SiteError):
                load_demo(path)


class TestPages(unittest.TestCase):
    def test_references_are_local_relative_and_present(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "USAGE.md").write_text("usage", encoding="utf-8")
            site = root / "site"
            site.mkdir()
            (site / "styles.css").write_text(
                "a { background: url(https://cdn.example.com/x.png); }", encoding="utf-8"
            )
            (site / "index.html").write_text(
                '<link rel="stylesheet" href="styles.css">'
                '<link rel="stylesheet" href="https://fonts.example.com/f.css">'
                '<script src="/site.js"></script>'
                '<img src="missing.svg" alt="">'
                '<a href="#top">top</a><a href="#nowhere">bad</a><h1 id="top">x</h1>'
                '<a href="./">home</a>'
                '<a href="http://github.com/sauljabin/kantrip">insecure</a>'
                '<a href="https://example.com/">elsewhere</a>'
                '<a href="https://github.com/sauljabin/kantrip/blob/main/USAGE.md">ok</a>'
                '<a href="https://github.com/sauljabin/kantrip/blob/main/GONE.md">gone</a>',
                encoding="utf-8",
            )
            errors = check_pages(site, root)
        self.assertEqual(
            [error.split(" ", 2)[:2] for error in errors],
            [
                ["site/index.html:", "<link>"],
                ["site/index.html:", "<script>"],
                ["site/index.html:", "<img>"],
                ["site/index.html:", "<a>"],
                ["site/index.html:", "<a>"],
                ["site/index.html:", "<a>"],
                ["site/index.html:", "<a>"],
                ["site/styles.css:", "url(https://cdn.example.com/x.png)"],
            ],
        )
        self.assertIn("third-party request", errors[0])
        self.assertIn("/kantrip/", errors[1])
        self.assertIn("GONE.md", errors[6])

    def test_social_preview_must_exist_with_its_declared_size(self) -> None:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (1280).to_bytes(4, "big")
        png += (640).to_bytes(4, "big")
        head = (
            '<meta property="og:url" content="https://sauljabin.github.io/kantrip/">'
            '<meta property="og:image" content="https://sauljabin.github.io/kantrip/card.png">'
            '<meta property="og:image:width" content="1280">'
        )
        with tempfile.TemporaryDirectory() as directory:
            site = Path(directory)
            (site / "index.html").write_text(head, encoding="utf-8")
            self.assertEqual(len(check_social_preview(site)), 1)
            (site / "card.png").write_bytes(png)
            self.assertIn("declares", check_social_preview(site)[0])
            (site / "index.html").write_text(
                head + '<meta property="og:image:height" content="640">', encoding="utf-8"
            )
            self.assertEqual(check_social_preview(site), [])

    def test_repository_site_passes(self) -> None:
        self.assertEqual(check_site(cli_help), [])


PASSWORD = "synthetic-lab-password"
SUCCESS = "\x1b[38;2;52;211;153m"
ACCENT = "\x1b[38;2;96;165;250m"
SECONDARY = "\x1b[38;2;34;211;238m"
PRIMARY = "\x1b[38;2;59;130;246m"
RESET = "\x1b[0m"
CAPTURE_STEPS: list[dict[str, Any]] = [
    {
        "prompt": PROMPT,
        "command": "kantrip add prod -b kafka.example.com:9093 --transport tls "
        "--ca-file ./ca.pem --auth scram-sha-512 --username app",
        "output": [],
    },
    {"prompt": PROMPT, "command": "kantrip ping prod", "spinner": "Checking", "output": []},
    {"prompt": PROMPT, "command": "kantrip exec prod -- kcat -L", "output": []},
    {"prompt": PROMPT, "command": "kantrip exec prod", "output": []},
    {"prompt": PROMPT, "command": "kafka-topics --list", "output": []},
    {"prompt": PROMPT, "command": "exit", "output": []},
]


class FakeLab:
    """Canned terminal output from a synthetic sandbox run."""

    def __init__(self, target: CaptureTarget, leak: str = "", fail: str = "") -> None:
        self.target = target
        self.leak = leak
        self.fail = fail
        self.terminal_calls: list[tuple[list[str], tuple[str, ...], str | None]] = []
        self.commands: list[list[str]] = []
        self.driver = ""

    def terminal(
        self,
        arguments: Sequence[str],
        inputs: Sequence[str],
        *,
        environment: Mapping[str, str],
        ready_text: str | None,
        timeout: float,
    ) -> tuple[int, str]:
        self.terminal_calls.append((list(arguments), tuple(inputs), ready_text))
        subcommand = " ".join(arguments[1:3])
        if subcommand == self.fail:
            return 1, "boom"
        if arguments[1] == "add":
            added = f"Added profile 'kantrip-site-demo' to {self.target.database}"
            return 0, f"Kafka password: \r\n{added}\r\n{self.leak}"
        if arguments[1] == "ping":
            return 0, (
                f"\x1b[?25l{PRIMARY}🌑{RESET} Checking profile 'kantrip-site-demo'\r\x1b[2K"
                f"\x1b[?25h{SUCCESS}✅ Kafka transport: verified TLS; "
                f"authentication: SCRAM-SHA-512{RESET}\r\n"
            )
        if "kcat" in arguments:
            return 0, (
                "% Reading configuration from file /var/folders/x/T/kantrip-501/sessions/"
                "session-0a1b/kcat.conf\r\n"
                "%3|1790364171.651|FAIL|rdkafka#producer-1| [thrd:sasl_ssl://localhost:9094/0]: "
                "Connect to ipv6#[::1]:9094 failed: Connection refused\r\n"
                "Metadata for all topics (from broker 0: sasl_ssl://localhost:9094/0):\r\n"
                " 1 brokers:\r\n  broker 0 at localhost:9094 (controller)\r\n"
                ' 2 topics:\r\n  topic "kantrip-auth-site-demo-payments" with 1 partitions:\r\n'
                "    partition 0, leader 0, replicas: 0, isrs: 0\r\n"
                '  topic "kantrip-auth-site-demo-orders" with 1 partitions:\r\n'
                "    partition 0, leader 0, replicas: 0, isrs: 0\r\n"
            )
        self.driver = Path(shlex.split(inputs[0])[1]).read_text(encoding="utf-8")
        prompt = f"{ACCENT}\U000f100f kantrip-site-demo {RESET}{SECONDARY}❯{RESET} "
        return 0, (
            f"{inputs[0]}\r\n{prompt}{inputs[0]}\r\n"
            "__KANTRIP_SITE_DEMO_0__\r\nkantrip-auth-site-demo-orders\r\n"
            "kantrip-auth-site-demo-payments\r\n__KANTRIP_SITE_DEMO_end__\r\n"
        )

    def command(self, arguments: Sequence[str], environment: Mapping[str, str]) -> tuple[int, str]:
        self.commands.append(list(arguments))
        return 0, ""


class TestDemoCapture(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.target = CaptureTarget(
            kantrip="/venv/bin/kantrip",
            ca_file=Path("/lab/state/ca.crt"),
            username="kantrip-scram",
            password=PASSWORD,
            database=Path(directory.name) / "profiles.db",
        )

    def capture(self, lab: FakeLab) -> dict[str, Any]:
        demo = {"capture": "old", "steps": CAPTURE_STEPS}
        return capture_demo(demo, DemoCapture(self.target, {}, lab.terminal, lab.command))

    def test_capture_runs_sandbox_values_and_writes_generic_output(self) -> None:
        lab = FakeLab(self.target)
        demo = self.capture(lab)
        add_arguments, add_inputs, ready = lab.terminal_calls[0]
        self.assertEqual(add_inputs, (PASSWORD,))
        self.assertEqual(ready, "Kafka password")
        self.assertEqual(
            add_arguments[:9],
            [
                "/venv/bin/kantrip",
                "add",
                "kantrip-site-demo",
                "-b",
                "localhost:9094",
                "--transport",
                "tls",
                "--ca-file",
                "/lab/state/ca.crt",
            ],
        )
        self.assertEqual(add_arguments[-1], "kantrip-scram")
        self.assertIn("__KANTRIP_SITE_DEMO_0__\nkafka-topics --list\nprintf", lab.driver)
        outputs = [step["output"] for step in demo["steps"]]
        self.assertEqual(
            outputs[0],
            [
                {"text": "Kafka password: ", "wait": 1400},
                {"text": "Added profile 'prod' to /home/demo/.local/share/kantrip/profiles.db"},
            ],
        )
        self.assertEqual(
            outputs[1],
            [
                {
                    "text": "✅ Kafka transport: verified TLS; authentication: SCRAM-SHA-512",
                    "style": "success",
                }
            ],
        )
        self.assertEqual(
            outputs[2][:2],
            [
                {
                    "text": "% Reading configuration from file /run/user/1000/kantrip/"
                    "sessions/session-5f0c2a9d4b7e41c8a3d6e9f1b2c4a7d0/kcat.conf"
                },
                {
                    "text": "Metadata for all topics (from broker 0: sasl_ssl://kafka.example.com:9093/0):"
                },
            ],
        )
        self.assertIn({"text": "  broker 0 at kafka.example.com:9093 (controller)"}, outputs[2])
        self.assertEqual(outputs[2][-4]["text"], '  topic "orders" with 1 partitions:')
        self.assertEqual(outputs[3:], [[], [{"text": "orders"}, {"text": "payments"}], []])
        session_prompt = [
            {"text": "\U000f100f prod ", "style": "accent"},
            {"text": "❯ ", "style": "secondary"},
        ]
        self.assertEqual(
            [step["prompt"] for step in demo["steps"][3:]], [PROMPT] + [session_prompt] * 2
        )
        self.assertEqual(demo["steps"][1]["spinner"], "Checking")
        self.assertIn("KANTRIP_PROFILE", demo["capture"])
        self.assertEqual(check_demo_content(demo), [])
        self.assertEqual([command[5] for command in lab.commands[:2]], ["--create"] * 2)
        self.assertEqual(lab.commands[-1][1:], ["remove", "kantrip-site-demo", "--yes"])

    def test_failed_step_still_removes_the_profile(self) -> None:
        lab = FakeLab(self.target, fail="exec kantrip-site-demo")
        with self.assertRaisesRegex(CaptureError, "exited with 1"):
            self.capture(lab)
        self.assertEqual(lab.commands[-1][1:], ["remove", "kantrip-site-demo", "--yes"])

    def test_password_in_output_is_never_written(self) -> None:
        lab = FakeLab(self.target, leak=PASSWORD)
        with self.assertRaises(CaptureError) as raised:
            self.capture(lab)
        self.assertNotIn(PASSWORD, str(raised.exception))
        self.assertEqual(lab.commands[-1][1:], ["remove", "kantrip-site-demo", "--yes"])

    def test_first_step_must_add_the_profile(self) -> None:
        lab = FakeLab(self.target)
        demo = {"capture": "old", "steps": CAPTURE_STEPS[1:]}
        with self.assertRaisesRegex(CaptureError, "first demo step"):
            capture_demo(demo, DemoCapture(self.target, {}, lab.terminal, lab.command))
        self.assertEqual(lab.commands, [])

    def test_screen_styles_must_be_known_and_single(self) -> None:
        with self.assertRaisesRegex(CaptureError, "mixed styles"):
            screen_lines(f"{SUCCESS}ok{PRIMARY}no{RESET}\r\n", self.target)
        with self.assertRaisesRegex(CaptureError, "unknown terminal colors"):
            screen_lines("\x1b[31mred\x1b[0m\r\n", self.target)
        self.assertEqual(
            screen_lines("\x1b[90mquiet\x1b[0m\r\n", self.target),
            [{"text": "quiet", "style": "muted"}],
        )

    def test_topic_blocks_are_sorted_by_name(self) -> None:
        lines = [
            {"text": " 2 topics:"},
            {"text": '  topic "b" with 1 partitions:'},
            {"text": "    partition 0"},
            {"text": '  topic "a" with 1 partitions:'},
            {"text": "    partition 0"},
            {"text": "after"},
        ]
        self.assertEqual(
            [line["text"] for line in sort_topic_blocks(lines)],
            [
                " 2 topics:",
                '  topic "a" with 1 partitions:',
                "    partition 0",
                '  topic "b" with 1 partitions:',
                "    partition 0",
                "after",
            ],
        )

    def test_capture_environment_uses_a_private_zsh_prompt(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("scripts.website.shutil.which", return_value="/usr/bin/zsh"),
        ):
            environment = capture_environment(Path(directory))
            wrapper = Path(environment["SHELL"])
            self.assertEqual(wrapper.name, "zsh")
            wrapper_text = wrapper.read_text(encoding="utf-8")
            zshrc = (Path(directory) / ".zshrc").read_text(encoding="utf-8")
        self.assertIn("kantrip_prompt_info()", zshrc)
        self.assertIn("\U000f100f", zshrc)
        self.assertIn("exec /usr/bin/zsh -d", wrapper_text)
        self.assertEqual(environment["ZDOTDIR"], directory)
        self.assertEqual(environment["KANTRIP_DATABASE"], str(Path(directory) / "profiles.db"))
        self.assertNotIn("NO_COLOR", environment)


if __name__ == "__main__":
    unittest.main()
