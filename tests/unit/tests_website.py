"""GitHub Pages site rendering and validation."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from kantrip.cli import cli
from scripts.website import (
    SiteError,
    check_demo_commands,
    check_demo_content,
    check_pages,
    check_site,
    check_transcript,
    help_options,
    kantrip_invocations,
    load_demo,
    render_index,
    render_transcript,
)

DEMO: dict[str, Any] = {
    "capture": "synthetic",
    "steps": [
        {
            "prompt": "$ ",
            "command": "kantrip add prod -b kafka.example.com:9093 --transport=tls --auth plain",
            "output": [{"text": "Kafka password: ", "wait": 900}, {"text": "Added <prod>"}],
        },
        {
            "prompt": "$ ",
            "command": "kantrip ping prod",
            "spinner": "Checking profile 'prod'",
            "output": [{"text": "ok", "style": "success"}],
        },
        {"prompt": "$ ", "command": "kantrip exec prod -- kcat -L -b other", "output": []},
        {"prompt": "$ ", "command": "kafka-topics --list --bogus", "output": []},
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
        demo["steps"][0]["command"] += " --bootstrap-server kafka.example.com:9093"
        self.assertEqual(
            check_demo_commands(demo, cli_help),
            ["site/demo.json: 'kantrip add' has no option --bootstrap-server"],
        )

    def test_unknown_command_fails(self) -> None:
        demo = copy.deepcopy(DEMO)
        demo["steps"][1]["command"] = "kantrip import prod"
        self.assertEqual(
            check_demo_commands(demo, cli_help),
            ["site/demo.json: 'kantrip import' is not a kantrip command"],
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
        self.assertEqual(html.count("\n"), 6)

    def test_stale_transcript_is_reported(self) -> None:
        page = "<pre><!-- demo-transcript:start -->old<!-- demo-transcript:end --></pre>"
        self.assertEqual(len(check_transcript(DEMO, page)), 1)
        self.assertEqual(check_transcript(DEMO, render_index(DEMO, page)), [])

    def test_invalid_demo_data_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "demo.json"
            path.write_text(
                '{"capture": "x", "steps": [{"prompt": "$ ", "command": "kantrip list",'
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

    def test_repository_site_passes(self) -> None:
        self.assertEqual(check_site(cli_help), [])


if __name__ == "__main__":
    unittest.main()
