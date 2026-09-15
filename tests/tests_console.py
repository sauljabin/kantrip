import io
import unittest
from unittest.mock import patch

from kantrip.console import (
    ARCANA_COLORS,
    ARCANA_THEME,
    colors_enabled,
    create_console,
    create_labels_text,
    create_profile_description,
    create_profile_table,
    create_status_text,
    create_structured_syntax,
    show_progress,
)


class TerminalBuffer(io.StringIO):
    def isatty(self) -> bool:
        return True


class TestConsole(unittest.TestCase):
    def test_arcana_theme_defines_every_semantic_color(self) -> None:
        for name, color in ARCANA_COLORS.items():
            with self.subTest(name=name):
                self.assertTrue(color.startswith("#"))
        self.assertIn("primary", ARCANA_THEME.styles)
        self.assertIn("error", ARCANA_THEME.styles)

    def test_terminal_color_can_be_disabled_explicitly(self) -> None:
        stream = TerminalBuffer()

        self.assertFalse(colors_enabled(stream, no_color=True, environment={}))

    def test_no_color_environment_disables_terminal_color(self) -> None:
        stream = TerminalBuffer()

        self.assertFalse(colors_enabled(stream, environment={"NO_COLOR": ""}))

    def test_dumb_terminal_disables_terminal_color(self) -> None:
        stream = TerminalBuffer()

        self.assertFalse(colors_enabled(stream, environment={"TERM": "dumb"}))

    def test_non_terminal_output_has_no_escape_sequences(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})

        console.print("failure", style="error")

        self.assertEqual("failure\n", stream.getvalue())

    def test_terminal_output_uses_arcana_styles(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})

        console.print("magic", style="primary")

        self.assertIn("\x1b[", stream.getvalue())

    def test_profile_table_uses_color_and_contains_profile_data(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})

        console.print(create_profile_table({"local": {"description": "Local development"}}))

        self.assertIn("\x1b[", stream.getvalue())
        self.assertIn("Profile", stream.getvalue())
        self.assertIn("local", stream.getvalue())
        self.assertIn("Local development", stream.getvalue())

    def test_profile_table_displays_labels_with_stable_presentation_colors(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})
        profile = {"labels": {"owner": "platform", "environment": "production"}}

        console.print(create_profile_table({"production": profile}))
        first = create_labels_text(profile["labels"])
        second = create_labels_text(profile["labels"])

        self.assertIn("environment=production", stream.getvalue())
        self.assertIn("owner=platform", stream.getvalue())
        self.assertEqual(first.spans, second.spans)

    def test_profile_description_uses_sections_and_safe_observation_fields(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})
        observation = {
            "name": "production",
            "id": "018f8f13-7c21-7cee-8000-000000000010",
            "revision": 2,
            "description": "Production cluster",
            "labels": {"environment": "production"},
            "kafka": {
                "bootstrapServers": ["kafka.example.com:9093"],
                "transport": "tls",
                "auth": {"type": "scram-sha-512"},
            },
            "registry": None,
        }

        console.print(create_profile_description(observation))

        output = stream.getvalue()
        for expected in (
            "Profile",
            "Revision",
            "Kafka",
            "kafka.example.com:9093",
            "Registry",
            "Not configured",
            "Labels",
            "environment=production",
        ):
            self.assertIn(expected, output)

    def test_profile_table_has_spacing_without_borders_or_lines(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})
        profiles = {
            "local": {},
            "test": {"description": "test"},
            "sandbox": {},
        }

        console.print(create_profile_table(profiles))

        output = stream.getvalue()
        self.assertRegex(output, r"local\s+-")
        self.assertRegex(output, r"sandbox\s+-")
        self.assertNotIn("─", output)
        self.assertNotIn("│", output)
        self.assertNotIn("╭", output)
        self.assertNotIn("╰", output)

        table = create_profile_table(profiles)
        self.assertFalse(table.expand)
        self.assertEqual("heading", table.header_style)
        self.assertEqual("secondary", table.columns[0].style)
        self.assertEqual(12, table.columns[0].min_width)
        self.assertEqual(12, table.columns[1].min_width)

    def test_profile_text_is_not_interpreted_as_rich_markup(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})
        profiles = {
            "[red]x[/red]": {
                "description": "plain",
                "labels": {"x": "[b]y[/b]"},
            }
        }

        console.print(create_profile_table(profiles))

        self.assertIn("[red]x[/red]", stream.getvalue())
        self.assertIn("x=[b]y[/b]", stream.getvalue())

    def test_structured_output_uses_color_on_a_terminal(self) -> None:
        for language, contents in (
            ("json", '{"name": "local"}\n'),
            ("yaml", "name: local\n"),
        ):
            with self.subTest(language=language):
                stream = TerminalBuffer()
                console = create_console(stream=stream, environment={})

                console.print(create_structured_syntax(contents, language), end="")

                self.assertIn("\x1b[", stream.getvalue())
                self.assertIn("local", stream.getvalue())

    def test_structured_output_is_plain_when_color_is_disabled(self) -> None:
        contents = '{"name": "local"}\n'
        stream = TerminalBuffer()
        console = create_console(stream=stream, no_color=True, environment={})

        console.print(create_structured_syntax(contents, "json"), end="")

        self.assertEqual(contents, stream.getvalue())

    def test_profile_table_does_not_display_registry_url_credentials_or_query(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})
        profiles = {
            "local": {
                "kafka": {"bootstrapServers": ["localhost:9092"]},
                "registry": {
                    "provider": "confluent",
                    "schema.registry.url": (
                        "http://user:secret@localhost:8081/path?token=classified"
                    ),
                },
            }
        }

        console.print(create_profile_table(profiles))

        output = stream.getvalue()
        self.assertIn("http://localhost:8081", output)
        self.assertIn("Confluent", output)
        self.assertIn("/path", output)
        self.assertNotIn("secret", output)
        self.assertNotIn("classified", output)

    def test_status_text_uses_text_markers_for_plain_output(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})

        for status in ("progress", "success", "cleanup", "warning", "error"):
            console.print(create_status_text(console, status, "example"))

        self.assertEqual(
            "[running] example\n"
            "[passed] example\n"
            "[cleanup] example\n"
            "[warning] example\n"
            "[failed] example\n",
            stream.getvalue(),
        )

    def test_status_text_uses_emoji_for_colored_terminal_output(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})

        for status in ("success", "warning", "error"):
            console.print(create_status_text(console, status, "example"))

        self.assertIn("✅ example", stream.getvalue())
        self.assertIn("⚠️ example", stream.getvalue())
        self.assertIn("❌ example", stream.getvalue())
        self.assertIn("\x1b[", stream.getvalue())

    def test_progress_is_a_stable_line_without_color(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})

        with show_progress(console, "checking connectivity"):
            pass

        self.assertEqual("[running] checking connectivity\n", stream.getvalue())

    def test_progress_uses_a_spinner_with_color(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})

        with (
            patch.object(console, "status", wraps=console.status) as status,
            show_progress(console, "checking connectivity"),
        ):
            pass

        status.assert_called_once_with(
            "checking connectivity", spinner="moon", spinner_style="primary"
        )


if __name__ == "__main__":
    unittest.main()
