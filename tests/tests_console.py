import io
import unittest
from unittest.mock import patch

from kantrip.console import (
    ARCANA_COLORS,
    ARCANA_THEME,
    colors_enabled,
    create_console,
    create_profile_table,
    create_status_text,
    create_yaml_syntax,
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

    def test_profile_table_does_not_display_registry_url_credentials_or_query(self) -> None:
        stream = io.StringIO()
        console = create_console(stream=stream, environment={})
        profiles = {
            "local": {
                "kafka": {"bootstrapServers": ["localhost:9092"]},
                "schemaRegistry": {
                    "url": "http://user:secret@localhost:8081/path?token=classified"
                },
            }
        }

        console.print(create_profile_table(profiles))

        output = stream.getvalue()
        self.assertIn("http://localhost:8081", output)
        self.assertIn("/path", output)
        self.assertNotIn("secret", output)
        self.assertNotIn("classified", output)

    def test_yaml_syntax_uses_color(self) -> None:
        stream = TerminalBuffer()
        console = create_console(stream=stream, environment={})

        console.print(create_yaml_syntax("transport: plaintext\n"), end="")

        self.assertIn("\x1b[", stream.getvalue())
        self.assertIn("transport", stream.getvalue())

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
            "checking connectivity", spinner="dots", spinner_style="primary"
        )


if __name__ == "__main__":
    unittest.main()
