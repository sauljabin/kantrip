import io
import unittest

from kantrip.console import ARCANA_COLORS, ARCANA_THEME, colors_enabled, create_console


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


if __name__ == "__main__":
    unittest.main()
