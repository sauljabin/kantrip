import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

from kantrip import APP_BANNER
from kantrip.console import ARCANA_COLORS
from scripts import normalize_svg
from scripts.banner import BANNER_SLOGAN, generate_banner, render_banner_svg


class TestScripts(unittest.TestCase):
    def test_ascii_banner_places_the_slogan_before_the_p_descender(self) -> None:
        lines = APP_BANNER.splitlines()

        self.assertEqual(6, len(lines))
        self.assertEqual("|_|\\_\\__,_|_| |_|\\__|_|  |_| .__/", lines[-2].strip())
        self.assertEqual("|_|", lines[-1].strip())
        self.assertEqual(" -* switch kafka profiles like |_| magic", BANNER_SLOGAN)
        self.assertEqual(lines[-1].index("|_|"), BANNER_SLOGAN.index("|_|"))

    def test_normalize_svg_adds_intrinsic_dimensions(self) -> None:
        svg = '<svg viewBox="0 0 100 50">\n</svg>  \n'

        normalized = normalize_svg(svg)
        root = ElementTree.fromstring(normalized)

        self.assertEqual("100", root.attrib["width"])
        self.assertEqual("50", root.attrib["height"])
        self.assertTrue(normalized.endswith("\n"))

    def test_banner_is_deterministic_and_uses_arcana_colors(self) -> None:
        first = render_banner_svg()
        second = render_banner_svg()

        self.assertEqual(first, second)
        self.assertIn(ARCANA_COLORS["primary"].lower(), first.lower())
        self.assertIn(ARCANA_COLORS["accent"].lower(), first.lower())
        root = ElementTree.fromstring(first)
        _, _, view_width, view_height = root.attrib["viewBox"].split()
        self.assertEqual(view_width, root.attrib["width"])
        self.assertEqual(view_height, root.attrib["height"])
        rendered_text = " ".join("".join(root.itertext()).split())
        self.assertIn(" ".join(BANNER_SLOGAN.split()), rendered_text)
        descender = next(element for element in root.iter() if element.text == "|_|")
        descender_class = descender.attrib["class"]
        primary_fill = f".{descender_class} {{ fill: {ARCANA_COLORS['primary']}"
        self.assertIn(primary_fill.lower(), first.lower())

    def test_generate_banner_writes_requested_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "banner.svg"

            result = generate_banner(path)

            self.assertEqual(path, result)
            self.assertTrue(path.read_text(encoding="utf-8").startswith("<svg"))


if __name__ == "__main__":
    unittest.main()
