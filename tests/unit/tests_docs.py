import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUIDES_WITH_CONTENTS = (
    "USAGE.md",
    "COMPATIBILITY.md",
    "ARCHITECTURE.md",
    "DEVELOPMENT.md",
    "THREAT_MODEL.md",
)
_HEADING = re.compile(r"^(#{2,3}) (.+)$", re.MULTILINE)
_CONTENTS_ENTRY = re.compile(r"^( *)- \[(.+)\]\(#([^)]+)\)$", re.MULTILINE)
_DIAGRAM = re.compile(r"!\[[^\]]*\]\((images/[^)]+\.svg)\)")
_STYLE = re.compile(r"  <style>.*?</defs>\n", re.DOTALL)


class TestGuideContents(unittest.TestCase):
    def test_contents_lists_every_section_heading_in_order(self) -> None:
        for guide in GUIDES_WITH_CONTENTS:
            with self.subTest(guide=guide):
                text = (ROOT / guide).read_text(encoding="utf-8")
                body = _without_code_blocks(text.split("## Contents", 1)[1])
                listed, rest = body.split("\n## ", 1)
                headings = [
                    (len(level) - 2, _anchor(title))
                    for level, title in _HEADING.findall("## " + rest)
                ]
                entries = [
                    (len(indent) // 2, anchor)
                    for indent, _, anchor in _CONTENTS_ENTRY.findall(listed)
                ]
                self.assertEqual(headings, entries)


class TestArchitectureDiagrams(unittest.TestCase):
    def test_every_diagram_shares_one_style_block(self) -> None:
        architecture = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
        diagrams = _DIAGRAM.findall(architecture)
        styles = {}
        for diagram in diagrams:
            svg = (ROOT / diagram).read_text(encoding="utf-8")
            match = _STYLE.search(svg)
            self.assertIsNotNone(match, diagram)
            assert match is not None
            styles[diagram] = match.group(0)
            self.assertIn('<text class="title"', svg, diagram)
        self.assertGreater(len(diagrams), 1)
        self.assertEqual(1, len(set(styles.values())), sorted(styles))


def _anchor(title: str) -> str:
    """Return GitHub's anchor for one heading."""
    return re.sub(r"[^a-z0-9 _-]", "", title.lower()).replace(" ", "-")


def _without_code_blocks(text: str) -> str:
    return re.sub(r"^```.*?^```", "", text, flags=re.MULTILINE | re.DOTALL)


if __name__ == "__main__":
    unittest.main()
