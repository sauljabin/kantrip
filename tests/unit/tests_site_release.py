"""Selecting and rendering the release shown on the GitHub Pages site."""

from __future__ import annotations

import io
import json
import unittest
from typing import Any
from unittest.mock import patch

from scripts.site_release import (
    PAGE_SIZE,
    RELEASES_URL,
    Release,
    check_release,
    current_release,
    fetch_releases,
    render_release,
    render_release_index,
)
from scripts.website import INDEX_PATH


def release(tag: str, *, prerelease: bool = False, draft: bool = False) -> dict[str, Any]:
    return {"tag_name": tag, "prerelease": prerelease, "draft": draft}


def page(text: str) -> str:
    return f"<main>\n<!-- release:start -->{text}<!-- release:end -->\n</main>"


class CurrentReleaseTests(unittest.TestCase):
    def test_only_pre_releases_select_the_newest_pre_release(self) -> None:
        releases = [
            release("v0.1.0a1", prerelease=True),
            release("v0.1.0rc1", prerelease=True),
            release("v0.1.0a2", prerelease=True),
            release("v0.1.0b1", prerelease=True),
        ]
        self.assertEqual(current_release(releases), Release("v0.1.0rc1", prerelease=True))

    def test_a_mix_selects_the_newest_stable_release(self) -> None:
        releases = [
            release("v0.2.0a1", prerelease=True),
            release("v0.1.1"),
            release("v0.1.0"),
            release("v0.1.0a2", prerelease=True),
        ]
        self.assertEqual(current_release(releases), Release("v0.1.1", prerelease=False))

    def test_stable_release_wins_over_a_newer_pre_release(self) -> None:
        releases = [release("v0.2.0rc1", prerelease=True), release("v0.1.0")]
        self.assertEqual(current_release(releases), Release("v0.1.0", prerelease=False))

    def test_versions_are_compared_by_number_not_by_order(self) -> None:
        releases = [release("v0.9.0"), release("v0.10.0"), release("v0.10.0a1", prerelease=True)]
        self.assertEqual(current_release(releases), Release("v0.10.0", prerelease=False))

    def test_drafts_are_ignored(self) -> None:
        releases = [
            release("v0.1.0", draft=True),
            release("v0.1.0a3", prerelease=True, draft=True),
            release("v0.1.0a2", prerelease=True),
        ]
        self.assertEqual(current_release(releases), Release("v0.1.0a2", prerelease=True))

    def test_tags_outside_the_release_format_are_ignored(self) -> None:
        releases = [release("nightly"), release("v1.0"), release("v0.1.0a2", prerelease=True)]
        self.assertEqual(current_release(releases), Release("v0.1.0a2", prerelease=True))

    def test_no_releases_fall_back(self) -> None:
        self.assertIsNone(current_release([]))
        self.assertIsNone(current_release([release("v0.1.0", draft=True)]))


class RenderReleaseTests(unittest.TestCase):
    def test_links_the_release_page(self) -> None:
        html = render_release(Release("v0.1.0a2", prerelease=True))
        self.assertIn(f'href="{RELEASES_URL}/tag/v0.1.0a2">v0.1.0a2</a>', html)
        self.assertIn("latest pre-release", html)
        self.assertIn("release-pre", html)

    def test_stable_release_is_labelled_as_a_release(self) -> None:
        html = render_release(Release("v0.1.0", prerelease=False))
        self.assertIn(">latest release<", html)
        self.assertIn("release-stable", html)

    def test_fallback_links_the_releases_list(self) -> None:
        self.assertIn(f'href="{RELEASES_URL}">', render_release(None))

    def test_replaces_only_the_marked_block(self) -> None:
        text = page(render_release(None))
        rendered = render_release_index(Release("v0.1.0", prerelease=False), text)
        self.assertEqual(rendered, page(render_release(Release("v0.1.0", prerelease=False))))
        self.assertEqual(render_release_index(None, rendered), text)


class CheckReleaseTests(unittest.TestCase):
    def test_accepts_the_fallback_and_rendered_releases(self) -> None:
        for current in (
            None,
            Release("v0.1.0a2", prerelease=True),
            Release("v1.2.3", prerelease=False),
        ):
            with self.subTest(current=current):
                self.assertEqual(check_release(page(render_release(current))), [])

    def test_committed_page_is_valid(self) -> None:
        self.assertEqual(check_release(INDEX_PATH.read_text(encoding="utf-8")), [])

    def test_rejects_missing_markers(self) -> None:
        self.assertIn("missing release markers", check_release("<main></main>")[0])

    def test_rejects_hand_edited_blocks(self) -> None:
        stale = render_release(Release("v0.1.0a2", prerelease=True))
        for block in (
            stale.replace(">v0.1.0a2<", ">v9.9.9<"),
            stale.replace("v0.1.0a2", "latest"),
            render_release(None).replace("on GitHub", "soon"),
        ):
            with self.subTest(block=block):
                self.assertEqual(len(check_release(page(block))), 1)


class FetchReleasesTests(unittest.TestCase):
    def test_reads_every_page_with_the_token(self) -> None:
        pages = [
            [release(f"v0.0.{index}") for index in range(PAGE_SIZE)],
            [release("v0.1.0a1", prerelease=True)],
        ]
        requests = []

        def urlopen(request: Any, timeout: float) -> io.BytesIO:
            requests.append(request)
            return io.BytesIO(json.dumps(pages[len(requests) - 1]).encode())

        with patch("urllib.request.urlopen", urlopen):
            releases = fetch_releases("token-value")
        self.assertEqual(len(releases), PAGE_SIZE + 1)
        self.assertEqual(
            [request.full_url.rsplit("page=", 1)[1] for request in requests], ["1", "2"]
        )
        self.assertEqual(requests[0].get_header("Authorization"), "Bearer token-value")

    def test_unauthenticated_without_a_token(self) -> None:
        requests = []

        def urlopen(request: Any, timeout: float) -> io.BytesIO:
            requests.append(request)
            return io.BytesIO(b"[]")

        with patch("urllib.request.urlopen", urlopen):
            self.assertEqual(fetch_releases(None), [])
        self.assertIsNone(requests[0].get_header("Authorization"))


if __name__ == "__main__":
    unittest.main()
