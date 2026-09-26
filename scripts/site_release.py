"""Resolve the current Kantrip release from GitHub and show it on the site."""

from __future__ import annotations

import html
import json
import os
import re
import urllib.request
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

RELEASES_API = "https://api.github.com/repos/sauljabin/kantrip/releases"
RELEASES_URL = "https://github.com/sauljabin/kantrip/releases"
RELEASE_START = "<!-- release:start -->"
RELEASE_END = "<!-- release:end -->"
# The release workflow accepts only these tags; anything else is not a Kantrip release.
RELEASE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?$")
PRE_RELEASE_ORDER = {"a": 0, "b": 1, "rc": 2}
PAGE_SIZE = 100


class ReleaseError(ValueError):
    """The release block in the page is missing or invalid."""


@dataclass(frozen=True)
class Release:
    tag: str
    prerelease: bool

    @property
    def url(self) -> str:
        return f"{RELEASES_URL}/tag/{self.tag}"


def _version_key(tag: str) -> tuple[int, ...]:
    match = RELEASE_TAG.match(tag)
    assert match is not None
    major, minor, patch, phase, number = match.groups()
    # A final release sorts after its own a, b, and rc pre-releases.
    pre = (PRE_RELEASE_ORDER[phase], int(number)) if phase else (len(PRE_RELEASE_ORDER), 0)
    return (int(major), int(minor), int(patch), *pre)


def current_release(releases: Iterable[Mapping[str, Any]]) -> Release | None:
    """The newest stable release, or the newest pre-release while none is stable.

    Drafts and tags outside the release tag format are ignored; versions are
    compared by number, not by publication date.
    """
    published = [
        Release(release["tag_name"], bool(release.get("prerelease")))
        for release in releases
        if not release.get("draft") and RELEASE_TAG.match(str(release.get("tag_name", "")))
    ]
    stable = [release for release in published if not release.prerelease]
    candidates = stable or published
    if not candidates:
        return None
    return max(candidates, key=lambda release: _version_key(release.tag))


def fetch_releases(token: str | None = None) -> list[dict[str, Any]]:
    """Every release of the repository, newest first, from the GitHub REST API."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "kantrip-site",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    releases: list[dict[str, Any]] = []
    page = 1
    while True:
        request = urllib.request.Request(
            f"{RELEASES_API}?per_page={PAGE_SIZE}&page={page}", headers=headers
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            batch = json.load(response)
        if not isinstance(batch, list):
            raise ReleaseError(f"{RELEASES_API}: expected a list of releases")
        releases.extend(batch)
        if len(batch) < PAGE_SIZE:
            return releases
        page += 1


def render_release(release: Release | None) -> str:
    """The release line; without a release, it points to the Releases page."""
    if release is None:
        state, label, href, text = "unknown", "latest release", RELEASES_URL, "on GitHub"
    elif release.prerelease:
        state, label, href, text = "pre", "latest pre-release", release.url, release.tag
    else:
        state, label, href, text = "stable", "latest release", release.url, release.tag
    return (
        f'<p class="release release-{state}"><span class="release-label">{label}</span> '
        f'<a href="{html.escape(href)}">{html.escape(text)}</a></p>'
    )


def _split(text: str) -> tuple[str, str, str]:
    start = text.find(RELEASE_START)
    end = text.find(RELEASE_END)
    if start < 0 or end < start:
        raise ReleaseError("site/index.html: missing release markers")
    start += len(RELEASE_START)
    return text[:start], text[start:end], text[end:]


def render_release_index(release: Release | None, text: str) -> str:
    """Replace the release block between its markers."""
    head, _, tail = _split(text)
    return f"{head}{render_release(release)}{tail}"


def check_release(index_text: str) -> list[str]:
    """The release block must be the fallback or a rendered release tag."""
    try:
        _, block, _ = _split(index_text)
    except ReleaseError as error:
        return [str(error)]
    allowed = {render_release(None)}
    tag = re.search(r'href="[^"]*/releases/tag/([^"]+)"', block)
    if tag and RELEASE_TAG.match(tag.group(1)):
        allowed |= {render_release(Release(tag.group(1), pre)) for pre in (False, True)}
    if block in allowed:
        return []
    return ["site/index.html: render the release block with python -m scripts.website release"]


def github_token() -> str | None:
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or None
