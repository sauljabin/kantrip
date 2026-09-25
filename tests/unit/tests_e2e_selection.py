"""Path selection for local and hosted E2E runs."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.analyze import validate_local_actions
from scripts.tests import (
    _run_staged_wheel,
    changed_paths,
    event_requires_e2e,
    push_requires_e2e,
    requires_e2e,
    valid_e2e_result,
)


class TestE2ESelection(unittest.TestCase):
    def test_non_runtime_paths_skip_e2e(self) -> None:
        paths = (
            "AGENT.md",
            "DEVELOPMENT.md",
            "images/banner.svg",
            "site/index.html",
            "site/demo.json",
            ".github/ISSUE_TEMPLATE/bug_report.yml",
            ".github/PULL_REQUEST_TEMPLATE/pull_request.md",
            "LICENSE",
            "tests/unit/tests_ping.py",
        )
        self.assertFalse(requires_e2e(paths))
        self.assertFalse(requires_e2e(()))

    def test_runtime_and_unknown_paths_require_e2e(self) -> None:
        for path in (
            "kantrip/ping.py",
            "schemas/profile.schema.json",
            "pyproject.toml",
            "uv.lock",
            "sandbox/kind.yaml",
            "tests/e2e/test_end_to_end.py",
            "scripts/tests.py",
            ".pre-commit-config.yaml",
            ".github/workflows/main.yml",
            "unrecognized/new_file.txt",
            "images/runner.py",
            "site/tool.py",
            ".github/ISSUE_TEMPLATE/run.sh",
            "../README.md",
        ):
            with self.subTest(path=path):
                self.assertTrue(requires_e2e((path,)))

    def test_mixed_changes_and_rename_sides(self) -> None:
        diff = b"R100\0README.md\0kantrip/readme.py\0D\0tests/unit/old.py\0"
        self.assertEqual(
            changed_paths(diff), ("README.md", "kantrip/readme.py", "tests/unit/old.py")
        )
        self.assertTrue(requires_e2e(changed_paths(diff)))
        self.assertFalse(requires_e2e(changed_paths(b"R095\0AGENT.md\0README.md\0")))
        self.assertTrue(requires_e2e(changed_paths(b"D\0kantrip/old.py\0")))
        self.assertFalse(requires_e2e(changed_paths(b"D\0tests/unit/old.py\0")))

    def test_malformed_git_diff_fails_closed(self) -> None:
        for diff in (b"M\0README.md", b"R100\0README.md\0", b"Q\0README.md\0"):
            with self.subTest(diff=diff), self.assertRaises(ValueError):
                changed_paths(diff)

    @patch("scripts.tests.subprocess.check_output", return_value=b"M\0README.md\0")
    def test_push_uses_complete_before_after_range(self, check_output: MagicMock) -> None:
        self.assertFalse(push_requires_e2e("before", "after"))
        check_output.assert_called_once_with(
            ("git", "diff", "--name-status", "-z", "--find-renames", "before", "after"),
            stderr=subprocess.DEVNULL,
        )

    @patch("scripts.tests.subprocess.check_output")
    def test_push_missing_base_and_diff_error_require_e2e(self, check_output: MagicMock) -> None:
        self.assertTrue(push_requires_e2e("0" * 40, "after"))
        check_output.assert_not_called()
        check_output.side_effect = subprocess.CalledProcessError(128, "git diff")
        self.assertTrue(push_requires_e2e("missing", "after"))

    @patch("scripts.tests.push_requires_e2e", return_value=False)
    def test_event_gate_is_one_shot_and_main_uses_range(self, push: MagicMock) -> None:
        self.assertFalse(event_requires_e2e("pull_request", action="synchronize", label="run-e2e"))
        self.assertFalse(event_requires_e2e("pull_request", action="labeled", label="other"))
        self.assertTrue(event_requires_e2e("pull_request", action="labeled", label="run-e2e"))
        self.assertTrue(event_requires_e2e("pull_request", action="opened", labels=("run-e2e",)))
        self.assertFalse(event_requires_e2e("pull_request", action="opened", labels=("other",)))
        self.assertFalse(
            event_requires_e2e("pull_request", action="synchronize", labels=("run-e2e",))
        )
        self.assertTrue(event_requires_e2e("workflow_dispatch"))
        self.assertFalse(event_requires_e2e("push", before="old", after="new"))
        push.assert_called_once_with("old", "new")
        with self.assertRaises(ValueError):
            event_requires_e2e("unknown")

    def test_ci_result_requires_success_only_when_selected(self) -> None:
        self.assertTrue(valid_e2e_result("true", "success"))
        self.assertTrue(valid_e2e_result("false", "skipped"))
        for selected, result in (
            ("true", "failed"),
            ("true", "cancelled"),
            ("true", "skipped"),
            ("false", "success"),
            ("false", "failed"),
            ("", "skipped"),
        ):
            with self.subTest(selected=selected, result=result):
                self.assertFalse(valid_e2e_result(selected, result))

    @patch("scripts.tests.staged_requires_e2e", return_value=False)
    @patch("scripts.tests.subprocess.check_output")
    @patch.dict("scripts.tests.os.environ", {"KANTRIP_E2E_FORCE": ""})
    def test_local_docs_only_skips_before_build(
        self, check_output: MagicMock, _: MagicMock
    ) -> None:
        _run_staged_wheel()
        check_output.assert_not_called()

    def test_stdlib_bootstrap_without_project_site_packages(self) -> None:
        result = subprocess.run(
            (
                "python3",
                "-S",
                "scripts/tests.py",
                "--ci-event",
                "pull_request",
                "--action",
                "synchronize",
                "--label",
                "run-e2e",
            ),
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "false")

    def test_local_composite_metadata_and_shells(self) -> None:
        validate_local_actions()
        with tempfile.TemporaryDirectory() as temporary:
            action = Path(temporary) / "action.yml"
            action.write_text(
                "name: bad\ndescription: missing shell\nruns:\n"
                "  using: composite\n  steps:\n    - run: echo hello\n",
                encoding="utf-8",
            )
            with (
                patch("scripts.analyze.Path.glob", return_value=[action]),
                self.assertRaisesRegex(ValueError, "explicit bash shell"),
            ):
                validate_local_actions()

    def test_workflow_gate_and_exact_release_wheel_remain_visible(self) -> None:
        main = Path(".github/workflows/main.yml").read_text(encoding="utf-8")
        e2e = Path(".github/workflows/e2e.yml").read_text(encoding="utf-8")
        release = Path(".github/workflows/release.yml").read_text(encoding="utf-8")
        self.assertIn("python3 scripts/tests.py --ci-event", main)
        self.assertIn("python3 scripts/tests.py --verify-e2e-result", main)
        self.assertIn("needs.e2e-selection.outputs.run_e2e == 'true'", main)
        self.assertIn("if: inputs.candidate-artifact != ''", e2e)
        self.assertIn('test "${#wheels[@]}" -eq 1', e2e)
        self.assertIn("candidate-artifact: release-bundle", release)
        self.assertIn("if: always()", e2e)


if __name__ == "__main__":
    unittest.main()
