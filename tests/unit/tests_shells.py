import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.adapters import ADAPTER_EXECUTABLES
from kantrip.shells import (
    ShellError,
    prepare_interactive_shell,
    resolve_interactive_shell,
)


class TestShellPlans(unittest.TestCase):
    def test_uses_configured_supported_shell(self) -> None:
        with patch("kantrip.shells.shutil.which", return_value="/opt/homebrew/bin/fish"):
            shell = resolve_interactive_shell({"SHELL": "/opt/homebrew/bin/fish"})

        self.assertEqual("/opt/homebrew/bin/fish", shell)

    def test_defaults_to_bash_when_shell_is_unset(self) -> None:
        with patch("kantrip.shells.shutil.which", return_value="/bin/bash") as which:
            shell = resolve_interactive_shell({"PATH": "/bin"})

        self.assertEqual("/bin/bash", shell)
        which.assert_called_once_with("bash", path="/bin")

    def test_rejects_unsupported_shell(self) -> None:
        with self.assertRaisesRegex(ShellError, "not supported; use Bash, Zsh, Fish"):
            resolve_interactive_shell({"SHELL": "/bin/sh"})

    def test_bash_loads_user_configuration_then_restores_every_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            startup = home / ".bashrc"
            startup.write_text("export CONTRACT_STARTUP=loaded\n", encoding="utf-8")
            session = root / "session"
            session.mkdir()
            plan = prepare_interactive_shell(
                "/bin/bash", session, session / "bin", {"HOME": str(home)}
            )
            startup_path = Path(plan.arguments[-1])
            contents = startup_path.read_text(encoding="utf-8")

            self.assertEqual(("/bin/bash", "--rcfile", str(startup_path)), plan.arguments)
            self.assertEqual({}, plan.environment_overrides)
            self.assertLess(contents.index(f"source {startup}"), contents.index("export PATH="))
            for executable in ADAPTER_EXECUTABLES:
                self.assertIn(f"unalias {executable}", contents)
                self.assertIn(f"unset -f {executable}", contents)
            self.assertTrue(contents.endswith("hash -r\n"))
            self.assertEqual(0o600, stat.S_IMODE(startup_path.stat().st_mode))

    def test_zsh_restores_zdotdir_and_normal_history_before_user_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            startup = home / ".zshrc"
            startup.write_text("export CONTRACT_STARTUP=loaded\n", encoding="utf-8")
            session = root / "session"
            session.mkdir()
            plan = prepare_interactive_shell(
                "/bin/zsh", session, session / "bin", {"HOME": str(home)}
            )
            generated = Path(plan.environment_overrides["ZDOTDIR"]) / ".zshrc"
            contents = generated.read_text(encoding="utf-8")

            self.assertEqual(("/bin/zsh",), plan.arguments)
            self.assertEqual("1", plan.environment_overrides["SHELL_SESSIONS_DISABLE"])
            self.assertLess(
                contents.index(f"HISTFILE={home / '.zsh_history'}"),
                contents.index(f"source {startup}"),
            )
            self.assertIn("unset ZDOTDIR", contents)
            self.assertIn("unset SHELL_SESSIONS_DISABLE", contents)
            self.assertNotIn("fc -R", contents)
            self.assertTrue(contents.endswith("rehash\n"))

    def test_fish_uses_init_command_without_replacing_user_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shim_directory = root / "session bin's"
            plan = prepare_interactive_shell(
                "/opt/homebrew/bin/fish",
                root,
                shim_directory,
                {
                    "HOME": str(root / "home"),
                    "XDG_CONFIG_HOME": str(root / "config"),
                    "XDG_DATA_HOME": str(root / "data"),
                },
            )

        self.assertEqual("/opt/homebrew/bin/fish", plan.arguments[0])
        self.assertEqual("--init-command", plan.arguments[1])
        self.assertEqual({}, plan.environment_overrides)
        command = plan.arguments[2]
        self.assertIn("functions --erase 'kcat'", command)
        self.assertIn("abbr --erase 'kcat'", command)
        self.assertIn("set -gx PATH '/", command)
        self.assertIn("session bin\\'s' $PATH", command)
        self.assertNotIn("HOME", command)
        self.assertNotIn("XDG_", command)

    def test_generated_session_files_are_not_written_to_home(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home"
            home.mkdir()
            session = root / "session"
            session.mkdir()
            prepare_interactive_shell("/bin/bash", session, session / "bin", {"HOME": str(home)})

            self.assertEqual([], list(home.iterdir()))
            self.assertTrue(os.path.commonpath((session, root)) == str(root))


if __name__ == "__main__":
    unittest.main()
