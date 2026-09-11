import stat
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from kantrip.session import SessionError, run_profile_session


class TestProfileSession(unittest.TestCase):
    def setUp(self) -> None:
        self.profile = {
            "kafka": {
                "bootstrapServers": ["localhost:9092", "localhost:9093"],
                "transport": "plaintext",
                "auth": {"type": "none"},
                "properties": {
                    "common": {"client.id": "kantrip"},
                    "librdkafka": {"enable.idempotence": True},
                },
            }
        }

    def test_runs_command_with_generated_kcat_configuration(self) -> None:
        observed: dict[str, object] = {}

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            config_path = Path(environment["KCAT_CONFIG"])
            observed["arguments"] = arguments
            observed["environment"] = environment
            observed["contents"] = config_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
            observed["session_directory"] = config_path.parent
            return subprocess.CompletedProcess(arguments, 23)

        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session.subprocess.run", side_effect=inspect_run),
        ):
            result = run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

        self.assertEqual(23, result)
        self.assertEqual(["kcat", "-L"], observed["arguments"])
        environment = observed["environment"]
        assert isinstance(environment, dict)
        self.assertEqual("local", environment["KANTRIP_PROFILE"])
        self.assertEqual("localhost:9092,localhost:9093", environment["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual(
            "bootstrap.servers=localhost:9092,localhost:9093\n"
            "client.id=kantrip\n"
            "enable.idempotence=true\n"
            "security.protocol=PLAINTEXT\n",
            observed["contents"],
        )
        self.assertEqual(0o600, observed["mode"])
        session_directory = observed["session_directory"]
        assert isinstance(session_directory, Path)
        self.assertFalse(session_directory.exists())

    def test_opens_configured_shell_when_command_is_omitted(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value=sys.executable),
            patch(
                "kantrip.session.subprocess.run",
                return_value=subprocess.CompletedProcess([sys.executable], 0),
            ) as run,
        ):
            run_profile_session("local", self.profile, [], environment={"SHELL": sys.executable})

        self.assertEqual([sys.executable], run.call_args.args[0])

    def test_missing_command_is_an_actionable_error(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value=None),
            self.assertRaisesRegex(SessionError, "command 'kcat' was not found"),
        ):
            run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

    def test_kcat_cannot_override_generated_configuration(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            self.assertRaisesRegex(SessionError, "-F option cannot override"),
        ):
            run_profile_session("local", self.profile, ["kcat", "-F", "other.conf"], environment={})

    def test_authenticated_profile_is_rejected_before_launch(self) -> None:
        self.profile["kafka"]["auth"] = {"type": "plain"}
        with (
            patch("kantrip.session.shutil.which", return_value="/usr/bin/kcat"),
            patch("kantrip.session.subprocess.run") as run,
            self.assertRaisesRegex(SessionError, "only plaintext profiles"),
        ):
            run_profile_session("local", self.profile, ["kcat", "-L"], environment={})

        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
