import stat
import subprocess
import sys
import tempfile
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
                    "java": {"request.timeout.ms": 30000},
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

    def test_rejects_a_nested_kantrip_session_before_launch(self) -> None:
        with (
            patch("kantrip.session.shutil.which") as which,
            patch("kantrip.session.subprocess.run") as run,
            self.assertRaisesRegex(SessionError, "session is already active"),
        ):
            run_profile_session(
                "local",
                self.profile,
                [],
                environment={
                    "KANTRIP_PROFILE": "development",
                    "KANTRIP_SESSION_ID": "existing-session",
                    "SHELL": sys.executable,
                },
            )

        which.assert_not_called()
        run.assert_not_called()

    def test_adapts_both_kafka_topics_executable_names(self) -> None:
        for executable in ("kafka-topics", "kafka-topics.sh"):
            with self.subTest(executable=executable):
                observed: dict[str, object] = {}

                def inspect_run(
                    arguments: list[str],
                    *,
                    _observed: dict[str, object] = observed,
                    **options: object,
                ) -> subprocess.CompletedProcess:
                    environment = options["env"]
                    assert isinstance(environment, dict)
                    config_path = Path(environment["KAFKA_JAVA_CONFIG_FILE"])
                    _observed["arguments"] = arguments
                    _observed["contents"] = config_path.read_text(encoding="utf-8")
                    _observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
                    return subprocess.CompletedProcess(arguments, 0)

                with (
                    patch("kantrip.session.shutil.which", return_value=f"/opt/kafka/{executable}"),
                    patch("kantrip.session.subprocess.run", side_effect=inspect_run),
                ):
                    run_profile_session(
                        "local", self.profile, [executable, "--list"], environment={}
                    )

                arguments = observed["arguments"]
                assert isinstance(arguments, list)
                self.assertEqual(
                    [
                        executable,
                        "--bootstrap-server",
                        "localhost:9092,localhost:9093",
                        "--command-config",
                    ],
                    arguments[:4],
                )
                self.assertEqual("kafka.properties", Path(arguments[4]).name)
                self.assertEqual(["--list"], arguments[5:])
                self.assertEqual(
                    "bootstrap.servers=localhost:9092,localhost:9093\n"
                    "client.id=kantrip\n"
                    "request.timeout.ms=30000\n"
                    "security.protocol=PLAINTEXT\n",
                    observed["contents"],
                )
                self.assertEqual(0o600, observed["mode"])

    def test_kafka_topics_cannot_override_profile_connection_options(self) -> None:
        for option in (
            "--bootstrap-server",
            "--bootstrap-server=other:9092",
            "--command-config",
            "--command-config=other.properties",
        ):
            with (
                self.subTest(option=option),
                patch("kantrip.session.shutil.which", return_value="/opt/kafka/kafka-topics"),
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session("local", self.profile, ["kafka-topics", option], environment={})

    def test_adapts_kaskade_admin_and_consumer_with_a_private_ini_file(self) -> None:
        for command, command_arguments in (
            ("admin", []),
            ("consumer", ["--topic", "orders"]),
        ):
            with self.subTest(command=command):
                observed: dict[str, object] = {}

                def inspect_run(
                    arguments: list[str],
                    *,
                    _observed: dict[str, object] = observed,
                    **options: object,
                ) -> subprocess.CompletedProcess:
                    environment = options["env"]
                    assert isinstance(environment, dict)
                    config_path = Path(arguments[3])
                    _observed["arguments"] = arguments
                    _observed["contents"] = config_path.read_text(encoding="utf-8")
                    _observed["mode"] = stat.S_IMODE(config_path.stat().st_mode)
                    _observed["environment"] = environment
                    return subprocess.CompletedProcess(arguments, 0)

                with (
                    patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
                    patch("kantrip.session.subprocess.run", side_effect=inspect_run),
                ):
                    run_profile_session(
                        "local",
                        self.profile,
                        ["kaskade", command, *command_arguments],
                        environment={},
                    )

                arguments = observed["arguments"]
                assert isinstance(arguments, list)
                self.assertEqual(["kaskade", command, "--config-file"], arguments[:3])
                self.assertEqual("kaskade.ini", Path(arguments[3]).name)
                self.assertEqual(command_arguments, arguments[4:])
                self.assertEqual(
                    "[kafka]\n"
                    "bootstrap.servers=localhost:9092,localhost:9093\n"
                    "client.id=kantrip\n"
                    "enable.idempotence=true\n"
                    "security.protocol=PLAINTEXT\n",
                    observed["contents"],
                )
                self.assertEqual(0o600, observed["mode"])
                environment = observed["environment"]
                assert isinstance(environment, dict)
                self.assertNotIn("KASKADE_CLIENT_CONFIG", environment)

    def test_kaskade_cannot_override_profile_connection_options(self) -> None:
        for option in (
            "-bother:9092",
            "--bootstrap-servers=other:9092",
            "--config-file=other.ini",
            "--kafka=bootstrap.servers=other:9092",
        ):
            with (
                self.subTest(option=option),
                patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
                self.assertRaisesRegex(SessionError, "cannot override"),
            ):
                run_profile_session(
                    "local", self.profile, ["kaskade", "admin", option], environment={}
                )

    def test_kaskade_root_options_are_not_adapted(self) -> None:
        with (
            patch("kantrip.session.shutil.which", return_value="/opt/bin/kaskade"),
            patch(
                "kantrip.session.subprocess.run",
                return_value=subprocess.CompletedProcess(["kaskade", "--help"], 0),
            ) as run,
        ):
            run_profile_session("local", self.profile, ["kaskade", "--help"], environment={})

        self.assertEqual(["kaskade", "--help"], run.call_args.args[0])

    def test_interactive_shell_contains_kafka_topics_shims(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            if executable in {"kafka-topics", "kafka-topics.sh"}:
                return f"/opt/kafka/bin/{executable}"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim_directory = Path(environment["PATH"].split(":", 1)[0])
            observed["directory"] = shim_directory
            observed["shims"] = {
                path.name: path.read_text(encoding="utf-8") for path in shim_directory.iterdir()
            }
            observed["modes"] = {
                path.name: stat.S_IMODE(path.stat().st_mode) for path in shim_directory.iterdir()
            }
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", side_effect=find_executable),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session.subprocess.run", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", self.profile, [], environment={"SHELL": sys.executable, "PATH": "/bin"}
            )

        shims = observed["shims"]
        assert isinstance(shims, dict)
        self.assertEqual({"kafka-topics", "kafka-topics.sh"}, set(shims))
        for name, contents in shims.items():
            self.assertIn(f"exec /opt/kafka/bin/{name}", contents)
            self.assertIn("--bootstrap-server localhost:9092,localhost:9093", contents)
            self.assertIn("--command-config", contents)
        self.assertEqual({"kafka-topics": 0o700, "kafka-topics.sh": 0o700}, observed["modes"])
        directory = observed["directory"]
        assert isinstance(directory, Path)
        self.assertFalse(directory.exists())

    def test_interactive_shell_contains_a_kaskade_shim(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == sys.executable:
                return sys.executable
            if executable == "kaskade":
                return "/opt/bin/kaskade"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            shim_path = Path(environment["PATH"].split(":", 1)[0]) / "kaskade"
            observed["directory"] = shim_path.parent
            observed["contents"] = shim_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(shim_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch("kantrip.session.shutil.which", side_effect=find_executable),
            patch("kantrip.adapters.shutil.which", side_effect=find_executable),
            patch("kantrip.session.subprocess.run", side_effect=inspect_run),
        ):
            run_profile_session(
                "local", self.profile, [], environment={"SHELL": sys.executable, "PATH": "/bin"}
            )

        self.assertIn("exec /opt/bin/kaskade", observed["contents"])
        self.assertIn("--config-file", observed["contents"])
        self.assertEqual(0o700, observed["mode"])
        directory = observed["directory"]
        assert isinstance(directory, Path)
        self.assertFalse(directory.exists())

    def test_zsh_restores_shims_after_loading_user_configuration(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == "/bin/zsh":
                return executable
            if executable == "kafka-topics":
                return "/opt/kafka/bin/kafka-topics"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            environment = options["env"]
            assert isinstance(environment, dict)
            startup_path = Path(environment["ZDOTDIR"]) / ".zshrc"
            observed["arguments"] = arguments
            observed["environment"] = environment
            observed["contents"] = startup_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(startup_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with tempfile.TemporaryDirectory() as home:
            user_startup = Path(home) / ".zshrc"
            user_startup.write_text('export PATH="/opt/homebrew/bin:$PATH"\n', encoding="utf-8")
            with (
                patch("kantrip.session.shutil.which", side_effect=find_executable),
                patch("kantrip.adapters.shutil.which", side_effect=find_executable),
                patch("kantrip.session.subprocess.run", side_effect=inspect_run),
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    [],
                    environment={"SHELL": "/bin/zsh", "PATH": "/bin", "HOME": home},
                )

        self.assertEqual(["/bin/zsh"], observed["arguments"])
        contents = observed["contents"]
        assert isinstance(contents, str)
        source_position = contents.index(f"source {user_startup}")
        path_position = contents.index("export PATH=")
        self.assertLess(source_position, path_position)
        self.assertIn("/bin", contents)
        self.assertTrue(contents.endswith("rehash\n"))
        self.assertEqual(0o600, observed["mode"])

    def test_bash_uses_a_session_rcfile_that_restores_shims_last(self) -> None:
        observed: dict[str, object] = {}

        def find_executable(executable: str, **options: object) -> str | None:
            if executable == "/bin/bash":
                return executable
            if executable == "kaskade":
                return "/opt/bin/kaskade"
            return None

        def inspect_run(arguments: list[str], **options: object) -> subprocess.CompletedProcess:
            observed["arguments"] = arguments
            startup_path = Path(arguments[-1])
            observed["contents"] = startup_path.read_text(encoding="utf-8")
            observed["mode"] = stat.S_IMODE(startup_path.stat().st_mode)
            return subprocess.CompletedProcess(arguments, 0)

        with tempfile.TemporaryDirectory() as home:
            user_startup = Path(home) / ".bashrc"
            user_startup.write_text('export PATH="/opt/homebrew/bin:$PATH"\n', encoding="utf-8")
            with (
                patch("kantrip.session.shutil.which", side_effect=find_executable),
                patch("kantrip.adapters.shutil.which", side_effect=find_executable),
                patch("kantrip.session.subprocess.run", side_effect=inspect_run),
            ):
                run_profile_session(
                    "local",
                    self.profile,
                    [],
                    environment={"SHELL": "/bin/bash", "PATH": "/bin", "HOME": home},
                )

        arguments = observed["arguments"]
        assert isinstance(arguments, list)
        self.assertEqual(["/bin/bash", "--rcfile"], arguments[:2])
        contents = observed["contents"]
        assert isinstance(contents, str)
        source_position = contents.index(f"source {user_startup}")
        path_position = contents.index("export PATH=")
        self.assertLess(source_position, path_position)
        self.assertTrue(contents.endswith("hash -r\n"))
        self.assertEqual(0o600, observed["mode"])

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
