"""Verify the adapter contract in real shells with generated fake clients."""

import json
import os
import shlex
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from kantrip.adapters import ADAPTER_EXECUTABLES, KAFKA_EXECUTABLES
from kantrip.config import add_profile
from scripts.terminal import run_terminal

_FAKE_CLIENT = r"""#!{python}
import json
import os
import stat
import sys
from pathlib import Path

name = Path(sys.argv[0]).name
arguments = sys.argv[1:]
config_path = os.environ.get("KCAT_CONFIG") if name in {{"kcat", "kafkacat"}} else None
for option in ("--consumer.config", "--producer.config", "--command-config", "--config-file"):
    if option in arguments:
        config_path = arguments[arguments.index(option) + 1]
        break
config = Path(config_path) if config_path else None
session_directory = os.environ.get("KANTRIP_SESSION_DIR", "")
record = {{
    "argv": arguments,
    "config_exists": bool(config and config.is_file()),
    "config_mode": stat.S_IMODE(config.stat().st_mode) if config and config.is_file() else None,
    "config_in_session": bool(config and session_directory and config.is_relative_to(session_directory)),
    "name": name,
    "profile": os.environ.get("KANTRIP_PROFILE"),
    "session_directory": session_directory,
}}
if "--topic" in arguments and name.startswith("kafka-console-producer"):
    record["stdin"] = sys.stdin.read()
with open(os.environ["KANTRIP_CONTRACT_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True) + "\n")
if name.startswith("kafka-console-consumer"):
    print("contract record")
else:
    print("contract-topic")
"""


class VerifyInteractiveShellContract(unittest.TestCase):
    maxDiff = None

    def test_bash_contract(self) -> None:
        self._assert_shell_contract("bash")

    def test_zsh_contract(self) -> None:
        self._assert_shell_contract("zsh")

    def test_fish_contract(self) -> None:
        self._assert_shell_contract("fish")

    def _assert_shell_contract(self, shell_name: str) -> None:
        shell = shutil.which(shell_name)
        if shell is None:
            if shell_name in _required_shells():
                self.fail(f"required shell '{shell_name}' was not found on PATH")
            self.skipTest(f"{shell_name} is not installed")

        with tempfile.TemporaryDirectory(prefix=f"kantrip-{shell_name}-contract-") as directory:
            root = Path(directory)
            home = root / "home"
            fake_bin = root / "clients"
            home.mkdir()
            fake_bin.mkdir()
            log_path = root / f"{shell_name}.jsonl"
            config_path = root / "config.yaml"
            add_profile("contract", config_path, bootstrap_servers=("contract.invalid:9092",))
            _write_fake_clients(fake_bin)
            environment = _contract_environment(
                shell=shell,
                home=home,
                fake_bin=fake_bin,
                config_path=config_path,
                log_path=log_path,
            )
            _write_startup(shell_name, home, environment)

            first_status, first_output = _run_kantrip_shell(
                environment, ("echo CONTRACT_PRIOR", "exit"), ready_text="__KANTRIP_READY__"
            )
            self.assertEqual(0, first_status, first_output)

            commands = [
                _history_check(shell_name),
                "echo __STARTUP__$CONTRACT_STARTUP",
                "echo __PROFILE__$KANTRIP_PROFILE",
                f"{shlex.quote(sys.executable)} -m kantrip.cli current",
                *_adapter_commands(),
                "kcat -F other.conf >/dev/null 2>&1 || echo __KCAT_OVERRIDE_OK__",
                (
                    "kafka-topics --bootstrap-server other.invalid:9092 --list "
                    ">/dev/null 2>&1 || echo __KAFKA_OVERRIDE_OK__"
                ),
                (
                    "kaskade admin --config-file other.ini >/dev/null 2>&1 "
                    "|| echo __KASKADE_OVERRIDE_OK__"
                ),
                f"{shlex.quote(sys.executable)} -m kantrip.cli exec contract",
                "echo CONTRACT_NEW",
                "exit",
            ]
            status, output = _run_kantrip_shell(
                environment, commands, ready_text="__KANTRIP_READY__"
            )
            self.assertEqual(0, status, output)
            self.assertIn("__HISTORY_OK__", output)
            self.assertIn("__STARTUP__loaded", output)
            self.assertIn("__PROFILE__contract", output)
            self.assertIn("__KCAT_OVERRIDE_OK__", output)
            self.assertIn("__KAFKA_OVERRIDE_OK__", output)
            self.assertIn("__KASKADE_OVERRIDE_OK__", output)
            self.assertIn("contract\r\n", output)
            self.assertIn("a Kantrip session is already active", output)
            self.assertNotIn("__BYPASS__", output)

            records: list[dict[str, Any]] = [
                json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(ADAPTER_EXECUTABLES, {record["name"] for record in records})
            self.assertEqual(18, len(records))
            for record in records:
                self.assertEqual("contract", record["profile"])
                self.assertTrue(record["config_exists"], record)
                self.assertEqual(0o600, record["config_mode"])
                self.assertTrue(record["config_in_session"], record)
                self.assertFalse(Path(record["session_directory"]).exists())
            producers = [
                record for record in records if record["name"].startswith("kafka-console-producer")
            ]
            self.assertEqual({"contract record\n"}, {record["stdin"] for record in producers})
            self.assertTrue(_history_path(shell_name, home, environment).is_file())
            self.assertIn(
                "CONTRACT_NEW",
                _history_path(shell_name, home, environment).read_text(encoding="utf-8"),
            )


def _required_shells() -> frozenset[str]:
    return frozenset(filter(None, os.environ.get("KANTRIP_REQUIRED_SHELLS", "").split(",")))


def _write_fake_clients(directory: Path) -> None:
    contents = _FAKE_CLIENT.format(python=sys.executable)
    for executable in ADAPTER_EXECUTABLES:
        path = directory / executable
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o700)


def _contract_environment(
    *,
    shell: str,
    home: Path,
    fake_bin: Path,
    config_path: Path,
    log_path: Path,
) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "HOME": str(home),
            "KANTRIP_CONFIG": str(config_path),
            "KANTRIP_CONTRACT_LOG": str(log_path),
            "PATH": f"{fake_bin}{os.pathsep}{environment.get('PATH', os.defpath)}",
            "SHELL": shell,
            "TERM": "xterm-256color",
            "XDG_CONFIG_HOME": str(home / ".config"),
            "XDG_DATA_HOME": str(home / ".local" / "share"),
            "ZDOTDIR": str(home),
        }
    )
    environment.pop("KANTRIP_PROFILE", None)
    environment.pop("KANTRIP_SESSION_ID", None)
    return environment


def _write_startup(shell_name: str, home: Path, environment: dict[str, str]) -> None:
    if shell_name == "bash":
        (home / ".bashrc").write_text(
            "export CONTRACT_STARTUP=loaded\n"
            "export PATH=/usr/bin:/bin\n"
            "export HISTFILE=$HOME/.bash_history\n"
            "export HISTCONTROL=\n"
            "shopt -s histappend\n"
            "PS1='__KANTRIP_READY__ '\n"
            "alias kcat='echo __BYPASS__'\n"
            "kafka-topics() { echo __BYPASS__; }\n",
            encoding="utf-8",
        )
    elif shell_name == "zsh":
        (home / ".zshrc").write_text(
            "export CONTRACT_STARTUP=loaded\n"
            "export PATH=/usr/bin:/bin\n"
            "HISTFILE=$HOME/.zsh_history\n"
            "HISTSIZE=100\n"
            "SAVEHIST=100\n"
            "setopt append_history\n"
            "fc -R $HISTFILE 2>/dev/null || true\n"
            "PROMPT='__KANTRIP_READY__ '\n"
            "alias kcat='echo __BYPASS__'\n"
            "function kafka-topics { echo __BYPASS__ }\n",
            encoding="utf-8",
        )
    else:
        config_directory = Path(environment["XDG_CONFIG_HOME"]) / "fish"
        config_directory.mkdir(parents=True)
        (config_directory / "config.fish").write_text(
            "set -gx CONTRACT_STARTUP loaded\n"
            "set -gx PATH /usr/bin /bin\n"
            "abbr --add kcat 'echo __BYPASS__'\n"
            "function kafka-topics; echo __BYPASS__; end\n"
            "function fish_prompt; echo -n '__KANTRIP_READY__ '; end\n",
            encoding="utf-8",
        )


def _run_kantrip_shell(
    environment: dict[str, str],
    commands: tuple[str, ...] | list[str],
    *,
    ready_text: str,
) -> tuple[int, str]:
    return run_terminal(
        (sys.executable, "-m", "kantrip.cli", "exec", "contract"),
        commands,
        environment=environment,
        ready_text=ready_text,
    )


def _history_check(shell_name: str) -> str:
    if shell_name == "fish":
        return "history search --exact CONTRACT_PRIOR >/dev/null; and echo __HISTORY_OK__"
    return "history | grep -F CONTRACT_PRIOR >/dev/null && echo __HISTORY_OK__"


def _adapter_commands() -> list[str]:
    commands: list[str] = []
    for executable in sorted(KAFKA_EXECUTABLES):
        if executable.startswith("kafka-console-producer"):
            commands.append(f"printf 'contract record\\n' | {executable} --topic contract")
        else:
            commands.append(f"{executable} --contract")
    commands.extend(("kcat -L", "kafkacat -L", "kaskade admin", "kaskade consumer"))
    return commands


def _history_path(shell_name: str, home: Path, environment: dict[str, str]) -> Path:
    if shell_name == "fish":
        return Path(environment["XDG_DATA_HOME"]) / "fish" / "fish_history"
    return home / f".{shell_name}_history"


if __name__ == "__main__":
    unittest.main()
