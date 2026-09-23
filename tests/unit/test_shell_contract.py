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

from kantrip.adapters import ADAPTER_EXECUTABLES, KAFKA_EXECUTABLES, KCAT_EXECUTABLES
from kantrip.profiles import add_profile
from scripts import run_terminal

_FAKE_CLIENT = r"""#!{python}
import json
import os
import stat
import sys
from pathlib import Path

name = Path(sys.argv[0]).name
arguments = sys.argv[1:]
config_path = os.environ.get("KCAT_CONFIG") if name in {kcat_executables} else None
for option in ("--consumer.config", "--producer.config", "--command-config", "--config-file"):
    if option in arguments:
        config_path = arguments[arguments.index(option) + 1]
        break
config = Path(config_path) if config_path else None
session_directory = os.environ.get("KANTRIP_SESSION_DIR", "")
record = {{
    "argv": arguments,
    "config_exists": bool(config and config.is_file()),
    "config_contents": config.read_text(encoding="utf-8") if config and config.is_file() else None,
    "config_mode": stat.S_IMODE(config.stat().st_mode) if config and config.is_file() else None,
    "config_in_session": bool(config and session_directory and config.is_relative_to(session_directory)),
    "name": name,
    "profile": os.environ.get("KANTRIP_PROFILE"),
    "session_directory": session_directory,
}}
registry_config_path = os.environ.get("SCHEMA_REGISTRY_CONFIG_FILE")
registry_config = Path(registry_config_path) if registry_config_path else None
record["registry_config_exists"] = bool(registry_config and registry_config.is_file())
record["registry_config_mode"] = (
    stat.S_IMODE(registry_config.stat().st_mode)
    if registry_config and registry_config.is_file()
    else None
)
record["registry_url"] = os.environ.get("SCHEMA_REGISTRY_URL")
if "--topic" in arguments and name.endswith("console-producer"):
    record["stdin"] = sys.stdin.read()
with open(os.environ["KANTRIP_CONTRACT_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, sort_keys=True) + "\n")
if name.endswith("console-consumer"):
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
            database_path = root / "profiles.db"
            add_profile(
                "contract",
                database_path,
                bootstrap_servers=("contract.invalid:9092",),
                registry_url="http://registry.invalid:8081",
            )
            _write_fake_clients(fake_bin)
            isolated_shell = _isolate_shell(shell_name, shell, root)
            environment = _contract_environment(
                shell=isolated_shell,
                home=home,
                fake_bin=fake_bin,
                database_path=database_path,
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
                "kcat -b other.invalid:9092 >/dev/null 2>&1 || echo __KCAT_BOOTSTRAP_BLOCKED__",
                "kcat -X security.protocol=PLAINTEXT >/dev/null 2>&1 || echo __KCAT_AUTH_BLOCKED__",
                (
                    "kafka-topics --bootstrap-server other.invalid:9092 --list "
                    ">/dev/null 2>&1 || echo __KAFKA_OVERRIDE_OK__"
                ),
                (
                    "kafka-console-consumer --consumer-property "
                    "bootstrap.servers=other.invalid:9092 >/dev/null 2>&1 "
                    "|| echo __JAVA_PROPERTY_BLOCKED__"
                ),
                (
                    "kafka-console-consumer --command-config other.properties "
                    ">/dev/null 2>&1 || echo __JAVA_CONFIG_BLOCKED__"
                ),
                (
                    "kaskade admin --config-file other.ini >/dev/null 2>&1 "
                    "|| echo __KASKADE_OVERRIDE_OK__"
                ),
                (
                    "kafka-avro-console-producer --property "
                    "schema.registry.url=http://other.invalid:8081 >/dev/null 2>&1 "
                    "|| echo __SCHEMA_OVERRIDE_OK__"
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
            self.assertIn("__KCAT_BOOTSTRAP_BLOCKED__", output)
            self.assertIn("__KCAT_AUTH_BLOCKED__", output)
            self.assertIn("__KAFKA_OVERRIDE_OK__", output)
            self.assertIn("__JAVA_PROPERTY_BLOCKED__", output)
            self.assertIn("__JAVA_CONFIG_BLOCKED__", output)
            self.assertIn("__KASKADE_OVERRIDE_OK__", output)
            self.assertIn("__SCHEMA_OVERRIDE_OK__", output)
            self.assertIn("contract\r\n", output)
            self.assertIn("a Kantrip session is already active", output)
            self.assertNotIn("__BYPASS__", output)

            records: list[dict[str, Any]] = [
                json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(ADAPTER_EXECUTABLES, {record["name"] for record in records})
            self.assertEqual(len(ADAPTER_EXECUTABLES) + 4, len(records))
            for record in records:
                self.assertEqual("contract", record["profile"])
                self.assertTrue(record["config_exists"], record)
                self.assertEqual(0o600, record["config_mode"])
                self.assertTrue(record["config_in_session"], record)
                self.assertFalse(Path(record["session_directory"]).exists())
                if record["name"] in {
                    "kafka-avro-console-consumer",
                    "kafka-avro-console-producer",
                    "kafka-json-schema-console-consumer",
                    "kafka-json-schema-console-producer",
                    "kafka-protobuf-console-consumer",
                    "kafka-protobuf-console-producer",
                }:
                    self.assertTrue(record["registry_config_exists"], record)
                    self.assertEqual(0o600, record["registry_config_mode"])
                    self.assertEqual("http://registry.invalid:8081", record["registry_url"])
                    self.assertIn(
                        "schema.registry.url=http://registry.invalid:8081\n",
                        record["config_contents"],
                    )
                if record["name"] in KCAT_EXECUTABLES and "-s" in record["argv"]:
                    self.assertEqual(["-r", "http://registry.invalid:8081"], record["argv"][:2])
                if record["name"] == "kaskade" and "registry" in record["argv"]:
                    self.assertIn(
                        "\n[registry]\nprovider=confluent\nurl=http://registry.invalid:8081\n",
                        record["config_contents"],
                    )
            producers = [
                record for record in records if record["name"].endswith("console-producer")
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
    contents = _FAKE_CLIENT.format(
        python=sys.executable,
        kcat_executables=repr(set(KCAT_EXECUTABLES)),
    )
    for executable in ADAPTER_EXECUTABLES:
        path = directory / executable
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o700)


def _isolate_shell(shell_name: str, shell: str, root: Path) -> str:
    if shell_name != "zsh":
        return shell
    directory = root / "shells"
    directory.mkdir()
    launcher = directory / "zsh"
    launcher.write_text(
        f'#!/bin/sh\nexec {shlex.quote(shell)} -d "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o700)
    return str(launcher)


def _contract_environment(
    *,
    shell: str,
    home: Path,
    fake_bin: Path,
    database_path: Path,
    log_path: Path,
) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "HOME": str(home),
            "KANTRIP_DATABASE": str(database_path),
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
    registry_commands: list[str] = []
    for executable in sorted(KAFKA_EXECUTABLES):
        if executable.endswith("console-producer"):
            command = f"printf 'contract record\\n' | {executable} --topic contract"
        else:
            command = f"{executable} --contract"
        if "-avro-" in executable or "-json-schema-" in executable or "-protobuf-" in executable:
            registry_commands.append(command)
        else:
            commands.append(command)
    commands.append("; ".join(registry_commands))
    for executable in sorted(KCAT_EXECUTABLES):
        commands.extend((f"{executable} -L", f"{executable} -C -s value=avro -t contract"))
    commands.extend(
        (
            "kaskade admin",
            "kaskade consumer --kafka group.id=kantrip-smoke-contract --kafka broker.address.family=v4",
            "kaskade consumer -v registry",
        )
    )
    return commands


def _history_path(shell_name: str, home: Path, environment: dict[str, str]) -> Path:
    if shell_name == "fish":
        return Path(environment["XDG_DATA_HOME"]) / "fish" / "fish_history"
    return home / f".{shell_name}_history"


if __name__ == "__main__":
    unittest.main()
