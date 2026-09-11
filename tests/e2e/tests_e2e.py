import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from testcontainers.community.kafka import KafkaContainer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SANDBOX_ENV = PROJECT_ROOT / "sandbox" / ".env"


def sandbox_version(name: str) -> str:
    prefix = f"{name}="
    value = next(
        (
            line.removeprefix(prefix)
            for line in SANDBOX_ENV.read_text(encoding="utf-8").splitlines()
            if line.startswith(prefix)
        ),
        "",
    )
    if not value:
        raise RuntimeError(f"missing {name} in {SANDBOX_ENV}")
    return value


KAFKA_IMAGE = f"confluentinc/cp-kafka:{sandbox_version('CONFLUENT_VERSION')}"


def installed_cli() -> list[str]:
    executable = os.environ.get("KANTRIP_E2E_EXECUTABLE")
    if executable:
        return [executable]
    return [sys.executable, "-m", "kantrip.cli"]


def run_cli(
    *arguments: str,
    environment: dict[str, str] | None = None,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*installed_cli(), *arguments],
        capture_output=True,
        text=True,
        input=input_text,
        env=environment,
        check=False,
    )


class TestInstalledCli(unittest.TestCase):
    def test_entry_point_reports_version(self) -> None:
        result = run_cli("--version")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertRegex(result.stdout, r"^kantrip, version [0-9]+\.[0-9]+\.[0-9]+")

    @unittest.skipUnless(shutil.which("kcat"), "kcat is required for the integration test")
    def test_kcat_produces_and_consumes_through_an_installed_cli(self) -> None:
        message = f"kantrip-e2e-{uuid.uuid4()}"
        topic = f"kantrip-e2e-{uuid.uuid4().hex}"

        with tempfile.TemporaryDirectory(prefix="kantrip-e2e-") as directory:
            environment = os.environ | {"KANTRIP_CONFIG": str(Path(directory) / "config.yaml")}
            with KafkaContainer(KAFKA_IMAGE).with_kraft() as kafka:
                added = run_cli(
                    "add",
                    "sandbox",
                    "--bootstrap-server",
                    kafka.get_bootstrap_server(),
                    environment=environment,
                )
                self.assertEqual(0, added.returncode, added.stderr)

                listed = run_cli("list", environment=environment)
                self.assertEqual(0, listed.returncode, listed.stderr)
                self.assertEqual("sandbox\n", listed.stdout)

                produced = run_cli(
                    "exec",
                    "sandbox",
                    "--",
                    "kcat",
                    "-P",
                    "-t",
                    topic,
                    environment=environment,
                    input_text=f"{message}\n",
                )
                self.assertEqual(0, produced.returncode, produced.stderr)

                consumed = run_cli(
                    "exec",
                    "sandbox",
                    "--",
                    "kcat",
                    "-C",
                    "-q",
                    "-t",
                    topic,
                    "-o",
                    "beginning",
                    "-c",
                    "1",
                    environment=environment,
                )
                self.assertEqual(0, consumed.returncode, consumed.stderr)
                self.assertEqual(f"{message}\n", consumed.stdout)


if __name__ == "__main__":
    unittest.main()
