import io
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import verify_release

ROOT = "kantrip-0.0.0"
_PROBE = """import unittest
from pathlib import Path


class Probe(unittest.TestCase):
    def test_marker(self):
        self.assertTrue(Path(".github/marker").is_file())
"""


def _sdist(directory: Path, files: dict[str, str]) -> Path:
    path = directory / f"{ROOT}.tar.gz"
    with tarfile.open(path, "w:gz") as archive:
        for name, contents in files.items():
            data = contents.encode()
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return path


class TestVerifyRelease(unittest.TestCase):
    def test_sdist_requirements_cover_bundled_repository_metadata(self) -> None:
        checkout = {
            path.as_posix()
            for pattern in (".github/actions/*/action.yml", ".github/workflows/*.yml")
            for path in Path().glob(pattern)
        }
        read_by_bundled_tests = {
            path
            for path in checkout
            if path.startswith(".github/actions/")
            or path.removeprefix(".github/workflows/") in {"main.yml", "e2e.yml", "release.yml"}
        }

        self.assertTrue(read_by_bundled_tests)
        self.assertLessEqual(read_by_bundled_tests, verify_release.SDIST_REQUIRED)

    def test_bundled_checks_run_inside_the_extracted_sdist(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(verify_release, "SDIST_SELF_CONTAINED_TESTS", ("probe",)),
        ):
            complete = _sdist(
                Path(directory),
                {f"{ROOT}/probe.py": _PROBE, f"{ROOT}/.github/marker": ""},
            )
            verify_release.verify_sdist_self_contained(complete, ROOT)

            incomplete_directory = Path(directory) / "incomplete"
            incomplete_directory.mkdir()
            incomplete = _sdist(incomplete_directory, {f"{ROOT}/probe.py": _PROBE})
            with self.assertRaisesRegex(ValueError, "extracted source distribution"):
                verify_release.verify_sdist_self_contained(incomplete, ROOT)

    def test_rejects_members_outside_the_extraction_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sdist = _sdist(Path(directory), {"../escape.py": "", f"{ROOT}/probe.py": _PROBE})
            with self.assertRaises((ValueError, tarfile.TarError)):
                verify_release.verify_sdist_self_contained(sdist, ROOT)
            self.assertFalse((Path(tempfile.gettempdir()) / "escape.py").exists())


if __name__ == "__main__":
    unittest.main()
