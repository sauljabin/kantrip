import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from kantrip.profiles import (
    DATABASE_SCHEMA_VERSION,
    ProfileStoreError,
    add_profile,
    load_profiles,
    remove_profile,
    resolve_database_path,
)


class TestProfiles(unittest.TestCase):
    def test_resolves_documented_database_precedence(self) -> None:
        environment = {
            "HOME": "/home/example",
            "XDG_DATA_HOME": "/xdg/data",
            "KANTRIP_DATABASE": "/explicit/profiles.db",
        }

        self.assertEqual(Path("/explicit/profiles.db"), resolve_database_path(environment))
        del environment["KANTRIP_DATABASE"]
        self.assertEqual(Path("/xdg/data/kantrip/profiles.db"), resolve_database_path(environment))
        del environment["XDG_DATA_HOME"]
        self.assertEqual(
            Path("/home/example/.local/share/kantrip/profiles.db"),
            resolve_database_path(environment),
        )

    def test_adds_loads_and_removes_a_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "kantrip" / "profiles.db"

            profiles = add_profile("local", path, description="Local development")

            self.assertEqual(
                ["localhost:9092"], profiles.profile("local")["kafka"]["bootstrapServers"]
            )
            self.assertEqual("Local development", profiles.profile("local")["description"])
            self.assertEqual(0o700, path.parent.stat().st_mode & 0o777)
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            self.assertEqual(["local"], list(load_profiles(path).profiles))

            profiles = remove_profile("local", path)

            self.assertEqual({}, profiles.profiles)
            self.assertEqual({}, load_profiles(path).profiles)

    def test_database_uses_wal_and_the_current_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with closing(sqlite3.connect(path)) as connection:
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                version = connection.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual("wal", journal_mode)
        self.assertEqual(DATABASE_SCHEMA_VERSION, version)

    def test_concurrent_writers_do_not_lose_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("initial", path)

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(add_profile, f"profile-{index}", path) for index in range(8)
                ]
                for future in futures:
                    future.result()

            self.assertEqual(
                ["initial", *(f"profile-{index}" for index in range(8))],
                list(load_profiles(path).profiles),
            )

    def test_missing_database_can_be_loaded_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing" / "profiles.db"

            profiles = load_profiles(path, missing_ok=True)

            self.assertEqual({}, profiles.profiles)
            self.assertFalse(path.parent.exists())

    def test_unknown_profile_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with self.assertRaisesRegex(ProfileStoreError, "profile 'missing' was not found"):
                load_profiles(path).profile("missing")

    def test_adds_a_confluent_registry_connection_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profiles = add_profile(
                "local",
                Path(directory) / "profiles.db",
                registry_url="http://localhost:8081",
            )

        self.assertEqual(
            {
                "provider": "confluent",
                "schema.registry.url": "http://localhost:8081",
            },
            profiles.profile("local")["registry"],
        )

    def test_adds_an_apicurio_registry_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            profiles = add_profile(
                "local",
                Path(directory) / "profiles.db",
                registry_provider="apicurio",
                registry_url="http://localhost:8082/apis/registry/v3",
            )

        self.assertEqual(
            {
                "provider": "apicurio",
                "apicurio.registry.url": "http://localhost:8082/apis/registry/v3",
            },
            profiles.profile("local")["registry"],
        )

    def test_add_rejects_invalid_input_without_creating_a_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            cases = (
                ({"registry_url": "https://registry.example.com"}, "supports only an http://"),
                ({"registry_provider": "apicurio"}, "requires --registry-url"),
            )
            for arguments, message in cases:
                with self.subTest(arguments=arguments):
                    with self.assertRaisesRegex(ProfileStoreError, message):
                        add_profile("local", path, **arguments)
                    self.assertFalse(path.exists())

    def test_add_refuses_to_replace_an_existing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with self.assertRaisesRegex(ProfileStoreError, "already exists"):
                add_profile("local", path)

    def test_rejects_unsafe_profile_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            for name in ("", ".local", "../local", "folder/local", "local\x1b[31m", "x" * 129):
                with (
                    self.subTest(name=name),
                    self.assertRaisesRegex(ProfileStoreError, "safe characters"),
                ):
                    add_profile(name, path)
            self.assertFalse(path.exists())

    def test_rejects_an_exposed_database_or_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "private" / "profiles.db"
            add_profile("local", database)
            database.chmod(0o644)

            with self.assertRaisesRegex(ProfileStoreError, "permissions are not private"):
                load_profiles(database)

            database.chmod(0o600)
            database.parent.chmod(0o755)
            with self.assertRaisesRegex(ProfileStoreError, "directory is not private"):
                load_profiles(database)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_rejects_a_symlink_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.db"
            database = root / "profiles.db"
            target.touch(mode=0o600)
            database.symlink_to(target)

            with self.assertRaisesRegex(ProfileStoreError, "not a regular file"):
                load_profiles(database)

    def test_rejects_corrupt_and_foreign_databases_without_echoing_contents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            corrupt = Path(directory) / "corrupt.db"
            corrupt.write_bytes(b"password=synthetic-secret")
            corrupt.chmod(0o600)

            with self.assertRaisesRegex(ProfileStoreError, "could not be read safely") as raised:
                load_profiles(corrupt)
            self.assertNotIn("synthetic-secret", str(raised.exception))

            foreign = Path(directory) / "foreign.db"
            with closing(sqlite3.connect(foreign)) as connection:
                connection.execute("CREATE TABLE foreign_data (value TEXT)")
                connection.commit()
            foreign.chmod(0o600)
            with self.assertRaisesRegex(ProfileStoreError, "schema is invalid"):
                add_profile("local", foreign)

    def test_rejects_an_unsupported_database_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA user_version = 99")
                connection.commit()

            with self.assertRaisesRegex(ProfileStoreError, "version 99 is not supported"):
                load_profiles(path)

    def test_rejects_invalid_stored_json_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.db"
            add_profile("local", path)

            with closing(sqlite3.connect(path)) as connection:
                connection.execute("UPDATE profiles SET document = ?", ("not-json",))
                connection.commit()
            with self.assertRaisesRegex(ProfileStoreError, "not valid JSON"):
                load_profiles(path)
            with self.assertRaisesRegex(ProfileStoreError, "not valid JSON"):
                add_profile("not-committed", path)
            with closing(sqlite3.connect(path)) as connection:
                names = connection.execute("SELECT name FROM profiles").fetchall()
            self.assertEqual([("local",)], names)

            valid = add_profile("other", Path(directory) / "other.db").profile("other")
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE profiles SET document = ?",
                    (json.dumps(valid, sort_keys=True, separators=(",", ":")),),
                )
                connection.commit()
            with self.assertRaisesRegex(ProfileStoreError, "inconsistent identity"):
                load_profiles(path)


if __name__ == "__main__":
    unittest.main()
