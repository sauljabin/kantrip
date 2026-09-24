"""Terminal cleanup must tolerate a client exiting just before teardown."""

from __future__ import annotations

import os
import time
import unittest

from tests.e2e.terminal import TerminalProcess


class TestTerminalProcess(unittest.TestCase):
    def test_close_after_child_exits(self) -> None:
        process = TerminalProcess(("sh", "-c", "exit 0"), os.environ)
        time.sleep(0.05)
        process._poll(0)
        process.close()
        self.assertIsNotNone(process.status)


if __name__ == "__main__":
    unittest.main()
