#!/usr/bin/env python3
"""Focused regression checks for tests/platform_support.py."""

import errno
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

import platform_support


class PlatformSupportTests(unittest.TestCase):
    def test_running_reports_current_process(self):
        self.assertTrue(platform_support.running(os.getpid()))

    def test_linux_proc_missing_and_esrch_return_false(self):
        for error in (
            FileNotFoundError(errno.ENOENT, "No such file or directory"),
            ProcessLookupError(errno.ESRCH, "No such process"),
        ):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(platform_support, "WINDOWS", False),
                patch.object(platform_support, "LINUX", True),
                patch.object(platform_support.Path, "read_text", side_effect=error),
            ):
                self.assertFalse(platform_support.running(12345))

    def test_linux_proc_other_inspection_errors_still_fail(self):
        for error in (
            PermissionError(errno.EACCES, "Permission denied"),
            OSError(errno.EIO, "Input/output error"),
        ):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(platform_support, "WINDOWS", False),
                patch.object(platform_support, "LINUX", True),
                patch.object(platform_support.Path, "read_text", side_effect=error),
                self.assertRaises(type(error)),
            ):
                platform_support.running(12345)

    def test_linux_proc_stat_zombie_and_live_parsing(self):
        for stat_line, expected in (
            ("12345 (a) b) Z 1 2 3", False),
            ("12345 (a) b) S 1 2 3", True),
        ):
            with (
                self.subTest(expected=expected),
                patch.object(platform_support, "WINDOWS", False),
                patch.object(platform_support, "LINUX", True),
                patch.object(platform_support.Path, "read_text", return_value=stat_line),
            ):
                self.assertEqual(platform_support.running(12345), expected)


if __name__ == "__main__":
    unittest.main()
