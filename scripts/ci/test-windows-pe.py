#!/usr/bin/env python3
"""Exercise Windows archive dependency gates without a Windows host."""

from pathlib import Path
import struct
import tempfile
import unittest

from windows_pe import check_windows_image, pe_imports


def fixture(machine=0x8664, normal="kernel32.dll", delayed=None, delay_va=False):
    data = bytearray(0xA00)
    def put(offset, fmt, *values):
        struct.pack_into("<" + fmt, data, offset, *values)
    data[:2] = b"MZ"
    put(0x3C, "I", 0x80)
    data[0x80:0x84] = b"PE\0\0"
    put(0x84, "HH", machine, 1)
    put(0x94, "H", 240)
    put(0x98, "H", 0x20B)
    put(0x98 + 24, "Q", 0x400000)
    put(0x98 + 60, "I", 0x200)
    put(0x98 + 108, "I", 16)
    put(0x188 + 8, "IIII", 0x1000, 0x1000, 0x600, 0x200)
    if normal:
        put(0x98 + 112 + 8, "II", 0x1000, 40)
        put(0x200 + 12, "I", 0x1080)
        name = normal.encode() + b"\0"
        data[0x280:0x280 + len(name)] = name
    if delayed:
        put(0x98 + 112 + 13 * 8, "II", 0x1100, 64)
        put(0x300, "II", 0 if delay_va else 1, 0x401180 if delay_va else 0x1180)
        name = delayed.encode() + b"\0"
        data[0x380:0x380 + len(name)] = name
    return data


class WindowsPeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.binary = Path(directory.name) / "mcpcli.exe"

    def check(self, data, platform="windows-amd64"):
        self.binary.write_bytes(data)
        return check_windows_image(self.binary, platform)

    def test_native_machines_and_os_supplied_crt_are_allowed(self):
        for machine, platform in [(0x8664, "windows-amd64"), (0xAA64, "windows-arm64")]:
            for name in ["api-ms-win-crt-runtime-l1-1-0.dll", "ucrtbase.dll", "msvcp_win.dll"]:
                with self.subTest(platform=platform, name=name):
                    result = self.check(fixture(machine, delayed=name), platform)
                    self.assertIn(name, result["imports"])

    def test_rejects_external_normal_and_delay_runtime_imports(self):
        for name in ["VCRUNTIME140.dll", "msvcp140_atomic_wait.dll", "CONCRT140.dll",
                     "MSVCR120.dll", "vccorlib140.dll", "ucrtbased.dll"]:
            images = [fixture(normal=name), fixture(delayed=name), fixture(delayed=name, delay_va=True)]
            for index, data in enumerate(images):
                with self.subTest(name=name, mode=index), self.assertRaisesRegex(ValueError, "external VC runtime"):
                    self.check(data)

    def test_rejects_wrong_machine_and_missing_imports(self):
        with self.assertRaisesRegex(ValueError, "machine"):
            self.check(fixture(0xAA64))
        with self.assertRaisesRegex(ValueError, "normal import table"):
            self.check(fixture(normal=None))

    def test_rejects_truncation_and_unterminated_directory(self):
        for length in [0, 63, 0x97, 0x180, 0x7FF]:
            with self.subTest(length=length), self.assertRaises(ValueError):
                pe_imports(fixture()[:length])
        data = fixture()
        struct.pack_into("<I", data, 0x98 + 112 + 8 + 4, 20)
        with self.assertRaisesRegex(ValueError, "unterminated PE import directory"):
            pe_imports(data)

    def test_rejects_addresses_beyond_file_backed_section(self):
        data = fixture()
        struct.pack_into("<I", data, 0x200 + 12, 0x1700)
        data[0x900:0x90D] = b"kernel32.dll\0"
        with self.assertRaisesRegex(ValueError, "file-backed section"):
            pe_imports(data)

    def test_rejects_unterminated_names_and_bad_directory_count(self):
        data = fixture()
        data[0x280:0x800] = b"x" * (0x800 - 0x280)
        with self.assertRaisesRegex(ValueError, "unterminated PE import name"):
            pe_imports(data)
        data = fixture()
        struct.pack_into("<I", data, 0x98 + 108, 17)
        with self.assertRaisesRegex(ValueError, "data-directory count"):
            pe_imports(data)


if __name__ == "__main__":
    unittest.main()
