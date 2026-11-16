#!/usr/bin/env python3
"""Check archive traversal/link rejection and portable extraction before building releases."""

import importlib.util
import io
from pathlib import Path
import stat
import tarfile
import tempfile
import unittest
import zipfile

spec = importlib.util.spec_from_file_location("release_smoke", Path(__file__).with_name("smoke-release.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class ArchiveTests(unittest.TestCase):
    def test_rejects_escaping_and_ambiguous_paths(self):
        for name in ("../escape", "/escape", "a/../../escape", "a/../b", "C:/escape",
                     "a\\..\\escape", "a/.. /escape"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                smoke.validate_member(name)

    def test_tar_rejects_links_and_extracts_regular_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
                path = root / "bad.tar.gz"
                with tarfile.open(path, "w:gz") as archive:
                    member = tarfile.TarInfo("package/escape")
                    member.type, member.linkname = kind, "../../escape"
                    archive.addfile(member)
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    smoke.extract_archive(path, root / "out")
            path = root / "good.tar.gz"
            with tarfile.open(path, "w:gz") as archive:
                member = tarfile.TarInfo("package/mcpcli")
                member.size, member.mode = 2, 0o4755
                archive.addfile(member, io.BytesIO(b"ok"))
            smoke.extract_archive(path, root / "out")
            self.assertEqual((root / "out/package/mcpcli").read_bytes(), b"ok")
            self.assertFalse((root / "out/package/mcpcli").stat().st_mode & stat.S_ISUID)

    def test_zip_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "bad.zip"
            with zipfile.ZipFile(path, "w") as archive:
                member = zipfile.ZipInfo("package/escape")
                member.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(member, "../../escape")
            with self.assertRaises(ValueError):
                smoke.extract_archive(path, root / "out")


if __name__ == "__main__":
    unittest.main()
