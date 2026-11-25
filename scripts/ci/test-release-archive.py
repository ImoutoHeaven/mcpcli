#!/usr/bin/env python3
"""Check archive traversal/link rejection, packaged files, and portable extraction before building releases."""

import importlib.util
import io
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile

CI_DIR = Path(__file__).resolve().parent
REPO_ROOT = CI_DIR.parents[1]

spec = importlib.util.spec_from_file_location("release_smoke", CI_DIR / "smoke-release.py")
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

    def test_package_bundles_notice_and_public_docs(self):
        expected_docs = {
            "LICENSE",
            "NOTICE",
            "README.md",
            "examples/mcpcli.json",
            "docs/mcpcli-design.md",
        }
        notice_bytes = (REPO_ROOT / "NOTICE").read_bytes()
        self.assertGreater(len(notice_bytes), 0)
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            cases = (
                ("linux-amd64", "mcpcli", "tar.gz"),
                ("windows-amd64", "mcpcli.exe", "zip"),
            )
            for platform, binary_name, ext in cases:
                with self.subTest(platform=platform):
                    bin_dir = temp / f"bin-{platform}"
                    bin_dir.mkdir()
                    binary = bin_dir / binary_name
                    binary.write_bytes(b"binary")
                    dist = temp / f"dist-{platform}"
                    subprocess.run(
                        [
                            sys.executable,
                            str(CI_DIR / "package.py"),
                            str(binary),
                            "0.5.1",
                            platform,
                            "--destination",
                            str(dist),
                        ],
                        cwd=REPO_ROOT,
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    archive = dist / f"mcpcli-0.5.1-{platform}.{ext}"
                    self.assertTrue(archive.is_file())
                    extracted = temp / f"out-{platform}"
                    smoke.extract_archive(archive, extracted)
                    pkg_root = extracted / f"mcpcli-0.5.1-{platform}"
                    files = {
                        p.relative_to(pkg_root).as_posix()
                        for p in pkg_root.rglob("*")
                        if p.is_file()
                    }
                    self.assertEqual(files, expected_docs | {binary_name})
                    self.assertEqual((pkg_root / "NOTICE").read_bytes(), notice_bytes)


if __name__ == "__main__":
    unittest.main()
