#!/usr/bin/env python3
"""Extract an archive and exercise its actual executable through persistent MCP calls."""

import argparse
import json
import os
import stat
import shutil
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
from cli_e2e import (Fixture, offline_skills, global_agents, state_and_discovery,
                     json_inputs_and_outputs, session_and_restart, http_optional_session_delete)
from platform_support import test_temp_base
from windows_pe import MACHINES, check_windows_image


def validate_member(name):
    parts = name.rstrip("/").split("/")
    if (not name or "\\" in name or ":" in name or "\0" in name
            or any(part in ("", ".", "..") or part.rstrip(" .") != part for part in parts)):
        raise ValueError(f"unsafe archive path: {name!r}")


def extract_archive(path, root):
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                validate_member(member.filename)
                if stat.S_IFMT(member.external_attr >> 16) not in (0, stat.S_IFREG, stat.S_IFDIR):
                    raise ValueError("release archives may contain only files and directories")
            archive.extractall(root)
    else:
        with tarfile.open(path) as archive:
            members = archive.getmembers()
            for member in members:
                validate_member(member.name)
                if not (member.isdir() or member.isreg()):
                    raise ValueError("release archives may contain only files and directories")
            # Copy regular files only; no ownership/special-mode metadata or version-specific filters.
            for member in members:
                target = root / member.name
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(member) as source, target.open("xb") as output:
                        shutil.copyfileobj(source, output)
                    target.chmod(member.mode & 0o755)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("version")
    options = parser.parse_args()
    base = test_temp_base()
    if os.name == "nt":
        os.environ.update(TEMP=str(base), TMP=str(base))
    with tempfile.TemporaryDirectory(prefix="mc-release-", dir=base) as directory:
        root = Path(directory)
        extract_archive(options.archive, root)
        candidates = list(root.glob("*/mcpcli.exe" if os.name == "nt" else "*/mcpcli"))
        assert len(candidates) == 1, f"archive must contain exactly one native executable: {candidates}"
        binary = candidates[0].resolve(strict=True)
        if os.name == "nt":
            names = {f"mcpcli-{options.version}-{platform}.zip": platform for platform in MACHINES}
            platform = names[options.archive.name]
            print(json.dumps(check_windows_image(binary, platform)), flush=True)
        version = subprocess.check_output([str(binary), "--version"], text=True,
                                          encoding="utf-8", timeout=5).strip()
        assert version == f"mcpcli {options.version}", version
        cases = [offline_skills, global_agents, state_and_discovery, json_inputs_and_outputs,
                 session_and_restart, http_optional_session_delete]
        for case in cases:
            fixture = Fixture(binary, root, case.__name__)
            try:
                print("START " + case.__name__, flush=True)
                case(fixture)
            finally:
                errors = fixture.cleanup()
                assert not errors, errors
            print("PASS " + case.__name__, flush=True)
        print(json.dumps({"archive": str(options.archive), "version": version, "passed": len(cases)}))


if __name__ == "__main__":
    main()
