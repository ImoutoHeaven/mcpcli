#!/usr/bin/env python3
"""Extract an archive and exercise its actual executable through persistent MCP calls."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
from cli_e2e import (Fixture, offline_skills, state_and_discovery, json_inputs_and_outputs,
                     session_and_restart, http_optional_session_delete)
from platform_support import test_temp_base


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
        if options.archive.suffix == ".zip":
            with zipfile.ZipFile(options.archive) as archive:
                archive.extractall(root)
        else:
            with tarfile.open(options.archive) as archive:
                archive.extractall(root, filter="data")
        candidates = list(root.glob("*/mcpcli.exe" if os.name == "nt" else "*/mcpcli"))
        assert len(candidates) == 1, f"archive must contain exactly one native executable: {candidates}"
        binary = candidates[0].resolve(strict=True)
        version = subprocess.check_output([str(binary), "--version"], text=True,
                                          encoding="utf-8", timeout=5).strip()
        assert version == f"mcpcli {options.version}", version
        for case in [offline_skills, state_and_discovery, json_inputs_and_outputs,
                     session_and_restart, http_optional_session_delete]:
            fixture = Fixture(binary, root, case.__name__)
            try:
                case(fixture)
            finally:
                errors = fixture.cleanup()
                assert not errors, errors
            print("PASS " + case.__name__, flush=True)
        print(json.dumps({"archive": str(options.archive), "version": version, "passed": 5}))


if __name__ == "__main__":
    main()
