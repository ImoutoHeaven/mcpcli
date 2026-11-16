#!/usr/bin/env python3
"""Package one native executable and its public documentation."""

import argparse
from pathlib import Path
import shutil
import tarfile
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("version")
    parser.add_argument("platform")
    parser.add_argument("--destination", type=Path, default=Path("dist"))
    options = parser.parse_args()
    assert options.version and all(c in "0123456789." for c in options.version)
    assert options.platform in {"linux-amd64", "linux-arm64", "windows-amd64", "windows-arm64",
                                "macos-amd64", "macos-arm64", "freebsd-amd64"}
    name = f"mcpcli-{options.version}-{options.platform}"
    root = options.destination / name
    root.mkdir(parents=True, exist_ok=False)
    shutil.copy2(options.binary, root / options.binary.name)
    for filename in ("LICENSE", "readme.md", "examples/mcpcli.json", "docs/mcpcli-design.md"):
        output = root / filename
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(filename, output)
    if options.platform.startswith("windows-"):
        archive = options.destination / f"{name}.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as handle:
            for path in sorted(root.rglob("*")):
                if path.is_file():
                    handle.write(path, path.relative_to(options.destination))
    else:
        archive = options.destination / f"{name}.tar.gz"
        with tarfile.open(archive, "w:gz") as handle:
            handle.add(root, arcname=name)
    print(archive)


if __name__ == "__main__":
    main()
