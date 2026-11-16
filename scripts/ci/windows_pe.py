#!/usr/bin/env python3
"""Check native Windows release architecture and external VC runtime imports."""

import argparse
import json
from pathlib import Path
import re
import struct

MACHINES = {"windows-amd64": 0x8664, "windows-arm64": 0xAA64}
REDISTRIBUTABLE = re.compile(r"(?:vcruntime|msvcp|msvcr|concrt|vccorlib|mfc|mfcm)\d.*\.dll", re.I)


def pe_imports(data):
    """Read PE32+ normal and delay imports with file and section bounds checks."""
    # Header layouts: https://learn.microsoft.com/windows/win32/debug/pe-format
    def unpack(fmt, offset):
        size = struct.calcsize(fmt)
        if offset < 0 or offset + size > len(data):
            raise ValueError("truncated PE structure")
        return struct.unpack_from(fmt, data, offset)

    if data[:2] != b"MZ":
        raise ValueError("missing DOS executable signature")
    pe, = unpack("<I", 0x3C)
    if data[pe:pe + 4] != b"PE\0\0":
        raise ValueError("missing PE executable signature")
    machine, sections = unpack("<HH", pe + 4)
    optional_size, = unpack("<H", pe + 20)
    optional = pe + 24
    magic, = unpack("<H", optional)
    if magic != 0x20B or optional_size < 112 or not 1 <= sections <= 96:
        raise ValueError("invalid PE32+ headers")
    image_base, = unpack("<Q", optional + 24)
    headers_size, = unpack("<I", optional + 60)
    directories, = unpack("<I", optional + 108)
    if directories < 2 or 112 + directories * 8 > optional_size:
        raise ValueError("invalid PE data-directory count")
    table = optional + optional_size
    if table + sections * 40 > headers_size or headers_size > len(data):
        raise ValueError("invalid PE section/header bounds")
    ranges = []
    for index in range(sections):
        _, address, raw_size, raw_offset = unpack("<IIII", table + index * 40 + 8)
        if raw_offset + raw_size > len(data):
            raise ValueError("truncated PE section")
        ranges.append((address, raw_size, raw_offset))

    def region(rva, size):
        if rva <= 0 or size <= 0:
            raise ValueError("invalid PE relative address")
        if rva + size <= headers_size:
            return rva, headers_size
        matches = [(raw + rva - address, raw + length)
                   for address, length, raw in ranges
                   if address <= rva and rva + size <= address + length]
        if len(matches) != 1:
            raise ValueError("PE relative address is outside one file-backed section")
        return matches[0]

    def dll_name(rva):
        start, limit = region(rva, 1)
        end = data.find(b"\0", start, limit)
        if end < 0:
            raise ValueError("unterminated PE import name")
        name = data[start:end].decode("ascii")
        if not re.fullmatch(r"[A-Za-z0-9_.+-]+\.dll", name, re.I):
            raise ValueError("invalid PE DLL import name")
        return name

    def imports(index, width):
        if index >= directories:
            return []
        rva, size = unpack("<II", optional + 112 + index * 8)
        if not rva and not size:
            return []
        if size < width:
            raise ValueError("truncated PE import directory")
        start, _ = region(rva, size)
        result = []
        for offset in range(start, start + size - width + 1, width):
            fields = unpack("<" + "I" * (width // 4), offset)
            if not any(fields):
                return result
            if index == 1:
                name = fields[3]
            else:
                attributes, name = fields[:2]
                if attributes & ~1:
                    raise ValueError("unsupported delay-import address attributes")
                if not attributes & 1:
                    name -= image_base
            result.append(dll_name(name))
        raise ValueError("unterminated PE import directory")

    normal = imports(1, 20)
    delayed = imports(13, 32)
    if not normal:
        raise ValueError("Windows release has no normal import table")
    return machine, sorted(set(normal + delayed), key=str.casefold)


def check_windows_image(path, platform):
    machine, imports = pe_imports(Path(path).read_bytes())
    if machine != MACHINES[platform]:
        raise ValueError(f"{platform} archive contains PE machine {machine:#x}")
    external = [name for name in imports
                if REDISTRIBUTABLE.fullmatch(name) or name.casefold() == "ucrtbased.dll"]
    if external:
        raise ValueError("Windows release requires external VC runtime DLLs: " + ", ".join(external))
    return {"platform": platform, "machine": f"{machine:#x}", "imports": imports}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("platform", choices=MACHINES)
    options = parser.parse_args()
    print(json.dumps(check_windows_image(options.binary, options.platform)))


if __name__ == "__main__":
    main()
