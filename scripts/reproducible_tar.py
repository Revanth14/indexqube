#!/usr/bin/env python3
"""Create a deterministic gzip-compressed ustar archive from one directory."""

from __future__ import annotations

import gzip
import pathlib
import sys
import tarfile


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: reproducible_tar.py SOURCE_DIR OUTPUT.tar.gz SOURCE_DATE_EPOCH", file=sys.stderr)
        return 2
    source = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    epoch = int(sys.argv[3])
    if not source.is_dir():
        raise SystemExit(f"source is not a directory: {source}")

    paths = sorted(source.rglob("*"), key=lambda path: path.relative_to(source).as_posix())
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                for path in paths:
                    name = path.relative_to(source).as_posix()
                    info = archive.gettarinfo(str(path), arcname=name)
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = epoch
                    info.mode = 0o755 if path.is_dir() or info.mode & 0o111 else 0o644
                    if path.is_file():
                        with path.open("rb") as handle:
                            archive.addfile(info, handle)
                    else:
                        archive.addfile(info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
