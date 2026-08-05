"""A stand-in for `lowriter`, so the real subprocess path is exercised without it.

Accepts the argument shape render.py uses and writes a genuine PDF, so the page-count
check is doing real work. Behaviour is steered by the input filename, which is how tests
provoke each failure mode:

    *-fails.*     exit non-zero
    *-silent.*    exit 0 having written nothing
    *-empty.*     write a zero-byte PDF
    *-garbage.*   write bytes that are not a PDF
    *-hangs.*     sleep past any sane timeout
    *-flaky.*     sleep past the timeout on the first attempt only, then succeed
"""

import sys
import time
from pathlib import Path

from pypdf import PdfWriter

VERSION = "FakeConverter 1.2.3"


def _write_pdf(path: Path, pages: int = 1) -> None:
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=612, height=792)
    with path.open("wb") as f:
        writer.write(f)


def main(argv: list[str]) -> int:
    if "--version" in argv:
        print(VERSION)
        return 0

    outdir = Path(argv[argv.index("--outdir") + 1])
    source = Path(argv[-1])
    stem = source.stem

    if stem.endswith("-fails"):
        print("conversion refused", file=sys.stderr)
        return 3
    if stem.endswith("-silent"):
        return 0
    if stem.endswith("-hangs"):
        time.sleep(30)
        return 0
    if stem.endswith("-flaky"):
        # A marker beside the output directory, so "first attempt" survives the fresh
        # working directory each attempt gets.
        marker = outdir.parent.parent / f"{stem}.attempted"
        if not marker.exists():
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("1")
            time.sleep(30)
            return 0

    outdir.mkdir(parents=True, exist_ok=True)
    target = outdir / f"{stem}.pdf"
    if stem.endswith("-empty"):
        target.write_bytes(b"")
    elif stem.endswith("-garbage"):
        target.write_bytes(b"this is not a pdf")
    else:
        _write_pdf(target, pages=2 if stem.endswith("-twopage") else 1)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
