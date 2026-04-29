# %%
# Fetch the file listing from each bioRxiv 10k zip on Zenodo using HTTP Range
# requests against the zip Central Directory — no full download needed.
#
# File naming inside each zip: {numeric_id}v{version}/{numeric_id}v{version}.xml
# The full DOI is: 10.1101/{numeric_id}

from __future__ import annotations

import io
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import requests

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer


class RangeHTTPFile(io.RawIOBase):
    """Seekable file-like object backed by HTTP Range requests.

    zipfile reads only the Central Directory (a few hundred KB at the end of
    the archive), so we never touch the actual compressed content.
    """

    def __init__(self, url: str, session: requests.Session):
        self._session = session
        self._pos = 0
        # Resolve any redirects once so all subsequent requests skip that hop.
        resp = session.head(url, allow_redirects=True, timeout=15)
        resp.raise_for_status()
        self.url = resp.url
        self._size = int(resp.headers["Content-Length"])
        print(f"  {url.split('/')[-1]}: {self._size / 1e9:.1f} GB")

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, pos: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self._pos = self._size + pos
        return self._pos

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        end = (self._pos + n - 1) if n > 0 else (self._size - 1)
        end = min(end, self._size - 1)
        if self._pos > end:
            return b""
        resp = self._session.get(
            self.url,
            headers={"Range": f"bytes={self._pos}-{end}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.content
        self._pos += len(data)
        return data

    def readinto(self, b: WriteableBuffer) -> int | None:
        data = self.read(len(b))  # type: ignore[arg-type]
        n = len(data)
        b[:n] = data  # type: ignore[index]
        return n


# %%
ZENODO_BASE = "https://zenodo.org/records/3873702/files"
SPLITS = {
    "train": f"{ZENODO_BASE}/biorxiv-10k-train-6000.zip",
    "validation": f"{ZENODO_BASE}/biorxiv-10k-validation-2000.zip",
    "test": f"{ZENODO_BASE}/biorxiv-10k-test-2000.zip",
}

session = requests.Session()

# Map split name -> list of all zip entries (directories and files)
split_raw_names: dict[str, list[str]] = {}

for split, url in SPLITS.items():
    print(f"\n{split}")
    raw = RangeHTTPFile(url, session)
    with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1 << 20)) as zf:
        names = zf.namelist()
    split_raw_names[split] = names
    print(f"  {len(names)} entries")
    print(f"  First 5: {names[:5]}")
    print(f"  Last  5: {names[-5:]}")

# %%
# Extract unique numeric IDs (strip version and extension).
# Entry format: "153551v1/" or "153551v1/153551v1.xml" -> numeric_id "153551"
# Case-insensitive: two train entries use uppercase V (e.g. "207027V1/")
_ID_RE = re.compile(r"^(\d+)v\d+", re.IGNORECASE)

split_ids: dict[str, set[str]] = {}
for split, names in split_raw_names.items():
    ids: set[str] = set()
    unrecognised: list[str] = []
    for name in names:
        folder = name.split("/")[0]
        m = _ID_RE.match(folder)
        if m:
            ids.add(m.group(1))
        elif "/" not in name:
            # Top-level entry that is not a versioned folder (e.g. a .tsv file)
            unrecognised.append(name)
    split_ids[split] = ids
    print(f"{split}: {len(ids)} unique IDs")
    if unrecognised:
        print(f"  non-paper entries: {unrecognised}")

# %%
# Load local dataset and extract numeric IDs from DOIs.
# DOI format: "10.1101/588491" -> numeric_id "588491"
LOCAL_DATASET = Path("output/biorxiv-jats-hf-dataset")
_DOI_RE = re.compile(r"10\.1101/(\d+)")

local_ids: set[str] = set()
# Map numeric_id -> local split name (e.g. "train", "validation", "test")
local_id_split: dict[str, str] = {}
for parquet_file in sorted(LOCAL_DATASET.glob("*.parquet")):
    split_name = parquet_file.name.split("-")[0]
    df = pd.read_parquet(parquet_file, columns=["doi"])
    for doi in df["doi"]:
        m = _DOI_RE.match(doi)
        if m:
            numeric_id = m.group(1)
            local_ids.add(numeric_id)
            local_id_split[numeric_id] = split_name
    print(f"{parquet_file.name}: {len(df)} rows")

print(f"\nLocal dataset: {len(local_ids)} unique IDs")

# %%
# Compare: which local IDs appear in the 10k dataset (any split)?
all_10k_ids = split_ids["train"] | split_ids["validation"] | split_ids["test"]

overlap = local_ids & all_10k_ids
print(f"Local IDs:       {len(local_ids)}")
print(f"10k IDs (total): {len(all_10k_ids)}")
print(f"Overlap:         {len(overlap)}")
print()
for split, ids in split_ids.items():
    in_split = local_ids & ids
    print(f"  overlap with {split:10s}: {len(in_split)}")
if overlap:
    print("\nOverlapping IDs (local split -> 10k split):")
    for numeric_id in sorted(overlap):
        local_split = local_id_split[numeric_id]
        in_10k_splits = [s for s, ids in split_ids.items() if numeric_id in ids]
        print(
            f"  10.1101/{numeric_id}  local:{local_split}  10k:{', '.join(in_10k_splits)}"
        )

# %%
# Identify IDs that reduce the unique count below the expected paper count.
# Case 1: multiple version folders for the same ID within one zip.
# Case 2: an ID appearing in more than one 10k split (cross-split leakage).

_VER_RE = re.compile(r"^(\d+)(v\d+)$", re.IGNORECASE)

print("--- Multi-version entries within each split ---")
for split, names in split_raw_names.items():
    versions_by_id: dict[str, list[str]] = defaultdict(list)
    for name in names:
        folder = name.split("/")[0]
        m = _VER_RE.match(folder)
        if m:
            numeric_id, ver = m.group(1), m.group(2)
            if ver not in versions_by_id[numeric_id]:
                versions_by_id[numeric_id].append(ver)

    multi_version = {
        numeric_id: vers for numeric_id, vers in versions_by_id.items() if len(vers) > 1
    }
    print(f"{split}: {len(multi_version)} IDs with multiple versions")
    for numeric_id, vers in sorted(multi_version.items()):
        print(f"  10.1101/{numeric_id}  versions={vers}")

print("\n--- IDs appearing in more than one 10k split ---")
split_names = list(split_ids.keys())
for i, s1 in enumerate(split_names):
    for s2 in split_names[i + 1 :]:
        cross = split_ids[s1] & split_ids[s2]
        print(f"{s1} ∩ {s2}: {len(cross)}")
        for numeric_id in sorted(cross):
            print(f"  10.1101/{numeric_id}")
