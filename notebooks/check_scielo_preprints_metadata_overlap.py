# %%
# Report overlap between scielo_preprints.parquet metadata and the JATS benchmark split,
# then generate a reshuffled split that keeps test free of parquet contamination.

import json
import re
import random
import collections
from pathlib import Path

SPLIT_CSV = Path("output/scielo-preprints-split.csv")
METADATA_JSONL = Path("output/scielo-preprints-metadata.jsonl")
XML_DIR = Path(".temp/scielo-preprints-xml")
NEW_SPLIT_CSV = Path("output/scielo-preprints-split-reshuffled.csv")

RANDOM_SEED = 42

# %%
# Load current split
split_map = {}
for line in SPLIT_CSV.read_text().splitlines()[1:]:
    ppr_id, split = line.strip().split(",")
    split_map[ppr_id] = split

print("Current split distribution:")
print(dict(collections.Counter(split_map.values())))

# %%
# Collect DOIs present in scielo_preprints.parquet (from locally downloaded XMLs)
parquet_dois = set()
for xml_file in XML_DIR.glob("*.xml"):
    m = re.search(
        r"<dc:identifier>(10\.1590/[^<]+)</dc:identifier>", xml_file.read_text()
    )
    if m:
        parquet_dois.add(m.group(1).lower())

print(f"\nUnique DOIs found in scielo_preprints.parquet XMLs: {len(parquet_dois)}")

# %%
# Map DOI -> ppr_id and load full metadata from JSONL
doi_to_ppr = {}
meta = {}
for line in METADATA_JSONL.read_text().splitlines():
    rec = json.loads(line)
    meta[rec["ppr_id"]] = rec
    if rec.get("doi"):
        doi_to_ppr[rec["doi"].lower()] = rec["ppr_id"]

ppr_in_parquet = {doi_to_ppr[d] for d in parquet_dois if d in doi_to_ppr}

# %%
# Report overlap per split
print("\nOverlap with scielo_preprints.parquet by split:")
print(f"{'split':<8} {'in_parquet':>12} {'not_in_parquet':>16} {'total':>8}")
print("-" * 48)
by_split: collections.defaultdict[str, dict[str, list[str]]] = collections.defaultdict(
    lambda: {"in_parquet": [], "not_in_parquet": []}
)
for ppr_id, split in split_map.items():
    key = "in_parquet" if ppr_id in ppr_in_parquet else "not_in_parquet"
    by_split[split][key].append(ppr_id)

for split in ("train", "val", "test"):
    counts = by_split[split]
    total = len(counts["in_parquet"]) + len(counts["not_in_parquet"])
    print(
        f"{split:<8} {len(counts['in_parquet']):>12} {len(counts['not_in_parquet']):>16} {total:>8}"
    )

all_in = sum(len(v["in_parquet"]) for v in by_split.values())
all_not = sum(len(v["not_in_parquet"]) for v in by_split.values())
print(f"{'TOTAL':<8} {all_in:>12} {all_not:>16} {all_in + all_not:>8}")

print(f"\n→ Max clean test size (no parquet overlap): {all_not}")

# %%
# Stratified reshuffle: within each language group, all non-parquet records go to test;
# in-parquet records are split into train/val preserving the original train:val ratio.
#
# Note: the original split was stratified on language. The non-parquet records are
# reasonably distributed across languages, but 'fr' has no non-parquet records so it
# will only appear in train/val after reshuffling.

# Original train:val ratio from current split (excluding test)
n_orig_train = len(by_split["train"]["in_parquet"]) + len(
    by_split["train"]["not_in_parquet"]
)
n_orig_val = len(by_split["val"]["in_parquet"]) + len(by_split["val"]["not_in_parquet"])
val_fraction = n_orig_val / (n_orig_train + n_orig_val)
print(
    f"\nOriginal train:val ratio — train={n_orig_train}, val={n_orig_val} "
    f"(val fraction={val_fraction:.2f})"
)

# Group records by language
by_lang: collections.defaultdict[str, dict[str, list[str]]] = collections.defaultdict(
    lambda: {"in_parquet": [], "not_in_parquet": []}
)
for ppr_id in split_map:
    lang = meta.get(ppr_id, {}).get("language", "") or "(empty)"
    key = "in_parquet" if ppr_id in ppr_in_parquet else "not_in_parquet"
    by_lang[lang][key].append(ppr_id)

rng = random.Random(RANDOM_SEED)
new_split = {}

for lang, groups in sorted(by_lang.items()):
    # All non-parquet → test
    for ppr_id in groups["not_in_parquet"]:
        new_split[ppr_id] = "test"

    # In-parquet → split into val/train at original ratio
    in_p = groups["in_parquet"][:]
    rng.shuffle(in_p)
    n_val = round(len(in_p) * val_fraction)
    for ppr_id in in_p[:n_val]:
        new_split[ppr_id] = "val"
    for ppr_id in in_p[n_val:]:
        new_split[ppr_id] = "train"

print("\nReshuffled split distribution:")
print(dict(collections.Counter(new_split.values())))
print("(test contains zero parquet-overlapping records)")

# Language breakdown of new split
print(f"\n{'language':<12} {'train':>7} {'val':>7} {'test':>7}")
for lang in sorted(by_lang):
    lang_counts: collections.Counter[str] = collections.Counter(
        new_split[p]
        for p in by_lang[lang]["in_parquet"] + by_lang[lang]["not_in_parquet"]
    )
    print(
        f"{lang:<12} {lang_counts['train']:>7} {lang_counts['val']:>7} {lang_counts['test']:>7}"
    )

# %%
# Write new split CSV
lines = ["ppr_id,split"] + [
    f"{ppr_id},{split}" for ppr_id, split in sorted(new_split.items())
]
NEW_SPLIT_CSV.write_text("\n".join(lines) + "\n")
print(f"\nSaved reshuffled split to {NEW_SPLIT_CSV}")
