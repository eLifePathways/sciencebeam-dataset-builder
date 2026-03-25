# %%
# Inspect the scielo-preprints-jats dataset on HF without downloading full Parquet files.
# Uses the HF datasets library to stream metadata only.

from datasets import load_dataset

REPO = "elifepathways/sciencebeam-v2-benchmarking"
DATA_DIR = "scielo-preprints-jats"

# %%
# Load just enough to see the schema — no data downloaded yet.
ds = load_dataset(REPO, data_dir=DATA_DIR, split="train", streaming=True)

print("Columns:", ds.features)

# %%
# Count rows per split without downloading the full files.
for split in ("train", "validation", "test"):
    split_ds = load_dataset(REPO, data_dir=DATA_DIR, split=split, streaming=True)
    count = sum(1 for _ in split_ds)
    print(f"{split}: {count} rows")

# %%
# Peek at one record (metadata only — skip the pdf bytes for display).
first = next(iter(ds))
for col, val in first.items():
    if col == "pdf":
        print(f"  pdf: <{len(val)} bytes>")
    else:
        print(f"  {col}: {val!r:.120}")
