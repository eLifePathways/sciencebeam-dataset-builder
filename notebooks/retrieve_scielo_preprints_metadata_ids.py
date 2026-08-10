# %%
# Retrieve only the `id` column from the scielo_preprints.parquet file on HuggingFace.
# Uses HfFileSystem + pyarrow to fetch only the id column via HTTP range requests,
# with per-row-group progress via tqdm.

from pathlib import Path
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem
from tqdm import tqdm

HF_PATH = "datasets/elifepathways/sciencebeam-v2-benchmarking/scielo_preprints.parquet"
OUTPUT_PATH = (
    Path(__file__).parent.parent / ".temp" / "scielo-preprints-metadata-ids.txt"
)

# %%
fs = HfFileSystem()
with fs.open(HF_PATH, "rb") as f:
    pf = pq.ParquetFile(f)
    print("Schema:", pf.schema_arrow)
    print(f"Row groups: {pf.num_row_groups}")

    ids = []
    for i in tqdm(range(pf.num_row_groups), desc="Row groups", unit=" rg"):
        batch = pf.read_row_group(i, columns=["id"])
        ids.extend(batch.column("id").to_pylist())

print(f"Retrieved {len(ids)} ids")

# %%
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_PATH, "w") as out:
    out.write("\n".join(ids) + "\n")

print(f"Saved to {OUTPUT_PATH}")
