# %%
# Download the XML for each document in scielo_preprints.parquet from HuggingFace.
# Reads only the `id` and `xml` columns via HTTP range requests.
# Saves each XML as .temp/scielo-preprints-xml/<id>.xml

from pathlib import Path
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem
from tqdm import tqdm

HF_PATH = "datasets/elifepathways/sciencebeam-v2-benchmarking/scielo_preprints.parquet"
OUTPUT_DIR = Path(__file__).parent.parent / ".temp" / "scielo-preprints-xml"

# %%
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

fs = HfFileSystem()
with fs.open(HF_PATH, "rb") as f:
    pf = pq.ParquetFile(f)
    print(f"Row groups: {pf.num_row_groups}")

    total_written = 0
    for i in tqdm(range(pf.num_row_groups), desc="Row groups", unit=" rg"):
        batch = pf.read_row_group(i, columns=["id", "xml"])
        ids = batch.column("id").to_pylist()
        xmls = batch.column("xml").to_pylist()
        for doc_id, xml_str in zip(ids, xmls):
            out_path = OUTPUT_DIR / f"{doc_id}.xml"
            out_path.write_text(xml_str, encoding="utf-8")
            total_written += 1

print(f"Saved {total_written} XML files to {OUTPUT_DIR}")
