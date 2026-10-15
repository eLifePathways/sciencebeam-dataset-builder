"""A source snapshot, a licence report and published repos, for tests of adding."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.corpus.additions import BuildInputs
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._repos import publish

LICENCES = {
    "biorxiv__a": "CC BY 4.0",
    "biorxiv__b": "CC BY-NC 4.0",
    "biorxiv__n1": "CC BY 4.0",
    "biorxiv__n2": "CC BY-NC 4.0",
    "biorxiv__n4": "CC BY 4.0",
    "biorxiv__n5": "CC BY 4.0",
    "ore__o1": "CC BY-NC 4.0",
}


def write_snapshot(data_dir: Path, folder: str, by_split: dict[str, list[str]]) -> None:
    for split, ids in by_split.items():
        path = data_dir / "2026-01-01" / folder / f"{split}-00000-of-00001.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"id": ids}), path)


def addition_world(tmp_path: Path) -> tuple[list[Repo], BuildInputs]:
    """biorxiv's a published open, b and c restricted; n1-n5 only in the snapshot,
    n3 with no licence recorded and n4 not harvested at all; ore's o1 non-commercial."""
    data = tmp_path / "data"
    write_snapshot(
        data,
        "biorxiv-jats",
        {"train": ["a", "b", "n1", "n2", "n5"], "test": ["c", "n3"]},
    )
    write_snapshot(data, "ore-jats", {"test": ["o1"]})
    report = tmp_path / "licences.csv"
    report.write_text(
        "uid,licence\n" + "".join(f"{u},{lic}\n" for u, lic in LICENCES.items()),
        encoding="utf-8",
    )
    hub = tmp_path / "hub"
    repos = [
        publish(hub, CORPORA["biorxiv"], Tier.OPEN, {"train": ["a"]}),
        publish(
            hub, CORPORA["biorxiv"], Tier.RESTRICTED, {"train": ["b"], "test": ["c"]}
        ),
        publish(hub, CORPORA["ore"], Tier.OPEN, {"train": ["o0"]}),
    ]
    return repos, BuildInputs(data, report)
