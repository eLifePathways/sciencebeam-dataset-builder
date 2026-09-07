"""Deleting manually reviewed documents from the published subsets.

Manual review of the rendered PDFs finds documents that should never have been in a
conversion benchmark - editorials, records with no abstract or no authors, fragments of
papers. The reviewers' verdicts live in `data/removals/` as `uid,reason` CSVs, one per
review pass, and this package turns them into filtered Parquet on the Hub.

Those lists judge identifiable third-party articles unfit for a benchmark, so they are
as private as the dataset itself and stay under the gitignored `data/` tree.

The pipeline mirrors the migration next door: fetch a pristine snapshot, write the
result locally, prove it offline, and only then upload.

* :mod:`.removal_list` reads the verdicts;
* :mod:`.filter` deletes the rows and proves only those rows went;
* :mod:`.remove_cli` runs the pipeline; :mod:`.verify_cli` checks its output.

Removal never re-splits. Split membership is a frozen hash bucket on `uid`, so the
documents that stay keep the split they were evaluated under.
"""
