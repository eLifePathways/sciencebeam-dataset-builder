# ScienceBeam Dataset Builder

Builds the [`elifepathways/sciencebeam-v2-benchmarking`](https://huggingface.co/datasets/elifepathways/sciencebeam-v2-benchmarking)
dataset: paired source PDF and XML for benchmarking ScienceBeam v2 document conversion.

## The dataset contract

Every source publishes rows in one shared schema, defined once in
[`dataset/schema.py`](sciencebeam_dataset_builder/dataset/schema.py). That module is the
single source of truth - the builders, the migration tooling and the published dataset
card all read from it, so they cannot drift apart.

Identity is carried by three columns rather than one:

| column | meaning |
| --- | --- |
| `source` | which corpus the row came from, so rows stay attributable after configs are concatenated |
| `id` | the source-native identifier, opaque and unique only within a source |
| `uid` | `{source}__{id}`, unique across the whole dataset |

`id` replaces the old `ppr_id` and `article_id` columns. `ppr_id` only ever described
SciELO Preprints, where the value is a EuropePMC accession (`PPR459180`); elsewhere it
held a mangled bioRxiv DOI, a SciELO PID or an OJS galley path. `doi` stays a separate
column because some sources have no DOI, and because bioRxiv's `id` is a
filesystem-safe mangling of its DOI rather than the DOI itself.

Sources and their `id` forms are registered in `SOURCES` in the same module.

## Layout

```
sciencebeam_dataset_builder/
  dataset/               source-agnostic: the contract and the Hub plumbing
    schema.py            canonical schema, field docs, source registry
    normalise.py         map any historical schema onto the canonical one
    split.py             deterministic, schema-preserving train/validation/test split
    card.py              render the Hub dataset card from the schema
    migrate_cli.py       bring published subsets onto the canonical schema
    card_cli.py          generate and upload the dataset card
    split_parquet_cli.py split a source's Parquet files
    removal/             delete manually reviewed documents from the subsets
      removal_list.py    read the `uid,reason` lists under data/removals/
      companions.py      follow a removal into the config that mirrors it
      filter.py          drop the listed rows; prove only those rows went
      remove_cli.py      fetch, filter, write, upload
      verify_cli.py      check the filtered output against its snapshot
  scielo_preprints/      source-specific retrieval and metadata extraction
docs/
  dataset-card-body.md   the hand-written half of the published dataset card
data/                    all dataset content - gitignored, never committed
  input/                 pristine copies as downloaded from the Hub (backup)
  input-current/         snapshot of the Hub taken before a removal run
  removals/              manual-review verdicts, `uid,reason` CSVs
  output/                everything generated locally
    migrated/            migrated Parquet, ready to upload
    removed/             filtered Parquet, ready to upload
    splits/              train/validation/test output from split-parquet
```

The dataset is **private**. Nothing under `data/` may be committed - `.gitignore`
covers the whole directory plus `*.parquet` / `*.pdf` / `*.jsonl` anywhere in the tree.

Only SciELO Preprints has a builder here so far. When a second one lands, the
source-specific packages should move under a `sources/` package.

## Common tasks

```sh
make install
make test
make lint

# SciELO Preprints, end to end
make scielo-preprints-retrieve
make scielo-preprints-metadata
make scielo-preprints-split
make scielo-preprints-hf-dataset

# Split an already-built source into train/validation/test
make split-parquet SOURCE=biorxiv INPUT_DIR=./input

# Regenerate the published dataset card
make dataset-card
```

## Migrating the published subsets

Subsets published before the schema was unified carry one of three older shapes. The
migration renames, adds and re-types columns; it never reorders, drops or rewrites rows,
and it fails if the `xml` or `pdf` payload changes. Each input split file is written back
as the same split file, so no row changes split.

```sh
export HF_TOKEN=...

make migrate-dry-run     # schema changes only, reads Parquet footers
make migrate             # download to data/input, normalise, write data/output/migrated
make migrate-upload      # push the checked local result back to the Hub
```

`make migrate` keeps every download under `data/input/` as a pristine backup, so you
retain a local copy of exactly what the Hub held before migration. Those files are never
written to again, and a re-run reuses them instead of downloading - so repeating a
migration after a code change costs nothing and needs no network.

Add `RUN_ARGS="--source biorxiv"` to work one source at a time.

Migration leaves stored values alone, including the empty strings that earlier builders
wrote for absent metadata. `RUN_ARGS="--nullify-empty"` turns those into nulls; it is off
by default because it rewrites data.

## Removing manually reviewed documents

Manual review of the rendered PDFs finds documents that do not belong in a conversion
benchmark - editorials, records with no abstract or no authors, fragments of papers. The
verdicts arrive as `uid,reason` CSVs, one per review pass, read from `data/removals/`.

They are **not** committed. This repository is public, and a verdict names an
identifiable third-party article and judges it unfit for a benchmark, so the lists are as
private as the dataset they describe and live under the gitignored `data/` tree. Keep the
durable copy alongside the dataset's other exclusion lists in the private Hub repo, not
only on one machine.

```sh
export HF_TOKEN=...

make remove-dry-run   # what would go, per config and split; writes nothing
make remove           # snapshot the Hub, filter, write data/output/removed
make verify-removal   # prove only the listed rows went, offline
make remove-upload    # push the checked local result back to the Hub
```

Removal deletes whole rows and nothing else: surviving rows keep every value and their
original order, which `make verify-removal` proves column by column before anything is
uploaded. It does **not** re-split - split membership is a frozen hash bucket on `uid`,
so removal shrinks the splits and the documents that remain keep the split they were
evaluated under. `data/output/removed/removed-manifest.csv` records what each run
deleted, with its source, split, reason, and whether it was reviewed or removed as a
companion.

### Companion rows

SciELO Preprints is published twice: `scielo-preprints-jats` holds the EuropePMC full
text, `scielo-preprints-metadata` the OAI Dublin Core record for the wider corpus.
Removing a preprint takes both renditions, so the metadata config cannot go on
advertising a document whose reviewed rendition is gone.

Neither identifier can be derived from the other, so the link is resolved from data at
run time: the JATS row's DOI `10.1590/scielopreprints.<N>` gives the OAI preprint number
that keys the metadata row. That is the join the dataset card documents; `dc:identifier`
is unusable for it. A removed preprint whose DOI is absent or shaped differently is
reported rather than guessed at.

`scielo-preprints-metadata` is on the older `id` / `xml` / `pdf` schema and has no `uid`
column, so removal derives one from `source` and `id`. It is 2 GB, and a review of
rendered PDFs usually leaves it untouched, so it is fetched only when it actually has
rows to lose - name it with `--source` to process it regardless.

The Hub snapshot goes to `data/input-current/`, deliberately not `data/input/`, which
holds the pre-migration backup and carries the legacy schema under the same file names.

Every listed `uid` must match a row, so a typo fails the run rather than passing
silently. Re-running over subsets that are already filtered needs `--allow-missing`, as
does a run narrowed with `--source`.

After a removal is uploaded, update the split table in `docs/dataset-card-body.md` and
regenerate the card with `make dataset-card-upload`; the counts there are hand-written.

## Splitting

Split assignment is a SHA-256 hash bucket on `uid`. That makes it independent of the
order input files happen to be read in, and means adding documents to a source never
reshuffles the documents already in it. Pass `--split-map` to reproduce a frozen
assignment exactly.

The default is 20 / 30 / 50 train / validation / test - most of the evaluation budget
sits in `test`, with `train` deliberately small.
