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
  scielo_preprints/      source-specific retrieval and metadata extraction
docs/
  dataset-card-body.md   the hand-written half of the published dataset card
```

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
make migrate             # download, normalise, write under ./output/migrated
make migrate-upload      # push the checked local result back to the Hub
```

Add `RUN_ARGS="--source biorxiv"` to work one source at a time.

Migration leaves stored values alone, including the empty strings that earlier builders
wrote for absent metadata. `RUN_ARGS="--nullify-empty"` turns those into nulls; it is off
by default because it rewrites data.

## Splitting

Split assignment is a SHA-256 hash bucket on `uid`. That makes it independent of the
order input files happen to be read in, and means adding documents to a source never
reshuffles the documents already in it. Pass `--split-map` to reproduce a frozen
assignment exactly.

The default is 20 / 30 / 50 train / validation / test - most of the evaluation budget
sits in `test`, with `train` deliberately small.
