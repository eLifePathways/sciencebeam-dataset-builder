## Splits

Every config is split `train` / `validation` / `test`. The benchmarking convention is
20 / 30 / 50 - most of the evaluation budget sits in `test`, with `train` deliberately
small.

Split membership is frozen. New assignments are a SHA-256 hash bucket on `uid`, so
harvesting more documents into a source never reshuffles the documents already in it.

Counts reflect the removal of documents that manual review found unfit for a conversion
benchmark: records with no abstract or no authors, editorials and other non-articles, and
pairs whose XML and PDF do not carry the same content - different versions of a paper,
different languages, or missing body sections. Removal deletes whole rows and never
re-splits, so a document that remains keeps the split it was always in. `pkp-jats` lost
the most by far, 223 of its original 700.

| config | train | validation | test | total |
| --- | --- | --- | --- | --- |
| `biorxiv-jats` | 29 | 41 | 75 | 145 |
| `ore-jats` | 37 | 58 | 97 | 192 |
| `pkp-jats` | 95 | 132 | 250 | 477 |
| `scielo_br-jats` | 123 | 181 | 315 | 619 |
| `scielo_mx-jats` | 30 | 39 | 64 | 133 |
| `scielo-preprints-jats` | 83 | 125 | 210 | 418 |
| `scielo-preprints-metadata` | 484 | 233 | 280 | 997 |

`scielo-preprints-metadata` is the exception twice over: it is roughly 49 / 24 / 28
rather than 20 / 30 / 50, because only the documents it shares with
`scielo-preprints-jats` carry a 20 / 30 / 50 label and the rest were assigned by an
earlier hash bucketing that used different fractions; and it still carries the legacy
three-column schema (see Schema exceptions). Do not assume a uniform test fraction when
pooling this config with the others.

Its metadata is not lost, only unextracted: `dc:title`, `dc:creator`, `dc:date`,
`dc:identifier`, `dc:subject`, `dc:language` and `dc:type` all sit in its `xml` column.
Migrating it is worth doing together with a Dublin Core extraction pass rather than
before one, since migration alone would add three populated columns and seventeen null
ones.

## SciELO Preprints

The two SciELO Preprints configs differ in what the `xml` column holds - see
`xml_format`. `scielo-preprints-metadata` carries the OAI Dublin Core record
(`<oai_dc:dc>`, no body) and is the wider corpus; `scielo-preprints-jats` carries full
JATS and is the subset for which a EuropePMC full text exists.

**Both use the same split labels**, so a document held out in one is held out in the
other. The assignment is persisted to `splits/scielo-preprints-splits.jsonl`
(`id`, `split`, `has_jats`, ...).

Join key is the OAI preprint number: metadata `..._preprint_<N>` <-> JATS DOI
`10.1590/scielopreprints.<N>`. Do **not** join on `dc:identifier` - 247 of the metadata
records carry the journal DOI of the published version instead, and 51 carry none.

Exclusions live in `splits/exclude_ids.txt` (source-level, applies to both configs) and
`splits/exclude_jats_ids.txt` (broken JATS rendition only; the metadata record stays and
keeps its split label).

### Known gap

The metadata harvest covers preprint ids 7-2480 only. It is complete within that range,
but the JATS corpus reaches id 7264, so 135 of its 422 documents have no metadata record
yet. Continuing the OAI harvest past 2480 would make metadata a true superset.

## Field coverage

Applies to the `canonical` configs. Only `scielo_preprints` has every metadata field populated, because EuropePMC supplies
retrieval provenance and a licence alongside the JATS. For the other sources
`license`, `version`, `xml_source_url`, `pdf_source_url`, `xml_downloaded_at` and
`pdf_downloaded_at` are null pending a backfill pass that re-derives them from the
stored `xml`. `subject_europepmc_category` is null for every non-EuropePMC source by
definition - no other publisher emits that `subj-group-type`.

`xml` and `pdf` are never null in any config.

## Legacy files

These predate the unified schema and are **not** registered configs. They carry a
three-column `id` / `xml` / `pdf` schema, hold a different and smaller set of rows than
the config of the same name, and are kept only so existing references do not break. Use
the configs above instead.

- `biorxiv.parquet`, `ore.parquet`, `pkp.parquet`, `scielo_br.parquet`, `scielo_mx.parquet`
- `biorxiv-jats.parquet` - superseded by the `biorxiv-jats` config
- `scielo-preprints-metadata-raw/` - the per-document XML and PDF staging area for
  `scielo-preprints-metadata`

## Loading

```python
from datasets import load_dataset

ds = load_dataset("elifepathways/sciencebeam-v2-benchmarking", "scielo-preprints-jats")
print(ds["test"][0]["uid"])
```
