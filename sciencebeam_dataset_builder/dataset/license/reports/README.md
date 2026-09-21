# Licence audit

Which documents in `elifepathways/sciencebeam-v2-benchmarking` may be used to train a
model, and which may not.

Audited snapshot: `data/2026-09-18`, all 2,981 documents across all seven configs.
Regenerate with `make licence-audit`.

## Result

**702 of 2,981 documents (23.5%) are excluded from training under the current policy.**
A further 301 are usable only if the trained model is never used commercially.

| verdict | documents | share | train | validation | test |
| --- | ---: | ---: | ---: | ---: | ---: |
| permissive — CC BY, CC0 | 1,978 | 66.4% | 673 | 538 | 767 |
| **unknown — no licence stated** | **610** | **20.5%** | 125 | 171 | 314 |
| non-commercial — CC BY-NC | 301 | 10.1% | 62 | 77 | 162 |
| **no-derivatives — CC BY-ND, CC BY-NC-ND** | **91** | **3.1%** | 21 | 23 | 47 |
| **all rights reserved** | **1** | 0.0% | 0 | 0 | 1 |
| total | 2,981 | | 881 | 809 | 1,291 |

Bold rows are excluded under the current policy (`NO_DERIVATIVES_BLOCKS_TRAINING = True`,
`NON_COMMERCIAL_BLOCKS_TRAINING = False`).

## Licence distribution

| licence | documents | share |
| --- | ---: | ---: |
| CC BY 4.0 | 1,978 | 66.4% |
| no licence — template placeholder | 477 | 16.0% |
| CC BY-NC 4.0 | 301 | 10.1% |
| no licence — no permissions element | 131 | 4.4% |
| CC BY-NC-ND 4.0 | 89 | 3.0% |
| CC BY-ND 4.0 | 2 | 0.1% |
| unparseable xml | 2 | 0.1% |
| All Rights Reserved | 1 | 0.0% |

No document in the corpus is CC0, so the dataset cannot be published as CC0 — see
[Two things to correct](#two-things-to-correct).

## Licence by config

| config | documents | licence |
| --- | ---: | --- |
| `ore-jats` | 192 | 100% CC BY 4.0 |
| `scielo-preprints-jats` | 418 | 100% CC BY 4.0 |
| `scielo-preprints-metadata` | 997 | 100% CC BY 4.0 |
| `scielo_br-jats` | 619 | 51.4% CC BY 4.0 · 45.9% CC BY-NC 4.0 · 2.7% CC BY-NC-ND 4.0 |
| `biorxiv-jats` | 145 | 49.7% CC BY-NC-ND 4.0 · 36.6% CC BY 4.0 · 11.7% CC BY-NC 4.0 · 1.4% CC BY-ND 4.0 · 1 All Rights Reserved |
| `pkp-jats` | 477 | **no licence — 100%** |
| `scielo_mx-jats` | 133 | **no licence — 100%** (2 of them unparseable) |

Three configs are entirely permissive and safe to expand freely — `ore-jats` and both
SciELO Preprints configs, 1,607 documents between them, 81% of everything usable today.
`biorxiv-jats` is the opposite: only 37% of it is permissive, and half carries ND.

## Policy options

Whether ND and NC block training is a policy decision, not a fact about the documents,
so it is two flags in `classify_license.py`. Changing either re-decides the corpus
without re-reading any Parquet. The four combinations:

| option | ND blocks | NC blocks | usable | excluded | usable train rows |
| --- | :---: | :---: | ---: | ---: | ---: |
| A — strictest | yes | yes | 1,978 (66.4%) | 1,003 | 673 of 881 |
| **B — current** | **yes** | **no** | **2,279 (76.5%)** | **702** | **735 of 881** |
| C | no | yes | 2,069 (69.4%) | 912 | 694 of 881 |
| D — most permissive | no | no | 2,370 (79.5%) | 611 | 756 of 881 |

Usable documents per config under each option:

| config | total | A | B (current) | C | D |
| --- | ---: | ---: | ---: | ---: | ---: |
| `biorxiv-jats` | 145 | 53 | 70 | 127 | 144 |
| `ore-jats` | 192 | 192 | 192 | 192 | 192 |
| `pkp-jats` | 477 | 0 | 0 | 0 | 0 |
| `scielo-preprints-jats` | 418 | 418 | 418 | 418 | 418 |
| `scielo-preprints-metadata` | 997 | 997 | 997 | 997 | 997 |
| `scielo_br-jats` | 619 | 318 | 602 | 335 | 619 |
| `scielo_mx-jats` | 133 | 0 | 0 | 0 | 0 |

What the options actually turn on:

- **Moving B → A** (also block NC) costs 301 documents, almost all of them
  `scielo_br-jats`, which drops from 602 to 318. Worth it only if the trained model may
  be used commercially — that is a question about ScienceBeam, not about the corpus.
- **Moving B → D** (stop blocking ND) gains 91 documents, mostly `biorxiv-jats`, which
  more than doubles from 70 to 144. This is the contested reading: ND forbids
  distributing adaptations, and whether model weights are an adaptation of the training
  text is unsettled.
- **No option recovers `pkp-jats` or `scielo_mx-jats`.** Those 610 documents state no
  licence at all, and that is not a policy dial — silence is not permission.

The spread between the strictest and the most permissive option is 392 documents, 13%
of the corpus. The 610 unlicensed documents are a bigger prize than either flag.

## `pkp-jats` is unattributable, not merely unlicensed

This is the finding that needs a decision from outside this repo.

All 477 `pkp-jats` documents carry JATS in which every identifying field is an
unsubstituted template placeholder left behind by the PKP JATS plugin:

```xml
<copyright-statement>© 2015 copyright-statement</copyright-statement>
<publisher-name>publisher-name</publisher-name>
<journal-title>journal-title</journal-title>
<issn>0000-0000</issn>
```

The same fake export date (`2015-11-18`) appears on every row, and `doi`, `title` and
`xml_source_url` are all null. Nothing in the dataset identifies which journal, which
publisher or which rightsholder a `pkp` document belongs to, so its licence cannot be
looked up and its owner cannot be asked.

`pkp-jats` is 16% of the corpus and was already the config that lost the most to manual
review — 223 of its original 700. Recovering the original harvest provenance (the OJS
instances and galley URLs the builder walked) is the only route to clearing it, and that
builder is not in this repository.

`scielo_mx-jats` is a smaller version of the same problem: 133 documents with no
`<permissions>` element at all, spread across 33 journals and 25 publishers. Here the
SciELO PID does identify the article, so the licence is recoverable per journal from
SciELO itself — 25 lookups, not 133.

Two `scielo_mx` documents are additionally not well-formed XML (a bad token mid-document,
beyond the entity repair the extractor does). That is a data-quality issue rather than a
licence one; both are excluded as unknown either way.

## Two things to correct

- **The published card declares the wrong licence.** `card.py` sets
  `LICENSE = "cc0-1.0"`, so the Hub card claims the whole dataset is CC0. Not one
  document in the corpus is CC0: it holds 91 NoDerivatives, 301 NonCommercial, one that
  reserves all rights, and 610 with no licence at all. This is a public claim that is
  wrong today, independent of any training decision.
- **The `license` column is null for five of seven configs.** The dataset card already
  records this as pending a backfill from the stored `xml`. `extract_license.py` is that
  backfill: the values exist now and only need writing into the column.

## Recommended path: tag, do not delete

Keep every row in the benchmark and filter at training time.

The removal pipeline exists and could be pointed at these 702 documents. It should not
be:

- **Benchmarking is not training.** A licence constrains what a model is trained on. It
  does not constrain holding a PDF/XML pair to measure conversion accuracy. Deleting
  these rows would cost evaluation coverage and gain nothing legally.
- **It would gut the benchmark.** `biorxiv-jats` would drop from 145 documents to 70,
  and `pkp-jats` and `scielo_mx-jats` would disappear entirely — 610 documents whose
  licences are unknown, not known to be unusable.
- **Removal is irreversible and does not re-split.** A publisher clarifying its terms, or
  a change to either policy flag above, could not be undone.
- **It conflates two different judgements.** The removal manifest means "unfit for a
  conversion benchmark". A licence verdict is a separate axis and belongs in a separate
  list.

Concretely: backfill `license` for every config from `extract_license.py`, publish
`training-exclusions.csv` alongside the dataset, and have the training pipeline filter on
it.

## Open decisions

1. **Does NoDerivatives block training?** Contested. Decides 91 documents, 74 of them in
   `biorxiv-jats`. Currently treated as blocking.
2. **Does NonCommercial block training?** Depends on whether the resulting model is used
   commercially, which is a question about ScienceBeam rather than about the documents.
   Decides 301 documents, 284 of them in `scielo_br-jats`. Currently not blocking.
3. **Can the `pkp` harvest provenance be recovered?** Decides whether 477 documents are
   recoverable or permanently unusable. The largest single question here.
4. **Is a per-journal SciELO Mexico licence lookup worth 25 queries?** Would clear or
   condemn 133 documents.

Decisions 1 and 2 are the two flags in `classify_license.py`.

## Where to expand first

Ranked by licence risk, for when the corpus grows:

1. **Open Research Europe and SciELO Preprints** — 100% CC BY 4.0, no per-document
   check needed beyond the audit that already runs.
2. **SciELO Brazil** — mixed but always explicit, so every new document self-declares;
   roughly half will be NC.
3. **bioRxiv** — explicit but ND-heavy: expect only ~37% of anything harvested to be
   permissive, and budget for that.
4. **OJS/PKP** — do not expand until the provenance question is answered. More documents
   through the same builder would add more unattributable rows.

## Files

| file | contents |
| --- | --- |
| `licence-per-document.csv` | every document with its licence, verdict and the evidence it was read from |
| `training-exclusions.csv` | the 702 excluded documents, in the `uid,reason` shape the removal lists use |
| `training-non-commercial.csv` | the 301 CC BY-NC documents, pending decision 2 |

## Method

Licences are read from the `xml` the dataset already stores, so the audit needs no
network and covers every document rather than a sample. Three renditions are parsed:
JATS `<license>`, NISO `<ali:license_ref>` and Dublin Core `<dc:rights>`. Documents that
fail to parse are repaired for undefined HTML entities and illegal control characters
before being given up on.

Silence is reported in three distinguishable kinds rather than one "unknown", because
they are three different problems: a real copyright statement with no licence, no
permissions element at all, and an unsubstituted template placeholder. Collapsing them
would have hidden what `pkp-jats` actually is.

This supersedes the sample analysis in
[ScienceBeam2.0#45](https://github.com/eLifePathways/ScienceBeam2.0/issues/45), which
read 147 loose XML files. That analysis was directionally right; three things change over
the full corpus:

- `scielo-preprints-metadata` does carry a licence, CC BY 4.0 in `dc:rights`. It had no
  rows in the sample and is the largest config.
- `pkp`'s missing licence is not a format quirk but a template placeholder, and the same
  placeholders erase the publisher and journal too.
- One bioRxiv document explicitly reserves all rights.

Absence of a licence is reported as absence, never as permission.
