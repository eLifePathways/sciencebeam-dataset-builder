# Licence audit

Which documents may be used to train a model, and which may not. Derived from the
licence each document states in its stored `xml`.

Snapshot `data/2026-09-18`, all 2,981 documents. Regenerate with `make licence-audit`.

## Result

**1,003 of 2,981 documents (33.6%) are excluded from training.** The 1,978 that remain
are all CC BY 4.0.

| verdict | documents | share | usable |
| --- | ---: | ---: | :---: |
| permissive — CC BY 4.0 | 1,978 | 66.4% | yes |
| unknown — no licence stated | 610 | 20.5% | no |
| non-commercial — CC BY-NC | 301 | 10.1% | no |
| no-derivatives — CC BY-ND, CC BY-NC-ND | 91 | 3.1% | no |
| all rights reserved | 1 | 0.0% | no |

## By corpus

| config | documents | licence | usable |
| --- | ---: | --- | ---: |
| `ore-jats` | 192 | 100% CC BY 4.0 | 192 |
| `scielo-preprints-jats` | 418 | 100% CC BY 4.0 | 418 |
| `scielo-preprints-metadata` | 997 | 100% CC BY 4.0 | 997 |
| `scielo_br-jats` | 619 | 51.4% CC BY 4.0 · 45.9% CC BY-NC 4.0 · 2.7% CC BY-NC-ND 4.0 | 318 |
| `biorxiv-jats` | 145 | 49.7% CC BY-NC-ND 4.0 · 36.6% CC BY 4.0 · 11.7% CC BY-NC 4.0 · 1.4% CC BY-ND 4.0 · 1 All Rights Reserved | 53 |
| `pkp-jats` | 477 | no licence stated | 0 |
| `scielo_mx-jats` | 133 | no licence stated | 0 |

## The two unlicensed corpora

`pkp-jats` is unattributable, not merely unlicensed. Every identifying field is an
unsubstituted PKP template placeholder — `© 2015 copyright-statement`,
`<publisher-name>publisher-name</publisher-name>`, `<issn>0000-0000</issn>` — and `doi`,
`title` and `xml_source_url` are null. Nothing says which journal or rightsholder a
document belongs to, so its licence cannot be looked up. Recovering the original harvest
provenance is the only route, and that builder is not in this repository.

`scielo_mx-jats` simply has no `<permissions>` element. The SciELO PID still identifies
the article, so the licence is recoverable per journal — 25 publishers, not 133 lookups.
Two of its documents are also not well-formed XML.

Silence is not permission, so both are treated as unusable rather than open.

## Policy

Both flags in `classify_license.py` are on: NoDerivatives and NonCommercial each block
training. NC blocks because the trained model is intended for commercial use.

ND is the one contested reading left. It costs 91 documents, 74 of them in
`biorxiv-jats`; turning it off would take that config from 53 usable to 127. Changing
either flag re-decides the corpus without re-reading any Parquet.

## Notes

The published dataset card declares `LICENSE = "cc0-1.0"` (`card.py`). No document in the
corpus is CC0. That is wrong independently of any training decision.

Licence verdicts do not feed the removal pipeline: a licence limits what may be trained
on, not what may be held to measure conversion accuracy, so the benchmark keeps every row
and training filters on `training-exclusions.csv`.

## Files

| file | contents |
| --- | --- |
| `licence-per-document.csv` | every document with its licence, verdict and evidence |
| `training-exclusions.csv` | the 1,003 excluded documents, as `uid,reason` |
| `training-non-commercial.csv` | the 301 CC BY-NC documents alone, a subset of the above |
