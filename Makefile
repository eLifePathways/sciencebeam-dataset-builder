HF_DATASET ?= elifepathways/sciencebeam-v2-benchmarking

# Xet's cas::get_reconstruction returns 416 Range Not Satisfiable partway through some
# of this repo's larger Parquet files and then wedges the transfer instead of failing.
# Plain HTTP has no such problem, so force it. Remove once hf_xet handles these files.
export HF_HUB_DISABLE_XET = 1
# Fail a stalled read instead of blocking forever, so retries can actually fire.
export HF_HUB_DOWNLOAD_TIMEOUT = 30

# All dataset content stays under DATA_DIR, which is gitignored - the dataset is
# private and must never be committed. INPUT_DIR holds pristine downloads (backup);
# everything generated goes under OUTPUT_DIR.
DATA_DIR ?= ./data
INPUT_DIR ?= $(DATA_DIR)/input
OUTPUT_DIR ?= $(DATA_DIR)/output
SPLIT_OUTPUT_DIR ?= $(OUTPUT_DIR)/splits
MIGRATE_DIR ?= $(OUTPUT_DIR)/migrated
CORPUS_OUTPUT_DIR ?= $(OUTPUT_DIR)/archive-cut

.PHONY: install lint format test typecheck \
	explore-scielo-preprints-jats \
	hf-login hf-logout \
	scielo-preprints-retrieve scielo-preprints-metadata scielo-preprints-split \
	scielo-preprints-hf-dataset scielo-preprints-upload-to-hf biorxiv-jats-upload-to-hf \
	split-parquet dataset-card dataset-card-upload \
	migrate-dry-run migrate verify migrate-upload \
	archive-cut archive-cut-render archive-cut-publish

install:
	uv sync --frozen

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy sciencebeam_dataset_builder notebooks

typecheck:
	uv run mypy sciencebeam_dataset_builder notebooks

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest

explore-scielo-preprints-jats:
	uv run python notebooks/explore_scielo_preprints_jats.py

hf-login:
	uv run hf auth login

hf-logout:
	uv run hf auth logout

scielo-preprints-retrieve:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.retrieve_cli \
		$(OUTPUT_DIR) $(RUN_ARGS)

scielo-preprints-metadata:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.metadata_cli \
		$(OUTPUT_DIR)/scielo-preprints $(OUTPUT_DIR)/scielo-preprints-metadata.jsonl

scielo-preprints-split:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.split_cli \
		$(OUTPUT_DIR)/scielo-preprints-metadata.jsonl \
		$(OUTPUT_DIR)/scielo-preprints-split.csv $(SPLIT_ARGS)

scielo-preprints-hf-dataset:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.hf_dataset_cli \
		$(OUTPUT_DIR)/scielo-preprints \
		$(OUTPUT_DIR)/scielo-preprints-split.csv \
		$(OUTPUT_DIR)/scielo-preprints-metadata.jsonl \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset $(RUN_ARGS)

# Everything corpus-specific arrives via CONFIG, so no repo id, stratum value or
# count belongs in this file. Pass extra flags through RUN_ARGS, e.g. --plan-only.
archive-cut:
	uv run -m sciencebeam_dataset_builder.archive_cut.cut_cli \
		$(CONFIG) $(CORPUS_OUTPUT_DIR) $(RUN_ARGS)

# Needs LibreOffice on PATH, which is why it is a step of its own.
archive-cut-render:
	uv run -m sciencebeam_dataset_builder.archive_cut.render_cli \
		$(CORPUS_OUTPUT_DIR) $(RUN_ARGS)

archive-cut-publish:
	uv run -m sciencebeam_dataset_builder.archive_cut.publish_cli \
		$(CORPUS_OUTPUT_DIR) $(RUN_ARGS)

# Split one source's Parquet files into train/validation/test.
# SOURCE is required, e.g. `make split-parquet SOURCE=biorxiv`.
split-parquet:
	uv run -m sciencebeam_dataset_builder.dataset.split_parquet_cli \
		--input-dir $(INPUT_DIR) \
		--output-dir $(SPLIT_OUTPUT_DIR) \
		--source $(SOURCE) $(RUN_ARGS)

# Regenerate the Hub dataset card from the canonical schema + docs/dataset-card-body.md.
dataset-card:
	uv run -m sciencebeam_dataset_builder.dataset.card_cli $(OUTPUT_DIR)/README.md

dataset-card-upload:
	uv run -m sciencebeam_dataset_builder.dataset.card_cli $(OUTPUT_DIR)/README.md \
		--repo-id $(HF_DATASET) --upload

# Bring the published subsets onto the canonical schema. Inspect, then upload.
migrate-dry-run:
	uv run -m sciencebeam_dataset_builder.dataset.migrate_cli $(MIGRATE_DIR) \
		--input-dir $(INPUT_DIR) --repo-id $(HF_DATASET) --dry-run $(RUN_ARGS)

migrate:
	uv run -m sciencebeam_dataset_builder.dataset.migrate_cli $(MIGRATE_DIR) \
		--input-dir $(INPUT_DIR) --repo-id $(HF_DATASET) $(RUN_ARGS)

# Prove the migration preserved the data, offline, before anything is uploaded.
verify:
	uv run -m sciencebeam_dataset_builder.dataset.verify_cli \
		--input-dir $(INPUT_DIR) --output-dir $(MIGRATE_DIR) $(RUN_ARGS)

migrate-upload:
	uv run -m sciencebeam_dataset_builder.dataset.migrate_cli $(MIGRATE_DIR) \
		--input-dir $(INPUT_DIR) --repo-id $(HF_DATASET) --upload $(RUN_ARGS)

scielo-preprints-upload-to-hf:
	uv run hf upload $(HF_DATASET) \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset \
		scielo-preprints-jats \
		--type dataset


biorxiv-jats-upload-to-hf:
	uv run hf upload $(HF_DATASET) \
		$(OUTPUT_DIR)/biorxiv-jats-hf-dataset \
		biorxiv-jats \
		--type dataset
