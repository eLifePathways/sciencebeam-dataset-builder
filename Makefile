INPUT_DIR ?= ./sciencebeam_dataset_builder/split_parquet_files/input_files
OUTPUT_DIR ?= ./output
SPLIT_OUTPUT_DIR ?= ./sciencebeam_dataset_builder/split_parquet_files/output_files
CORPUS_OUTPUT_DIR ?= $(OUTPUT_DIR)/archive-cut

.PHONY: install lint format run test build clean typecheck metadata split explore-scielo-preprints-jats split-parquet archive-cut archive-cut-render archive-cut-publish

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

split-parquet:
	uv run python sciencebeam_dataset_builder/split_parquet_files/split_parquet.py \
		--input-dir $(INPUT_DIR) \
		--output-dir $(SPLIT_OUTPUT_DIR)

scielo-preprints-upload-to-hf:
	uv run hf upload elifepathways/sciencebeam-v2-benchmarking \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset \
		scielo-preprints-jats \
		--type dataset


biorxiv-jats-upload-to-hf:
	uv run hf upload elifepathways/sciencebeam-v2-benchmarking \
		$(OUTPUT_DIR)/biorxiv-jats-hf-dataset \
		biorxiv-jats \
		--type dataset
