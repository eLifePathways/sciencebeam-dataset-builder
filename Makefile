OUTPUT_DIR ?= ./output

.PHONY: install lint format run test build clean typecheck metadata split explore-scielo-preprints-jats

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

scielo-preprints-hf-dataset:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.hf_dataset_cli \
		$(OUTPUT_DIR)/scielo-preprints \
		$(OUTPUT_DIR)/scielo-preprints-split.csv \
		$(OUTPUT_DIR)/scielo-preprints-metadata.jsonl \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset $(RUN_ARGS)

scielo-preprints-split:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.split_cli \
		$(OUTPUT_DIR)/scielo-preprints-metadata.jsonl \
		$(OUTPUT_DIR)/scielo-preprints-split.csv $(SPLIT_ARGS)

scielo-preprints-upload-to-hf:
	uv run hf upload elifepathways/sciencebeam-v2-benchmarking \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset \
		scielo-preprints-jats \
		--type dataset
