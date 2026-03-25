OUTPUT_DIR ?= ./output

.PHONY: install lint format run test build clean typecheck metadata split

install:
	uv sync --frozen

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy sciencebeam_dataset_builder

typecheck:
	uv run mypy sciencebeam_dataset_builder

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest

hf-login:
	uv run hf auth login

hf-logout:
	uv run hf auth logout

scielo-preprints-retrieve:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.retrieve_cli \
		$(OUTPUT_DIR) $(RUN_ARGS)

scielo-preprints-metadata:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.metadata_cli \
		$(OUTPUT_DIR)/scielo-preprints $(OUTPUT_DIR)/scielo-preprints-metadata.csv

scielo-preprints-hf-dataset:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.hf_dataset_cli \
		$(OUTPUT_DIR)/scielo-preprints \
		$(OUTPUT_DIR)/scielo-preprints-split.csv \
		$(OUTPUT_DIR)/scielo-preprints-metadata.csv \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset $(RUN_ARGS)

scielo-preprints-split:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.split_cli \
		$(OUTPUT_DIR)/scielo-preprints-metadata.csv \
		$(OUTPUT_DIR)/scielo-preprints-split.csv $(SPLIT_ARGS)

scielo-preprints-upload-to-hf:
	uv run hf upload elifepathways/sciencebeam-v2-benchmarking \
		$(OUTPUT_DIR)/scielo-preprints-hf-dataset \
		scielo-preprints-jats \
		--type dataset
