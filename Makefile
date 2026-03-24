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

scielo-preprints-retrieve:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.retrieve_cli \
		$(OUTPUT_DIR) $(RUN_ARGS)

scielo-preprints-metadata:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.metadata_cli \
		$(OUTPUT_DIR)/scielo-preprints $(OUTPUT_DIR)/scielo-preprints-metadata.csv

scielo-preprints-split:
	uv run -m sciencebeam_dataset_builder.scielo_preprints.split_cli \
		$(OUTPUT_DIR)/scielo-preprints-metadata.csv \
		$(OUTPUT_DIR)/scielo-preprints-split.csv $(SPLIT_ARGS)

test:
	uv run pytest

build:
	uv build

clean:
	rm -rf build dist *.egg-info .pytest_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
