OUTPUT_DIR ?= ./output

.PHONY: install lint format run test build clean typecheck

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

run-scielo-preprints:
	uv run -m sciencebeam_dataset_builder.scielo_preprints_cli \
		$(OUTPUT_DIR) $(RUN_ARGS)

test:
	uv run pytest

build:
	uv build

clean:
	rm -rf build dist *.egg-info .pytest_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
