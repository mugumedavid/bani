.PHONY: lint format typecheck test all clean docker smoke-test

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

format-check:
	uv run ruff format --check src/ tests/

typecheck:
	uv run mypy --strict src/bani/

test:
	uv run pytest

test-integration:
	uv run pytest -m integration

benchmark:
	uv run pytest benchmarks/ -m benchmark

all: lint format-check typecheck test

docker:
	cd ui && npm run build
	docker compose build bani

smoke-test:
	./scripts/docker/smoke_test.sh

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	rm -rf dist/ build/ *.egg-info
