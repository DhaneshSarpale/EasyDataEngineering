# ==================================================================
# DataForge - Makefile. Run `make help` for a summary.
# ==================================================================

PYTHON ?= python3
VENV   ?= .venv
PIP     = $(VENV)/bin/pip
PY      = $(VENV)/bin/python
RECORDS ?= small   # small | medium | full  (see config/data_profiles.yaml)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

.PHONY: setup
setup: $(VENV)/bin/activate ## Create virtualenv and install all dependencies
	$(PIP) install -e ".[spark,api,sftp,dq,files,aws,dev]"
	@echo "Environment ready. Activate with: source $(VENV)/bin/activate"

.PHONY: setup-lite
setup-lite: $(VENV)/bin/activate ## Install minimal deps (no Spark) for quick exploration
	$(PIP) install -e ".[api,files,dev]"

.PHONY: generate-data
generate-data: ## Generate synthetic banking data (RECORDS=small|medium|full)
	$(PY) -m dataforge.data_generation.generate --profile $(RECORDS)

.PHONY: run-local
run-local: ## Run the full local batch pipeline end-to-end (pandas engine)
	$(PY) -m dataforge.pipelines.local_batch_pipeline

.PHONY: run-incremental
run-incremental: ## Run the incremental (watermark + MERGE) pipeline demo
	$(PY) -m dataforge.pipelines.incremental_pipeline

.PHONY: run-dq
run-dq: ## Run the data quality suite and produce a report
	$(PY) -m dataforge.data_quality.run_checks

.PHONY: stream-demo
stream-demo: ## Run the local streaming + fraud detection demo
	$(PY) -m dataforge.streaming.local_stream_demo

.PHONY: api
api: ## Start the local mock REST API (FX rates) on :5001
	$(PY) -m dataforge.ingestion.api.mock_api

.PHONY: test
test: ## Run the full pytest suite
	$(VENV)/bin/pytest

.PHONY: test-fast
test-fast: ## Run tests excluding slow Spark tests
	$(VENV)/bin/pytest -m "not spark"

.PHONY: lint
lint: ## Run ruff linter
	$(VENV)/bin/ruff check src tests scripts

.PHONY: format
format: ## Format code with black + ruff
	$(VENV)/bin/black src tests scripts
	$(VENV)/bin/ruff check --fix src tests scripts

.PHONY: typecheck
typecheck: ## Run mypy static type checks
	$(VENV)/bin/mypy src

.PHONY: clean
clean: ## Remove generated data, caches, and build artifacts
	rm -rf data/generated data/lake data/warehouse logs
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

.PHONY: deploy
deploy: ## Deploy AWS infrastructure via Terraform (dev). WARNING: incurs AWS cost.
	bash scripts/deploy.sh dev

.PHONY: destroy
destroy: ## Destroy AWS infrastructure via Terraform (dev)
	bash scripts/destroy.sh dev
