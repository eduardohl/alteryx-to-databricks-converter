.DEFAULT_GOAL := help
.PHONY: help install dev test test-cov lint format typecheck clean all frontend serve run lock deploy-dev deploy-prod bundle-validate

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install package
	pip install -e .

dev: ## Install with all dev dependencies
	pip install -e ".[all]"

test: ## Run all tests (no coverage; faster)
	pytest tests/ --no-cov

test-cov: ## Run tests with coverage report (term + html)
	pytest tests/ --cov-report=html

lint: ## Lint with ruff
	ruff check src/ tests/ server/

format: ## Format code with ruff
	ruff format src/ tests/ server/

typecheck: ## Type check with mypy
	mypy src/a2d/
	mypy server/

clean: ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info htmlcov/ .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

all: lint typecheck test ## Lint + typecheck + test

frontend: ## Build React frontend
	cd frontend && npm install && npm run build

serve: ## Start FastAPI dev server
	PYTHONPATH=src:. uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload

run: dev frontend serve ## Full setup and run

lock: ## Generate requirements.lock
	pip-compile --strip-extras -o requirements.lock pyproject.toml

deploy-dev: frontend ## Deploy to Databricks Apps (dev)
	databricks bundle deploy -t dev

deploy-prod: frontend ## Deploy to Databricks Apps (prod)
	databricks bundle deploy -t prod

bundle-validate: ## Validate DAB configuration
	databricks bundle validate
