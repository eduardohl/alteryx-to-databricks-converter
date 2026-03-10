.PHONY: install dev test lint format typecheck clean all frontend serve lock

install:
	pip install -e .

dev:
	pip install -e ".[all]"

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=a2d --cov-report=term-missing --cov-report=html

lint:
	ruff check src/ tests/ server/

format:
	ruff format src/ tests/ server/

typecheck:
	mypy src/a2d/
	mypy server/ || true

clean:
	rm -rf build/ dist/ *.egg-info htmlcov/ .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

all: lint typecheck test

frontend:
	cd frontend && npm install && npm run build

serve:
	uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload

lock:
	pip-compile --strip-extras -o requirements.lock pyproject.toml
