.PHONY: check-secrets format lint test

check-secrets:
	python scripts/check_secrets.py

format:
	ruff format src tests

lint:
	ruff check src tests

test:
	pytest
