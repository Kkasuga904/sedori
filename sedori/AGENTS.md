# Repository Guidelines

## Project Structure & Module Organization
This repository automates the sedori (retail arbitrage) workflow. Keep agent entrypoints under `src/agents/`, grouped by task (for example `scrape`, `profit`, `listing`, `inventory`, `reporting`). Shared adapters (e-commerce APIs, Selenium drivers, Slack/LINE notifiers) live in `src/services/`. Place reusable utilities in `src/common/` to avoid circular imports. Configuration defaults belong in `config/settings.yml` with environment-specific overrides in `config/env/`. Store domain docs and runbooks in `docs/`, and mirror the source layout under `tests/` for pytest suites.

## Build, Test, and Development Commands
Create a virtual environment with `python -m venv .venv` and activate it before installing tools. Install dependencies using `pip install -r requirements.txt`. Run an agent locally with `python -m agents.cli --task scrape --category electronics` to exercise the CLI. Execute the full regression suite via `pytest` and add `pytest --cov=src` before opening a PR. Format and lint with `ruff format src tests` followed by `ruff check src tests` (or `make format` / `make lint` if you prefer the make targets).

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation and annotate public functions with type hints. Name packages and modules in snake_case, classes in CapWords, and CLI flags with hyphenated verbs (`--profit-check`). Limit functions to 50 lines; move complex flows into helpers within `src/common/`. Document non-obvious business rules with concise docstrings and sparing inline comments. Run `ruff` and `black` (or `make format`) before pushing to keep diffs tight.

## Testing Guidelines
Author tests with `pytest`, mirroring the source path (e.g., `src/agents/scrape` -> `tests/agents/scrape/test_scrape.py`). Maintain >=85% branch coverage, focusing on price-difference heuristics and stock alerts. Use `pytest -k task_name` for focused runs and `pytest --maxfail=1 --disable-warnings` in CI. Mock outbound HTTP and place sanitized fixtures or cassettes under `assets/cassettes/`.

## Commit & Pull Request Guidelines
Adopt Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) in imperative mood. Reference issue IDs in subjects when applicable (e.g., `feat: add mercari scraper (#42)`). Every PR needs a summary of changes, test proof (`pytest` output line), and terminal captures or screenshots if CLI UX changes. Request review from another maintainer and resolve todos before merging.
