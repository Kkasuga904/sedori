# Repository Guidelines

## Project Structure & Module Organization
This codebase targets CLI-driven automation for the sedori pipeline. Place executable agent flows under `src/agents/`, one subfolder per task (`scrape`, `profit_check`, `list_product`, `stock_check`, `daily_report`). Shared adapters (requests clients, Selenium drivers, Slack/LINE notifiers) live in `src/services/`. Keep cross-cutting utilities in `src/common/` and avoid circular imports. Configuration defaults go in `config/settings.yml`; stage-specific overrides belong in `config/env/`. All user-facing docs, including the Japanese planning notes currently kept as `New Text Document.txt`, belong in `docs/`. Tests mirror the package layout under `tests/`; create directories if they are missing rather than mixing code and tests.

## Build, Test, and Development Commands
Create a virtual environment with `python -m venv .venv` and activate it before installing tooling. Install dependencies via `pip install -r requirements.txt`. Run agents locally with `python -m agents.cli --task scrape --category electronics` to exercise the CLI entry point. Validate the full suite with `pytest` and add `pytest --cov=src` before opening a PR. Use `ruff format src tests` and `ruff check src tests` (or the equivalent `make format` / `make lint` targets) prior to committing.

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation and type hints on public functions. Name modules and packages in snake_case, classes in CapWords, and CLI-facing commands with hyphenated verbs (`profit-check`). Keep functions below 50 lines; promote shared logic into helpers in `src/common/`. Document non-obvious behavior with concise docstrings and inline comments only where business logic is opaque. Run `ruff` and `black` (or `make format`) before pushing.

## Testing Guidelines
Author tests with `pytest`, mirroring package paths (`src/agents/scrape` -> `tests/agents/scrape/test_runner.py`). Target >=85% branch coverage, especially around marketplace pricing heuristics. Use `pytest -k task_name` for focused runs and `pytest --maxfail=1 --disable-warnings` in CI pipelines. Mock outbound network calls; place recorded payloads under `assets/cassettes/` and sanitize identifiers.

## Commit & Pull Request Guidelines
Use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) written in the imperative mood. Reference tracked issue IDs in the subject (e.g., `feat: add mercari scrape agent (#42)`). Each PR must include a short summary, testing evidence (`pytest` line), and screenshots or CLI transcripts when UI/terminal output changes. Request review from at least one maintainer and ensure todos are resolved before merge.
