# Agent Guidelines

## Communication
- Communicate with humans primarily in Japanese. All code-related artifacts (including source code, commit messages, branch names, PR descriptions, and comments inside code) must be written in English.

## Coding Standards
- Follow PEP 8 and PEP 257 for all Python code.

## Environment Management
- Use `uv` for environment management, dependency installation, and command execution during development. Documentation (e.g., `README.md`) should continue to describe workflows with `pip`.
    - Use `uv sync` to install dependencies.
- Manage project dependencies through `pyproject.toml` (and keep `requirements.txt` synchronized accordingly).

## Project-Specific Practices
- Build and activate virtual environments exclusively with `uv`.
- Adopt the `src` layout for Python packages and rely on editable installs during development.
- Treat the contents of the `api-result-examples` directory as reference material only; production code must not import from or otherwise depend on files within this directory.
- On Windows, use PowerShell (version 7.0 or later) for all non-Python scripting.

## Testing
- Use `pytest` as the testing framework.
    - Use `uv run pytest -q` to run tests.
- Whenever implementing or modifying functionality, ensure there are corresponding tests that cover the changes and that they pass.

## Design Principles
- Prioritize separation of concerns. Even if a feature could be implemented inline with a few lines, consider extracting logic into dedicated modules or functions when it aligns with future maintainability.
- Significant refactoring is acceptable when tests exist and remain valid.

## Documentation
- Recommend documenting any new CLI commands or environment setup steps in `README.md` to keep user instructions up to date.
