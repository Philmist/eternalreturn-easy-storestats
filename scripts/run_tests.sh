#!/usr/bin/env bash
set -euo pipefail

if ! command -v uv >/dev/null 2>&1; then
  echo "The 'uv' CLI is required to install dependencies and run tests." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Syncing project dependencies (including test extras) via uv..."
uv sync --extra test --frozen

echo "Installing project in editable mode via uv..."
uv pip install --editable .

echo "Running pytest with uv..."
uv run pytest "$@"
