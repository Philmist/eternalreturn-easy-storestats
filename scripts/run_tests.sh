#!/usr/bin/env bash
set -euo pipefail

if [ -n "${PYTHON:-}" ]; then
  python_cmd="$PYTHON"
elif command -v python3 >/dev/null 2>&1; then
  python_cmd="python3"
elif command -v python >/dev/null 2>&1; then
  python_cmd="python"
else
  echo "Unable to locate a Python interpreter. Set the PYTHON environment variable to continue." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "Using virtual environment: $VIRTUAL_ENV"
fi

echo "Installing project with test dependencies..."
"$python_cmd" -m pip install -e ".[test]"

echo "Running pytest..."
"$python_cmd" -m pytest "$@"
