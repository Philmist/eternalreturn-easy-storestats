#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

function Assert-UvAvailable {
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw "The 'uv' CLI is required to install dependencies and run tests."
    }
}

Assert-UvAvailable

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RootDir

Write-Host "Syncing project dependencies (including test extras) via uv..."
& uv sync --extra test --frozen

Write-Host "Installing project in editable mode via uv..."
& uv pip install --editable .

Write-Host "Running pytest with uv..."
& uv run pytest @PytestArgs
exit $LASTEXITCODE
