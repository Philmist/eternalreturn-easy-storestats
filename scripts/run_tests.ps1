#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

param(
    [string]$Python = $env:PYTHON,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

function Resolve-PythonCommand {
    param([string]$Preferred)

    if ($Preferred) {
        return $Preferred
    }

    foreach ($candidate in @('python3', 'python')) {
        if (Get-Command $candidate -ErrorAction SilentlyContinue) {
            return $candidate
        }
    }

    throw "Unable to locate a Python interpreter. Specify one with the PYTHON environment variable or --Python parameter."
}

$Python = Resolve-PythonCommand -Preferred $Python

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RootDir

if ($env:VIRTUAL_ENV) {
    Write-Host "Using virtual environment: $($env:VIRTUAL_ENV)"
}

Write-Host "Installing project with test dependencies..."
& $Python -m pip install -e ".[test]"

Write-Host "Running pytest..."
& $Python -m pytest @PytestArgs
exit $LASTEXITCODE
