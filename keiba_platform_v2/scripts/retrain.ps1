$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$Python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Run scripts/setup_windows.ps1 first."
}

& $Python -m keiba_v2.cli build-training
$Training = "data\training\training.csv"
& $Python -m keiba_v2.cli train --input $Training
& $Python -m pytest -q

Write-Host "Model retraining completed."
