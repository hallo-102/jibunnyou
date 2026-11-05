$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.11 -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -e ".[ml,collection,dev]"
& .\.venv\Scripts\python.exe -m playwright install chromium

powershell -NoProfile -ExecutionPolicy Bypass -File ".\scripts\self_test.ps1"
if ($LASTEXITCODE -ne 0) {
    throw "Keiba Platform V2 self test failed."
}

Write-Host "Keiba Platform V2 setup completed."
