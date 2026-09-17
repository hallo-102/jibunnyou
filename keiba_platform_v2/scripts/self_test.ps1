$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$Python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Run scripts/setup_windows.ps1 first."
}

Write-Host "[1/4] Python compile check"
& $Python -m compileall -q src
if ($LASTEXITCODE -ne 0) { throw "compileall failed" }

Write-Host "[2/4] Unit/regression tests"
& $Python -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "pytest failed" }

Write-Host "[3/4] Runtime doctor"
& $Python -m keiba_v2.cli doctor
if ($LASTEXITCODE -ne 0) { throw "doctor failed" }

Write-Host "[4/4] CLI surface check"
& $Python -m keiba_v2.cli --help | Out-Null
if ($LASTEXITCODE -ne 0) { throw "CLI help failed" }

Write-Host "SELF TEST PASSED"
