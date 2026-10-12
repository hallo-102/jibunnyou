param(
    [Parameter(Mandatory=$true)][string]$RaceDate
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$Python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Run scripts/setup_windows.ps1 first."
}

& $Python -m keiba_v2.cli acceptance --date $RaceDate
if ($LASTEXITCODE -ne 0) {
    throw "Raceday acceptance FAILED for $RaceDate. See data\output\acceptance_$RaceDate.json"
}

Write-Host "Raceday acceptance PASSED for $RaceDate"
Write-Host "Report: data\output\acceptance_$RaceDate.json"
