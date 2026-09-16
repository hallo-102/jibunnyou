param(
    [Parameter(Mandatory=$true)][string]$RaceDate
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$Python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Run scripts/setup_windows.ps1 first."
}

$Entries = "data\raw\netkeiba_entries_$RaceDate.csv"
$Bets = "data\output\strategy_bets_$RaceDate.json"
$Results = "data\results\results_$RaceDate.csv"
$Payouts = "data\results\payouts_$RaceDate.csv"

& $Python -m keiba_v2.cli collect-results --entries $Entries --date $RaceDate
if (Test-Path $Bets) {
    & $Python -m keiba_v2.cli settle --bets $Bets --results $Results --payouts $Payouts
}
& $Python -m keiba_v2.cli build-training

Write-Host "Results/history pipeline completed for $RaceDate"
