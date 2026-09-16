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
$Results = "data\results\results_$RaceDate.csv"
$Payouts = "data\results\payouts_$RaceDate.csv"

& $Python -m keiba_v2.cli collect-results --entries $Entries --date $RaceDate

$BetFiles = Get-ChildItem -Path "data\output" -Filter "strategy_bets_$RaceDate*.json" -File -ErrorAction SilentlyContinue
foreach ($BetFile in $BetFiles) {
    $Settled = Join-Path $BetFile.DirectoryName ($BetFile.BaseName + "_settled.csv")
    & $Python -m keiba_v2.cli settle --bets $BetFile.FullName --results $Results --payouts $Payouts --output $Settled
}

& $Python -m keiba_v2.cli build-training
& $Python -m keiba_v2.cli report --directory "data\output" --output "data\output\performance_report.xlsx"

Write-Host "Results/history/settlement/report pipeline completed for $RaceDate"
