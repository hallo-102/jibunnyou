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
if ($LASTEXITCODE -ne 0) { throw "collect-results failed" }

# Morning strategy_bets_YYYYMMDD.json is preview only.
# KPI settlement includes only actual T-5 SHADOW decision files.
$BetFiles = Get-ChildItem -Path "data\output" -Filter "strategy_bets_$RaceDate*_T5.json" -File -ErrorAction SilentlyContinue
foreach ($BetFile in $BetFiles) {
    $Settled = Join-Path $BetFile.DirectoryName ($BetFile.BaseName + "_settled.csv")
    & $Python -m keiba_v2.cli settle --bets $BetFile.FullName --results $Results --payouts $Payouts --output $Settled
    if ($LASTEXITCODE -ne 0) { throw "settlement failed: $($BetFile.FullName)" }
}

& $Python -m keiba_v2.cli build-training
if ($LASTEXITCODE -ne 0) { throw "build-training failed" }

& $Python -m keiba_v2.cli report --directory "data\output" --output "data\output\performance_report.xlsx"
if ($LASTEXITCODE -ne 0) { throw "report failed" }

& $Python -m keiba_v2.cli acceptance --date $RaceDate
if ($LASTEXITCODE -ne 0) { throw "raceday acceptance failed" }

Write-Host "Results/history/T-5 settlement/report/acceptance pipeline completed for $RaceDate"
