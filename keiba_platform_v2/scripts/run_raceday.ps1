param(
    [Parameter(Mandatory=$true)][string]$RaceDate,
    [switch]$ShowBrowser
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$Python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Run scripts/setup_windows.ps1 first."
}

$collectArgs = @("-m", "keiba_v2.cli", "collect", "--date", $RaceDate)
if ($ShowBrowser) { $collectArgs += "--show-browser" }
& $Python @collectArgs
# ネイティブコマンドの失敗はErrorActionPreferenceだけでは停止しないため、段階ごとに検査します。
if ($LASTEXITCODE -ne 0) { throw "collect failed for $RaceDate" }

$Input = "data\input\races_$RaceDate.csv"
$CombinationOdds = "data\raw\jra_combination_odds_$RaceDate.csv"
& $Python -m keiba_v2.cli validate --input $Input
if ($LASTEXITCODE -ne 0) { throw "input validation failed for $RaceDate" }
& $Python -m keiba_v2.cli run --input $Input --date $RaceDate --combination-odds $CombinationOdds
if ($LASTEXITCODE -ne 0) { throw "morning SHADOW analysis failed for $RaceDate" }

Write-Host "Raceday SHADOW pipeline completed for $RaceDate"
