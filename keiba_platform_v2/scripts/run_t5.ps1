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

$Input = "data\input\races_$RaceDate.csv"
$Schedule = "data\raw\race_schedule_$RaceDate.csv"
if (-not (Test-Path $Input)) { throw "Missing input: $Input" }
if (-not (Test-Path $Schedule)) { throw "Missing schedule: $Schedule" }

$argsList = @("-m", "keiba_v2.cli", "t5-runtime", "--input", $Input, "--schedule", $Schedule, "--date", $RaceDate)
if ($ShowBrowser) { $argsList += "--show-browser" }
& $Python @argsList

Write-Host "T-5 SHADOW runtime completed for $RaceDate"
