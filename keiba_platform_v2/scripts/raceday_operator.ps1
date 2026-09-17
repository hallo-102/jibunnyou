param(
    [ValidateSet("Status", "Prepare", "Morning", "T5", "Results", "Verify")]
    [string]$Action = "Status",
    [string]$RaceDate = "",
    [switch]$ShowBrowser
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Get-JstDateString {
    $jst = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, "Tokyo Standard Time")
    return $jst.ToString("yyyyMMdd")
}

if ([string]::IsNullOrWhiteSpace($RaceDate)) {
    $RaceDate = Get-JstDateString
}
if ($RaceDate -notmatch '^20\d{6}$') {
    throw "RaceDate must be YYYYMMDD: $RaceDate"
}

$Python = ".\.venv\Scripts\python.exe"
$Input = "data\input\races_$RaceDate.csv"
$Schedule = "data\raw\race_schedule_$RaceDate.csv"
$Entries = "data\raw\netkeiba_entries_$RaceDate.csv"
$Results = "data\results\results_$RaceDate.csv"
$Payouts = "data\results\payouts_$RaceDate.csv"
$Acceptance = "data\output\acceptance_$RaceDate.json"
$Model = "data\runtime\model.txt"
$HistoryDb = "data\runtime\history.sqlite3"

function Show-Status {
    Write-Host "=== Keiba V2 Raceday Status ==="
    Write-Host "RaceDate       : $RaceDate"
    Write-Host "Project        : $ProjectRoot"
    Write-Host "venv           : $(Test-Path $Python)"
    Write-Host "trained model  : $(Test-Path $Model)"
    Write-Host "history db     : $(Test-Path $HistoryDb)"
    Write-Host "morning input  : $(Test-Path $Input)"
    Write-Host "schedule       : $(Test-Path $Schedule)"
    Write-Host "entries        : $(Test-Path $Entries)"
    Write-Host "results        : $(Test-Path $Results)"
    Write-Host "payouts        : $(Test-Path $Payouts)"
    Write-Host "acceptance     : $(Test-Path $Acceptance)"

    if (Test-Path $Acceptance) {
        try {
            $report = Get-Content $Acceptance -Raw -Encoding UTF8 | ConvertFrom-Json
            Write-Host "acceptance ok  : $($report.ok)"
        } catch {
            Write-Warning "Could not parse $Acceptance"
        }
    }

    if (Test-Path $Python) {
        Write-Host ""
        Write-Host "--- doctor ---"
        & $Python -m keiba_v2.cli doctor
        if ($LASTEXITCODE -ne 0) { throw "doctor failed" }
    }
}

switch ($Action) {
    "Status" {
        Show-Status
    }
    "Prepare" {
        & powershell -ExecutionPolicy Bypass -File ".\scripts\self_test.ps1"
        if ($LASTEXITCODE -ne 0) { throw "self_test failed" }
        & $Python -m keiba_v2.cli doctor
        if ($LASTEXITCODE -ne 0) { throw "doctor failed" }
        Write-Host "PREPARE PASSED for $RaceDate"
    }
    "Morning" {
        $argsList = @("-ExecutionPolicy", "Bypass", "-File", ".\scripts\run_raceday.ps1", "-RaceDate", $RaceDate)
        if ($ShowBrowser) { $argsList += "-ShowBrowser" }
        & powershell @argsList
        if ($LASTEXITCODE -ne 0) { throw "morning raceday pipeline failed" }
        Show-Status
    }
    "T5" {
        if (-not (Test-Path $Input)) { throw "Morning input is missing: $Input" }
        if (-not (Test-Path $Schedule)) { throw "Race schedule is missing: $Schedule" }
        if (-not (Test-Path $Model)) { throw "Trained model is missing: $Model" }
        $argsList = @("-ExecutionPolicy", "Bypass", "-File", ".\scripts\run_t5.ps1", "-RaceDate", $RaceDate)
        if ($ShowBrowser) { $argsList += "-ShowBrowser" }
        & powershell @argsList
        if ($LASTEXITCODE -ne 0) { throw "T-5 runtime failed" }
        Show-Status
    }
    "Results" {
        if (-not (Test-Path $Entries)) { throw "Entries are missing: $Entries" }
        & powershell -ExecutionPolicy Bypass -File ".\scripts\run_results.ps1" -RaceDate $RaceDate
        if ($LASTEXITCODE -ne 0) { throw "results pipeline failed" }
        Show-Status
    }
    "Verify" {
        & powershell -ExecutionPolicy Bypass -File ".\scripts\verify_raceday.ps1" -RaceDate $RaceDate
        if ($LASTEXITCODE -ne 0) { throw "raceday verification failed" }
        Show-Status
    }
}
