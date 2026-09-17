param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("morning", "t5", "results")]
    [string]$Mode
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$RaceDate = (Get-Date).ToString("yyyyMMdd")
$LogDir = Join-Path $Root "data\runtime\task_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$LogPath = Join-Path $LogDir ("{0}_{1}.log" -f $RaceDate, $Mode)

function Write-RunLog([string]$Message) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Tee-Object -FilePath $LogPath -Append
}

try {
    Write-RunLog "START mode=$Mode race_date=$RaceDate"
    switch ($Mode) {
        "morning" {
            & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "run_raceday.ps1") -RaceDate $RaceDate *>> $LogPath
        }
        "t5" {
            $Input = Join-Path $Root "data\input\races_$RaceDate.csv"
            $Schedule = Join-Path $Root "data\raw\race_schedule_$RaceDate.csv"
            if (-not (Test-Path $Input) -or -not (Test-Path $Schedule)) {
                Write-RunLog "SKIP T-5: morning input/schedule not found. This is normal on non-race days."
                exit 0
            }
            & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "run_t5.ps1") -RaceDate $RaceDate *>> $LogPath
        }
        "results" {
            $Entries = Join-Path $Root "data\raw\netkeiba_entries_$RaceDate.csv"
            if (-not (Test-Path $Entries)) {
                Write-RunLog "SKIP results: entries file not found. This is normal on non-race days."
                exit 0
            }
            & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "run_results.ps1") -RaceDate $RaceDate *>> $LogPath
        }
    }
    if ($LASTEXITCODE -ne 0) {
        throw "child process failed with exit code $LASTEXITCODE"
    }
    Write-RunLog "SUCCESS mode=$Mode"
    exit 0
}
catch {
    Write-RunLog ("FAILED mode={0} error={1}" -f $Mode, $_.Exception.Message)
    exit 1
}
