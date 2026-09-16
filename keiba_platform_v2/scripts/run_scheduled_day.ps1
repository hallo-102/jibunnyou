$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$RaceDate = Get-Date -Format "yyyyMMdd"
$LogDir = Join-Path $Root "data\runtime\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir "scheduled_day_$RaceDate.log"

try {
    "[$(Get-Date -Format o)] START scheduled day $RaceDate" | Tee-Object -FilePath $Log -Append
    powershell -ExecutionPolicy Bypass -File ".\scripts\run_raceday.ps1" -RaceDate $RaceDate *>&1 | Tee-Object -FilePath $Log -Append
    powershell -ExecutionPolicy Bypass -File ".\scripts\run_t5.ps1" -RaceDate $RaceDate *>&1 | Tee-Object -FilePath $Log -Append
    "[$(Get-Date -Format o)] SUCCESS scheduled day $RaceDate" | Tee-Object -FilePath $Log -Append
    exit 0
}
catch {
    "[$(Get-Date -Format o)] FAILED $($_.Exception.Message)" | Tee-Object -FilePath $Log -Append
    exit 1
}
