$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$RaceDate = Get-Date -Format "yyyyMMdd"
$LogDir = Join-Path $Root "data\runtime\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir "scheduled_results_$RaceDate.log"

try {
    "[$(Get-Date -Format o)] START scheduled results $RaceDate" | Tee-Object -FilePath $Log -Append
    powershell -ExecutionPolicy Bypass -File ".\scripts\run_results.ps1" -RaceDate $RaceDate *>&1 | Tee-Object -FilePath $Log -Append
    "[$(Get-Date -Format o)] SUCCESS scheduled results $RaceDate" | Tee-Object -FilePath $Log -Append
    exit 0
}
catch {
    "[$(Get-Date -Format o)] FAILED $($_.Exception.Message)" | Tee-Object -FilePath $Log -Append
    exit 1
}
