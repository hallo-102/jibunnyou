param(
    [string]$MorningTime = "08:20",
    [string]$T5Time = "09:00",
    [string]$ResultsTime = "19:00"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Runner = Join-Path $PSScriptRoot "task_runner.ps1"
if (-not (Test-Path $Runner)) {
    throw "task_runner.ps1 not found: $Runner"
}

function Register-KeibaTask {
    param(
        [string]$TaskName,
        [string]$Mode,
        [string]$At
    )

    $action = New-ScheduledTaskAction `
        -Execute "powershell.exe" `
        -Argument ("-NoProfile -ExecutionPolicy Bypass -File `"{0}`" -Mode {1}" -f $Runner, $Mode) `
        -WorkingDirectory $Root

    $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday,Sunday -At $At
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -MultipleInstances IgnoreNew

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description "Keiba Platform V2 isolated SHADOW workflow" `
        -Force | Out-Null

    Write-Host "Registered: $TaskName ($Mode at $At, Sat/Sun)"
}

Register-KeibaTask -TaskName "KeibaV2_Morning" -Mode "morning" -At $MorningTime
Register-KeibaTask -TaskName "KeibaV2_T5" -Mode "t5" -At $T5Time
Register-KeibaTask -TaskName "KeibaV2_Results" -Mode "results" -At $ResultsTime

Write-Host "Keiba Platform V2 scheduled tasks registered."
Write-Host "Logs: data\runtime\task_logs\YYYYMMDD_<mode>.log"
Write-Host "Holiday/weekday JRA meetings are not auto-added by this weekend-only registration; run task_runner.ps1 manually for those dates."
