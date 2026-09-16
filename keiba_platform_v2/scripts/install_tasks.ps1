$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$DayScript = Join-Path $Root "scripts\run_scheduled_day.ps1"
$ResultScript = Join-Path $Root "scripts\run_scheduled_results.ps1"

$DayTask = "KeibaPlatformV2-Raceday"
$ResultTask = "KeibaPlatformV2-Results"

$DayAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$DayScript`""
$ResultAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ResultScript`""

$DayTriggerSat = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At 8:30AM
$DayTriggerSun = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 8:30AM
$ResultTriggerSat = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At 6:30PM
$ResultTriggerSun = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 6:30PM

$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $DayTask -Action $DayAction -Trigger @($DayTriggerSat, $DayTriggerSun) -Settings $Settings -Description "Keiba Platform V2: collect/predict then T-5 SHADOW on weekends" -Force | Out-Null
Register-ScheduledTask -TaskName $ResultTask -Action $ResultAction -Trigger @($ResultTriggerSat, $ResultTriggerSun) -Settings $Settings -Description "Keiba Platform V2: collect results, settle SHADOW and update history" -Force | Out-Null

Write-Host "Installed tasks:"
Write-Host "  $DayTask      Saturday/Sunday 08:30"
Write-Host "  $ResultTask   Saturday/Sunday 18:30"
Write-Host "Holiday/weekday JRA meetings must be started manually with run_raceday.ps1 and run_t5.ps1."
