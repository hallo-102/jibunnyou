$ErrorActionPreference = "Stop"

$TaskNames = @(
    "KeibaV2_Morning",
    "KeibaV2_T5",
    "KeibaV2_Results"
)

foreach ($TaskName in $TaskNames) {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -ne $task) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Removed: $TaskName"
    }
    else {
        Write-Host "Not found: $TaskName"
    }
}
