$ErrorActionPreference = "Stop"
$Tasks = @("KeibaPlatformV2-Raceday", "KeibaPlatformV2-Results")
foreach ($Task in $Tasks) {
    $existing = Get-ScheduledTask -TaskName $Task -ErrorAction SilentlyContinue
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $Task -Confirm:$false
        Write-Host "Removed: $Task"
    }
    else {
        Write-Host "Not installed: $Task"
    }
}
