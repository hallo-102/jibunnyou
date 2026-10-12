param(
    [string]$CodexHome = "$env:USERPROFILE\.codex"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Source = Join-Path $ProjectRoot "skills\keiba-v2-raceday"
$SkillRoot = Join-Path $CodexHome "skills"
$Destination = Join-Path $SkillRoot "keiba-v2-raceday"

if (-not (Test-Path (Join-Path $Source "SKILL.md"))) {
    throw "Skill source is missing: $Source"
}

New-Item -ItemType Directory -Path $SkillRoot -Force | Out-Null
if (Test-Path $Destination) {
    Remove-Item $Destination -Recurse -Force
}
Copy-Item $Source $Destination -Recurse -Force

Write-Host "Codex skill installed: $Destination"
Write-Host "Restart Codex if the skill is not discovered in the current session."
Write-Host "Example request: 今日の競馬V2を進めて"
