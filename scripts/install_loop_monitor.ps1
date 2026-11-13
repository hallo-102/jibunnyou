param([string]$BuildFolder = "", [switch]$Launch)
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
if (-not $BuildFolder) { $BuildFolder = Join-Path $env:LOCALAPPDATA 'JRALoopMonitorBuild/dist/JRALoopMonitor' }
if (-not (Test-Path -LiteralPath (Join-Path $BuildFolder 'JRALoopMonitor.exe'))) { throw 'ビルド済みのexeがありません。' }
# 利用者のアプリ領域へ配置し、デスクトップから起動できるようにする。
$InstallFolder = Join-Path $env:LOCALAPPDATA 'Programs/JRALoopMonitor'
$AppPath = Join-Path $InstallFolder 'JRALoopMonitor.exe'
$ActiveApp = Get-Process -Name JRALoopMonitor -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $AppPath }
if ($ActiveApp) { throw '更新前にJRA改善ループの画面を閉じてください。' }
New-Item -ItemType Directory -Path $InstallFolder -Force | Out-Null
Get-ChildItem -LiteralPath $BuildFolder | ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination $InstallFolder -Recurse -Force }
$ShortcutPath = Join-Path ([Environment]::GetFolderPath('Desktop')) 'JRA改善ループ.lnk'
$Shortcut = (New-Object -ComObject WScript.Shell).CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $AppPath
$Shortcut.WorkingDirectory = $InstallFolder
$Shortcut.Description = 'NASのJRA予想・回顧・改善ループを5秒ごとに確認'
$IconPath = Join-Path $InstallFolder 'loop_monitor.ico'
$Shortcut.IconLocation = if (Test-Path -LiteralPath $IconPath) { $IconPath } else { $AppPath }
$Shortcut.Save()
if ($Launch) { Start-Process -FilePath $AppPath }
Write-Output $AppPath
Write-Output $ShortcutPath
