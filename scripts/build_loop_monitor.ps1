param([string]$Python = "", [string]$Destination = "")
$ErrorActionPreference = "Stop"
# 既存のQt/PyInstaller環境を再利用し、予想処理の仮想環境は変更しない。
$ProjectRoot = Split-Path -Parent $PSScriptRoot
if (-not $Python) {
    $Python = Join-Path (Split-Path -Parent $ProjectRoot) 'jra_odds_strategy_monitor/.venv/Scripts/python.exe'
}
if (-not $Destination) {
    $Destination = Join-Path $env:LOCALAPPDATA 'JRALoopMonitorBuild'
}
& $Python -m PyInstaller --noconfirm --clean --distpath (Join-Path $Destination 'dist') --workpath (Join-Path $Destination 'work') (Join-Path $PSScriptRoot 'loop_monitor.spec')
if ($LASTEXITCODE -ne 0) { throw 'JRALoopMonitorのビルドに失敗しました。' }
Write-Output (Join-Path $Destination 'dist/JRALoopMonitor/JRALoopMonitor.exe')
