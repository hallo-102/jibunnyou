param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8765
)

$ErrorActionPreference = "Stop"

# Resolve paths from this launcher location so it works after moving the shell's current directory.
$RootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonPath = Join-Path $RootDir ".venv\Scripts\python.exe"
$ServerScript = Join-Path $RootDir "scripts\serve_bet_viewer.py"
$StdOutLog = Join-Path $RootDir "bet_viewer_server.out.log"
$StdErrLog = Join-Path $RootDir "bet_viewer_server.err.log"
$ViewerUrl = "http://${HostName}:${Port}/"

# Prefer the project virtualenv, then fall back to the Python found in PATH.
if (-not (Test-Path -LiteralPath $PythonPath)) {
    $PythonPath = "python"
}

if (-not (Test-Path -LiteralPath $ServerScript)) {
    throw "Server script was not found: $ServerScript"
}

# If the port is not listening, start the local viewer server in the background.
$Listener = Get-NetTCPConnection -LocalAddress $HostName -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if (-not $Listener) {
    Start-Process `
        -FilePath $PythonPath `
        -ArgumentList @($ServerScript, "--host", $HostName, "--port", [string]$Port) `
        -WorkingDirectory $RootDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutLog `
        -RedirectStandardError $StdErrLog | Out-Null
}

# Wait briefly for the server to become reachable before opening the browser.
for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
    try {
        $Response = Invoke-WebRequest -UseBasicParsing -Uri $ViewerUrl -TimeoutSec 2
        if ($Response.StatusCode -eq 200) {
            break
        }
    } catch {
        Start-Sleep -Milliseconds 500
    }
}

# Open the viewer with the default browser.
Start-Process $ViewerUrl
