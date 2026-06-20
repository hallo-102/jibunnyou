@echo off
REM Start the bet viewer local server, then open it in the default browser.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_bet_viewer_server.ps1"
