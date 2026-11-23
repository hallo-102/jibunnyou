@echo off
setlocal
rem プロジェクトの仮想環境を優先し、pythonがPATHにない環境でも起動します。
cd /d "%~dp0"
if exist "%~dp0..\.venv\Scripts\python.exe" (
    "%~dp0..\.venv\Scripts\python.exe" "%~dp0app.py"
) else (
    rem 仮想環境がない場合はWindowsのPythonランチャーを使います。
    py -3 "%~dp0app.py"
)
if errorlevel 1 pause
