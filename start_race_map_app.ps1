$ErrorActionPreference = "Stop"
$Python = if (Test-Path "$PSScriptRoot\.venv\Scripts\python.exe") { "$PSScriptRoot\.venv\Scripts\python.exe" } else { "python" }
& $Python -m streamlit run "$PSScriptRoot\app\race_map\app.py"
