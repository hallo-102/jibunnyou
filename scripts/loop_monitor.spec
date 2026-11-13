# -*- mode: python ; coding: utf-8 -*-
"""既存OSMのWindowsビルドと同じICU衝突回避を適用する。"""
from pathlib import Path

project_root = Path(SPECPATH).parent
analysis = Analysis(
    [str(project_root / "scripts/loop_monitor_entry.py")],
    pathex=[str(project_root)], binaries=[], datas=[], hiddenimports=[],
    hookspath=[], runtime_hooks=[],
    excludes=["PySide6.QtWebEngineCore", "PySide6.QtWebEngineWidgets", "PySide6.QtCharts"],
    noarchive=False,
)
# PATH上のPoppler用ICUはQtとシンボルが異なるため、Windows標準ICUを使う。
analysis.binaries = [entry for entry in analysis.binaries if Path(entry[0]).name.lower() != "icuuc.dll"]
pyz = PYZ(analysis.pure)
executable = EXE(pyz, analysis.scripts, [], exclude_binaries=True,
    name="JRALoopMonitor", console=False, debug=False,
    bootloader_ignore_signals=False, strip=False, upx=False)
collection = COLLECT(executable, analysis.binaries, analysis.datas,
    strip=False, upx=False, name="JRALoopMonitor")
