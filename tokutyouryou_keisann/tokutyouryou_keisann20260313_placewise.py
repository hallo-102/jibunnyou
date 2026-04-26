# -*- coding: utf-8 -*-
"""
tokutyouryou_keisann20260313_placewise.py

tokutyouryou_keisann フォルダ内の実行入口。
このファイル単体を実行しても runner.main() が動くようにする。
"""

from __future__ import annotations

import sys
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tokutyouryou_keisann.runner import main


if __name__ == "__main__":
    main()
