from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_ROOT = PROJECT_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    # リポジトリ直下からbackendのappパッケージを読み込めるようにする。
    sys.path.insert(0, str(BACKEND_ROOT))

from app.legacy_bridge.prediction_cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
