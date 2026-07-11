import os
import sys
import tempfile
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    # テスト対象のappパッケージを、リポジトリ直下から確実に読み込めるようにする。
    sys.path.insert(0, str(BACKEND_ROOT))

# テストプロセスごとに独立した一時領域を作り、前回実行のDBや成果物を再利用しない。
TEST_RUNTIME_ROOT = Path(tempfile.mkdtemp(prefix="keiba-ai-studio-tests-"))
os.environ["KEIBA_DATABASE_URL"] = f"sqlite:///{TEST_RUNTIME_ROOT / 'test.db'}"
os.environ["KEIBA_REDIS_URL"] = "redis://127.0.0.1:6399/15"
os.environ["KEIBA_ENVIRONMENT"] = "test"
os.environ["KEIBA_APP_VERSION"] = "test"
os.environ["KEIBA_STAGING_DIR"] = str(TEST_RUNTIME_ROOT / "staging")
os.environ["KEIBA_EXPORTS_DIR"] = str(TEST_RUNTIME_ROOT / "exports")
os.environ["KEIBA_LOGS_DIR"] = str(TEST_RUNTIME_ROOT / "logs")
os.environ["KEIBA_RAW_SNAPSHOTS_DIR"] = str(TEST_RUNTIME_ROOT / "raw_snapshots")
os.environ["KEIBA_NORMALIZED_DIR"] = str(TEST_RUNTIME_ROOT / "normalized")
os.environ["KEIBA_SNAPSHOTS_DIR"] = str(TEST_RUNTIME_ROOT / "snapshots")
os.environ["KEIBA_JOB_EXECUTION_MODE"] = "inline"
os.environ["KEIBA_PREDICTION_RUNNER_MODE"] = "dry_run"
