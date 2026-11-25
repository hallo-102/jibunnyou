"""検証モデルの固定値が現行の重みファイルと実効重みに対応することを確認する。"""

import hashlib
import importlib
import json

import pytest

from model_registry import ModelConfigError, load_model_spec
from shadow_validation import PROJECT_ROOT, load_shadow_runtime


def test_d12_shadow_pins_actual_lf_bytes_and_production_effective_weights(tmp_path):
    production = importlib.import_module("1_keibayosou_config").PRODUCTION_MODEL_SPEC
    runtime = load_shadow_runtime()
    shadow = runtime.spec

    assert shadow.resolved_weight_path == production.resolved_weight_path
    assert shadow.file_sha256 == production.file_sha256
    assert shadow.effective_sha256 == production.effective_sha256
    assert shadow.scoring_model_version == production.scoring_model_version

    # 更新前の設定が固定していた CRLF 版の SHA では、LF の実ファイルを読めない。
    raw_weights = shadow.resolved_weight_path.read_bytes()
    crlf_sha = hashlib.sha256(raw_weights.replace(b"\n", b"\r\n")).hexdigest()
    previous = tmp_path / "shadow_model_crlf.json"
    previous.write_text(
        json.dumps({**shadow.raw, "file_sha256": crlf_sha}),
        encoding="utf-8",
    )
    with pytest.raises(ModelConfigError, match="重みファイルSHA-256") as error:
        load_model_spec(PROJECT_ROOT, previous, expected_role="shadow")
    assert crlf_sha in str(error.value)
