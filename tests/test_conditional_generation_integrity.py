"""Integrity regression: preserve historical generations and reject unreviewed changes."""
from pathlib import Path
from copy import deepcopy
import importlib
import json
import shutil
import hashlib
import pytest
from model_registry import ModelConfigError, normalized_weights_sha256
import shadow_validation as shadow

ROOT = Path(__file__).resolve().parents[1]

def test_previous_generations_are_not_rewritten():
    original = json.loads((ROOT / 'tests/fixtures/conditional_generation/forward_before.json').read_text(encoding='utf-8'))
    candidate = json.loads((ROOT / 'config/forward_validation.json').read_text(encoding='utf-8'))
    # Existing generations keep every threshold, model reference and frozen digest.
    for name, generation in original['generations'].items():
        assert candidate['generations'][name] == generation
    current = shadow.load_forward_validation_policy()
    assert current['purpose'] == 'offline_integration_replay'
    assert current['production_activation_requires_approval'] is True
    assert current['do_not_aggregate_with_previous_generations'] is True

@pytest.mark.parametrize('relative', ['config/production_model.json', '1_keibayosou_best_import_roi_runner.py', '1_keibayosou_conditional_rerank.py'])
def test_fixed_audit_still_rejects_tampered_files(tmp_path, monkeypatch, relative):
    raw = json.loads((ROOT / 'config/forward_validation.json').read_text(encoding='utf-8'))
    policy = raw['generations'][raw['active_generation']]
    for name in policy['frozen_files']:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target)
    (tmp_path / 'config/forward_validation.json').write_text(json.dumps(raw), encoding='utf-8')
    monkeypatch.setattr(shadow, 'PROJECT_ROOT', tmp_path)
    monkeypatch.setattr(shadow, 'FORWARD_CONFIG_PATH', tmp_path / 'config/forward_validation.json')
    shadow.load_forward_validation_policy()
    # Even a harmless appended byte must be rejected; audit is not bypassed.
    with (tmp_path / relative).open('ab') as stream:
        stream.write(b'\n')
    with pytest.raises(ModelConfigError, match='固定ファイルSHAが不一致'):
        shadow.load_forward_validation_policy()

def test_shadow_sha_migration_is_line_endings_only():
    old = json.loads((ROOT / 'tests/fixtures/conditional_generation/shadow_before.json').read_text(encoding='utf-8'))
    new = json.loads((ROOT / 'config/shadow_model.json').read_text(encoding='utf-8'))
    raw = (ROOT / new['weight_file']).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == new['file_sha256']
    assert hashlib.sha256(raw.replace(b'\n', b'\r\n')).hexdigest() == old['file_sha256']
    # Registration changes representation only, never numerical model identity or permissions.
    old.pop('file_sha256')
    new.pop('file_sha256')
    assert old == new

def test_baseline_preserves_production_effective_identity():
    config = importlib.import_module('1_keibayosou_config')
    baseline = importlib.import_module('tokutyouryou_keisann.baseline')
    effective = baseline.load_effective_weights_file(config.PRODUCTION_MODEL_SPEC.resolved_weight_path)
    assert normalized_weights_sha256(effective) == config.PRODUCTION_MODEL_SPEC.effective_sha256
    assert len(effective) == 31
    assert sum(map(len, effective.values())) == 3131
    # Computed-but-unweighted features retain an implicit zero rather than altering model SHA.
    assert all(group.get('field_size_adjusted_finish', 0.0) == 0.0 for group in effective.values())
