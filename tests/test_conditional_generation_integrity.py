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

def test_offline_generation_cannot_start_without_explicit_scope(monkeypatch):
    monkeypatch.delenv('KEIBA_FORWARD_SCOPE')
    with pytest.raises(ModelConfigError, match='本番転用は禁止'):
        shadow.load_forward_validation_policy()

def test_offline_output_has_a_separate_generation_directory():
    runtime = shadow.load_shadow_runtime()
    assert runtime.output_dir == ROOT / 'data/output/shadow_comparison/offline/cr_offline_20261013'

def test_offline_data_cannot_enter_forward_aggregate():
    report = importlib.import_module('tools.report_forward_validation')
    with pytest.raises(ModelConfigError, match='本番前向き集計へ使用できません'):
        report.main()

@pytest.mark.parametrize('status,approval', [('pending_approval', True), ('active', True)])
def test_production_generation_requires_approval(monkeypatch, status, approval):
    raw = json.loads((ROOT / 'config/forward_validation.json').read_text(encoding='utf-8'))
    generation = deepcopy(raw['generations'][raw['active_generation']])
    generation.update(purpose='production_forward', status=status, production_activation_requires_approval=approval)
    raw['generations']['future_forward_test'] = generation
    raw['active_generation'] = 'future_forward_test'
    monkeypatch.setattr(shadow, '_read_json_object', lambda *args: raw)
    with pytest.raises(ModelConfigError, match='有効化は承認待ち'):
        shadow.load_forward_validation_policy()

def test_written_replay_records_identify_scope_and_generation(tmp_path):
    from dataclasses import replace
    runtime = replace(shadow.load_shadow_runtime(), output_dir=tmp_path)
    path = shadow.write_shadow_pre_payload(runtime, {'result_status': 'pre_result', 'comparison_date': '20261012'})
    record = json.loads(path.read_text(encoding='utf-8'))
    assert record['generation'] == 'cr_offline_20261013'
    assert record['validation_scope'] == 'offline_integration_replay'

def test_scoped_json_path_fits_windows_atomic_write_limit():
    # 実行済み隔離フォルダと同じ長さで、通常名・一時名の両方を確認する。
    base = ROOT / 'reports/isolated_20261013_064031'
    runtime = shadow.load_shadow_runtime()
    relative = runtime.output_dir.relative_to(ROOT)
    filename = 'shadow_comparison_20261012_pre_20261013T064637+0900.json'
    temporary = base / relative / '20261012' / ('.' + filename + '.abcdefgh.tmp')
    assert len(str(temporary)) < 260

@pytest.mark.parametrize('scope,generation', [('offline_integration_replay', 'future'), ('production_forward', 'wrong_generation')])
def test_forward_aggregate_rejects_foreign_records(monkeypatch, tmp_path, scope, generation):
    from types import SimpleNamespace
    report = importlib.import_module('tools.report_forward_validation')
    monkeypatch.setattr(report, 'load_forward_validation_policy', lambda: {'purpose': 'production_forward', 'active_generation': 'future'})
    monkeypatch.setattr(report, 'load_shadow_runtime', lambda: SimpleNamespace(output_dir=tmp_path))
    monkeypatch.setattr(report, '_load_latest_post_per_day', lambda _: [{'generation': generation, 'validation_scope': scope}])
    with pytest.raises(ModelConfigError, match='異なる世代・scope'):
        report.main()
