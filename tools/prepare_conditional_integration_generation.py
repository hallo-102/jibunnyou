"""Create a bounded, provenance-checked offline generation; never refresh arbitrary SHAs."""
from pathlib import Path
from copy import deepcopy
import hashlib
import json

ROOT = Path(__file__).resolve().parents[1]
GENERATION = 'cr_offline_20261013'
LF_SHA = 'bb086384eb58c5abae6ef7104de94e83005c318b3be3900ef5c4df15fca1020d'
CRLF_SHA = '83e6126fd01af24ebe553039946dd603c3e6e326b8f6bee0e0a73a7ffd32dcbb'

def blob(path):
    # Use exact repository bytes, as required by the retained fixed-file audit.
    raw = path.read_bytes()
    return hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()

def main():
    forward_path = ROOT / 'config/forward_validation.json'
    shadow_path = ROOT / 'config/shadow_model.json'
    forward = json.loads(forward_path.read_text(encoding='utf-8'))
    if forward['active_generation'] != 'field_size_adjusted_finish_20260905':
        raise ValueError('既存世代からの一度限りの移行案のみ生成できます')
    old = forward['generations'][forward['active_generation']]
    # Only the approved production promotion and PR runner diff may differ from history.
    allowed = {'config/production_model.json': '2dee82c1a45fca1b253c2fc2a1713d8b4752c3a9',
               '1_keibayosou_best_import_roi_runner.py': 'ff2fb72eeb16d482505882bc0f23d563ae9179fb',
               'shadow_validation.py': '76e42b7d2c695ccccd5dcc98670e1eaf1f09f189'}
    frozen = {}
    for relative, expected in old['frozen_files'].items():
        actual = blob(ROOT / relative)
        if actual != allowed.get(relative, expected):
            raise ValueError(f'未レビューの固定ファイル変更: {relative}: {actual}')
        frozen[relative] = actual
    weight = (ROOT / 'yosou_py/best_feature_weights_20260817.py').read_bytes()
    # Prove the registered shadow SHA is exactly the same file with CRLF line endings.
    if b'\r' in weight or hashlib.sha256(weight).hexdigest() != LF_SHA or hashlib.sha256(weight.replace(b'\n', b'\r\n')).hexdigest() != CRLF_SHA:
        raise ValueError('shadow不一致は改行差だけという証明が成立しません')
    shadow = json.loads(shadow_path.read_text(encoding='utf-8'))
    if shadow['file_sha256'] != CRLF_SHA or shadow['effective_sha256'] != '8325f5c0748aa49076631b02424f2104a7924d0b39a4148d4703f687a937a789':
        raise ValueError('既知のshadowモデル世代ではありません')
    rerank_blob = blob(ROOT / '1_keibayosou_conditional_rerank.py')
    if rerank_blob != '1f0053b476f49b01a14bd39c5c15e68a9e89d64e':
        raise ValueError('回帰検証済みID修正以外の変更です')
    shadow['file_sha256'] = LF_SHA
    shadow_path.write_text(json.dumps(shadow, ensure_ascii=False, indent=2) + '\n', encoding='utf-8', newline='\n')
    frozen['config/shadow_model.json'] = blob(shadow_path)
    frozen['1_keibayosou_conditional_rerank.py'] = rerank_blob
    # 集計分離のレビュー済み版のみを追加監査対象にする。
    report_blob = blob(ROOT / 'tools/report_forward_validation.py')
    if report_blob != '7bbc414127975aef6a21e34c681b61aa86c08fc0':
        raise ValueError('未レビューの前向き集計コードです')
    frozen['tools/report_forward_validation.py'] = report_blob
    candidate = deepcopy(old)
    candidate.update({'status': 'validation_only_pending_activation_approval',
                      'purpose': 'offline_integration_replay',
                      'start_date': '20261012', 'optimizer_holdout_start_date': '20261012',
                      'created_at': '2026-10-13', 'previous_generation': forward['active_generation'],
                      'production_activation_requires_approval': True,
                      'do_not_aggregate_with_previous_generations': True,
                      'output_dir': f'data/output/shadow_comparison/offline/{GENERATION}',
                      'frozen_files': frozen,
                      'provenance': {'production_promotion_commit': '265e0a4fc021f10007c25cc09b9861e3ff8aefda',
                                     'lf_policy_commit': 'd66c425e0163174ca6be377931bb87c498345b18',
                                     'pr_head_commit': 'd7d779bb28d1f4deefbcd0854c4589192428aa82',
                                     'race_id_fix_commit': '290321b',
                                     'shadow_sha_change': 'CRLF to LF only; identical effective weights',
                                     'comparison_role': 'same-model identity/integration check, not model efficacy'}})
    forward['generations'][GENERATION] = candidate
    forward['active_generation'] = GENERATION
    forward_path.write_text(json.dumps(forward, ensure_ascii=False, indent=2) + '\n', encoding='utf-8', newline='\n')
    print(json.dumps({'new_generation': GENERATION, 'frozen_count': len(frozen), 'production_model_modified': False, 'weight_modified': False, 'approval_required_for_production': True}, ensure_ascii=False))

if __name__ == '__main__':
    main()
