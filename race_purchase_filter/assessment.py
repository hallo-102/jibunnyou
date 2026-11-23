"""人気対AIの第1判定と、係数確定前の第2研究判定を明確に分離する。"""
import hashlib
import json
from pathlib import Path
from datetime import datetime
from statistics import fmean, pstdev

from evaluation_utils import JST, integer, number, parse_date, parse_timestamp, race_start
from research_features import BETA_CANDIDATES, horse_features

VERSION = 'v3.1-nas-odds-20261122'
POLICY_PATH = Path(__file__).with_name('assessment_policy.json')
FEATURE_NAMES = ('ability_recent3', 'ability_decay', 'clock_advantage', 'next_start_strength')


def load_policy():
    policy = json.loads(POLICY_PATH.read_text(encoding='utf-8-sig'))
    if not isinstance(policy, dict) or integer(policy.get('long_rest_days')) is None or integer(policy['long_rest_days']) <= 0:
        raise ValueError('評価設定の長期休養日数が不正です。')
    policy['long_rest_days'] = integer(policy['long_rest_days'])
    return policy


def policy_version(policy):
    digest = hashlib.sha256(json.dumps(policy, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return VERSION + '-' + digest[:12]


def policy_issues(policy, race, cutoff):
    issues = []
    if policy.get('beta') not in BETA_CANDIDATES or isinstance(policy.get('beta'), bool):
        issues.append('β未確定（候補0,2,5,10,15,20,30）')
    weights = policy.get('weights')
    if (not isinstance(weights, dict) or not weights or not set(weights).issubset(FEATURE_NAMES)
            or any(number(v) is None or number(v) < 0 for v in weights.values())
            or not any(number(v) and number(v) > 0 for v in weights.values())):
        issues.append('最終重み未確定・不正')
    threshold = integer(policy.get('rank_gap_threshold'))
    if threshold is None or threshold < 1:
        issues.append('不一致順位差の閾値未確定')
    start, end = parse_date(policy.get('selection_start')), parse_date(policy.get('selection_end'))
    frozen = parse_timestamp(policy.get('frozen_at'))
    target = parse_date(race.get('date'))
    if not (start and end and target and start <= end < target and frozen and frozen <= cutoff
            and frozen.date() > end and policy.get('selection_evidence')):
        issues.append('過去の選択期間・事前固定日時・選択根拠が未確認')
    return issues


def _judge(count, unknown, errors):
    if unknown or errors:
        return '判定保留'
    return '見送り候補' if count >= 2 else '購入候補'


def _roster_issues(race):
    horses = race.get('horses', [])
    field = integer(race.get('field_size'))
    errors = list(race.get('data_issues', []))
    if field is None or field < 1 or field < len(horses):
        errors.append('頭数が不明、または馬別件数と矛盾')
    if race.get('race_id') in (None, '', '不明'):
        errors.append('レースID不明')
    numbers = [integer(h.get('horse_no')) for h in horses]
    if any(n is None or n <= 0 for n in numbers) or len(set(numbers)) != len(numbers):
        errors.append('今走馬番の未取得・重複')
    missing = max(0, (field or len(horses)) - len(horses))
    if missing:
        errors.append(f'出走馬が{missing}頭分未取得')
    for key, label in (('production_rank', 'AI'), ('popularity_rank', '人気')):
        # 同じ単勝オッズの人気は順位区間であり、架空の一意順位を付けない。
        ranks = [integer(h.get(key)) for h in horses if integer(h.get(key)) is not None
                 and not (key == 'popularity_rank' and h.get('popularity_rank_interval'))]
        if len(set(ranks)) != len(ranks) or any(v < 1 or (field and v > field) for v in ranks):
            errors.append(f'{label}順位の重複・範囲外')
    return errors, missing


def _first_horse(horse, race, as_of):
    popularity, ai = integer(horse.get('popularity_rank')), integer(horse.get('production_rank'))
    low = integer(horse.get('popularity_rank_min')) if horse.get('popularity_rank_interval') else popularity
    high = integer(horse.get('popularity_rank_max')) if horse.get('popularity_rank_interval') else popularity
    start = race_start(race)
    pop_stamp = parse_timestamp(horse.get('popularity_observed_at'))
    ai_stamp = parse_timestamp(horse.get('ai_observed_at'))
    reasons = []
    if horse.get('popularity_error'):
        reasons.append(horse['popularity_error'])
    field = integer(race.get('field_size'))
    for value, label in ((popularity, '人気'), (ai, '本番AI')):
        if value is None or value <= 0 or (field and value > field):
            reasons.append(f'{label}順位が欠損・不正')
    if low is None or high is None or low < 1 or high < low or (field and high > field):
        reasons.append('人気順位区間が欠損・不正')
    for stamp, label in ((pop_stamp, '人気'), (ai_stamp, 'AI')):
        if stamp is None or start is None or stamp >= start or stamp > as_of:
            reasons.append(f'{label}値の発走前取得時刻を確認できない')
    no = integer(horse.get('horse_no'))
    if not horse.get('horse_name') or horse.get('horse_name') == '不明' or no is None or no <= 0:
        reasons.append('今走馬番・馬名が不足')
    condition = None
    if low is not None and high is not None and ai is not None and 0 < low <= high and ai > 0:
        if high <= 5 and ai >= 6:
            condition = True
        elif low > 5 or ai <= 5:
            condition = False
        else:
            reasons.append('同じ単勝オッズの順位区間が5番人気の境界をまたぐため確定できない')
    if reasons:
        status, is_mismatch = '未判定', None
    elif condition:
        status, is_mismatch = '不一致', True
        reasons = ['人気5位以内かつ本番AI能力順位6位以下の両条件に該当']
    else:
        status, is_mismatch = ('不一致対象外' if low > 5 else '一致'), False
        reasons = ['人気6位以下のため不一致対象外' if low > 5 else '本番AI順位が5位以内のため不一致なし']
    return {'horse_no': integer(horse.get('horse_no')), 'horse_name': horse.get('horse_name'),
            'popularity_rank': popularity, 'production_rank': ai,
            'popularity_rank_min': low, 'popularity_rank_max': high,
            'popularity_rank_display': str(low) if low == high and low is not None else f'{low}〜{high}',
            'rank_difference': ai - low if ai is not None and low is not None and low == high else None,
            'rank_difference_min': ai - high if ai is not None and high is not None else None,
            'rank_difference_max': ai - low if ai is not None and low is not None else None,
            'popularity_odds': horse.get('popularity_odds'), 'popularity_method': horse.get('popularity_method'),
            'popularity_source': horse.get('popularity_source'),
            'status': status, 'is_mismatch': is_mismatch, 'reference_condition_match': condition,
            'reasons': reasons, 'popularity_observed_at': horse.get('popularity_observed_at'),
            'ai_observed_at': horse.get('ai_observed_at'), 'popularity_time_source': horse.get('popularity_time_source'),
            'input_source': horse.get('input_source')}


def analyze_race(race, as_of=None, policy=None, context_sha256=None):
    as_of = as_of or datetime.now(JST)
    if as_of.tzinfo is None:
        raise ValueError('評価時刻にはタイムゾーンが必要です。')
    as_of = as_of.astimezone(JST)
    policy = load_policy() if policy is None else policy
    start = race_start(race)
    cutoff = min(as_of, start) if start else as_of
    errors, missing = _roster_issues(race)
    first_horses = [_first_horse(h, race, as_of) for h in race.get('horses', [])]
    count = sum(h['is_mismatch'] is True for h in first_horses)
    unknown = sum(h['is_mismatch'] is None for h in first_horses) + missing
    first = {'definition': '人気順位<=5 AND 本番AI能力順位>=6',
             'judgement': _judge(count, unknown, errors), 'disagreement_count': count,
             'unjudged_count': unknown, 'horses': first_horses,
             'reference_condition_count': sum(h['reference_condition_match'] is True for h in first_horses),
             'disagreement_horse_numbers': [h['horse_no'] for h in first_horses if h['is_mismatch'] is True]}
    pending = policy_issues(policy, race, cutoff)
    second_horses = []
    for h in race.get('horses', []):
        features = horse_features(h, race, cutoff, policy)
        second_horses.append({'horse_no': integer(h.get('horse_no')), 'horse_name': h.get('horse_name'),
                              'production_rank': integer(h.get('production_rank')), 'features': features,
                              'score': None, 'independent_rank_min': None, 'independent_rank_max': None,
                              'rank_difference': None, 'is_mismatch': None, 'status': '判定保留',
                              'reasons': pending[:] if pending else []})
    # 重み・βが過去の選択期間で確定したときだけ統合スコアを作る。
    if not pending:
        beta = str(integer(policy['beta']))
        active = {key: number(value) for key, value in policy['weights'].items() if number(value) > 0}
        values = []
        for h in second_horses:
            f = h['features']
            aggregate = f['ability_by_beta'][beta]
            value = {'ability_recent3': aggregate['recent3_mean'], 'ability_decay': aggregate['decay_mean'],
                     'clock_advantage': f['clock_advantage'], 'next_start_strength': f['next_start_strength']}
            values.append(value)
            missing_features = [key for key in active if value[key] is None]
            if missing_features:
                h['reasons'].append('特徴量不足：' + ','.join(missing_features))
        usable = [i for i, h in enumerate(second_horses) if not h['reasons']]
        for i in usable:
            score = 0.0
            for key, weight in active.items():
                cohort = [values[j][key] for j in usable]
                sd = pstdev(cohort)
                score += weight * ((values[i][key] - fmean(cohort)) / sd if sd else 0)
            second_horses[i]['score'] = score
        unknown_scores = len(second_horses) - len(usable) + missing
        scores = [second_horses[i]['score'] for i in usable]
        threshold = integer(policy['rank_gap_threshold'])
        for h in second_horses:
            if h['score'] is None:
                continue
            best = 1 + sum(v > h['score'] for v in scores)
            worst = sum(v >= h['score'] for v in scores) + unknown_scores
            h['independent_rank_min'], h['independent_rank_max'] = best, worst
            ai = h['production_rank']
            if ai is None or ai < 1:
                h['reasons'].append('本番AI順位不足')
            elif max(best - ai, ai - worst, 0) >= threshold:
                h['is_mismatch'], h['status'] = True, '不一致'
            elif max(abs(best - ai), abs(worst - ai)) < threshold:
                h['is_mismatch'], h['status'] = False, '一致'
            else:
                h['reasons'].append('欠損・同点馬の順位で結論が変わる')
            h['rank_difference'] = ai - best if ai is not None and best == worst else None
    second_count = sum(h['is_mismatch'] is True for h in second_horses) if not pending else None
    second_unknown = sum(h['is_mismatch'] is None for h in second_horses) + missing
    # 第2ロジックの「2頭で見送り」は未検証。閾値確定後も第1の購入可否へ混ぜない。
    second = {'status': '係数・閾値未確定' if pending else ('判定保留' if second_unknown or errors else '比較可能・購入効果未検証'),
              'judgement': '判定保留', 'disagreement_count': second_count,
              'unjudged_count': second_unknown, 'horses': second_horses, 'policy_issues': pending,
              'purchase_effect': '未検証', 'policy': policy}
    top5 = [integer(n) for n in race.get('predicted_top5', [])]
    if not top5:
        ordered = sorted([h for h in first_horses if h['production_rank'] is not None], key=lambda h: h['production_rank'])
        if [h['production_rank'] for h in ordered[:5]] == list(range(1, min(5, integer(race.get('field_size')) or 5) + 1)):
            top5 = [h['horse_no'] for h in ordered[:5]]
    if (not top5 or any(n is None or n <= 0 for n in top5) or len(set(top5)) != len(top5)
            or len(top5) != min(5, integer(race.get('field_size')) or 5)):
        top5 = []
    known_numbers = [h['horse_no'] for h in first_horses]
    if (any(n not in known_numbers for n in top5)
            or any(h['production_rank'] and 1 <= h['production_rank'] <= len(top5)
                   and top5[h['production_rank'] - 1] != h['horse_no'] for h in first_horses)
            or any(h['ai_observed_at'] is None or parse_timestamp(h['ai_observed_at']) is None
                   or parse_timestamp(h['ai_observed_at']) > cutoff for h in first_horses)):
        top5 = []
    independent_top5 = []
    if not pending and not errors and all(h['score'] is not None for h in second_horses) and second_horses:
        ordered = sorted(second_horses, key=lambda h: h['score'], reverse=True)
        if len(ordered) <= 5 or ordered[4]['score'] > ordered[5]['score']:
            independent_top5 = [h['horse_no'] for h in ordered[:5]]
    version = policy_version(policy)
    fingerprint = hashlib.sha256(json.dumps({'context': context_sha256, 'policy': policy}, sort_keys=True).encode()).hexdigest()
    return {'version': version, 'as_of': as_of.isoformat(), 'data_cutoff': cutoff.isoformat(),
            'judgement': first['judgement'], 'disagreement_count': count, 'unjudged_count': unknown,
            'disagreement_horse_numbers': first['disagreement_horse_numbers'], 'roster_errors': errors,
            'missing_horse_count': missing, 'predicted_top5': top5, 'independent_top5': independent_top5,
            'logic1': first, 'logic2': second, 'horses': second_horses,
            'difficulty_horse_count': sum(bool(h['features']['difficulty_flags']) for h in second_horses) + missing,
            'parameters': {'history_days': 365, 'max_runs': 5, 'half_life_days': 180, 'beta_candidates': list(BETA_CANDIDATES)},
            'context_sha256': context_sha256, 'research_fingerprint': fingerprint}


def format_analysis(race, analysis, record=None):
    if 'logic1' not in analysis:
        return '旧方式の保存記録です。新しい第1・第2ロジックの判定とは区別してください。'
    first, second = analysis['logic1'], analysis['logic2']
    lines = [f"• {race.get('date')} {race.get('place')} {race.get('race_no')}R {race.get('race_name')}",
             f"• 第1判定：{first['judgement']}／不一致{first['disagreement_count']}頭／未判定{first['unjudged_count']}頭",
             f"• 定義：{first['definition']}。第2のデータ不足・評価困難フラグとは別に記録します。",
             f"• 第2判定：{second['status']}／不一致数 {second['disagreement_count'] if second['disagreement_count'] is not None else '未算出'}／見送り効果未検証",
             f"• 評価困難フラグあり：{analysis['difficulty_horse_count']}頭／データ締切：{analysis['data_cutoff']}"]
    if record:
        lines.append(f"• 固定保存：{record['recorded_at']}／{'発走前' if record['is_prospective'] else '事後・時刻不明（前向き検証外）'}")
    lines.extend('• 整合性不足：' + reason for reason in analysis['roster_errors'])
    lines.extend('• 第2未確定項目：' + reason for reason in second['policy_issues'])
    if first['reference_condition_count'] != first['disagreement_count']:
        lines.append(f"• 時刻未確認などを含む参考条件該当：{first['reference_condition_count']}頭（確定不一致数へは加算しません）")
    by_number = {(h['horse_no'], h['horse_name']): h for h in second['horses']}
    for horse in first['horses']:
        lines.append(f"\n• [第1:{horse['status']}] {horse['horse_no'] or '馬番不明'} {horse['horse_name']}："
                     f"人気{horse['popularity_rank_display']}位／AI{horse['production_rank']}位／差(AI−人気){horse['rank_difference'] if horse['rank_difference'] is not None else str(horse['rank_difference_min'])+'〜'+str(horse['rank_difference_max'])}")
        lines.append('  - 不一致理由：' + '／'.join(horse['reasons']))
        lines.append(f"  - 人気の根拠：単勝オッズ {horse['popularity_odds']}／{horse['popularity_method']}／出典 {horse['popularity_source']}")
        lines.append(f"  - 取得時刻：人気 {horse['popularity_observed_at']}（{horse['popularity_time_source']}）／AI {horse['ai_observed_at']}／出典 {horse['input_source']}")
        h = by_number[(horse['horse_no'], horse['horse_name'])]
        lines.append(f"  - 第2：{h['status']}／独立順位 {h['independent_rank_min']}〜{h['independent_rank_max']}／差(AI−独立){h['rank_difference']}")
        for flag in h['features']['difficulty_flags']:
            lines.append(f"  - {flag['category']}：{flag['reason']}／根拠 {flag['data']}")
        for evidence in h['features']['history']:
            lines.append(f"  - 根拠 {evidence['date']} {evidence['distance']}：相手レベル{evidence['race_level']}({evidence['race_level_source']})、"
                         f"勝ち馬時計{evidence['winner_clock_seconds']}秒、自馬時計{evidence['own_clock_seconds']}秒、着差{evidence['margin_seconds']}秒、基準差{evidence['clock_difference']}秒 "
                         f"[シート{evidence['source_sheet']} 行{evidence['source_row']}／マスター{evidence['context_source']}]")
            lines.append(f"    レベル補正能力(β候補別) {evidence['ability_by_beta']}／次走着順比 {evidence['next_start_mean_finish_ratio']}・確認率 {evidence['next_start_coverage']}")
            baseline = {key: evidence['baseline'].get(key) for key in ('seconds', 'place', 'surface', 'distance', 'sample_count', 'created_at', 'data_cutoff', 'source_sha256', 'method')}
            lines.append(f"    基準時計の作成・締切 {baseline}／レース全体ラップ {evidence['lap']}（各馬固有の末脚ではありません）")
            lines.append(f"    自馬を除く相手rating根拠：{evidence['opponent_rating_evidence']}")
            lines.append(f"    相手次走の根拠：{evidence['next_start_evidence']}")
            if evidence['missing_reasons']:
                lines.append('    不足：' + '／'.join(evidence['missing_reasons']))
        lines.append(f"  - 近3走平均・180日半減期加重平均(β別)：{h['features']['ability_by_beta']}")
    return '\n'.join(lines)
