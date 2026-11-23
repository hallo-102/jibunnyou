"""予想時点までに判明した履歴だけを使う、第2ロジックの特徴量。"""
import math
import re
from datetime import datetime
from evaluation_utils import JST, number, integer, parse_date, parse_timestamp, course_parts, clock_seconds

BETA_CANDIDATES = (0, 2, 5, 10, 15, 20, 30)


def lap_features(value, distance=None):
    if isinstance(value, (list, tuple)):
        laps = [number(v) for v in value]
    else:
        text = str(value or '').strip()
        if not re.fullmatch(r'\d+(?:\.\d+)?(?:\s*[-,]\s*\d+(?:\.\d+)?)+', text):
            return None
        laps = [number(v.strip()) for v in re.split(r'[-,]', text)]
    if not laps or any(v is None or v <= 0 or v > 40 for v in laps):
        return None
    # 200m刻みと確認できない距離は、前半3F等へ無理に換算しない。
    if distance is None or distance % 200 or len(laps) != distance // 200:
        return {'scope': 'race', 'laps': laps, 'segment_metrics': None,
                'reason': '200m刻みを確認できないため区間指標は未算出'}
    fastest = min(laps)
    triples = [round(sum(laps[i:i + 3]), 3) for i in range(len(laps) - 2)]
    return {'scope': 'race', 'laps': laps,
            'early3f': sum(laps[:3]) if len(laps) >= 3 else None,
            'late3f': sum(laps[-3:]) if len(laps) >= 3 else None,
            'early4f': sum(laps[:4]) if len(laps) >= 4 else None,
            'late4f': sum(laps[-4:]) if len(laps) >= 4 else None,
            'fastest1f': fastest, 'fastest1f_positions': [i + 1 for i, v in enumerate(laps) if v == fastest],
            'fastest3f': min(triples) if triples else None,
            'fastest3f_start_positions': [i + 1 for i, v in enumerate(triples) if v == min(triples)] if triples else []}


def _available(value, cutoff):
    stamp = parse_timestamp(value)
    return stamp is not None and stamp <= cutoff


def past_features(past, cutoff):
    """時刻付きの相手ratingを最優先し、レースレベルは第2候補とする。"""
    surface, distance = course_parts(past.get('distance'))
    own = clock_seconds(past.get('clock'))
    winner = clock_seconds(past.get('winner_clock'))
    reasons = []
    if winner is None and integer(past.get('finish')) == 1 and own is not None:
        winner = own
    margin = own - winner if own is not None and winner is not None else None
    if margin is not None and margin < -0.001:
        reasons.append('勝ち馬時計より自馬時計が速い矛盾')
        margin = None
    subject = past.get('subject_horse_name')
    opponents = [o for o in past.get('opponents', []) if subject is None or o.get('horse_name') != subject]
    ratings = [number(o.get('pre_rating')) for o in opponents
               if _available(o.get('rating_available_at'), cutoff) and number(o.get('pre_rating')) is not None]
    rating_evidence = [{'horse_name': o.get('horse_name'), 'pre_rating': number(o.get('pre_rating')),
                        'available_at': o.get('rating_available_at'), 'source_sheet': o.get('source_sheet'),
                        'source_row': o.get('source_row')} for o in opponents
                       if _available(o.get('rating_available_at'), cutoff) and number(o.get('pre_rating')) is not None]
    expected_opponents = max(0, (integer(past.get('field_size')) or 1) - 1)
    level, level_source = None, None
    if expected_opponents and len(opponents) == expected_opponents and len(ratings) == expected_opponents:
        level, level_source = sum(ratings) / len(ratings), '自馬を除く対戦相手の発走前rating平均'
    elif number(past.get('race_level')) is not None and _available(past.get('race_level_available_at'), cutoff):
        level, level_source = number(past['race_level']), 'race_levelsのレースレベル'
    else:
        reasons.append('時点確認済みの相手rating／race_levelsが不足')
    if margin is None:
        reasons.append('自馬・勝ち馬の走破時計不足')
    baseline = past.get('baseline') or {}
    base_value = number(baseline.get('seconds'))
    base_surface, base_distance = baseline.get('surface'), integer(baseline.get('distance'))
    base_created = parse_timestamp(baseline.get('created_at'))
    data_cutoff = parse_timestamp(baseline.get('data_cutoff'))
    reconstructed = '時点再構成' in str(baseline.get('method'))
    baseline_valid = (base_value is not None and base_created is not None and data_cutoff is not None
                      and (base_created <= cutoff or reconstructed)
                      and data_cutoff <= cutoff and _available(baseline.get('source_available_at', baseline.get('created_at')), cutoff)
                      and bool(past.get('place')) and baseline.get('place') == past.get('place') and base_surface == surface and base_distance == distance)
    clock_difference = own - base_value if own is not None and baseline_valid else None
    if clock_difference is None:
        reasons.append('同競馬場・芝ダート・距離と作成時点を確認できる基準時計が不足')
    next_ratios = []
    next_evidence = []
    past_day = parse_date(past.get('date'))
    for opponent in opponents:
        result = opponent.get('next_start') or {}
        day = parse_date(result.get('date'))
        rank, field = integer(result.get('finish')), integer(result.get('field_size'))
        available_stamp = parse_timestamp(result.get('available_at'))
        if (day and past_day and day > past_day and _available(result.get('available_at'), cutoff)
                and available_stamp.date() >= day
                and day <= cutoff.date() and field and rank and 1 <= rank <= field):
            next_ratios.append(rank / field)
            next_evidence.append({'horse_name': opponent.get('horse_name'), **result, 'finish_ratio': rank / field})
    passing = past.get('passing')
    passing = str(passing) if passing is not None and re.fullmatch(r'\d+(?:-\d+)+', str(passing)) else None
    lap = lap_features(past.get('laps'), distance)
    trouble = past.get('trouble')
    trouble_flag = isinstance(trouble, str) and bool(re.search(r'不利|出遅|詰|躓|接触|大外|落鉄', trouble))
    return {'date': str(past.get('date')), 'past_race_id': past.get('past_race_id'),
            'source_sheet': past.get('source_sheet'), 'source_row': past.get('source_row'),
            'context_source': past.get('context_source'), 'distance': past.get('distance'), 'place': past.get('place'),
            'race_level': level, 'race_level_source': level_source, 'opponent_rating_count': len(ratings),
            'opponent_rating_evidence': rating_evidence, 'race_level_available_at': past.get('race_level_available_at'),
            'expected_opponents': expected_opponents, 'own_clock_seconds': own, 'winner_clock_seconds': winner,
            'margin_seconds': margin,
            'ability_by_beta': {str(beta): level - beta * margin if level is not None and margin is not None else None for beta in BETA_CANDIDATES},
            'clock_difference': clock_difference, 'baseline': baseline, 'lap': lap, 'passing': passing,
            'trouble': trouble, 'trouble_flag': trouble_flag,
            'next_start_mean_finish_ratio': sum(next_ratios) / len(next_ratios) if next_ratios else None,
            'next_start_coverage': len(next_ratios) / expected_opponents if expected_opponents else None,
            'next_start_evidence': next_evidence, 'missing_reasons': reasons,
            'pace_adjustment_available': number(past.get('pace_adjustment')) is not None,
            'schema_issues': past.get('schema_issues', [])}


def horse_features(horse, race, cutoff, policy):
    day = parse_date(race.get('date'))
    surface, distance = course_parts(race.get('course'))
    eligible, seen = [], set()
    for past in horse.get('history', []):
        pday = parse_date(past.get('date'))
        if not day or not pday or pday >= day or pday > cutoff.date():
            continue
        if past.get('available_at') and not _available(past.get('available_at'), cutoff):
            continue
        key = (pday, past.get('past_race_id'), past.get('distance'))
        if key in seen:
            continue
        seen.add(key)
        eligible.append((pday, past))
    eligible.sort(key=lambda pair: pair[0], reverse=True)
    # データ欠損のある直近走を飛ばして、都合のよい古い走りへ入れ替えない。
    recent = [(d, p) for d, p in eligible if day and 0 < (day - d).days <= 365][:5]
    features = [past_features({**p, 'subject_horse_name': horse.get('horse_name')}, cutoff) for _, p in recent]
    aggregated = {}
    for beta in BETA_CANDIDATES:
        values = [f['ability_by_beta'][str(beta)] for f in features]
        first3 = values[:3]
        # 近3走が不足するときは近3走平均を計算せず、標本数を明示する。
        mean3 = sum(first3) / 3 if len(first3) == 3 and all(v is not None for v in first3) else None
        weights = [2 ** (-(day - d).days / 180) for d, _ in recent] if day else []
        decay = sum(v * w for v, w in zip(values, weights)) / sum(weights) if values and all(v is not None for v in values) else None
        aggregated[str(beta)] = {'recent3_mean': mean3, 'decay_mean': decay, 'runs': len(values),
                                 'known_runs': sum(v is not None for v in values)}
    clocks = [f['clock_difference'] for f in features]
    next_ratios = [f['next_start_mean_finish_ratio'] for f in features]
    same = [p for _, p in eligible if course_parts(p.get('distance'))[0] == surface]
    near = [p for p in same if distance and course_parts(p.get('distance'))[1] is not None
            and abs(course_parts(p['distance'])[1] - distance) <= max(200, distance * 0.1)]
    flags = []
    def flag(code, reason, data):
        flags.append({'category': code, 'reason': reason, 'data': data})
    if len(eligible) < 3:
        flag('①過去走不足', '能力判断の材料が少ない', {'known_career_runs': len(eligible)})
    if not same or not near or surface is None:
        flag('②条件比較困難', '同芝ダートまたは近似距離の経験不足・条件不明',
             {'same_surface_runs': len(same), 'near_distance_runs': len(near), 'target_course': race.get('course')})
    if not features or any(f['clock_difference'] is None or not f['pace_adjustment_available'] for f in features):
        flag('③時計比較困難', '基準時計または馬場・ペース差を正当に補正する情報が不足',
             {'baseline_known_runs': sum(v is not None for v in clocks), 'recent_runs': len(features), 'same_day_correction': '第2段階・未実装'})
    if not features or any(f['race_level'] is None or f['next_start_coverage'] is None or f['next_start_coverage'] < 1 for f in features):
        flag('④相手比較困難', '相手の発走前rating／レースレベル／次走成績が不足',
             {'race_level_known_runs': sum(f['race_level'] is not None for f in features),
              'next_start_coverage_by_run': [f['next_start_coverage'] for f in features]})
    if not features or any(not f['passing'] or not f['lap'] or f['trouble_flag'] or f['schema_issues'] for f in features):
        flag('⑤内容評価困難', '位置取り・ラップ情報不足、原本列の整合性不足、または不利の記載あり',
             {'passing_known_runs': sum(bool(f['passing']) for f in features),
              'lap_known_runs': sum(bool(f['lap']) for f in features), 'trouble_runs': sum(f['trouble_flag'] for f in features)})
    rest = (day - eligible[0][0]).days if day and eligible else None
    age = integer(horse.get('age'))
    if rest is None or rest >= policy.get('long_rest_days', 180) or (age is not None and age <= 3):
        flag('⑥現在能力不確実', '休養期間・成長期などにより過去成績の現在への適用に注意',
             {'days_since_last_start': rest, 'long_rest_threshold_days': policy.get('long_rest_days', 180),
              'age': age, 'growth_change': '能力変化量は未確認'})
    return {'history': features, 'ability_by_beta': aggregated, 'difficulty_flags': flags,
            'recent_runs': len(recent), 'career_runs_observed': len(eligible),
            'clock_advantage': -sum(clocks) / len(clocks) if clocks and all(v is not None for v in clocks) else None,
            'next_start_strength': 1 - sum(next_ratios) / len(next_ratios) if next_ratios and all(v is not None for v in next_ratios) else None}
