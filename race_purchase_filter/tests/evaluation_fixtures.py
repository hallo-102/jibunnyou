"""実ファイルに依存しない、独立評価の境界条件用データ。"""
from datetime import date, timedelta


def sample_race(size=8):
    horses = []
    for n in range(1, size + 1):
        history = []
        for i in range(3):
            history.append({'date': (date(2026, 11, 22) - timedelta(days=30 + i * 30)).isoformat(),
                            'distance': 'ダ1800', 'finish': n, 'field_size': 12, 'race_name': '3歳以上2勝クラス',
                            'clock': '1:53.0', 'time_index': 110 - n * 5, 'past_race_id': f'past-{n}-{i}',
                            'source_sheet': '202608050511', 'source_row': n * 3 + i})
        horses.append({'horse_no': n, 'horse_name': f'馬{n}', 'production_rank': n,
                       'production_score': 100 - n, 'history': history,
                       'popularity_rank': n, 'popularity_observed_at': '2026-11-22T09:00:00+09:00',
                       'ai_observed_at': '2026-11-22T09:00:00+09:00', 'age': 4,
                       'popularity_time_source': 'テスト用事前記録'})
    return {'race_id': '202608050511', 'date': '2026-11-22', 'place': '京都', 'race_no': 11,
            'race_name': '2勝クラス', 'course': 'ダ1800', 'field_size': size, 'start_time': '15:40',
            'horses': horses, 'predicted_top5': list(range(1, min(size, 5) + 1))}
