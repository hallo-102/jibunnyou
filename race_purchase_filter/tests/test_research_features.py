import copy
import unittest
from datetime import datetime, timedelta
from evaluation_utils import JST
from research_features import past_features, horse_features, lap_features
from independent import load_policy, analyze_race
from evaluation_fixtures import sample_race

NOW = datetime(2026, 11, 22, 10, tzinfo=JST)


def rich_past():
    return {'date': '2026-10-01', 'distance': 'ダ1800', 'place': '京都', 'finish': 2, 'field_size': 3,
            'clock': '1:52.0', 'winner_clock': '1:50.0', 'race_level': 1400,
            'race_level_available_at': '2026-11-01T09:00:00+09:00',
            'opponents': [{'horse_name': '相手1', 'pre_rating': 1500, 'rating_available_at': '2026-11-01T09:00:00+09:00',
                           'next_start': {'date': '2026-10-15', 'finish': 2, 'field_size': 10, 'available_at': '2026-11-01T09:00:00+09:00'}},
                          {'horse_name': '相手2', 'pre_rating': 1600, 'rating_available_at': '2026-11-01T09:00:00+09:00'}],
            'baseline': {'place': '京都', 'surface': 'ダ', 'distance': 1800, 'seconds': 111,
                         'created_at': '2026-11-01T09:00:00+09:00', 'data_cutoff': '2026-10-31T23:00:00+09:00'}}


class TestResearchFeatures(unittest.TestCase):
    def test_opponent_mean_precedes_race_level_and_margin_is_clock_difference(self):
        result = past_features(rich_past(), NOW)
        self.assertEqual(result['race_level'], 1550)
        self.assertEqual(result['margin_seconds'], 2)
        self.assertEqual(result['ability_by_beta']['5'], 1540)
        self.assertEqual(result['clock_difference'], 1)
        self.assertEqual(result['next_start_coverage'], 0.5)
        self.assertEqual(result['next_start_mean_finish_ratio'], 0.2)

    def test_future_ratings_fall_back_and_unknown_level_not_zero(self):
        past = rich_past()
        past['opponents'][0]['rating_available_at'] = '2026-12-01T00:00:00+09:00'
        self.assertEqual(past_features(past, NOW)['race_level'], 1400)
        past['race_level_available_at'] = None
        self.assertIsNone(past_features(past, NOW)['ability_by_beta']['0'])

    def test_future_next_start_and_baseline_are_not_used(self):
        past = rich_past()
        past['opponents'][0]['next_start']['available_at'] = '2026-12-01T00:00:00+09:00'
        past['baseline']['created_at'] = '2026-12-01T00:00:00+09:00'
        result = past_features(past, NOW)
        self.assertEqual(result['next_start_coverage'], 0)
        self.assertIsNone(result['next_start_mean_finish_ratio'])
        self.assertIsNone(result['clock_difference'])

    def test_laps_are_race_scope_and_find_fastest_positions(self):
        result = lap_features([13, 12, 11, 12, 11, 12], 1200)
        self.assertEqual(result['scope'], 'race')
        self.assertEqual(result['early3f'], 36)
        self.assertEqual(result['late3f'], 35)
        self.assertEqual(result['fastest1f_positions'], [3, 5])
        self.assertEqual(result['fastest3f_start_positions'], [3])
        self.assertIsNone(lap_features('13 - 12 - 11', 1700).get('segment_metrics'))

    def test_max_five_runs_365_days_and_half_life_180(self):
        race = sample_race()
        horse = race['horses'][0]
        horse['history'] = []
        for days, level in ((10, 1600), (190, 1400), (366, 9999)):
            past = rich_past()
            past.update(date=(NOW.date() - timedelta(days=days)).isoformat(), opponents=[], race_level=level,
                        past_race_id=str(days), winner_clock='1:52.0')
            horse['history'].append(past)
        result = horse_features(horse, race, NOW, load_policy())
        self.assertEqual(result['recent_runs'], 2)
        beta = result['ability_by_beta']['0']
        self.assertIsNone(beta['recent3_mean'])
        self.assertAlmostEqual(beta['decay_mean'], (1600 * 2 + 1400) / 3)

    def test_six_difficulty_categories_are_recorded_independently(self):
        race = sample_race()
        race['horses'][0].update(history=[], age=2)
        result = horse_features(race['horses'][0], race, NOW, load_policy())
        self.assertEqual(len(result['difficulty_flags']), 6)

    def test_fixed_policy_can_rank_but_does_not_issue_second_purchase_recommendation(self):
        race = sample_race()
        for i, horse in enumerate(race['horses']):
            for past in horse['history']:
                past.update(winner_clock='1:50.0', race_level=1600 - i * 20,
                            race_level_available_at='2026-11-01T00:00:00+09:00')
        policy = load_policy()
        policy.update(beta=5, weights={'ability_decay': 1}, rank_gap_threshold=3,
                      selection_start='2026-09-01', selection_end='2026-10-31',
                      frozen_at='2026-11-01T00:00:00+09:00', selection_evidence='過去選択の固定済み結果')
        result = analyze_race(race, as_of=NOW, policy=policy)
        self.assertEqual(result['logic2']['disagreement_count'], 0)
        self.assertEqual(result['logic2']['judgement'], '判定保留')
        self.assertEqual(result['independent_top5'], [1, 2, 3, 4, 5])


if __name__ == '__main__':
    unittest.main()
