"""第1判定の明示式と、第2・評価困難診断の分離を検証する。"""
import copy
import unittest
from datetime import datetime
from independent import analyze_race, load_policy
from evaluation_utils import JST
from evaluation_fixtures import sample_race

NOW = datetime(2026, 11, 22, 10, tzinfo=JST)


class TestFirstLogic(unittest.TestCase):
    def test_four_user_examples(self):
        for popularity, ai, expected in ((2, 6, True), (5, 11, True), (3, 4, False), (7, 2, False)):
            with self.subTest(popularity=popularity, ai=ai):
                race = sample_race(12)
                race['horses'] = [race['horses'][0]]
                horse = race['horses'][0]
                horse.update(popularity_rank=popularity, production_rank=ai)
                result = analyze_race(race, as_of=NOW)['logic1']['horses'][0]
                self.assertEqual(result['is_mismatch'], expected)
                self.assertEqual(result['rank_difference'], ai - popularity)

    def test_zero_one_two_mismatches(self):
        for count, expected in ((0, '購入候補'), (1, '購入候補'), (2, '見送り候補')):
            race = sample_race()
            if count >= 1:
                race['horses'][0]['production_rank'], race['horses'][5]['production_rank'] = 6, 1
            if count == 2:
                race['horses'][1]['production_rank'], race['horses'][6]['production_rank'] = 7, 2
            analysis = analyze_race(race, as_of=NOW)
            self.assertEqual(analysis['logic1']['disagreement_count'], count)
            self.assertEqual(analysis['judgement'], expected)

    def test_missing_popularity_or_ai_never_counts_as_known_zero(self):
        for key in ('popularity_rank', 'production_rank'):
            race = sample_race()
            race['horses'][-1][key] = None
            result = analyze_race(race, as_of=NOW)
            self.assertEqual(result['judgement'], '判定保留')
            self.assertGreater(result['unjudged_count'], 0)
            self.assertIsNone(result['logic1']['horses'][-1]['is_mismatch'])

    def test_after_start_missing_and_future_observation_times_hold(self):
        for value in (None, '2026-11-22T16:00:00+09:00', '2026-11-22T11:00:00+09:00'):
            race = sample_race()
            race['horses'][0]['popularity_observed_at'] = value
            self.assertEqual(analyze_race(race, as_of=NOW)['judgement'], '判定保留')

    def test_history_difficulty_does_not_change_first_formula(self):
        race = sample_race()
        race['horses'][0]['history'] = []
        result = analyze_race(race, as_of=NOW)
        self.assertEqual(result['judgement'], '購入候補')
        self.assertGreater(result['difficulty_horse_count'], 0)
        self.assertEqual(result['logic2']['status'], '係数・閾値未確定')
        self.assertIsNone(result['logic2']['disagreement_count'])
        self.assertEqual(result['independent_top5'], [])

    def test_no_production_or_input_mutation(self):
        race = sample_race()
        before = copy.deepcopy(race)
        analyze_race(race, as_of=NOW)
        self.assertEqual(race, before)

    def test_future_or_unconfirmed_coefficient_policy_is_not_applied(self):
        race = sample_race()
        policy = load_policy()
        policy.update(beta=5, weights={'ability_decay': 1}, rank_gap_threshold=3,
                      selection_start='2026-10-01', selection_end='2026-11-22',
                      frozen_at='2026-11-22T12:00:00+09:00', selection_evidence='test')
        result = analyze_race(race, as_of=NOW, policy=policy)
        self.assertEqual(result['logic2']['status'], '係数・閾値未確定')
        self.assertIsNone(result['logic2']['disagreement_count'])


if __name__ == '__main__':
    unittest.main()
