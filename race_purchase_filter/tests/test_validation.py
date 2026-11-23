import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from evaluation_fixtures import sample_race
from independent import analyze_race
from storage import EvaluationStore
from validation import JST, build_evaluation_record, aggregate_capture, parse_finish_text


def record_at(hour=10):
    race = sample_race()
    return build_evaluation_record(race, analyze_race(race, as_of=datetime(2026, 11, 22, hour, 0, tzinfo=JST)), 'test.xlsx', 'sha',
                                   datetime(2026, 11, 22, hour, 0, tzinfo=JST))


class TestValidation(unittest.TestCase):
    def test_finish_parser_rejects_garbage_duplicates_and_missing_numbers(self):
        for text in ('1,2,3,余分', '1,1,3', '1,2', '1,2,-3'):
            self.assertIsNone(parse_finish_text(text))
        self.assertEqual(parse_finish_text('1,2,3'), [1, 2, 3])

    def test_retrospective_and_unknown_start_are_excluded(self):
        self.assertTrue(record_at(10)['is_prospective'])
        self.assertFalse(record_at(16)['is_prospective'])
        race = sample_race()
        race['start_time'] = '不明'
        self.assertFalse(build_evaluation_record(race, analyze_race(race), 'x', 's')['is_prospective'])

    def test_snapshot_is_append_only_and_reused(self):
        with tempfile.TemporaryDirectory() as folder:
            store = EvaluationStore(str(Path(folder) / 'evaluations.jsonl'))
            original = store.get_or_append(record_at())
            changed = record_at(16)
            changed['analysis']['judgement'] = '見送り候補'
            self.assertEqual(store.get_or_append(changed), original)
            self.assertEqual(len(store.load_all()), 1)
            changed['condition_version'] = 'next-version'
            store.get_or_append(changed)
            self.assertEqual(len(store.load_all()), 2)

    def test_capture_without_tickets_and_duplicates_count_once(self):
        record = record_at()
        result = {'evaluation_id': record['evaluation_id'], 'race_id': record['race_id'], 'finish_top3': [1, 2, 8]}
        stats = aggregate_capture([record], [result, result])['versions'][record['condition_version']]['購入候補']
        self.assertEqual(stats['race_count'], 1)
        self.assertAlmostEqual(stats['production']['rate'], 200 / 3)
        self.assertEqual(stats['production']['captured'], 2)
        self.assertEqual(stats['independent']['captured'], 0)
        self.assertEqual(stats['independent']['recorded_races'], 0)

    def test_late_records_not_used_as_prospective(self):
        record = record_at(16)
        result = {'evaluation_id': record['evaluation_id'], 'race_id': record['race_id'], 'finish_top3': [1, 2, 3]}
        stats = aggregate_capture([record], [result])
        self.assertEqual(stats['versions'], {})
        self.assertEqual(stats['excluded']['事後・時刻不明'], 1)

    def test_conflicting_finishes_or_ambiguous_originals_are_not_selected(self):
        record = record_at()
        first = {'evaluation_id': record['evaluation_id'], 'race_id': record['race_id'], 'finish_top3': [1, 2, 3]}
        second = {**first, 'finish_top3': [1, 2, 4]}
        stats = aggregate_capture([record], [first, second])
        self.assertEqual(stats['versions'], {})
        self.assertEqual(stats['excluded']['結果矛盾'], 1)
        other = record_at(11)
        other['sha256'] = 'other'
        stats = aggregate_capture([record, other], [first])
        self.assertEqual(stats['versions'], {})
        self.assertEqual(stats['excluded']['複数の事前保存'], 2)

    def test_unknown_snapshot_and_wrong_race_are_not_joined(self):
        record = record_at()
        bad = {'evaluation_id': record['evaluation_id'], 'race_id': 'wrong', 'finish_top3': [1, 2, 3]}
        unknown = {**bad, 'evaluation_id': 'unknown'}
        stats = aggregate_capture([record], [bad, unknown])
        self.assertEqual(stats['excluded']['結果矛盾'], 1)
        self.assertEqual(stats['excluded']['保存IDなし・不明'], 1)
        self.assertEqual(stats['versions'], {})

    def test_evaluation_versions_are_separate(self):
        first, second = record_at(), record_at()
        second['condition_version'] = 'second-version'
        results = [{'evaluation_id': e['evaluation_id'], 'race_id': e['race_id'], 'finish_top3': [1, 2, 3]}
                   for e in (first, second)]
        stats = aggregate_capture([first, second], results)
        self.assertEqual(len(stats['versions']), 2)

    def test_first_and_second_groups_and_virtual_profit_are_separate(self):
        record = record_at()
        record['virtual_tickets'] = [{'ticket_type': '馬連', 'combination': '1-2', 'amount': 200},
                                     {'ticket_type': '単勝', 'combination': '8', 'amount': 100}]
        result = {'evaluation_id': record['evaluation_id'], 'race_id': record['race_id'],
                  'finish_top3': [1, 2, 3], 'virtual_payouts': [
                      {'ticket_type': '馬連', 'combination': '2-1', 'payout_per_100': 1000}]}
        stats = aggregate_capture([record], [result, result])
        first = stats['versions'][record['condition_version']]['購入候補']
        self.assertEqual(first['virtual']['investment'], 300)
        self.assertEqual(first['virtual']['payout'], 2000)
        self.assertEqual(first['virtual']['profit'], 1700)
        second = stats['logic2_versions'][record['condition_version']]
        self.assertEqual(second['未算出・保留']['race_count'], 1)
        self.assertNotIn('購入候補', second)

    def test_missing_virtual_plan_or_winning_payout_is_not_zero(self):
        record = record_at()
        result = {'evaluation_id': record['evaluation_id'], 'race_id': record['race_id'], 'finish_top3': [1, 2, 3]}
        first = aggregate_capture([record], [result])['versions'][record['condition_version']]['全体']
        self.assertIsNone(first['virtual']['investment'])
        record['virtual_tickets'] = [{'ticket_type': '単勝', 'combination': '1', 'amount': 100}]
        first = aggregate_capture([record], [result])['versions'][record['condition_version']]['全体']
        self.assertEqual(first['virtual']['investment'], 100)
        self.assertIsNone(first['virtual']['payout'])
        self.assertIsNone(first['virtual']['profit'])

    def test_context_change_preserves_old_snapshot(self):
        with tempfile.TemporaryDirectory() as folder:
            store = EvaluationStore(str(Path(folder) / 'evaluations.jsonl'))
            first = record_at()
            second = record_at()
            first['input_fingerprint'], second['input_fingerprint'] = 'first-context', 'second-context'
            store.get_or_append(first)
            store.get_or_append(second)
            self.assertEqual(len(store.load_all()), 2)


if __name__ == '__main__':
    unittest.main()
