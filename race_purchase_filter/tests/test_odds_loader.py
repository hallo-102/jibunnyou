"""NAS CSVの単勝人気算出・強い照合・欠損・同オッズ・固定保存を検証する。"""
import copy
import csv
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from assessment import analyze_race
from evaluation_utils import JST
from evaluation_fixtures import sample_race
from odds_loader import attach_nas_odds, read_odds_csv

NOW = datetime(2026, 11, 22, 10, tzinfo=JST)
FIELDS = ['date', 'racecourse', 'race', 'name', 'bet_type', 'combination', 'odds']


class TestOddsLoader(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.race = sample_race()
        self.data = {'file_name': 'test.xlsx', 'sha256': 'excel-sha', 'races': [self.race], 'warnings': []}
        self.rows = [{'date': '20261122', 'racecourse': '京都', 'race': '11R', 'name': f'馬{n}',
                      'bet_type': '単勝', 'combination': n, 'odds': n + 1} for n in range(1, 9)]

    def _write(self, rows=None, fields=FIELDS, encoding='utf-8-sig'):
        path = self.folder / 'OZZU_20261122.csv'
        with path.open('w', encoding=encoding, newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(self.rows if rows is None else rows)
        return path

    def _attach(self, now=NOW):
        return attach_nas_odds(self.data, directory=self.folder, archive_dir=self.folder / 'archive', now=now)

    def test_single_win_only_and_ascending_odds_override_excel_popularity(self):
        self.rows.reverse()
        self.rows.append({**self.rows[0], 'bet_type': '複勝', 'odds': 0.1})
        self._write()
        self.race['horses'][0]['popularity_rank'] = 8
        result = self._attach()
        horses = result['races'][0]['horses']
        self.assertEqual(horses[0]['popularity_rank'], 1)
        self.assertEqual(horses[-1]['popularity_rank'], 8)
        self.assertEqual(horses[0]['popularity_odds'], 2)
        self.assertEqual(self.race['horses'][0]['popularity_rank'], 8)
        self.assertEqual(result['odds_matched_horses'], 8)
        self.assertEqual(analyze_race(result['races'][0], as_of=NOW)['judgement'], '購入候補')

    def test_missing_csv_does_not_fall_back_to_excel_or_another_date(self):
        result = self._attach()
        self.assertTrue(result['warnings'])
        self.assertTrue(all(h['popularity_rank'] is None for h in result['races'][0]['horses']))
        self.assertEqual(analyze_race(result['races'][0], as_of=NOW)['judgement'], '判定保留')

    def test_csv_mismatch_date_race_horse_name_and_incomplete_rows_hold(self):
        for problem in ('date', 'race', 'name', 'missing', 'invalid_odds', 'duplicate'):
            with self.subTest(problem=problem):
                rows = copy.deepcopy(self.rows)
                if problem == 'date':
                    rows[0]['date'] = '20261121'
                elif problem == 'race':
                    for row in rows:
                        row['racecourse'] = '東京'
                elif problem == 'name':
                    rows[0]['name'] = '別の馬'
                elif problem == 'missing':
                    rows.pop()
                elif problem == 'invalid_odds':
                    rows[0]['odds'] = '---'
                else:
                    rows.append({**rows[0], 'odds': 10})
                self._write(rows)
                result = self._attach()
                self.assertEqual(analyze_race(result['races'][0], as_of=NOW)['judgement'], '判定保留')

    def test_equal_odds_at_fifth_boundary_are_unknown_not_arbitrary_horse_order(self):
        self.rows[4]['odds'] = self.rows[5]['odds'] = 6
        self._write()
        self.race['horses'][4]['production_rank'], self.race['horses'][5]['production_rank'] = 6, 5
        result = self._attach()
        analysis = analyze_race(result['races'][0], as_of=NOW)
        self.assertEqual(analysis['logic1']['horses'][4]['popularity_rank_display'], '5〜6')
        self.assertIsNone(analysis['logic1']['horses'][4]['is_mismatch'])
        self.assertEqual(analysis['judgement'], '判定保留')

    def test_equal_odds_inside_top_five_can_still_be_judged(self):
        self.rows[0]['odds'] = self.rows[1]['odds'] = 2
        self._write()
        self.race['horses'][0]['production_rank'], self.race['horses'][5]['production_rank'] = 6, 1
        analysis = analyze_race(self._attach()['races'][0], as_of=NOW)
        self.assertEqual(analysis['disagreement_count'], 1)
        self.assertEqual(analysis['judgement'], '購入候補')

    def test_snapshot_hash_unchanged_on_reread_and_changes_with_csv(self):
        path = self._write()
        before = path.read_bytes()
        first = self._attach()
        second = self._attach(NOW + timedelta(minutes=1))
        self.assertEqual(first['odds_fingerprint'], second['odds_fingerprint'])
        archived = Path(first['odds_sources'][0]['archive_path'])
        self.assertEqual(archived.read_bytes(), before)
        self.assertEqual(path.read_bytes(), before)
        self.rows[0]['odds'] = 1.5
        self._write()
        self.assertNotEqual(first['odds_fingerprint'], self._attach()['odds_fingerprint'])

    def test_official_popularity_column_is_used_when_present(self):
        for row in self.rows:
            row['popularity_rank'] = 9 - row['combination']
        self._write(fields=FIELDS + ['popularity_rank'])
        result = self._attach()
        self.assertEqual(result['races'][0]['horses'][0]['popularity_rank'], 8)
        self.assertEqual(result['odds_sources'][0]['method'], 'CSV記載の人気順位')

    def test_cp932_csv_and_roster_supplement_without_ai_invention(self):
        self._write(encoding='cp932')
        self.race['horses'] = []
        result = self._attach()
        horses = result['races'][0]['horses']
        self.assertEqual(len(horses), 8)
        self.assertTrue(all(h['production_rank'] is None for h in horses))
        self.assertEqual(result['odds_sources'][0]['encoding'], 'cp932')
        self.assertEqual(analyze_race(result['races'][0], as_of=NOW)['judgement'], '判定保留')


if __name__ == '__main__':
    unittest.main()
