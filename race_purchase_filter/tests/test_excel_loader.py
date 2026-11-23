"""今走と過去走を取り違えず、原本の出走馬を読むための回帰テスト。"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from excel_loader import _find_column, load_races_from_xlsx

try:
    import openpyxl
except ImportError:
    openpyxl = None


class TestColumnMatching(unittest.TestCase):
    def test_place_does_not_match_track_condition(self):
        # 短い「場」が「馬場」や「馬場指数」に部分一致してはいけない。
        self.assertIsNone(_find_column(['馬 場', '馬場指数'], ['競馬場', '場', 'place']))

    def test_spaces_in_headers_are_ignored(self):
        self.assertEqual(_find_column(['馬 番', '馬 名'], ['馬番']), 0)


@unittest.skipIf(openpyxl is None, 'openpyxl is optional')
class TestWorkbookLayout(unittest.TestCase):
    def _save_and_load(self, wb, name='馬の競走成績_with_feat_20261121.xlsx'):
        # テスト用ブックは一時領域のみへ保存し、ユーザーの原本は変更しない。
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / name
            wb.save(path)
            wb.close()
            return load_races_from_xlsx(str(path))

    def test_current_sheet_and_summary_replace_active_history(self):
        wb = openpyxl.Workbook()
        past = wb.active
        past.title = '202603030501'
        past.append(['馬名', '日付', 'R', '馬 場', '馬 番', 'race_id', 'レース名'])
        past.append(['過去走の馬', '2026/08/09', 5, '良', 9, '202601010605', '2歳新馬'])
        current = wb.create_sheet('今走レース情報')
        current.append(['レースID', 'rid_str', '場所', '馬番', '馬名', 'レース名', '頭数'])
        current.append(['202603030501', '202603030501', '福島', 1, 'ヌーベルフィズ', '2歳未勝利', 2])
        current.append(['202603030501', '202603030501', '福島', 2, 'シュトラールモルン', '2歳未勝利', 2])
        summary = wb.create_sheet('買い目_レース別1行')
        summary.append(['レースID', '場所', 'レース名', '頭数'])
        summary.append([202603030501, '福島', '2歳未勝利', 2])
        summary.append(['202608050505', '京都', '2歳新馬', 16])
        result = self._save_and_load(wb)
        self.assertEqual(len(result['races']), 2)
        first, missing = result['races']
        self.assertEqual((first['date'], first['place'], first['race_no'], first['race_id']),
                         ('2026-11-21', '福島', 1, '202603030501'))
        self.assertEqual([{k: h[k] for k in ('horse_no', 'horse_name')} for h in first['horses']], [
            {'horse_no': 1, 'horse_name': 'ヌーベルフィズ'},
            {'horse_no': 2, 'horse_name': 'シュトラールモルン'}])
        self.assertEqual(missing['race_no'], 5)
        self.assertEqual(missing['horses'], [])
        self.assertEqual(missing['field_size'], 16)
        self.assertTrue(missing['horse_data_incomplete'])
        self.assertEqual(result['source_sheets'], ['今走レース情報', '買い目_レース別1行', '202603030501'])

    def test_current_sheet_without_summary_uses_rid_str(self):
        wb = openpyxl.Workbook()
        wb.active.title = '今走レース情報'
        wb.active.append(['rid_str', '場所', '馬 番', '馬名', 'レース名'])
        wb.active.append(['202608050511', '京都', 12, 'メルカントゥール', 'シトリンS'])
        race = self._save_and_load(wb)['races'][0]
        self.assertEqual((race['race_id'], race['race_no'], race['place']), ('202608050511', 11, '京都'))

    def test_unknown_filename_does_not_invent_date(self):
        wb = openpyxl.Workbook()
        wb.active.title = '今走レース情報'
        wb.active.append(['レースID', '場所', '馬番', '馬名'])
        wb.active.append(['202608050511', '京都', 12, 'メルカントゥール'])
        self.assertEqual(self._save_and_load(wb, 'input.xlsx')['races'][0]['date'], '不明')

    def test_generic_table_still_reads_explicit_columns(self):
        wb = openpyxl.Workbook()
        wb.active.append(['開催日', '競馬場', 'R', 'race_id', '馬 番', '馬名', 'レース名'])
        wb.active.append(['2026-11-21', '京都', 11, '202608050511', 12, 'メルカントゥール', 'シトリンS'])
        race = self._save_and_load(wb, 'input.xlsx')['races'][0]
        self.assertEqual((race['date'], race['place'], race['race_no']), ('2026-11-21', '京都', 11))
        self.assertEqual(race['horses'][0]['horse_no'], 12)

    def test_history_only_workbook_fails_instead_of_showing_past_races(self):
        wb = openpyxl.Workbook()
        wb.active.title = '202608050511'
        wb.active.append(['日付', '開催', 'R', '馬 場', '馬名', 'race_id', 'RaceData01'])
        wb.active.append(['2026/08/09', '1札幌6', 5, '良', '過去走の馬', '202601010605', '発走情報'])
        with self.assertRaisesRegex(ValueError, '今走レース情報'):
            self._save_and_load(wb)

    def test_original_judgements_top5_and_roi_are_read_without_inventing_missing_values(self):
        wb = openpyxl.Workbook()
        wb.active.title = '買い目_レース別1行'
        wb.active.append(['レースID', '場所', 'レース名', 'ランク(S/A/B)', '判定', '理由',
                          '1位馬番', '2位馬番', '3位馬番', '4位馬番', '5位馬番', '予想状態'])
        wb.active.append(['202603030511', '福島', '福島記念', 'S', '推奨レース', '原本の理由',
                          13, 16, 11, 1, 2, 'PREDICTED'])
        wb.active.append(['202603030512', '福島', '1勝クラス', 'B', '少額・参考'])
        roi = wb.create_sheet('回収率重視_買い目候補')
        roi.append(['レースID', '購入判定', '購入理由', '合計購入金額'])
        roi.append([202603030511, '購入', '原本の購入理由', 700])
        result = self._save_and_load(wb)
        first, other = result['races']
        self.assertEqual(first['excel_rank'], 'S')
        self.assertEqual(first['excel_judgement'], '推奨レース')
        self.assertEqual(first['predicted_top5'], [13, 16, 11, 1, 2])
        self.assertEqual(first['roi_judgement'], '購入')
        self.assertEqual(first['roi_amount'], 700)
        self.assertEqual(other.get('roi_judgement'), '不明')
        self.assertNotIn('disagreement_count', first)

    def test_missing_race_ids_do_not_merge_different_venues_or_duplicate_metadata(self):
        wb = openpyxl.Workbook()
        wb.active.title = '今走レース情報'
        wb.active.append(['場所', 'R', '馬番', '馬名'])
        wb.active.append(['福島', 1, 1, '福島の馬'])
        wb.active.append(['京都', 1, 1, '京都の馬'])
        summary = wb.create_sheet('買い目_レース別1行')
        summary.append(['場所', 'R', '頭数'])
        summary.append(['福島', 1, 1])
        summary.append(['京都', 1, 1])
        races = self._save_and_load(wb)['races']
        self.assertEqual(len(races), 2)
        self.assertEqual({r['race_id'] for r in races}, {'不明'})
        self.assertEqual(len({r['race_key'] for r in races}), 2)
        self.assertEqual([r['horses'][0]['horse_name'] for r in races], ['福島の馬', '京都の馬'])


if __name__ == '__main__':
    unittest.main()
