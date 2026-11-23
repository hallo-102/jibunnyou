import unittest
from datetime import datetime
from evaluation_utils import JST
from research_context import enrich_race
from evaluation_fixtures import sample_race


class TestContext(unittest.TestCase):
    def _context(self):
        return {'path': 'test-master', 'sha256': 'sha', 'available_at': '2026-11-01T00:00:00+09:00',
                'races': {'past-1-0': {'date': '2026-10-23', 'ground': 'ダ', 'distance': 1800, 'place': '京都'}},
                'levels': {'past-1-0': {'race_level_score': 1500}},
                'entries': {'past-1-0': [
                    {'horse_id': 1, 'horse_name': '馬1', 'rank': 2, 'time_sec': 112, 'pre_rating': 9999},
                    {'horse_id': 2, 'horse_name': '相手', 'rank': 1, 'time_sec': 110, 'pre_rating': 1500}]},
                'by_horse': {}}

    def test_self_rating_is_excluded_and_source_is_frozen(self):
        race = sample_race()
        context = self._context()
        now = datetime(2026, 11, 22, 10, tzinfo=JST)
        result = enrich_race(race, context, now, now)
        past = result['horses'][0]['history'][0]
        self.assertEqual(len(past['opponents']), 1)
        self.assertEqual(past['opponents'][0]['pre_rating'], 1500)
        self.assertEqual(past['winner_clock'], 110)
        self.assertNotIn('opponents', race['horses'][0]['history'][0])

    def test_wrong_historical_date_or_future_vintage_is_not_joined(self):
        race = sample_race()
        now = datetime(2026, 11, 22, 10, tzinfo=JST)
        context = self._context()
        context['races']['past-1-0']['date'] = '2025-10-23'
        result = enrich_race(race, context, now, now)
        self.assertNotIn('opponents', result['horses'][0]['history'][0])
        context = self._context()
        context['available_at'] = '2026-12-01T00:00:00+09:00'
        result = enrich_race(race, context, now, now)
        self.assertNotIn('opponents', result['horses'][0]['history'][0])


if __name__ == '__main__':
    unittest.main()
