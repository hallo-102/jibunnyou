import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from core import aggregate_methods, check_ticket_hit, compute_top5_capture, judge_c, judge_disagreement  # noqa: E402
from storage import DuplicateSnapshotError, ResultStore, SnapshotStore, compute_sha256  # noqa: E402


class TestJudge(unittest.TestCase):
    def test_zero_buy(self):
        self.assertEqual(judge_disagreement(0), "購入候補")

    def test_one_buy(self):
        self.assertEqual(judge_disagreement(1), "購入候補")

    def test_two_skip(self):
        self.assertEqual(judge_disagreement(2), "見送り候補")
        self.assertEqual(judge_disagreement(5), "見送り候補")

    def test_empty_hold(self):
        self.assertEqual(judge_disagreement(None), "判定保留")
        self.assertEqual(judge_disagreement(""), "判定保留")

    def test_c_always_hold(self):
        self.assertEqual(judge_c(), "判断保留・未算出")


class TestSnapshotImmutable(unittest.TestCase):
    def test_no_update_delete_api(self):
        self.assertFalse(hasattr(SnapshotStore, "update_snapshot"))
        self.assertFalse(hasattr(SnapshotStore, "delete_snapshot"))
        self.assertFalse(hasattr(SnapshotStore, "update"))
        self.assertFalse(hasattr(SnapshotStore, "delete"))

    def test_duplicate_error(self):
        with tempfile.TemporaryDirectory() as d:
            store = SnapshotStore(os.path.join(d, "snapshots.jsonl"))
            rec = {"race_id": "202601010101", "sha256": "abc123"}
            store.append_snapshot(rec)
            with self.assertRaises(DuplicateSnapshotError):
                store.append_snapshot(dict(rec))
            # Different sha is allowed
            store.append_snapshot({"race_id": "202601010101", "sha256": "other"})
            self.assertEqual(len(store.load_all()), 2)

    def test_sha256(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "x.txt")
            with open(p, "wb") as f:
                f.write(b"hello")
            self.assertEqual(
                compute_sha256(p),
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            )

    def test_results_separate_file(self):
        with tempfile.TemporaryDirectory() as d:
            snap = SnapshotStore(os.path.join(d, "snapshots.jsonl"))
            res = ResultStore(os.path.join(d, "results.jsonl"))
            snap.append_snapshot({"race_id": "1", "sha256": "s"})
            res.append_result({"race_id": "1", "finish_top3": [1, 2, 3]})
            self.assertEqual(len(snap.load_all()), 1)
            self.assertNotIn("finish_top3", snap.load_all()[0])


class TestHit(unittest.TestCase):
    finish = [3, 5, 7]

    def test_umaren(self):
        self.assertTrue(check_ticket_hit("馬連", "5-3", self.finish))
        self.assertFalse(check_ticket_hit("馬連", "3-7", self.finish))

    def test_sanrenpuku(self):
        self.assertTrue(check_ticket_hit("3連複", "7-3-5", self.finish))
        self.assertFalse(check_ticket_hit("3連複", "3-5-8", self.finish))

    def test_sanrentan(self):
        self.assertTrue(check_ticket_hit("3連単", "3-5-7", self.finish))
        self.assertFalse(check_ticket_hit("3連単", "5-3-7", self.finish))

    def test_tansho(self):
        self.assertTrue(check_ticket_hit("単勝", "3", self.finish))
        self.assertFalse(check_ticket_hit("単勝", "5", self.finish))

    def test_fukusho_wide_umatan(self):
        self.assertTrue(check_ticket_hit("複勝", "7", self.finish))
        self.assertFalse(check_ticket_hit("複勝", "9", self.finish))
        self.assertTrue(check_ticket_hit("ワイド", "5-7", self.finish))
        self.assertFalse(check_ticket_hit("ワイド", "3-9", self.finish))
        self.assertTrue(check_ticket_hit("馬単", "3-5", self.finish))
        self.assertFalse(check_ticket_hit("馬単", "5-3", self.finish))

    def test_unknown_ticket_returns_none(self):
        self.assertIsNone(check_ticket_hit("不明券種", "3-5", self.finish))
        self.assertIsNone(check_ticket_hit("馬連", "abc", self.finish))


class TestTop5(unittest.TestCase):
    def test_capture(self):
        self.assertEqual(compute_top5_capture([3, 5, 7], [3, 5, 9, 11, 13]), 2)
        self.assertIsNone(compute_top5_capture([3, 5, 7], []))
        self.assertIsNone(compute_top5_capture([3, 5, 7], None))


class TestAggregate(unittest.TestCase):
    def _data(self):
        snapshots = [
            {"race_id": "R1", "disagreement_count": 0, "sha256": "a"},
            {"race_id": "R2", "disagreement_count": 2, "sha256": "b"},
            {"race_id": "R3", "disagreement_count": 1, "sha256": "c"},
        ]
        results = [
            {"race_id": "R1", "finish_top3": [1, 2, 3], "predicted_top5": [1, 2, 3, 4, 5],
             "tickets": [{"ticket_type": "馬連", "combination": "1-2", "amount": 1000, "payout": 2000}]},
            {"race_id": "R2", "finish_top3": [1, 2, 3], "predicted_top5": [1, 9, 10, 11, 12],
             "tickets": [{"ticket_type": "馬連", "combination": "1-2", "amount": 1000, "payout": 0}]},
            {"race_id": "R3", "finish_top3": [4, 5, 6], "predicted_top5": [4, 5, 9, 10, 11],
             "tickets": [{"ticket_type": "馬連", "combination": "4-5", "amount": 1000, "payout": 1500}]},
        ]
        return snapshots, results

    def test_a_b_hand_calc(self):
        snapshots, results = self._data()
        agg = aggregate_methods(snapshots, results)
        a = agg["A"]
        # A: 3 races, investment 3000, payout 3500, profit 500, recovery 116.666...
        self.assertEqual(a["race_count"], 3)
        self.assertEqual(a["investment"], 3000)
        self.assertEqual(a["payout"], 3500)
        self.assertEqual(a["profit"], 500)
        self.assertAlmostEqual(a["recovery_rate"], 3500 / 3000 * 100)
        # TOP5 A: captures 3+1+2=6 / 9 slots
        self.assertAlmostEqual(a["top5_capture_rate"], 6 / 9 * 100)
        b = agg["B"]
        # B: R1+R3 only, investment 2000, payout 3500, profit 1500, recovery 175%
        self.assertEqual(b["race_count"], 2)
        self.assertEqual(b["investment"], 2000)
        self.assertEqual(b["payout"], 3500)
        self.assertEqual(b["profit"], 1500)
        self.assertAlmostEqual(b["recovery_rate"], 175.0)
        self.assertEqual(b["excluded_count"], 1)

    def test_c_uncalculated(self):
        snapshots, results = self._data()
        agg = aggregate_methods(snapshots, results)
        self.assertEqual(agg["C"]["status"], "未算出")
        self.assertIsNone(agg["C"]["recovery_rate"])
        self.assertIsNone(agg["C"]["profit"])

    def test_unknown_not_filled_zero(self):
        snapshots = [{"race_id": "R1", "disagreement_count": 0, "sha256": "a"}]
        results = [
            {"race_id": "R1", "finish_top3": [1, 2, 3], "predicted_top5": [],
             "tickets": [{"ticket_type": "馬連", "combination": "1-2", "amount": 1000, "payout": None}]},
        ]
        agg = aggregate_methods(snapshots, results)
        # payout unknown -> recovery must be None (未計算), not 0-filled
        self.assertIsNone(agg["A"]["recovery_rate"])
        self.assertIsNone(agg["A"]["payout"])
        self.assertIsNone(agg["A"]["top5_capture_rate"])

    def test_zero_investment_recovery_none(self):
        snapshots = [{"race_id": "R1", "disagreement_count": 0, "sha256": "a"}]
        results = [
            {"race_id": "R1", "finish_top3": [1, 2, 3],
             "tickets": [{"ticket_type": "単勝", "combination": "1", "amount": 0, "payout": 0}]},
        ]
        agg = aggregate_methods(snapshots, results)
        self.assertIsNone(agg["A"]["recovery_rate"])


if __name__ == "__main__":
    unittest.main()
