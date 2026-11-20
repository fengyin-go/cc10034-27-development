"""回填区间留档的单元测试：区间运算、接入、重开领取和并发追加。

运行：cd backend && .venv/bin/python -m unittest discover -s tests
"""
from __future__ import annotations

import threading
import unittest

from app.services.backfill_ledger import (
    BackfillLedger,
    LedgerError,
    ledger,
    normalize_intervals,
    split_attribution,
    subtract_intervals,
)


class IntervalMathTests(unittest.TestCase):
    def test_normalize_merges_overlap_and_adjacent(self) -> None:
        self.assertEqual(
            normalize_intervals([(800, 1200), (400, 600), (600, 800), (2000, 2000)]),
            [(400, 1200)],
        )

    def test_subtract_keeps_uncovered_tail_and_middle(self) -> None:
        self.assertEqual(subtract_intervals((400, 1200), [(400, 600)]),
                         [(600, 1200)])
        self.assertEqual(subtract_intervals((400, 1200), [(400, 600), (800, 900)]),
                         [(600, 800), (900, 1200)])

    def test_split_attribution_marks_extension(self) -> None:
        self.assertEqual(split_attribution(600, 1200, 400, 800),
                         [(600, 800, "原范围"), (800, 1200, "延伸段")])
        self.assertEqual(split_attribution(400, 600, 400, 800),
                         [(400, 600, "原范围")])
        self.assertEqual(split_attribution(100, 200, 400, 800),
                         [(100, 200, "前置段")])


class LedgerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = BackfillLedger()
        first = self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031",
            start_pos=400, end_pos=600)
        self.assertTrue(first["ok"])

    def start(self, ledger: BackfillLedger | None = None) -> dict:
        target = ledger or self.ledger
        return target.create_record(
            task_id=1, task_name="orders_patch", instance="mysql-orders-01",
            binlog_name="000031", range_start=400, range_end=800,
            status="回填中")

    def test_record_uses_confirmed_bounds_not_requested_tail(self) -> None:
        record = self.start()
        self.assertEqual(record["original_range"]["label"], "400..800")
        self.assertEqual(record["confirmed_range"]["label"], "400..600")
        self.assertEqual(record["pending_unconfirmed"], 200)

    def test_extension_only_accepts_uncovered_tail(self) -> None:
        self.start()
        result = self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031",
            start_pos=600, end_pos=1200, expected_end=600)
        self.assertEqual([item["range"] for item in result["accepted"]], ["600..1200"])
        self.assertFalse(result["stale"])
        self.assertEqual(result["confirmed_range"]["label"], "400..1200")

    def test_duplicate_and_stale_submission_is_ignored(self) -> None:
        self.start()
        self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031", end_pos=1200)
        duplicate = self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031", end_pos=1200)
        self.assertEqual(duplicate["accepted"], [])
        self.assertTrue(duplicate["ignored"])
        self.assertEqual(duplicate["confirmed_range"]["label"], "400..1200")
        stale = self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031",
            end_pos=1000, expected_end=600)
        self.assertTrue(stale["stale"])
        self.assertEqual(stale["accepted"], [])
        self.assertEqual(stale["confirmed_range"]["label"], "400..1200")

    def test_extension_gap_is_rejected(self) -> None:
        self.start()
        with self.assertRaises(LedgerError):
            self.ledger.confirm_endpoints(
                instance="mysql-orders-01", binlog_name="000031",
                start_pos=900, end_pos=1200)

    def test_reopen_claim_keeps_original_and_marks_extension(self) -> None:
        self.start()
        first = self.ledger.claim_next(task_id=1)
        self.assertEqual([c["range"] for c in first["claims"]], ["400..600"])

        self.ledger.confirm_endpoints(
            instance="mysql-orders-01", binlog_name="000031", end_pos=1200)
        self.ledger.set_status(task_id=1, status="已中断")
        with self.assertRaises(LedgerError):
            self.ledger.claim_next(task_id=1)

        reopened = self.ledger.reopen(task_id=1)
        self.assertEqual(reopened["original_range"]["label"], "400..800")
        self.assertEqual(
            [(seg["range"], seg["segment_kind"]) for seg in reopened["claimable"]],
            [("600..800", "原范围"), ("800..1200", "延伸段")])

        second = self.ledger.claim_next(task_id=1)
        self.assertEqual(
            [(c["range"], c["segment_kind"]) for c in second["claims"]],
            [("600..800", "原范围"), ("800..1200", "延伸段")])

        final = self.ledger.get_record(task_id=1)
        self.assertEqual([c["range"] for c in final["claims"]],
                         ["400..600", "600..800", "800..1200"])
        self.assertEqual(final["claimable"], [])
        self.assertTrue(final["consistency"]["ok"])
        self.assertEqual(final["version"], len(final["history"]))

    def test_prefix_before_task_start_is_not_claimable(self) -> None:
        record = self.ledger.create_record(
            task_id=2, task_name="late_start", instance="mysql-orders-01",
            binlog_name="000031", range_start=700, range_end=900,
            status="回填中")
        self.assertEqual(record["claimable"], [])

    def test_record_creation_is_idempotent_per_task(self) -> None:
        first = self.start()
        again = self.start()
        self.assertEqual(first["id"], again["id"])
        self.assertEqual(len(self.ledger.list_records()), 1)

    def test_save_rejects_claim_outside_confirmed_range(self) -> None:
        record = self.start()
        with self.assertRaises(LedgerError):
            self.ledger._commit(
                self.ledger._find_record(task_id=1), "手工改坏",
                lambda candidate: candidate["claims"].append({
                    "id": 999, "start_pos": 400, "end_pos": 1000,
                    "range": "400..1000", "status": "已领取"}))


class ConcurrencyTests(unittest.TestCase):
    def test_parallel_submissions_accept_only_uncovered_ranges(self) -> None:
        book = BackfillLedger()
        book.confirm_endpoints(instance="mysql-orders-01", binlog_name="000031",
                               start_pos=400, end_pos=600)
        book.create_record(task_id=1, task_name="orders_patch",
                           instance="mysql-orders-01", binlog_name="000031",
                           range_start=400, range_end=800, status="回填中")

        accepted: list[tuple[int, int]] = []
        lock = threading.Lock()

        def submit(end: int) -> None:
            result = book.confirm_endpoints(
                instance="mysql-orders-01", binlog_name="000031", end_pos=end)
            with lock:
                accepted.extend((a["start_pos"], a["end_pos"]) for a in result["accepted"])

        threads = [threading.Thread(target=submit, args=(end,))
                   for end in (1200, 1500, 1200, 1800, 1600, 2000)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        bounds = book.get_record(task_id=1)["confirmed_range"]
        self.assertEqual(bounds["label"], "400..2000")
        self.assertEqual(normalize_intervals(accepted), [(600, 2000)])

        book.reopen(task_id=1)
        result = book.claim_next(task_id=1)
        self.assertEqual(result["claims"][0]["range"], "400..800")


class SingletonStartupTests(unittest.TestCase):
    """应用启动时补档的全局台账：示例任务都应能查询到区间记录。"""

    def test_bootstrapped_records_are_consistent(self) -> None:
        from app.main import bootstrap  # noqa: F401  (触发启动补档)
        from app.store import store

        for entry in store.rows("backfill_position"):
            if entry["status"] == "待回填":
                continue
            record = ledger.find_by_task(int(entry["id"]))
            self.assertIsNotNone(record, f"任务 {entry['id']} 缺少区间留档")
            self.assertTrue(record["original_range"]["label"])
            self.assertTrue(record["consistency"]["ok"])


if __name__ == "__main__":
    unittest.main()
