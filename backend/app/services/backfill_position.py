"""回填位点领域服务：基于位点范围执行历史数据回填。

上游：position_store 提供源实例、Binlog 和两个已确认端点
留档：backfill_ledger 持久化接单区间，重开任务时用留档计算剩余可领取日志
消费：pipeline_bus 回填行数统计供 lag_monitor 消费
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.seed import SEED_ENDPOINT_CONFIRMATIONS
from app.store import store
from app.services.backfill_ledger import (
    LedgerError,
    ledger,
    range_label,
    to_int,
)
from app.pipeline import pipeline_bus


class BackfillPositionService(BaseModuleService):
    MODULE = "backfill_position"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务名称", "源位点", "目标位点", "扫描范围", "批次大小", "并发度", "预估记录数"]
    # 接单留档需要但不在列表字段里的扩展列：只存值，不改变模块对外字段契约。
    EXTRA_FIELDS = ["源实例", "Binlog名称", "回填区间", "待核对区间"]
    REQUIRED_FIELDS = ["任务名称", "源位点", "目标位点"]
    STATUS_ORDER = ["待回填", "回填中", "已完成", "已中断"]
    ACTION_RULES = {"开始回填": "待回填", "中断回填": "回填中", "完成回填": "回填中"}
    NEGATIVE_ACTIONS = ["中断回填"]
    PARAM_ACTIONS = {
        "开始回填": "按接单区间生成回填区间留档",
        "中断回填": "中断后保留原区间记录，等待重新打开",
        "完成回填": "完成时核对未覆盖区间",
        "重新打开回填": "保留原范围与已领取记录，恢复领取资格",
        "领取下一段": "按留档计算剩余日志并标记归属",
        "确认端点": "向后延伸源实例的已确认端点",
    }

    # ------------------------------------------------------------------
    # 任务预估
    # ------------------------------------------------------------------
    def create_backfill_task(self, config: dict[str, Any]) -> dict[str, Any]:
        """创建回填任务并计算预估指标。

        基于源位点和目标位点的差值，结合平均行大小参数
        估算需回填的记录数和批次数。
        """
        task_name = config["任务名称"]
        source_pos_str = config.get("源位点", "0")
        target_pos_str = config.get("目标位点", "0")
        batch_size = int(config.get("批次大小", "1000") or 1000)
        parallelism = int(config.get("并发度", "2") or 2)
        avg_row_bytes = 256

        source_pos = self._parse_pos(source_pos_str)
        target_pos = self._parse_pos(target_pos_str)

        estimate = pipeline_bus.compute_backfill_estimate(
            source_pos, target_pos, batch_size, avg_row_bytes
        )

        task = {
            "task_name": task_name,
            "source_pos": source_pos,
            "target_pos": target_pos,
            "batch_size": batch_size,
            "parallelism": parallelism,
            "estimated_rows": estimate["estimated_rows"],
            "estimated_batches": estimate["estimated_batches"],
            "position_diff_bytes": estimate["position_diff_bytes"],
            "created_at": datetime.now().isoformat(),
        }

        pipeline_bus.write_audit(
            "回填任务创建", task_name,
            f"回填任务创建，预估 {estimate['estimated_rows_str']} 条记录",
            "backfill_position", "已记录"
        )
        return task

    # ------------------------------------------------------------------
    # 接单留档
    # ------------------------------------------------------------------
    def open_interval_record(self, entry: dict[str, Any],
                             params: dict[str, Any] | None = None) -> dict[str, Any]:
        """按接单时的区间为任务留档，并回写任务上的回填区间摘要。

        源实例/Binlog 优先取任务本身填写值，其次取动作参数，最后按位点存储的
        当前位点推导（扫描范围里写的是 ``000031:400..800`` 时也能解析出来）。
        留档时若位点存储已确认过端点，确认范围直接采用确认记录；没有独立确认时
        以接单范围兜底留档，保证区间与确认记录在保存时一致。
        接单后端点继续推进只会扩展确认范围，原范围 ``range_start/range_end``
        保持接单时的值不变。
        """
        params = params or {}
        task_id = int(entry["id"])
        existing = ledger.find_by_task(task_id)
        if existing is not None:
            entry["回填区间"] = existing["original_range"]["label"]
            entry["源实例"] = existing["instance"]
            entry["Binlog名称"] = existing["binlog_name"]
            return existing

        instance, binlog_name = self._resolve_source(entry, params)
        start = to_int(params.get("源位点") or entry.get("源位点"), "源位点")
        end = to_int(params.get("目标位点") or entry.get("目标位点"), "目标位点")
        record = ledger.create_record(
            task_id=task_id,
            task_name=str(entry.get("任务名称") or ""),
            instance=instance,
            binlog_name=binlog_name,
            range_start=start,
            range_end=end,
            status="回填中",
        )
        entry["回填区间"] = record["original_range"]["label"]
        entry["源实例"] = record["instance"]
        entry["Binlog名称"] = record["binlog_name"]
        pipeline_bus.write_audit(
            "回填区间留档", str(entry.get("任务名称") or ""),
            f"按接单区间 {instance}:{binlog_name}:{range_label(start, end)} 留档，"
            f"接手时确认范围 {record['confirmed_range']['label']}",
            "backfill_position", "已记录"
        )
        return record

    def _resolve_source(self, entry: dict[str, Any],
                        params: dict[str, Any]) -> tuple[str, str]:
        """解析任务的源实例和 Binlog 名称，兼容通用登记弹窗只填扫描范围的情况。"""
        instance = str(params.get("源实例") or entry.get("源实例") or "").strip()
        binlog_name = str(params.get("Binlog名称") or entry.get("Binlog名称") or "").strip()
        scan = str(entry.get("扫描范围") or "")
        if (not instance or not binlog_name) and ":" in scan:
            scan_binlog, _, _ = scan.partition(":")
            binlog_name = binlog_name or scan_binlog.strip()
        if not instance:
            candidates = [
                pos for pos in pipeline_bus.positions.values()
                if not binlog_name or pos.binlog_name == binlog_name
            ]
            if candidates:
                instance = str(candidates[-1].instance)
                binlog_name = binlog_name or str(candidates[-1].binlog_name)
        if not instance:
            # 管道里还没有注册位点时，退回位点存储的持久记录找同一 Binlog。
            stored = [
                row for row in store.rows("position_store")
                if not binlog_name or str(row.get("Binlog名称")) == binlog_name
            ]
            if stored:
                instance = str(stored[-1].get("数据库实例") or "")
                binlog_name = binlog_name or str(stored[-1].get("Binlog名称") or "")
        if not instance:
            raise LedgerError(
                "回填任务缺少源实例或Binlog名称，无法留档接单区间；"
                "请在扫描范围或动作参数中给出，例如 000031:400..800")
        if not binlog_name:
            raise LedgerError(
                "回填任务缺少Binlog名称，无法留档接单区间；"
                "请在扫描范围中写明，例如 000031:400..800")
        return instance, binlog_name



    # ------------------------------------------------------------------
    # 动作实现
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        result = self.run_param_action(entry_id, action, {})
        entry = result.get("entry")
        return entry, result.get("message", "")

    def run_param_action(self, entry_id: int, action: str,
                         params: dict[str, Any]) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档",
                    "entry": None}
        try:
            if action == "开始回填":
                return self.start_backfill(entry, params)
            if action == "中断回填":
                return self.interrupt_backfill(entry)
            if action == "完成回填":
                return self.finish_backfill(entry)
            if action == "重新打开回填":
                return self.reopen_task(entry, params)
            if action == "领取下一段":
                return self.claim_next(entry, params)
            if action == "确认端点":
                return self.confirm_endpoints(entry, params)
        except LedgerError as exc:
            return {"ok": False, "message": str(exc), "entry": entry,
                    "interval": self.interval_detail(entry)}
        entry, message = BaseModuleService.run_action(self, entry_id, action)
        return {"ok": entry is not None, "message": message, "entry": entry}

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记任务时保留源实例/Binlog 等扩展列，供接单留档使用。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            for field in self.EXTRA_FIELDS:
                entry[field] = values.get(field)
        return entry, missing

    def start_backfill(self, entry: dict[str, Any],
                       params: dict[str, Any] | None = None) -> dict[str, Any]:
        if entry.get("status") == "回填中":
            return {"ok": False, "message": "回填任务已在处理中",
                    "entry": entry, "interval": self.interval_detail(entry)}
        record = self.open_interval_record(entry, params)
        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        self.create_backfill_task(entry)
        pipeline_bus.write_audit(
            "回填开始", entry.get("任务名称", ""),
            f"回填任务 {entry['id']} 已开始，接单区间 {record['original_range']['label']}，"
            f"确认范围 {record['confirmed_range']['label']}",
            "backfill_position", "回填中"
        )
        result = self.interval_detail(entry)
        return {
            "ok": True,
            "message": f"回填已开始，接单区间 {record['original_range']['label']}",
            "entry": entry,
            "interval": result,
        }

    def interrupt_backfill(self, entry: dict[str, Any]) -> dict[str, Any]:
        if entry.get("status") != "回填中":
            return {"ok": False, "message": "只有回填中的任务可以中断",
                    "entry": entry, "interval": self.interval_detail(entry)}
        record = ledger.set_status(task_id=int(entry["id"]), status="已中断",
                                   reason="中断回填")
        entry["status"] = "已中断"
        entry["pending"] = True
        entry["abnormal"] = True
        pipeline_bus.write_audit(
            "回填中断", entry.get("任务名称", ""),
            f"回填任务 {entry['id']} 已中断，原区间记录保留，"
            f"已领 {record['claimed_length']} 字节",
            "backfill_position", "已中断"
        )
        return {
            "ok": True,
            "message": f"回填已中断，已保留原区间 {record['original_range']['label']}",
            "entry": entry,
            "interval": record,
        }

    def finish_backfill(self, entry: dict[str, Any]) -> dict[str, Any]:
        if entry.get("status") != "回填中":
            return {"ok": False, "message": "只有回填中的任务可以完成",
                    "entry": entry, "interval": self.interval_detail(entry)}
        record = ledger.set_status(task_id=int(entry["id"]), status="已完成",
                                   reason="回填完成")
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        if record and record["claimable_length"] > 0:
            entry["待核对区间"] = "、".join(seg["range"] for seg in record["claimable"])
        pipeline_bus.write_audit(
            "回填完成", entry.get("任务名称", ""),
            f"回填任务 {entry['id']} 已完成，累计回填 "
            f"{pipeline_bus.pipeline_metrics['backfill_rows']} 行，"
            f"剩余未领取 {record['claimable_length'] if record else 0} 字节",
            "backfill_position", "已完成"
        )
        return {
            "ok": True,
            "message": "回填已完成",
            "entry": entry,
            "interval": record,
        }

    def reopen_task(self, entry: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        """重新打开任务，保留原范围与已领取记录，并按留档算出剩余日志。"""
        if entry.get("status") == "待回填":
            raise LedgerError("任务尚未开始回填，无需重新打开")
        self._ensure_record(entry)
        detail = ledger.reopen(task_id=int(entry["id"]))
        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        pipeline_bus.write_audit(
            "回填重开", str(entry.get("任务名称") or ""),
            f"任务 {entry['id']} 重新打开，原范围 {detail['original_range']['label']}，"
            f"确认范围 {detail['confirmed_range']['label']}，"
            f"剩余 {detail['claimable_length']} 字节",
            "backfill_position", "回填中"
        )
        return {
            "ok": True,
            "message": (
                f"已重新打开，原范围 {detail['original_range']['label']}，"
                f"剩余可领取 {detail['claimable_length']} 字节"
            ),
            "entry": entry,
            "interval": detail,
        }

    def _ensure_record(self, entry: dict[str, Any]) -> dict[str, Any]:
        """取任务的区间留档；历史任务（先于留档功能开始回填）当场补档。"""
        record = ledger.find_by_task(int(entry["id"]))
        if record is not None:
            return record
        if entry.get("status") == "待回填":
            raise LedgerError("该任务还没有回填区间留档，请先开始回填")
        return self.open_interval_record(entry, {})

    def seed_confirmations(self) -> None:
        """把示例位点已经确认过的端点写入区间台账（幂等）。"""
        for item in SEED_ENDPOINT_CONFIRMATIONS:
            if ledger.list_confirmations(item["instance"], item["binlog_name"]):
                continue
            ledger.confirm_endpoints(
                instance=item["instance"],
                binlog_name=item["binlog_name"],
                start_pos=item["start_pos"],
                end_pos=item["end_pos"],
                source=item["source"],
                note=item.get("note", ""),
            )

    def bootstrap_records(self) -> int:
        """给已有任务补齐接单区间留档（幂等），让历史任务也能持久查询。"""
        self.seed_confirmations()
        created = 0
        for entry in store.rows(self.MODULE):
            if entry.get("status") == "待回填":
                continue
            record = ledger.find_by_task(int(entry["id"]))
            if record is not None:
                entry["回填区间"] = record["original_range"]["label"]
                continue
            try:
                self.open_interval_record(entry)
            except LedgerError:
                continue
            created += 1
        return created

    def claim_next(self, entry: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        """重新打开后领取下一段未覆盖日志，返回每一段的归属。"""
        self._ensure_record(entry)
        result = ledger.claim_next(
            task_id=int(entry["id"]),
            max_end=params.get("领取上限") or params.get("max_end"),
        )
        if not result["ok"]:
            return {"ok": False, "message": result["message"], "entry": entry,
                    "claims": [], "interval": result["record"]}
        pipeline_bus.write_audit(
            "回填领取", str(entry.get("任务名称") or ""),
            result["message"],
            "backfill_position", "回填中"
        )
        detail = result["record"]
        attribution = "、".join(
            f"{claim['range']}={claim['segment_kind']}" for claim in result["claims"]
        )
        return {
            "ok": True,
            "message": f"已领取 {attribution}" if attribution else result["message"],
            "entry": entry,
            "claims": result["claims"],
            "claimable": detail["claimable"],
            "interval": detail,
        }

    def confirm_endpoints(self, entry: dict[str, Any],
                          params: dict[str, Any]) -> dict[str, Any]:
        """从任务出发确认源实例端点，用于处理期间端点继续推进的场景。"""
        instance = str(params.get("源实例") or entry.get("源实例") or "").strip()
        binlog_name = str(params.get("Binlog名称") or entry.get("Binlog名称") or "").strip()
        end_pos = params.get("结束端点") or params.get("端点") or entry.get("目标位点")
        result = ledger.confirm_endpoints(
            instance=instance,
            binlog_name=binlog_name,
            start_pos=params.get("起始端点"),
            end_pos=end_pos,
            expected_end=params.get("期望端点"),
            source=str(params.get("来源") or "workbench"),
            note=str(params.get("备注") or ""),
        )
        pipeline_bus.write_audit(
            "端点确认", f"{instance}:{binlog_name}",
            result["message"],
            "backfill_position", "已记录"
        )
        result["entry"] = entry
        result["interval"] = self.interval_detail(entry)
        return result

    # ------------------------------------------------------------------
    # 查询辅助
    # ------------------------------------------------------------------
    def interval_detail(self, entry: dict[str, Any] | None) -> dict[str, Any] | None:
        if not entry or not entry.get("id"):
            return None
        return ledger.find_by_task(int(entry["id"]))

    def remaining_after_reopen(self, entry: dict[str, Any]) -> dict[str, Any]:
        """用区间记录计算重开后允许领取的剩余日志。"""
        detail = self.interval_detail(entry)
        if detail is None:
            return {"found": False, "claimable": [], "claimable_length": 0}
        return {
            "found": True,
            "instance": detail["instance"],
            "binlog_name": detail["binlog_name"],
            "original_range": detail["original_range"],
            "confirmed_range": detail["confirmed_range"],
            "claimed_ranges": detail["claimed_ranges"],
            "claimable": detail["claimable"],
            "claimable_length": detail["claimable_length"],
        }

    def execute_batch(self, task: dict[str, Any], batch_no: int) -> dict[str, Any]:
        """执行单批回填。"""
        batch_size = task["batch_size"]
        pipeline_bus.pipeline_metrics["backfill_rows"] += batch_size
        return {
            "batch_no": batch_no,
            "rows_processed": batch_size,
            "task": task["task_name"],
            "processed_at": datetime.now().isoformat(),
            "cumulative": pipeline_bus.pipeline_metrics["backfill_rows"],
        }

    def estimate_progress(self, task: dict[str, Any], completed_batches: int) -> dict[str, Any]:
        """估算回填进度。"""
        total_batches = task.get("estimated_batches", 1)
        pct = min(round(completed_batches / max(total_batches, 1) * 100, 1), 100.0)
        parallelism = task.get("parallelism", 1)
        eta_batches = max(total_batches - completed_batches, 0) / max(parallelism, 1)
        return {
            "task": task["task_name"],
            "progress_pct": pct,
            "completed_batches": completed_batches,
            "total_batches_estimate": total_batches,
            "eta_remaining_batches": int(eta_batches),
        }

    def validate_position_range(self, source_pos: int, target_pos: int) -> dict[str, Any]:
        """校验位点范围合法性。"""
        if source_pos < 0 or target_pos < 0:
            return {"valid": False, "reason": "位点不能为负"}
        if source_pos > target_pos:
            return {"valid": False, "reason": "源位点不能大于目标位点"}
        return {"valid": True, "range_size_bytes": target_pos - source_pos}

    def _parse_pos(self, pos_str: str) -> int:
        try:
            pos_str = str(pos_str).replace(",", "").replace("_", "")
            return int(pos_str)
        except (ValueError, TypeError):
            return 0
