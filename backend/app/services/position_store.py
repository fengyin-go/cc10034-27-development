"""位点存储领域服务：持久化和管理Binlog位点信息。

上游：checkpoint_manager 创建检查点时联动更新位点
消费：cdc_channel 启动时查询位点；failover_group 切换时读取位点
下游：backfill_position 基于位点范围执行历史回填
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.backfill_ledger import LedgerError, ledger
from app.pipeline import PositionSnapshot, pipeline_bus


class PositionStoreService(BaseModuleService):
    MODULE = "position_store"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录ID", "数据库实例", "Binlog名称", "位点值", "Gtid集合", "更新时间", "位点来源"]
    REQUIRED_FIELDS = ["记录ID", "数据库实例", "Binlog名称"]
    STATUS_ORDER = ["当前位点", "历史位点", "归档位点"]
    ACTION_RULES = {"标记历史": "当前位点", "归档位点": "历史位点"}
    NEGATIVE_ACTIONS = []
    PARAM_ACTIONS = {"确认端点": "登记源实例的两个已确认端点并留档回填区间"}

    def run_param_action(self, entry_id: int, action: str,
                         params: dict[str, Any]) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档",
                    "entry": None}
        if action == "确认端点":
            instance = str(params.get("源实例") or entry.get("数据库实例") or "").strip()
            binlog_name = str(params.get("Binlog名称") or entry.get("Binlog名称") or "").strip()
            try:
                result = self.confirm_endpoints(
                    instance,
                    binlog_name,
                    end_pos=params.get("结束端点") or params.get("端点"),
                    start_pos=params.get("起始端点"),
                    expected_end=params.get("期望端点"),
                    note=str(params.get("备注") or ""),
                )
            except LedgerError as exc:
                return {"ok": False, "message": str(exc), "entry": entry}
            # 位点记录同步成最新确认端点，避免位点值和确认记录脱节。
            entry["位点值"] = str(result["confirmed_range"]["end"])
            entry["更新时间"] = datetime.now().date().isoformat()
            entry["位点来源"] = "端点确认"
            result["entry"] = entry
            return result
        entry, message = self.run_action(entry_id, action)
        return {"ok": entry is not None, "message": message, "entry": entry}

    def save_position(self, instance: str, binlog_name: str, position: int,
                      gtid_set: str = "", source: str = "manual") -> PositionSnapshot:
        """保存当前位点到管道总线。"""
        ps = PositionSnapshot(
            instance=instance,
            binlog_name=binlog_name,
            position=position,
            gtid_set=gtid_set,
            updated_at=datetime.now(),
            source=source,
        )
        pipeline_bus.register_position(ps)
        pipeline_bus.write_audit(
            "位点存储", f"{instance}:{binlog_name}",
            f"位点更新至 {binlog_name}:{position}",
            "position_store", "已记录"
        )
        return ps

    def confirm_endpoints(self, instance: str, binlog_name: str, end_pos: Any,
                          start_pos: Any = None, expected_end: Any = None,
                          note: str = "") -> dict[str, Any]:
        """登记源实例的两个已确认端点，并把结果留档到回填区间台账。

        端点向后延伸时，台账只接纳尚未覆盖的尾部区间；回填还在处理旧范围时
        重复提交同一端点会被忽略，不会重复覆盖已领过的日志。
        """
        result = ledger.confirm_endpoints(
            instance=instance,
            binlog_name=binlog_name,
            start_pos=start_pos,
            end_pos=end_pos,
            expected_end=expected_end,
            note=note,
            source="position_store",
        )
        pipeline_bus.write_audit(
            "端点确认", f"{instance}:{binlog_name}",
            f"{result['message']}，确认范围 {result['confirmed_range']['label']}",
            "position_store", "已记录"
        )
        return result

    def query_confirmed_endpoints(self, instance: str,
                                  binlog_name: str | None = None) -> dict[str, Any]:
        """查询实例（可按 Binlog 过滤）已确认的端点与留档记录。"""
        confirmations = ledger.list_confirmations(instance, binlog_name)
        accepted = [
            item for item in confirmations
            if item["accepted_start"] is not None
        ]
        bounds = None
        if accepted:
            bounds = {
                "start": min(int(item["accepted_start"]) for item in accepted),
                "end": max(int(item["accepted_end"]) for item in accepted),
            }
        return {
            "instance": instance,
            "binlog_name": binlog_name,
            "confirmed": bounds,
            "confirmations": confirmations,
            "records": ledger.list_records(instance=instance),
        }

    def query_current_position(self, instance: str) -> dict[str, Any]:
        """查询指定实例的当前位点。"""
        ps = pipeline_bus.get_position(instance)
        if ps is None:
            return {"instance": instance, "found": False}
        return {
            "instance": instance,
            "found": True,
            "binlog_name": ps.binlog_name,
            "position": ps.position,
            "gtid_set": ps.gtid_set,
            "updated_at": ps.updated_at.isoformat(),
            "source": ps.source,
        }

    def compare_positions(self, pos_a: dict[str, Any], pos_b: dict[str, Any]) -> dict[str, Any]:
        """比较两个位点的差异。"""
        same_file = pos_a.get("binlog_name") == pos_b.get("binlog_name")
        offset_a = int(pos_a.get("position", 0))
        offset_b = int(pos_b.get("position", 0))
        lag_bytes = abs(offset_a - offset_b) if same_file else None
        return {
            "same_binlog_file": same_file,
            "offset_a": offset_a,
            "offset_b": offset_b,
            "lag_bytes": lag_bytes,
            "files_differ": not same_file,
        }

    def parse_gtid(self, gtid_str: str) -> dict[str, Any]:
        """解析 Gtid 集合字符串。"""
        uuid_set: list[str] = []
        intervals: list[dict[str, Any]] = []
        for part in gtid_str.split(","):
            part = part.strip()
            if not part:
                continue
            match = re.match(r'([a-f0-9-]+):(\d+)(?:-(\d+))?', part, re.IGNORECASE)
            if match:
                uuid_val = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3)) if match.group(3) else start
                uuid_set.append(uuid_val)
                intervals.append({"uuid": uuid_val, "start": start, "end": end, "count": end - start + 1})
        return {"uuid_count": len(set(uuid_set)), "total_transactions": sum(it["count"] for it in intervals), "intervals": intervals}

    def estimate_binlog_lag_events(self, source_pos: PositionSnapshot,
                                   target_pos: PositionSnapshot, avg_event_bytes: int = 256) -> int:
        """估算位点之间的积压事件数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1
        diff = abs(source_pos.position - target_pos.position)
        return diff // max(avg_event_bytes, 1)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "标记历史":
            pipeline_bus.write_audit(
                "位点归档", entry.get("数据库实例", ""),
                f"位点 {entry_id} 已标记为历史",
                "position_store", "已记录"
            )
        return entry, msg