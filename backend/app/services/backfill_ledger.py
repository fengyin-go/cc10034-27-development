"""回填区间留档：把回填任务接单时的源日志区间持久化成可查询的区间记录。

业务背景
--------
值班人员从回填任务返回后，原先只能看到任务的「源位点/目标位点」两个数字，
缺少一份与接单时点绑定的凭据：任务当时到底扫的是哪个源实例、哪个 Binlog、
哪一段区间，以及那段区间当时确认到了哪里。回填中断后重新打开时，如果没有
这份凭据，就无法判断哪些日志已经领过、哪些是后来新增的延伸段。

本模块负责：
1. 保存位点存储给出的源实例、Binlog 和两个已确认端点（``confirm_endpoints``）；
2. 按接单时的请求区间生成不可变的原范围记录（``create_record``）；
3. 回填过程中端点向后延伸时，只接纳尚未覆盖的区间，原范围和已领取数据保持不变；
4. 重新打开任务后，用区间记录与领取记录计算剩余可领取日志，并按「原范围 / 延伸段」
   标注归属（``claim_next``）。

区间统一使用左闭右开 ``[start, end)``，与 Binlog 偏移量语义一致。
所有写操作都在 ``RLock`` 内完成，并且先在一份副本上完成校验再整体提交，
保证并发追加时不会重复覆盖同一段日志，失败时也不会留下半截状态。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any, Callable, Iterable

Interval = tuple[int, int]

# 这些状态的领取记录都算「已覆盖」，重新打开后不会再被领一次。
COVERED_CLAIM_STATUS = ("已领取", "回填中", "已完成")

# 端点归属口径：接单上界之内是原范围，越过上界的部分是延伸段；
# 早于接单起点的日志不属于本任务，只在历史记录里标成前置段。
SEGMENT_ORIGINAL = "原范围"
SEGMENT_EXTENSION = "延伸段"
SEGMENT_PREFIX = "前置段"


class LedgerError(ValueError):
    """区间留档的领域校验失败。"""


def to_int(value: Any, field: str) -> int:
    """把位点值转成整数，允许逗号/下划线分隔。"""
    if isinstance(value, bool):
        raise LedgerError(f"{field}必须是整数")
    try:
        return int(str(value).replace(",", "").replace("_", "").strip())
    except (TypeError, ValueError) as exc:
        raise LedgerError(f"{field}必须是整数") from exc


def range_label(start: int, end: int) -> str:
    return f"{start}..{end}"


def normalize_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    """排序并合并重叠/相邻区间。"""
    cleaned = sorted((int(s), int(e)) for s, e in intervals if int(e) > int(s))
    merged: list[Interval] = []
    for start, end in cleaned:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def subtract_intervals(base: Interval, covered: Iterable[Interval]) -> list[Interval]:
    """从 base 中扣除 covered，返回未被覆盖的子区间。"""
    base_start, base_end = base
    if base_end <= base_start:
        return []
    result: list[Interval] = []
    cursor = base_start
    for start, end in normalize_intervals(covered):
        if end <= cursor:
            continue
        if start >= base_end:
            break
        if start > cursor:
            result.append((cursor, min(start, base_end)))
        cursor = max(cursor, end)
        if cursor >= base_end:
            break
    if cursor < base_end:
        result.append((cursor, base_end))
    return [segment for segment in result if segment[1] > segment[0]]


def split_attribution(start: int, end: int, original_start: int,
                      original_end: int) -> list[tuple[int, int, str]]:
    """按接单原范围切分区间，标注每一段的归属。"""
    parts: list[tuple[int, int, str]] = []
    if start < original_start:
        parts.append((start, min(end, original_start), SEGMENT_PREFIX))
    overlap_start = max(start, original_start)
    overlap_end = min(end, original_end)
    if overlap_start < overlap_end:
        parts.append((overlap_start, overlap_end, SEGMENT_ORIGINAL))
    if end > original_end:
        parts.append((max(start, original_end), end, SEGMENT_EXTENSION))
    return parts


class BackfillLedger:
    """内存版回填区间台账（真实项目这里替换成数据库表，锁换成事务）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._confirmations: list[dict[str, Any]] = []
        self._records: list[dict[str, Any]] = []
        self._next_confirmation_id = 1
        self._next_record_id = 1
        self._next_claim_id = 1

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _confirmed_bounds(self, instance: str, binlog_name: str) -> Interval | None:
        """取该实例/Binlog 下所有已接纳确认区间的合并外包络。"""
        parts = [
            (int(item["accepted_start"]), int(item["accepted_end"]))
            for item in self._confirmations
            if item["instance"] == instance
            and item["binlog_name"] == binlog_name
            and item["accepted_start"] is not None
            and item["accepted_end"] is not None
            and int(item["accepted_end"]) > int(item["accepted_start"])
        ]
        if not parts:
            return None
        merged = normalize_intervals(parts)
        return merged[0][0], merged[-1][1]

    def _append_confirmation(self, *, instance: str, binlog_name: str, start: int,
                             end: int, source: str, note: str = "",
                             acceptance: str = "已接纳") -> dict[str, Any]:
        confirmation = {
            "id": self._next_confirmation_id,
            "instance": instance,
            "binlog_name": binlog_name,
            "start_pos": start,
            "end_pos": end,
            "accepted_start": start if end > start else None,
            "accepted_end": end if end > start else None,
            "range": range_label(start, end),
            "confirmed_at": self._now(),
            "source": source,
            "status": acceptance,
            "note": note,
        }
        self._next_confirmation_id += 1
        self._confirmations.append(confirmation)
        return confirmation

    def _find_record(self, *, record_id: int | None = None,
                     task_id: int | None = None) -> dict[str, Any] | None:
        for record in self._records:
            if record_id is not None and int(record["id"]) == int(record_id):
                return record
            if task_id is not None and record.get("task_id") is not None \
                    and int(record["task_id"]) == int(task_id):
                return record
        return None

    def _covered_claims(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            claim for claim in record["claims"]
            if claim["status"] in COVERED_CLAIM_STATUS
        ]

    def _claimable_segments(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """可领取区间 = 已确认范围 ∩ 只从接单起点往后。

        任务只负责自己接单起点之后的日志，因此确认区间里早于接单起点的部分
        （前置段）不可领取；端点向后延伸超过接单上界的部分仍然可领取，
        并在归属上标成延伸段。
        """
        covered = [
            (int(claim["start_pos"]), int(claim["end_pos"]))
            for claim in self._covered_claims(record)
        ]
        remaining = subtract_intervals(
            (max(int(record["confirmed_start"]), int(record["range_start"])),
             int(record["confirmed_end"])),
            covered,
        )
        segments: list[dict[str, Any]] = []
        for start, end in remaining:
            for seg_start, seg_end, kind in split_attribution(
                    start, end, int(record["range_start"]), int(record["range_end"])):
                segments.append({
                    "start_pos": seg_start,
                    "end_pos": seg_end,
                    "range": range_label(seg_start, seg_end),
                    "length": seg_end - seg_start,
                    "segment_kind": kind,
                    "归属": kind,
                })
        return segments

    def _pending_end(self, record: dict[str, Any]) -> int:
        """接单上界中尚未被端点确认覆盖的部分，长度 0 表示已全部确认。"""
        return max(int(record["range_end"]) - int(record["confirmed_end"]), 0)

    def _snapshot(self, record: dict[str, Any], reason: str,
                  detail: dict[str, Any] | None = None) -> dict[str, Any]:
        snapshot = {
            "version": record["version"],
            "saved_at": self._now(),
            "reason": reason,
            "status": record["status"],
            "original_range": range_label(record["range_start"], record["range_end"]),
            "confirmed_range": range_label(record["confirmed_start"], record["confirmed_end"]),
            "claim_ranges": [
                claim["range"] for claim in self._covered_claims(record)
            ],
        }
        if detail:
            snapshot.update(detail)
        return snapshot

    def _validate(self, record: dict[str, Any]) -> None:
        """保存前校验：区间必须与确认记录一致，领取区间不能越界或互相重叠。"""
        start = int(record["range_start"])
        end = int(record["range_end"])
        if end <= start:
            raise LedgerError("接单区间必须满足起始位点小于目标位点")
        bounds = self._confirmed_bounds(record["instance"], record["binlog_name"])
        if bounds is None:
            raise LedgerError("缺少端点确认记录，无法保存回填区间")
        if (int(record["confirmed_start"]), int(record["confirmed_end"])) != bounds:
            raise LedgerError(
                "回填区间与端点确认记录不一致：确认记录为 "
                f"{range_label(*bounds)}，区间记录为 "
                f"{range_label(record['confirmed_start'], record['confirmed_end'])}")
        confirmed_start, confirmed_end = bounds
        claimable_start = max(confirmed_start, start)
        active: list[tuple[int, int, int]] = []
        for claim in record["claims"]:
            if claim["status"] not in COVERED_CLAIM_STATUS:
                continue
            claim_start = int(claim["start_pos"])
            claim_end = int(claim["end_pos"])
            if claim_end <= claim_start:
                raise LedgerError(f"领取区间 {claim['range']} 非法")
            if claim_start < claimable_start or claim_end > confirmed_end:
                raise LedgerError(
                    f"领取区间 {claim['range']} 超出可领取范围 "
                    f"{range_label(claimable_start, confirmed_end)}")
            active.append((claim_start, claim_end, int(claim["id"])))
        active.sort()
        for previous, current in zip(active, active[1:]):
            if current[0] < previous[1]:
                raise LedgerError(
                    f"领取区间 {previous[0]}..{previous[1]} 与 "
                    f"{current[0]}..{current[1]} 重叠")

    def _commit(self, record: dict[str, Any], reason: str,
                mutate: Callable[[dict[str, Any]], None] | None = None,
                detail: dict[str, Any] | None = None) -> dict[str, Any]:
        """在副本上完成变更、校验和留痕，全部通过后再整体写回。"""
        candidate = copy.deepcopy(record)
        if mutate is not None:
            mutate(candidate)
        candidate["version"] = int(candidate["version"]) + 1
        candidate["updated_at"] = self._now()
        self._validate(candidate)
        candidate["history"] = list(candidate.get("history") or [])
        candidate["history"].append(self._snapshot(candidate, reason, detail))
        record.clear()
        record.update(candidate)
        return record

    def _decorate(self, record: dict[str, Any]) -> dict[str, Any]:
        out = copy.deepcopy(record)
        out["original_range"] = {
            "start": record["range_start"],
            "end": record["range_end"],
            "label": range_label(record["range_start"], record["range_end"]),
        }
        out["confirmed_range"] = {
            "start": record["confirmed_start"],
            "end": record["confirmed_end"],
            "label": range_label(record["confirmed_start"], record["confirmed_end"]),
        }
        out["claimable"] = self._claimable_segments(record)
        covered = self._covered_claims(record)
        out["claimed_ranges"] = [
            {"start": claim["start_pos"], "end": claim["end_pos"],
             "range": claim["range"], "segment_kind": claim["segment_kind"]}
            for claim in covered
        ]
        out["claimed_length"] = sum(
            claim["end_pos"] - claim["start_pos"] for claim in covered)
        out["claimable_length"] = sum(seg["length"] for seg in out["claimable"])
        out["pending_unconfirmed"] = self._pending_end(record)
        out["consistency"] = {
            "ok": True,
            "message": "区间与端点确认记录一致",
            "confirmation_count": sum(
                1 for item in self._confirmations
                if item["instance"] == record["instance"]
                and item["binlog_name"] == record["binlog_name"]
                and item["accepted_start"] is not None
            ),
        }
        return out

    def _sync_records(self, instance: str, binlog_name: str,
                      accepted: Interval | None) -> list[dict[str, Any]]:
        """端点延伸后同步区间记录的确认范围，原范围和已领取记录保持不变。"""
        bounds = self._confirmed_bounds(instance, binlog_name)
        changed: list[dict[str, Any]] = []
        if bounds is None:
            return changed
        for record in self._records:
            if record["instance"] != instance or record["binlog_name"] != binlog_name:
                continue
            if (int(record["confirmed_start"]), int(record["confirmed_end"])) == bounds:
                continue

            def mutate(candidate: dict[str, Any], bounds: Interval = bounds) -> None:
                candidate["confirmed_start"], candidate["confirmed_end"] = bounds

            self._commit(
                record, "端点确认延伸", mutate,
                {"accepted": range_label(*accepted) if accepted else None},
            )
            changed.append(self._decorate(record))
        return changed

    # ------------------------------------------------------------------
    # 对外：端点确认（由 position_store 调用）
    # ------------------------------------------------------------------
    def confirm_endpoints(self, *, instance: str, binlog_name: str, end_pos: Any,
                          start_pos: Any = None, source: str = "position_store",
                          note: str = "", expected_end: Any = None) -> dict[str, Any]:
        """登记源实例的两个已确认端点；向后延伸时只接纳尚未覆盖的区间。

        ``expected_end`` 是可选的乐观并发版本：提交方带上自己看到的端点，
        若在此期间已被别的提交推进，则标记 ``stale``，但只追加未覆盖的尾部区间。
        """
        instance = str(instance or "").strip()
        binlog_name = str(binlog_name or "").strip()
        if not instance or not binlog_name:
            raise LedgerError("源实例和Binlog名称不能为空")
        end = to_int(end_pos, "结束端点")
        if end < 0:
            raise LedgerError("位点不能为负")
        expected = None if expected_end in (None, "") else to_int(expected_end, "期望端点")

        with self._lock:
            bounds = self._confirmed_bounds(instance, binlog_name)
            if bounds is None:
                if start_pos in (None, ""):
                    raise LedgerError("首次确认需要同时给出起始端点和结束端点")
                start = to_int(start_pos, "起始端点")
                if start < 0:
                    raise LedgerError("位点不能为负")
                if end <= start:
                    raise LedgerError("起始端点必须小于结束端点")
                accepted: Interval | None = (start, end)
                stale = expected is not None and expected != end
                confirmation = self._append_confirmation(
                    instance=instance, binlog_name=binlog_name, start=start,
                    end=end, source=source, note=note)
                ignored: list[dict[str, Any]] = []
            else:
                confirmed_start, confirmed_end = bounds
                request_start = confirmed_end if start_pos in (None, "") \
                    else to_int(start_pos, "起始端点")
                if request_start > confirmed_end:
                    raise LedgerError(
                        f"端点 {request_start} 与已确认端点 {confirmed_end} 之间存在空洞，"
                        "只能向后连续延伸")
                stale = expected is not None and expected != confirmed_end
                if end <= confirmed_end:
                    accepted = None
                    duplicate_start = max(confirmed_start, min(request_start, end))
                    confirmation = self._append_confirmation(
                        instance=instance, binlog_name=binlog_name,
                        start=duplicate_start, end=end, source=source,
                        note=note, acceptance="重复提交，未接纳")
                    ignored = [{
                        "start_pos": duplicate_start,
                        "end_pos": end,
                        "range": range_label(duplicate_start, end),
                        "reason": "该段已被既有确认区间覆盖",
                    }]
                else:
                    accepted = (confirmed_end, end)
                    confirmation = self._append_confirmation(
                        instance=instance, binlog_name=binlog_name,
                        start=confirmed_end, end=end, source=source, note=note)
                    ignored = [{
                        "start_pos": request_start,
                        "end_pos": confirmed_end,
                        "range": range_label(request_start, confirmed_end),
                        "reason": "重叠段已被既有确认区间覆盖",
                    }] if request_start < confirmed_end else []

            changed = self._sync_records(instance, binlog_name, accepted)
            bounds = self._confirmed_bounds(instance, binlog_name) or (None, None)
            accepted_items = [{
                "start_pos": accepted[0], "end_pos": accepted[1],
                "range": range_label(*accepted),
                "length": accepted[1] - accepted[0],
            }] if accepted else []
            if accepted_items:
                message = f"已接纳确认区间 {accepted_items[0]['range']}"
            else:
                message = f"端点 {end} 已被既有确认区间覆盖，未重复接纳"
            if stale:
                message += f"；提交携带的端点版本已过期（当前 {bounds[1]}）"
            return {
                "ok": True,
                "message": message,
                "instance": instance,
                "binlog_name": binlog_name,
                "confirmation": copy.deepcopy(confirmation),
                "accepted": accepted_items,
                "ignored": ignored,
                "stale": stale,
                "confirmed_range": {
                    "start": bounds[0],
                    "end": bounds[1],
                    "label": range_label(bounds[0], bounds[1])
                    if bounds[0] is not None else "",
                },
                "records": changed,
            }

    # ------------------------------------------------------------------
    # 对外：区间记录
    # ------------------------------------------------------------------
    def create_record(self, *, task_id: int | None, task_name: str, instance: str,
                      binlog_name: str, range_start: Any, range_end: Any,
                      confirmed_start: Any = None, confirmed_end: Any = None,
                      status: str = "待回填", source: str = "task_accept") -> dict[str, Any]:
        """按接单时的请求区间生成原范围记录；task_id 相同则幂等返回。

        区间记录里的 ``confirmed_start/end`` 始终等于端点确认记录的合并结果；
        接单时显式传入的确认端点会先落确认记录，未提供时以接单范围兜底留档，
        保证「区间与确认记录一致」在保存时成立。
        """
        instance = str(instance or "").strip()
        binlog_name = str(binlog_name or "").strip()
        if not instance or not binlog_name:
            raise LedgerError("源实例和Binlog名称不能为空")
        start = to_int(range_start, "接单起始位点")
        end = to_int(range_end, "接单目标位点")
        if start < 0 or end < 0:
            raise LedgerError("位点不能为负")
        if end <= start:
            raise LedgerError("接单区间必须满足起始位点小于目标位点")

        with self._lock:
            if task_id is not None:
                existing = self._find_record(task_id=task_id)
                if existing is not None:
                    return self._decorate(existing)

            if confirmed_start not in (None, "") or confirmed_end not in (None, ""):
                cs = start if confirmed_start in (None, "") else to_int(
                    confirmed_start, "确认起始端点")
                ce = end if confirmed_end in (None, "") else to_int(
                    confirmed_end, "确认结束端点")
                if ce <= cs:
                    raise LedgerError("确认起始端点必须小于确认结束端点")
                bounds = self._confirmed_bounds(instance, binlog_name)
                if bounds is None:
                    self._append_confirmation(
                        instance=instance, binlog_name=binlog_name, start=cs,
                        end=ce, source=source)
                elif ce > bounds[1]:
                    if cs > bounds[1]:
                        raise LedgerError("确认端点与既有确认区间之间存在空洞")
                    self._append_confirmation(
                        instance=instance, binlog_name=binlog_name,
                        start=bounds[1], end=ce, source=source)

            bounds = self._confirmed_bounds(instance, binlog_name)
            if bounds is None:
                self._append_confirmation(
                    instance=instance, binlog_name=binlog_name, start=start,
                    end=end, source="task_accept_fallback",
                    note="接单时未提供独立端点确认，使用接单范围兜底留档")
                bounds = (start, end)

            record = {
                "id": self._next_record_id,
                "task_id": task_id,
                "task_name": task_name,
                "instance": instance,
                "binlog_name": binlog_name,
                "range_start": start,
                "range_end": end,
                "confirmed_start": bounds[0],
                "confirmed_end": bounds[1],
                "version": 0,
                "status": status,
                "created_at": self._now(),
                "updated_at": self._now(),
                "reopened_at": None,
                "reopen_count": 0,
                "claim_seq": 0,
                "claims": [],
                "history": [],
            }
            self._next_record_id += 1
            self._commit(record, "接单留档")
            self._records.append(record)
            return self._decorate(record)

    def list_records(self, instance: str | None = None,
                     task_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            records = self._records
            if instance:
                records = [r for r in records if r["instance"] == instance]
            if task_id is not None:
                records = [r for r in records if r.get("task_id") == task_id]
            return [self._decorate(record) for record in records]

    def _record_confirmations(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """该区间记录对应的全部端点提交记录（含被忽略的重复提交）。"""
        return [
            copy.deepcopy(item) for item in self._confirmations
            if item["instance"] == record["instance"]
            and item["binlog_name"] == record["binlog_name"]
        ]

    def get_record(self, *, record_id: int | None = None,
                   task_id: int | None = None) -> dict[str, Any] | None:
        with self._lock:
            record = self._find_record(record_id=record_id, task_id=task_id)
            if record is None:
                return None
            detail = self._decorate(record)
            detail["confirmations"] = self._record_confirmations(record)
            return detail

    def find_by_task(self, task_id: int) -> dict[str, Any] | None:
        return self.get_record(task_id=task_id)

    def list_confirmations(self, instance: str | None = None,
                           binlog_name: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = self._confirmations
            if instance:
                items = [i for i in items if i["instance"] == instance]
            if binlog_name:
                items = [i for i in items if i["binlog_name"] == binlog_name]
            return [copy.deepcopy(item) for item in items]

    def reopen(self, *, record_id: int | None = None,
               task_id: int | None = None) -> dict[str, Any]:
        """重新打开回填任务：保留原范围、确认范围和已领取记录，只恢复领取资格。"""
        with self._lock:
            record = self._find_record(record_id=record_id, task_id=task_id)
            if record is None:
                raise LedgerError("未找到对应的回填区间记录")
            if record["status"] == "待回填":
                raise LedgerError("任务尚未开始回填，无需重新打开")
            def mutate(candidate: dict[str, Any]) -> None:
                candidate["status"] = "回填中"
                candidate["reopened_at"] = self._now()
                candidate["reopen_count"] = int(candidate["reopen_count"]) + 1
            self._commit(record, "重新打开回填", mutate)
            detail = self._decorate(record)
            detail["confirmations"] = self._record_confirmations(record)
            return detail

    def set_status(self, *, task_id: int | None = None, record_id: int | None = None,
                   status: str, reason: str = "") -> dict[str, Any] | None:
        with self._lock:
            record = self._find_record(record_id=record_id, task_id=task_id)
            if record is None:
                return None
            if record["status"] == status:
                return self._decorate(record)

            def mutate(candidate: dict[str, Any]) -> None:
                candidate["status"] = status
                if status == "已完成":
                    for claim in candidate["claims"]:
                        if claim["status"] in ("已领取", "回填中"):
                            claim["status"] = "已完成"
            self._commit(record, reason or f"状态流转至{status}", mutate)
            return self._decorate(record)

    def claim_next(self, *, record_id: int | None = None, task_id: int | None = None,
                   max_end: Any = None) -> dict[str, Any]:
        """领取下一段未覆盖日志，按接单原范围与延伸段拆开归属。"""
        cap = None if max_end in (None, "") else to_int(max_end, "领取上限")
        with self._lock:
            record = self._find_record(record_id=record_id, task_id=task_id)
            if record is None:
                return {"ok": False, "message": "未找到对应的回填区间记录",
                        "claims": [], "record": None}
            if record["status"] != "回填中":
                raise LedgerError(
                    f"回填任务当前状态为「{record['status']}」，请先重新打开回填再领取")
            remaining = subtract_intervals(
                (max(int(record["confirmed_start"]), int(record["range_start"])),
                 int(record["confirmed_end"])),
                [(int(c["start_pos"]), int(c["end_pos"]))
                 for c in self._covered_claims(record)],
            )
            if not remaining:
                return {"ok": False, "message": "没有可领取的未覆盖区间",
                        "claims": [], "record": self._decorate(record)}
            block_start, block_end = remaining[0]
            if cap is not None:
                block_end = min(block_end, cap)
            if block_end <= block_start:
                return {"ok": False, "message": "没有可领取的未覆盖区间",
                        "claims": [], "record": self._decorate(record)}

            batch_no = int(record["claim_seq"]) + 1
            new_claims: list[dict[str, Any]] = []
            for seg_start, seg_end, kind in split_attribution(
                    block_start, block_end, int(record["range_start"]),
                    int(record["range_end"])):
                new_claims.append({
                    "id": self._next_claim_id,
                    "record_id": record["id"],
                    "task_id": record.get("task_id"),
                    "task_name": record["task_name"],
                    "instance": record["instance"],
                    "binlog_name": record["binlog_name"],
                    "start_pos": seg_start,
                    "end_pos": seg_end,
                    "range": range_label(seg_start, seg_end),
                    "length": seg_end - seg_start,
                    "segment_kind": kind,
                    "归属": kind,
                    "status": "已领取",
                    "batch_no": batch_no,
                    "claimed_at": self._now(),
                })
                self._next_claim_id += 1

            def mutate(candidate: dict[str, Any]) -> None:
                candidate["claim_seq"] = batch_no
                candidate["claims"].extend(copy.deepcopy(new_claims))
            self._commit(record, "领取下一段", mutate,
                         {"claimed": [claim["range"] for claim in new_claims]})
            detail = self._decorate(record)
            labels = "、".join(
                f"{claim['range']}（{claim['segment_kind']}）" for claim in new_claims)
            return {
                "ok": True,
                "message": f"已领取 {labels}" if labels else "没有可领取的未覆盖区间",
                "claims": copy.deepcopy(new_claims),
                "record": detail,
                "claimable": detail["claimable"],
            }


ledger = BackfillLedger()
