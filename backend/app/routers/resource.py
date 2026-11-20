"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.backfill_ledger import LedgerError, ledger
from app.services.backfill_position import BackfillPositionService
from app.services.position_store import PositionStoreService

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# ---------------------------------------------------------------------------
# 回填区间留档：可持久查询的区间记录与确认记录。
# 这些固定路径必须注册在 /{module}/{entry_id} 动态路径之前。
# ---------------------------------------------------------------------------
@router.get("/backfill_position/ledger", response_model=dict)
def list_interval_records(instance: str | None = Query(default=None),
                          task_id: int | None = Query(default=None)) -> dict[str, Any]:
    records = ledger.list_records(instance=instance, task_id=task_id)
    return {
        "items": records,
        "total": len(records),
        "confirmations": ledger.list_confirmations(instance),
    }


@router.get("/backfill_position/ledger/{task_id}", response_model=dict)
def get_interval_record(task_id: int) -> dict[str, Any]:
    record = ledger.get_record(task_id=task_id)
    if record is None:
        raise HTTPException(status_code=404, detail="该任务还没有回填区间留档")
    return record


@router.get("/position_store/confirmations", response_model=dict)
def list_endpoint_confirmations(instance: str | None = Query(default=None),
                                binlog_name: str | None = Query(default=None)) -> dict[str, Any]:
    return {
        "items": ledger.list_confirmations(instance, binlog_name),
        "total": len(ledger.list_confirmations(instance, binlog_name)),
        "records": ledger.list_records(instance=instance),
    }


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    detail: dict[str, Any] = dict(entry)
    interval = _service_interval(service, entry)
    if interval is not None:
        detail["interval"] = interval
    return detail


def _service_interval(service: Any, entry: dict[str, Any]) -> dict[str, Any] | None:
    finder = getattr(service, "interval_detail", None)
    if finder is None:
        return None
    return finder(entry)


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    action = payload.action.strip()
    if action in getattr(service, "PARAM_ACTIONS", {}):
        try:
            result = service.run_param_action(entry_id, action, payload.values)
        except LedgerError as exc:
            entry = service.get_entry(entry_id)
            return ActionResult(ok=False, message=str(exc), entry=entry,
                                details={"interval": _service_interval(service, entry or {})})
        entry = result.pop("entry", None)
        message = result.pop("message", "")
        ok = bool(result.pop("ok", entry is not None))
        return ActionResult(ok=ok, message=message, entry=entry, details=result)
    entry, message = service.run_action(entry_id, action)
    return ActionResult(ok=entry is not None, message=message, entry=entry)

