"""采集流水线 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.data_pipeline import DataPipelineService
from app.services.pipeline_executor import pipeline_executor

router = APIRouter(prefix="/api/data_pipeline", tags=["采集流水线"])
service = DataPipelineService()
LIST_FIELDS = ["段编号", "输入来源", "转换算子", "输出目标", "依赖段", "并行度", "失败策略", "超时分钟"]
STATUSES = ["待编排", "运行中", "已完成", "异常中断"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=data_pipeline.csv"})


class ExecuteDagBody(BaseModel):
    policy: str = "continue_branch"


class FailureSimBody(BaseModel):
    enabled: bool


@router.get("/joint/states")
def list_pipeline_states() -> list[dict[str, Any]]:
    """查询流水线段执行状态记录（每段一条，按段幂等更新）。"""
    return pipeline_executor.list_states()


@router.get("/joint/runs")
def list_pipeline_runs() -> list[dict[str, Any]]:
    """查询有序流水线执行记录数据集。"""
    return pipeline_executor.list_runs()


@router.get("/execution-overview")
def execution_overview() -> dict[str, Any]:
    """页面用综合视图：已完成段、受阻段、可继续分支与结果批次。"""
    return pipeline_executor.execution_overview()


@router.get("/sync-checklist")
def sync_checklist() -> dict[str, Any]:
    """跨模块入口同步清单。"""
    return pipeline_executor.sync_checklist()


@router.post("/execute-dag")
def execute_dag(payload: ExecuteDagBody | None = None) -> dict[str, Any]:
    """按依赖关系执行整图：读取依赖段、既有批次与执行记录后按拓扑分层执行。"""
    policy = payload.policy if payload else "continue_branch"
    res = pipeline_executor.execute_dag(policy=policy)
    if "error" in res:
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/resume")
def resume_pipeline(payload: ExecuteDagBody | None = None) -> dict[str, Any]:
    """失败段中断后从可继续分支续做，重复提交只写入一次执行记录。"""
    policy = payload.policy if payload else "continue_branch"
    res = pipeline_executor.resume(policy=policy)
    if "error" in res:
        raise HTTPException(status_code=400, detail=res.get("error"))
    return res


@router.post("/reset")
def reset_pipeline() -> dict[str, Any]:
    return pipeline_executor.reset_all()


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流水线段 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流水线段已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/execute")
def execute_segment(entry_id: int) -> dict[str, Any]:
    """执行流水线段，将结果批次写入采集仓储；依赖未满足时返回阻断关系。"""
    return pipeline_executor.execute_segment(entry_id)


@router.post("/{entry_id}/failure-simulation")
def set_failure_simulation(entry_id: int, payload: FailureSimBody) -> ActionResult:
    """开关某段的失败注入，用于演示失败分支与执行边界策略。"""
    entry, message = service.set_failure_simulation(entry_id, payload.enabled)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
