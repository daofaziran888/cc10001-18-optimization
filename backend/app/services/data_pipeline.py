"""采集流水线 DAG 图服务：解析依赖段、生成节点/边结构与流转规则。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "data_pipeline"
REQUIRED_FIELDS = ["段编号", "输入来源", "转换算子"]
STATUS_ORDER = ["待编排", "运行中", "已完成", "异常中断"]
ACTION_RULES = {"启动流水线": "运行中", "暂停执行": "已完成", "继续执行": "异常中断"}
NEGATIVE_ACTIONS = []


def parse_dependency_ids(value: Any) -> list[int]:
    """把「依赖段」单元格解析成流水线段 id 列表。

    兼容逗号、分号、顿号、空白等分隔形式；段编号（DATA-0002）也可识别，
    无法识别为整数或已存在段的内容在构图阶段会被忽略。
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    ids: list[int] = []
    for token in re.split(r"[,，;；、\s]+", text):
        token = token.strip()
        if not token:
            continue
        if token.isdigit():
            num = int(token)
        else:
            match = re.search(r"(\d+)$", token)
            if not match:
                continue
            num = int(match.group(1))
        if num not in ids:
            ids.append(num)
    return ids


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class DataPipelineService:
    def get_graph(self) -> dict[str, Any]:
        """按「依赖段」声明构建 DAG：节点带执行态，边只反映真实依赖关系。"""
        rows = store.rows(MODULE)
        state_map = {
            int(s["segment_id"]): s
            for s in store.joint_rows("pipeline_state")
            if s.get("segment_id") is not None
        }
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            sid = int(row.get("id", i + 1))
            exec_state = state_map.get(sid, {})
            node = {
                "id": sid,
                "name": str(row.get("段编号", "")),
                "label": str(row.get("段编号", "")),
                "status": str(row.get("status", STATUS_ORDER[0])),
                "exec_status": exec_state.get("status", "待执行"),
                "batch_no": exec_state.get("batch_no"),
                "blocked_by": exec_state.get("blocked_by", []),
                "block_reason": exec_state.get("block_reason", ""),
                "error": exec_state.get("error", ""),
            }
            node.update(
                {k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")}
            )
            nodes.append(node)

        for row in rows:
            sid = int(row["id"])
            for dep in parse_dependency_ids(row.get("依赖段")):
                if dep == sid:
                    continue
                if any(n["id"] == dep for n in nodes):
                    edges.append({"from": dep, "to": sid, "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for extra in ("输出目标", "依赖段", "并行度", "失败策略", "超时分钟"):
            if extra in values:
                entry[extra] = values.get(extra)
        entry.setdefault("依赖段", "")
        entry["模拟失败"] = bool(values.get("模拟失败"))
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流水线段 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集流水线可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流水线段已{action}"

    def set_failure_simulation(
        self, entry_id: int, enabled: bool
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流水线段 {entry_id} 不存在或已归档"
        entry["模拟失败"] = bool(enabled)
        return entry, f"段 {entry_id} 失败注入已{'开启' if enabled else '关闭'}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
