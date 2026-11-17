"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持联合存储表。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import json
import os
from typing import Any

from app.seed import SEED_ROWS

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 联合存储表：跨模块数据流的持久化产物
        self._joint: dict[str, list[dict[str, Any]]] = {
            "pipeline_state": [],      # 流水线段执行状态（每段唯一，按 segment_id 幂等更新）
            "pipeline_runs": [],       # 有序流水线执行记录（每次提交一个运行单）
            "audit_records": [],       # 质量审计记录
            "sync_positions": [],      # 增量同步水位
            "dispatch_logs": [],       # 数据分发日志
            "proxy_assignments": [],   # 代理分配记录
            "cookie_rotations": [],    # 凭证轮换记录
            "scheduler_triggers": [],  # 调度触发记录
        }
        self._load_joint()

    def _joint_path(self, key: str) -> str:
        os.makedirs(_DATA_DIR, exist_ok=True)
        return os.path.join(_DATA_DIR, f"{key}.json")

    def _load_joint(self) -> None:
        for key in self._joint:
            path = self._joint_path(key)
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        self._joint[key] = json.load(f)
                except Exception:
                    self._joint[key] = []

    def _save_joint(self, key: str) -> None:
        path = self._joint_path(key)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._joint[key], f, ensure_ascii=False, indent=2)

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        # 跨模块数据流统计
        pipeline_runs = len(self._joint.get("pipeline_state", []))
        run_records = len(self._joint.get("pipeline_runs", []))
        audit_count = len(self._joint.get("audit_records", []))
        dispatch_count = len(self._joint.get("dispatch_logs", []))
        cards.append({"label": "流水线执行", "value": pipeline_runs})
        cards.append({"label": "执行记录", "value": run_records})
        cards.append({"label": "审计记录", "value": audit_count})
        cards.append({"label": "分发日志", "value": dispatch_count})
        return {"cards": cards, "modules": modules}

    # 联合存储表操作
    def joint_rows(self, key: str) -> list[dict[str, Any]]:
        return self._joint.setdefault(key, [])

    def joint_append(self, key: str, record: dict[str, Any]) -> None:
        self._joint.setdefault(key, []).append(record)
        self._save_joint(key)

    def joint_find(self, key: str, record_id: int) -> dict[str, Any] | None:
        for row in self.joint_rows(key):
            if int(row.get("id", 0)) == record_id:
                return row
        return None

    def joint_next_id(self, key: str) -> int:
        return max((int(row.get("id", 0)) for row in self.joint_rows(key)), default=0) + 1

    def joint_upsert(self, key: str, match_field: str, record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """按业务键幂等写入：同键记录原地更新而不是追加，返回 (记录, 是否新建)。

        竞态提交同一业务键时只会留下一个版本，避免重复执行记录与重复结果批次。
        """
        rows = self.joint_rows(key)
        match_value = record.get(match_field)
        for row in rows:
            if row.get(match_field) == match_value:
                row.update(record)
                self._save_joint(key)
                return row, False
        if "id" not in record:
            record = {**record, "id": self.joint_next_id(key)}
        rows.append(record)
        self._save_joint(key)
        return record, True

    def joint_clear(self, key: str) -> None:
        self._joint[key] = []
        self._save_joint(key)

    def joint_all(self) -> dict[str, list[dict[str, Any]]]:
        return dict(self._joint)


store = Store()
