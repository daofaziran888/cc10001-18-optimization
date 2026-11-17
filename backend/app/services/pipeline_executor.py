"""流水线依赖执行引擎：读取依赖段、既有结果批次与依赖关系，按 DAG 约束驱动执行。

处理流程（对照数据流 data_pipeline -> result_store）：
1. 读取流水线段（data_pipeline）、段上的「依赖段」声明、既有执行状态与结果批次；
2. 解析依赖关系并做拓扑分层，循环依赖或依赖不存在的段直接拒绝执行；
3. 按层执行：依赖段未成功的后继段一律不启动，只记「依赖受阻」与阻断关系；
4. 成功段按确定性输入指纹幂等生成结果批次，写入采集仓储（result_store）；
5. 每次提交生成一张有序流水线执行记录（pipeline_runs），重复提交不重复写。

执行边界（两种策略，产出不同的已完成段与待续做范围）：
- continue_branch：一个分支失败时，已满足依赖的独立分支继续执行，只阻塞受影响后继；
- stop_layer：整图停在失败所在层，后续层统一等待修复，任何后继都不越过失败依赖。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any

from app.store import store
from app.services.data_pipeline import parse_dependency_ids
from app.services.result_store import ResultStoreService

MODULE = "data_pipeline"
RESULT_MODULE = "result_store"
STATE_TABLE = "pipeline_state"
RUN_TABLE = "pipeline_runs"

# 段执行状态（写在 pipeline_state 上，每段唯一一条）
ST_DONE = "已完成"
ST_FAILED = "执行失败"
ST_BLOCKED = "依赖受阻"
ST_READY = "待执行"

# 执行边界策略
POLICY_CONTINUE = "continue_branch"
POLICY_STOP = "stop_layer"
POLICY_LABELS = {
    POLICY_CONTINUE: "独立分支继续，仅阻塞受影响后继",
    POLICY_STOP: "整图停在当前层，统一等待修复",
}

VALID_POLICIES = set(POLICY_LABELS)


class PipelineExecutor:
    def __init__(self) -> None:
        self._result_svc = ResultStoreService()
        # 检查-判定-写入全程持锁，并发/竞态提交不会产生重复执行版本
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 依赖图与指纹
    # ------------------------------------------------------------------
    def _rows(self) -> list[dict[str, Any]]:
        return sorted(store.rows(MODULE), key=lambda r: int(r.get("id", 0)))

    def _build_relations(
        self, rows: list[dict[str, Any]]
    ) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
        """返回 (段 -> 依赖段列表, 段 -> 后继段列表)，只保留图内真实存在的段 id。"""
        ids = {int(r["id"]) for r in rows}
        deps: dict[int, list[int]] = {}
        children: dict[int, list[int]] = {sid: [] for sid in ids}
        for row in rows:
            sid = int(row["id"])
            dep_ids: list[int] = []
            for dep in parse_dependency_ids(row.get("依赖段")):
                if dep in ids and dep != sid and dep not in dep_ids:
                    dep_ids.append(dep)
            deps[sid] = dep_ids
            for dep in dep_ids:
                children[dep].append(sid)
        return deps, children

    def _topo_layers(
        self, ids: set[int], deps: dict[int, list[int]]
    ) -> list[list[int]]:
        """Kahn 拓扑分层：第 0 层无依赖，之后每层的依赖都在更早层满足。"""
        remaining = set(ids)
        resolved: set[int] = set()
        layers: list[list[int]] = []
        while remaining:
            layer = sorted(sid for sid in remaining if set(deps[sid]) <= resolved)
            if not layer:
                raise ValueError("依赖关系存在循环或指向了不存在的段，无法按依赖执行")
            layers.append(layer)
            resolved.update(layer)
            remaining.difference_update(layer)
        return layers

    def _descendants(self, sid: int, children: dict[int, list[int]]) -> set[int]:
        out: set[int] = set()
        stack = list(children.get(sid, []))
        while stack:
            cur = stack.pop()
            if cur in out:
                continue
            out.add(cur)
            stack.extend(children.get(cur, []))
        return out

    def _ancestors(self, sid: int, deps: dict[int, list[int]]) -> set[int]:
        out: set[int] = set()
        stack = list(deps.get(sid, []))
        while stack:
            cur = stack.pop()
            if cur in out:
                continue
            out.add(cur)
            stack.extend(deps.get(cur, []))
        return out

    def _input_fingerprint(self, seg: dict[str, Any], dep_ids: list[int]) -> str:
        """相同段 + 相同输入声明 => 相同指纹，指纹决定唯一有效批次。"""
        payload = {
            "segment_id": int(seg["id"]),
            "段编号": seg.get("段编号", ""),
            "输入来源": seg.get("输入来源", ""),
            "转换算子": seg.get("转换算子", ""),
            "输出目标": seg.get("输出目标", ""),
            "依赖段": dep_ids,
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _state_map(self) -> dict[int, dict[str, Any]]:
        return {
            int(r["segment_id"]): r
            for r in store.joint_rows(STATE_TABLE)
            if r.get("segment_id") is not None
        }

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # 单段执行
    # ------------------------------------------------------------------
    def _find_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(RESULT_MODULE):
            if row.get("批次编号") == batch_no:
                return row
        return None

    def _upsert_state(self, record: dict[str, Any]) -> dict[str, Any]:
        saved, _ = store.joint_upsert(STATE_TABLE, "segment_id", record)
        return saved

    def _execute_once(
        self,
        seg: dict[str, Any],
        dep_ids: list[int],
        fp: str,
        states: dict[int, dict[str, Any]],
        run_id: int,
    ) -> str:
        """真正执行一个依赖已满足的段，返回 done / failed。"""
        sid = int(seg["id"])
        code = str(seg.get("段编号", sid))
        now = self._now()
        seg["status"] = "运行中"
        seg["pending"] = True
        previous = states.get(sid, {})

        # 失败注入：段上带「模拟失败」标记时该次执行失败
        if bool(seg.get("模拟失败")):
            error = (
                f"段 {code} 执行失败：转换算子「{seg.get('转换算子', '')}」触发失败策略"
                f"「{seg.get('失败策略', '中断后继')}」，下游后继段已被阻断"
            )
            record = {
                "segment_id": sid,
                "segment_code": code,
                "input_fingerprint": fp,
                "status": ST_FAILED,
                "batch_id": None,
                "batch_no": None,
                "raw_rows": 0,
                "valid_rows": 0,
                "checksum": "",
                "attempts": int(previous.get("attempts", 0)) + 1,
                "run_ids": list(previous.get("run_ids", [])),
                "blocked_by": [],
                "block_reason": "",
                "error": error,
                "started_at": previous.get("started_at", now),
                "finished_at": now,
                "updated_at": now,
            }
            self._upsert_state(record)
            states[sid] = record
            seg["status"] = "异常中断"
            seg["pending"] = True
            seg["abnormal"] = True
            return "failed"

        # 确定性产物：同段同输入永远得到同一个批次编号与校验摘要
        raw_rows = 120 + sid * 10
        valid_rows = raw_rows - (2 + sid % 5)
        checksum = fp[:16]
        batch_no = f"BATCH-{sid}-{fp[:10]}"

        existing_batch = self._find_batch(batch_no)
        if existing_batch is not None:
            batch = existing_batch
            reused_batch = True
        else:
            batch_values = {
                "批次编号": batch_no,
                "关联任务": code,
                "原始行数": str(raw_rows),
                "有效行数": str(valid_rows),
                "存储路径": f"/store/batch/{sid}/{checksum}.json",
                "压缩格式": "gzip",
                "校验摘要": checksum,
                "落地时间": now,
            }
            batch, missing = self._result_svc.create_entry(batch_values)
            if batch is None:
                raise RuntimeError(f"结果批次写入失败：缺少 {missing}")
            reused_batch = False

        record = {
            "segment_id": sid,
            "segment_code": code,
            "input_fingerprint": fp,
            "status": ST_DONE,
            "batch_id": batch["id"],
            "batch_no": batch_no,
            "raw_rows": raw_rows,
            "valid_rows": valid_rows,
            "checksum": checksum,
            "attempts": int(previous.get("attempts", 0)) + 1,
            "run_ids": list(previous.get("run_ids", [])),
            "blocked_by": [],
            "block_reason": "",
            "error": "",
            "reused_batch": reused_batch,
            "started_at": previous.get("started_at", now),
            "finished_at": now,
            "updated_at": now,
        }
        self._upsert_state(record)
        states[sid] = record
        seg["status"] = "已完成"
        seg["pending"] = False
        seg["abnormal"] = False
        return "done"

    def _mark_blocked(
        self,
        seg: dict[str, Any],
        dep_ids: list[int],
        fp: str,
        states: dict[int, dict[str, Any]],
        run_id: int,
        reason: str,
        blocked_by: list[int],
    ) -> str:
        sid = int(seg["id"])
        now = self._now()
        previous = states.get(sid, {})
        record = {
            "segment_id": sid,
            "segment_code": str(seg.get("段编号", sid)),
            "input_fingerprint": fp,
            "status": ST_BLOCKED,
            "batch_id": previous.get("batch_id"),
            "batch_no": previous.get("batch_no"),
            "raw_rows": previous.get("raw_rows", 0),
            "valid_rows": previous.get("valid_rows", 0),
            "checksum": previous.get("checksum", ""),
            "attempts": int(previous.get("attempts", 0)),
            "run_ids": list(previous.get("run_ids", [])),
            "blocked_by": blocked_by,
            "block_reason": reason,
            "error": previous.get("error", ""),
            "started_at": previous.get("started_at", now),
            "finished_at": now,
            "updated_at": now,
        }
        self._upsert_state(record)
        states[sid] = record
        seg["status"] = "待编排"
        seg["pending"] = True
        return "blocked"

    def execute_segment(self, segment_id: int) -> dict[str, Any]:
        """执行单个流水线段；依赖未满足时不启动，只返回/记录阻断关系。"""
        with self._lock:
            seg = store.find(MODULE, segment_id)
            if seg is None:
                return {"ok": False, "error": f"流水线段 {segment_id} 不存在"}
            rows = self._rows()
            deps, children = self._build_relations(rows)
            try:
                self._topo_layers({int(r["id"]) for r in rows}, deps)
            except ValueError as exc:
                return {"ok": False, "error": str(exc)}

            run_id = store.joint_next_id(RUN_TABLE)
            states = self._state_map()
            outcome = self._evaluate(seg, deps, states, run_id)
            result = self._build_run_result(
                trigger="execute-segment", policy=POLICY_CONTINUE,
                target=segment_id, states=states, rows=rows, deps=deps,
                children=children, run_id=run_id,
                touched=outcome in ("done", "failed", "blocked"),
                has_failure=outcome in ("failed", "blocked"),
                order=[segment_id],
            )
            if outcome == "blocked":
                state = states[segment_id]
                return {
                    "ok": False, "blocked": True, "reused": False,
                    "error": state["block_reason"], **result,
                }
            if outcome == "failed":
                state = states[segment_id]
                return {
                    "ok": False, "blocked": False, "reused": False,
                    "error": state["error"], **result,
                }
            return {"ok": True, "reused": outcome == "reused", **result}

    # ------------------------------------------------------------------
    # 整图按依赖执行 / 失败续做
    # ------------------------------------------------------------------
    def _evaluate(
        self,
        seg: dict[str, Any],
        deps: dict[int, list[int]],
        states: dict[int, dict[str, Any]],
        run_id: int,
    ) -> str:
        """评估单段：reused / done / failed / blocked。"""
        sid = int(seg["id"])
        dep_ids = deps[sid]
        fp = self._input_fingerprint(seg, dep_ids)
        state = states.get(sid)

        # 同段同输入已有成功批次：直接复用，不再写批次、不再生成新版本
        if state and state.get("status") == ST_DONE and state.get("input_fingerprint") == fp:
            seg["status"] = "已完成"
            seg["pending"] = False
            return "reused"

        unsatisfied = [
            d for d in dep_ids
            if not (states.get(d) and states[d].get("status") == ST_DONE)
        ]
        if unsatisfied:
            failed_deps = [
                d for d in unsatisfied
                if states.get(d) and states[d].get("status") in (ST_FAILED, ST_BLOCKED)
            ]
            blocked_by = sorted(set(failed_deps)) or sorted(set(unsatisfied))
            reasons = []
            for d in blocked_by:
                dep_state = states.get(d)
                dep_code = dep_state.get("segment_code", d) if dep_state else d
                if dep_state and dep_state.get("status") == ST_FAILED:
                    reasons.append(
                        f"上游依赖段 {dep_code}(id={d}) 执行失败：{dep_state.get('error', '')}"
                    )
                else:
                    reasons.append(f"上游依赖段 {dep_code}(id={d}) 尚未成功")
            reason = "；".join(reasons) + "，后继段不能启动"
            return self._mark_blocked(seg, dep_ids, fp, states, run_id, reason, blocked_by)

        return self._execute_once(seg, dep_ids, fp, states, run_id)

    def execute_dag(
        self,
        *,
        policy: str = POLICY_CONTINUE,
        trigger: str = "execute-dag",
        target: int | None = None,
    ) -> dict[str, Any]:
        """按 DAG 依赖约束执行全部段；policy 决定失败时的执行边界。"""
        with self._lock:
            if policy not in VALID_POLICIES:
                return {"ok": False, "error": f"未知执行边界策略：{policy}"}
            rows = self._rows()
            deps, children = self._build_relations(rows)
            ids = {int(r["id"]) for r in rows}
            try:
                layers = self._topo_layers(ids, deps)
            except ValueError as exc:
                return {"ok": False, "error": str(exc)}

            run_id = store.joint_next_id(RUN_TABLE)
            states = self._state_map()
            row_by_id = {int(r["id"]): r for r in rows}
            order: list[int] = []
            touched = False
            stop_after_layer = False

            for layer in layers:
                layer_had_failure = False
                for sid in layer:
                    order.append(sid)
                    outcome = self._evaluate(row_by_id[sid], deps, states, run_id)
                    if outcome in ("done", "failed", "blocked"):
                        touched = True
                    if outcome == "failed":
                        layer_had_failure = True
                if layer_had_failure and policy == POLICY_STOP:
                    stop_after_layer = True
                    break

            # 停层策略：后续层全部记为受阻（含不相关分支），统一等待修复
            if stop_after_layer:
                processed = set(order)
                failed_ids = {
                    sid for sid, st in states.items() if st.get("status") == ST_FAILED
                }
                for sid in sorted(ids - processed):
                    seg = row_by_id[sid]
                    dep_ids = deps[sid]
                    fp = self._input_fingerprint(seg, dep_ids)
                    failed_ancestors = sorted(
                        self._ancestors(sid, deps) & failed_ids
                    )
                    if failed_ancestors:
                        blocked_by = failed_ancestors
                        reason = (
                            "停层策略触发："
                            + "、".join(
                                f"失败段 {states[d].get('segment_code', d)}(id={d})"
                                for d in failed_ancestors
                            )
                            + " 未修复，后继段不能启动"
                        )
                    else:
                        blocked_by = []
                        reason = (
                            "停层策略触发：当前层存在失败段，整图统一等待修复，"
                            "该段虽无失败依赖也不得越过当前层执行"
                        )
                    self._mark_blocked(seg, dep_ids, fp, states, run_id, reason, blocked_by)
                    touched = True

            has_failure = any(
                st.get("status") in (ST_FAILED, ST_BLOCKED) for st in states.values()
            )
            return self._build_run_result(
                trigger=trigger, policy=policy, target=target, states=states,
                rows=rows, deps=deps, children=children, run_id=run_id,
                touched=touched, order=order, has_failure=has_failure,
            )

    def resume(self, *, policy: str = POLICY_CONTINUE) -> dict[str, Any]:
        """失败段中断后续做：已完成段幂等复用，从可继续分支接着跑。"""
        return self.execute_dag(policy=policy, trigger="resume")

    # ------------------------------------------------------------------
    # 有序执行记录与跨模块视图
    # ------------------------------------------------------------------
    def _snapshot_fingerprint(
        self,
        rows: list[dict[str, Any]],
        states: dict[int, dict[str, Any]],
        policy: str,
        trigger: str,
        target: int | None,
    ) -> str:
        parts = [policy, trigger, str(target)]
        for seg in rows:
            sid = int(seg["id"])
            dep_ids = parse_dependency_ids(seg.get("依赖段"))
            st = states.get(sid)
            parts.append("|".join([
                str(sid),
                self._input_fingerprint(seg, dep_ids),
                str(bool(seg.get("模拟失败"))),
                st.get("status", "") if st else "",
                st.get("input_fingerprint", "") if st else "",
            ]))
        return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

    def _build_run_result(
        self,
        *,
        trigger: str,
        policy: str,
        target: int | None,
        states: dict[int, dict[str, Any]],
        rows: list[dict[str, Any]],
        deps: dict[int, list[int]],
        children: dict[int, list[int]],
        run_id: int,
        touched: bool,
        order: list[int] | None = None,
        has_failure: bool = False,
    ) -> dict[str, Any]:
        ids = {int(r["id"]) for r in rows}
        completed = sorted(sid for sid in ids if states.get(sid, {}).get("status") == ST_DONE)
        failed = sorted(sid for sid in ids if states.get(sid, {}).get("status") == ST_FAILED)
        blocked = sorted(sid for sid in ids if states.get(sid, {}).get("status") == ST_BLOCKED)
        reused = sorted(
            sid for sid in ids
            if states.get(sid, {}).get("status") == ST_DONE
            and sid not in (order or [])
        ) if order else []

        # 阻断关系：每个失败段阻断了哪些后继（失败信息里带完整链路）
        blocking_relations = []
        for f in failed:
            blocked_set = self._descendants(f, children)
            f_state = states[f]
            blocking_relations.append({
                "failed_segment": f,
                "failed_code": f_state.get("segment_code", f),
                "error": f_state.get("error", ""),
                "blocks": sorted(blocked_set),
            })

        # 可继续分支：未完成且依赖当前全部成功的段，续做时从这里接着跑
        ready_branches = sorted(
            sid for sid in ids
            if states.get(sid, {}).get("status") != ST_DONE
            and all(states.get(d, {}).get("status") == ST_DONE for d in deps[sid])
        )

        if failed:
            run_status = "存在失败段"
        elif blocked:
            run_status = "存在受阻段"
        elif len(completed) == len(ids):
            run_status = "全部完成"
        else:
            run_status = "部分完成"

        # 独立分支在失败期间继续跑完的段（失败段的非后继、且已完成）
        failed_descendants: set[int] = set()
        for f in failed:
            failed_descendants |= self._descendants(f, children)
        continued_branches = sorted(
            sid for sid in completed if sid not in failed_descendants
        ) if failed else []

        fingerprint = self._snapshot_fingerprint(
            rows, states, policy, trigger, target
        )

        # 重复提交：快照与上一张执行记录一致时不再写新记录
        runs = store.joint_rows(RUN_TABLE)
        last = runs[-1] if runs else None
        if last is not None and last.get("run_fingerprint") == fingerprint:
            return {
                "ok": not has_failure,
                "deduplicated": True,
                "run": last,
                "states": [states[sid] for sid in sorted(states)],
                "blocking_relations": blocking_relations,
                "ready_branches": ready_branches,
            }

        if not touched and last is not None:
            # 没有任何状态变化的空转提交，同样不新增执行记录
            return {
                "ok": not has_failure,
                "deduplicated": True,
                "run": last,
                "states": [states[sid] for sid in sorted(states)],
                "blocking_relations": blocking_relations,
                "ready_branches": ready_branches,
            }

        record = {
            "id": run_id,
            "run_no": f"RUN-{run_id:04d}",
            "trigger": trigger,
            "policy": policy,
            "policy_label": POLICY_LABELS[policy],
            "target_segment": target,
            "status": run_status,
            "segment_order": order or sorted(ids),
            "completed": completed,
            "failed": failed,
            "blocked": [
                {
                    "segment_id": sid,
                    "segment_code": states[sid].get("segment_code", sid),
                    "blocked_by": states[sid].get("blocked_by", []),
                    "reason": states[sid].get("block_reason", ""),
                }
                for sid in blocked
            ],
            "reused_segments": reused,
            "continued_branches": continued_branches,
            "ready_branches": ready_branches,
            "blocking_relations": blocking_relations,
            "run_fingerprint": fingerprint,
            "summary": self._summary_text(
                run_status, completed, failed, blocked, reused, ready_branches, policy
            ),
            "created_at": self._now(),
        }
        store.joint_append(RUN_TABLE, record)
        # 运行记录真正落盘后，再把运行单 id 回填到本次评估过的段状态
        evaluated = order if order else sorted(ids)
        for sid in evaluated:
            state = states.get(sid)
            if state is None:
                continue
            linked = list(state.get("run_ids", []))
            if run_id not in linked:
                linked.append(run_id)
                state["run_ids"] = sorted(linked)
                store.joint_upsert(STATE_TABLE, "segment_id", state)
        return {
            "ok": not has_failure,
            "deduplicated": False,
            "run": record,
            "states": [states[sid] for sid in sorted(states)],
            "blocking_relations": blocking_relations,
            "ready_branches": ready_branches,
        }

    def _summary_text(
        self, status, completed, failed, blocked, reused, ready, policy
    ) -> str:
        parts = [
            f"执行边界：{POLICY_LABELS[policy]}",
            f"已完成 {len(completed)} 段",
            f"失败 {len(failed)} 段",
            f"受阻 {len(blocked)} 段",
        ]
        if reused:
            parts.append(f"幂等复用 {len(reused)} 段")
        if ready:
            parts.append(f"可继续分支 {len(ready)} 段")
        return f"【{status}】" + "，".join(parts)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_states(self) -> list[dict[str, Any]]:
        return list(store.joint_rows(STATE_TABLE))

    def get_state(self, state_id: int) -> dict[str, Any] | None:
        return store.joint_find(STATE_TABLE, state_id)

    def list_runs(self) -> list[dict[str, Any]]:
        return list(store.joint_rows(RUN_TABLE))

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return store.joint_find(RUN_TABLE, run_id)

    def execution_overview(self) -> dict[str, Any]:
        """有序流水线执行记录 + 结果批次数据集 + 阻断/可继续分支的综合视图。"""
        with self._lock:
            rows = self._rows()
            deps, children = self._build_relations(rows)
            try:
                layers = self._topo_layers({int(r["id"]) for r in rows}, deps)
            except ValueError:
                layers = []
            layer_of: dict[int, int] = {
                sid: idx for idx, layer in enumerate(layers) for sid in layer
            }
            states = self._state_map()
            segments = []
            for seg in rows:
                sid = int(seg["id"])
                st = states.get(sid)
                segments.append({
                    "id": sid,
                    "segment_code": seg.get("段编号", ""),
                    "operator": seg.get("转换算子", ""),
                    "input_source": seg.get("输入来源", ""),
                    "output_target": seg.get("输出目标", ""),
                    "deps": deps[sid],
                    "layer": layer_of.get(sid),
                    "segment_status": seg.get("status", ""),
                    "simulate_failure": bool(seg.get("模拟失败")),
                    "exec_status": st.get("status", ST_READY) if st else ST_READY,
                    "batch_no": (st or {}).get("batch_no"),
                    "blocked_by": (st or {}).get("blocked_by", []),
                    "block_reason": (st or {}).get("block_reason", ""),
                    "error": (st or {}).get("error", ""),
                    "attempts": (st or {}).get("attempts", 0),
                })

            # 结果批次数据集：只取流水线产出的批次（批次编号 BATCH- 前缀）
            batches = [
                {
                    "id": b["id"],
                    "批次编号": b.get("批次编号"),
                    "关联任务": b.get("关联任务"),
                    "原始行数": b.get("原始行数"),
                    "有效行数": b.get("有效行数"),
                    "校验摘要": b.get("校验摘要"),
                    "落地时间": b.get("落地时间"),
                    "status": b.get("status"),
                }
                for b in store.rows(RESULT_MODULE)
                if str(b.get("批次编号", "")).startswith("BATCH-")
            ]

            failed_ids = [s["id"] for s in segments if s["exec_status"] == ST_FAILED]
            blocking_relations = []
            for f in failed_ids:
                st = states[f]
                blocking_relations.append({
                    "failed_segment": f,
                    "failed_code": st.get("segment_code", f),
                    "error": st.get("error", ""),
                    "blocks": sorted(self._descendants(f, children)),
                })
            ready_branches = sorted(
                s["id"] for s in segments
                if s["exec_status"] != ST_DONE
                and all(states.get(d, {}).get("status") == ST_DONE for d in deps[s["id"]])
            )
            return {
                "segments": segments,
                "edges": [
                    {"from": dep, "to": sid, "label": "依赖"}
                    for sid, dep_list in deps.items() for dep in dep_list
                ],
                "layers": layers,
                "runs": self.list_runs(),
                "states": self.list_states(),
                "batches": batches,
                "blocking_relations": blocking_relations,
                "ready_branches": ready_branches,
                "policy_options": [
                    {"value": k, "label": v} for k, v in POLICY_LABELS.items()
                ],
            }

    def sync_checklist(self) -> dict[str, Any]:
        """跨模块入口同步清单：核对段、执行状态、结果批次与执行记录的一致性。"""
        with self._lock:
            rows = self._rows()
            states = self._state_map()
            state_rows = self.list_states()
            runs = self.list_runs()
            batches = store.rows(RESULT_MODULE)
            batch_by_no = {str(b.get("批次编号")): b for b in batches}

            checks: list[dict[str, Any]] = []

            done_states = [st for st in state_rows if st.get("status") == ST_DONE]
            missing_batch = [
                st["segment_id"] for st in done_states
                if not st.get("batch_no") or st["batch_no"] not in batch_by_no
            ]
            checks.append({
                "name": "每个已完成段都对应一个存在的结果批次",
                "ok": not missing_batch,
                "detail": "缺失批次的段：" + ",".join(map(str, missing_batch))
                if missing_batch else f"{len(done_states)} 个已完成段全部有有效批次",
            })

            seen: dict[str, int] = {}
            duplicate_batches: list[str] = []
            for st in done_states:
                no = st.get("batch_no")
                if no is None:
                    continue
                if no in seen:
                    duplicate_batches.append(no)
                seen[no] = int(st["segment_id"])
            checks.append({
                "name": "同一段在相同输入下只有一个有效批次",
                "ok": not duplicate_batches,
                "detail": "重复批次：" + "、".join(duplicate_batches)
                if duplicate_batches else "未发现重复执行版本",
            })

            blocked_missing_reason = [
                st["segment_id"] for st in state_rows
                if st.get("status") == ST_BLOCKED and not st.get("block_reason")
            ]
            checks.append({
                "name": "每个受阻段都记录了阻断关系与原因",
                "ok": not blocked_missing_reason,
                "detail": "缺少阻断原因的段：" + ",".join(map(str, blocked_missing_reason))
                if blocked_missing_reason else "阻断关系完整",
            })

            failed_missing_error = [
                st["segment_id"] for st in state_rows
                if st.get("status") == ST_FAILED and not st.get("error")
            ]
            checks.append({
                "name": "每个失败段都记录了失败信息",
                "ok": not failed_missing_error,
                "detail": "缺少失败信息的段：" + ",".join(map(str, failed_missing_error))
                if failed_missing_error else "失败信息完整",
            })

            entries = [
                {"entry": "data_pipeline 流水线段", "count": len(rows)},
                {"entry": "pipeline_state 段执行状态", "count": len(state_rows)},
                {"entry": "result_store 流水线结果批次",
                 "count": sum(1 for b in batches if str(b.get("批次编号", "")).startswith("BATCH-"))},
                {"entry": "pipeline_runs 有序执行记录", "count": len(runs)},
            ]
            return {
                "ok": all(c["ok"] for c in checks),
                "entries": entries,
                "checks": checks,
                "checked_at": self._now(),
            }

    def reset_all(self) -> dict[str, Any]:
        """清空流水线运行态（执行状态、执行记录）并把段复位为待编排，演示重放用。"""
        with self._lock:
            store.joint_clear(STATE_TABLE)
            store.joint_clear(RUN_TABLE)
            for seg in store.rows(MODULE):
                seg["status"] = "待编排"
                seg["pending"] = True
                seg["abnormal"] = False
                # 复位到种子声明的失败注入，保证重放演示可重现
                seg["模拟失败"] = bool(seg.get("模拟失败"))
            return {"ok": True, "message": "流水线执行状态与执行记录已清空"}


pipeline_executor = PipelineExecutor()
