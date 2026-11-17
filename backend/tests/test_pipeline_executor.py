"""流水线依赖执行引擎的端到端测试。

运行：cd backend && .venv/bin/python -m unittest tests.test_pipeline_executor -v
"""
from __future__ import annotations

import threading
import unittest

from app.services.pipeline_executor import (
    POLICY_CONTINUE,
    POLICY_STOP,
    ST_BLOCKED,
    ST_DONE,
    ST_FAILED,
    pipeline_executor as pe,
)
from app.store import store

ALL_IDS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}


class PipelineExecutorCase(unittest.TestCase):
    def setUp(self) -> None:
        pe.reset_all()
        # 恢复默认失败注入：段 2 失败
        for sid in ALL_IDS:
            store.find("data_pipeline", sid)["模拟失败"] = (sid == 2)

    def tearDown(self) -> None:
        pe.reset_all()

    # ----------------------------------------------------------------
    def test_01_dependency_blocks_successor_and_independent_branch_continues(self) -> None:
        """段2失败：后继段受阻，独立分支(8->9)与同层段3继续完成。"""
        res = pe.execute_dag(policy=POLICY_CONTINUE)
        self.assertFalse(res["ok"])  # 含失败/受阻段
        states = {s["segment_id"]: s for s in res["states"]}

        self.assertEqual(states[1]["status"], ST_DONE)
        self.assertEqual(states[2]["status"], ST_FAILED)
        self.assertTrue(states[2]["error"])  # 失败信息必须记录
        self.assertEqual(states[3]["status"], ST_DONE)       # 同层独立段继续
        self.assertEqual(states[8]["status"], ST_DONE)       # 独立分支继续
        self.assertEqual(states[9]["status"], ST_DONE)
        self.assertEqual(states[10]["status"], ST_DONE)      # 独立分支深层段继续
        for sid in (4, 5, 6, 7):
            self.assertEqual(states[sid]["status"], ST_BLOCKED)
            self.assertTrue(states[sid]["blocked_by"])       # 阻断关系
            self.assertIn("不能启动", states[sid]["block_reason"])

        # 任何后继段都不能越过失败依赖：5 的阻断源包含失败链路上的段
        self.assertIn(2, states[4]["blocked_by"])
        blocking = {r["failed_segment"]: set(r["blocks"]) for r in res["blocking_relations"]}
        self.assertEqual(blocking[2], {4, 5, 6, 7})

        # 可继续分支：失败段2的依赖已满足，它本身就是修复后的续做入口
        self.assertEqual(res["ready_branches"], [2])
        # 独立分支在失败期间继续跑完的段
        self.assertEqual(set(res["run"]["continued_branches"]), {1, 3, 8, 9, 10})

    def test_02_stop_layer_policy_holds_whole_graph(self) -> None:
        """停层策略：失败层之后的所有段统一受阻，包括无失败依赖的独立分支深层段。"""
        res = pe.execute_dag(policy=POLICY_STOP)
        states = {s["segment_id"]: s for s in res["states"]}
        # 第 0 层完成；第 1 层段2失败，段3、段9完成
        self.assertEqual(states[1]["status"], ST_DONE)
        self.assertEqual(states[8]["status"], ST_DONE)
        self.assertEqual(states[2]["status"], ST_FAILED)
        # 失败层之后：受失败影响的 4,5,6,7 与独立分支的 10 全部受阻
        for sid in (4, 5, 6, 7, 10):
            self.assertEqual(states[sid]["status"], ST_BLOCKED)
        # 段10 没有失败祖先，阻断原因来自停层策略本身
        reason_10 = states[10]["block_reason"]
        self.assertIn("停层策略", reason_10)
        self.assertEqual(states[10]["blocked_by"], [])
        run = res["run"]
        self.assertEqual(run["policy"], POLICY_STOP)

    def test_03_duplicate_submission_writes_single_run_and_single_batch(self) -> None:
        """重复提交只写一次执行记录，同段同输入只有一个有效批次。"""
        first = pe.execute_dag(policy=POLICY_CONTINUE)
        run_count_after_first = len(pe.list_runs())
        second = pe.execute_dag(policy=POLICY_CONTINUE)
        self.assertTrue(second["deduplicated"])
        self.assertEqual(len(pe.list_runs()), run_count_after_first)

        # 再重复单段执行已完成段：复用，不新增批次
        batch_before = [b for b in store.rows("result_store") if b.get("批次编号")]
        single = pe.execute_segment(1)
        self.assertTrue(single.get("reused"))
        batch_after = [b for b in store.rows("result_store") if b.get("批次编号")]
        self.assertEqual(len(batch_before), len(batch_after))

        # 每个完成段只有一个 BATCH 批次号
        states = {s["segment_id"]: s for s in pe.list_states()}
        batch_nos = [s["batch_no"] for s in states.values() if s["status"] == ST_DONE]
        self.assertEqual(len(batch_nos), len(set(batch_nos)))

    def test_04_resume_after_fix_completes_remaining_branches(self) -> None:
        """修复失败段后续做：从可继续分支接着跑，全图收口。"""
        pe.execute_dag(policy=POLICY_CONTINUE)
        # 修复段 2
        store.find("data_pipeline", 2)["模拟失败"] = False
        res = pe.resume(policy=POLICY_CONTINUE)
        states = {s["segment_id"]: s for s in res["states"]}
        for sid in ALL_IDS:
            self.assertEqual(states[sid]["status"], ST_DONE, f"段 {sid} 应收口为已完成")
        self.assertEqual(res["run"]["status"], "全部完成")

        # 续做只产生两张执行记录（首次 + 修复后续做）
        self.assertEqual(len(pe.list_runs()), 2)
        # 首批次不被重写：段 1 的批次号在两次运行间保持不变
        self.assertEqual(len({s["batch_no"] for s in states.values()}), len(ALL_IDS))

    def test_05_segment_execute_refuses_unsatisfied_dependency(self) -> None:
        """依赖段未成功时后继段不能启动。"""
        blocked = pe.execute_segment(4)
        self.assertFalse(blocked["ok"])
        self.assertTrue(blocked.get("blocked"))
        state = next(s for s in blocked["states"] if s["segment_id"] == 4)
        self.assertEqual(state["status"], ST_BLOCKED)
        self.assertIn("尚未成功", state["block_reason"])

        # 先执行段 1，再执行段 2（段2带失败注入）-> 失败
        self.assertTrue(pe.execute_segment(1)["ok"])
        failed = pe.execute_segment(2)
        self.assertFalse(failed["ok"])
        self.assertIn("执行失败", failed["error"])

    def test_06_race_does_not_duplicate_versions(self) -> None:
        """并发竞态提交同一段：只产生一个执行状态版本与一个结果批次。"""
        errors: list[Exception] = []
        results: list[dict] = []

        def worker() -> None:
            try:
                results.append(pe.execute_segment(1))
            except Exception as exc:  # pragma: no cover - 测试兜底
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors)
        states = [s for s in pe.list_states() if s["segment_id"] == 1]
        self.assertEqual(len(states), 1)
        batch_nos = [
            b["批次编号"] for b in store.rows("result_store")
            if str(b.get("批次编号", "")).startswith("BATCH-1-")
        ]
        self.assertEqual(len(batch_nos), 1)
        self.assertTrue(all(r["ok"] for r in results))

    def test_07_cyclic_dependency_rejected(self) -> None:
        """循环依赖被拒绝执行。"""
        store.find("data_pipeline", 1)["依赖段"] = "4"  # 1->2->4->1 成环
        try:
            res = pe.execute_dag()
            self.assertFalse(res["ok"])
            self.assertIn("循环", res["error"])
        finally:
            store.find("data_pipeline", 1)["依赖段"] = ""

    def test_08_sync_checklist(self) -> None:
        """跨模块入口同步清单：批次、阻断关系、失败信息一致性。"""
        pe.execute_dag(policy=POLICY_CONTINUE)
        checklist = pe.sync_checklist()
        self.assertTrue(checklist["ok"], checklist)
        names = {c["name"] for c in checklist["checks"]}
        self.assertEqual(len(names), 4)

        # 全部修复收口后清单仍然通过
        store.find("data_pipeline", 2)["模拟失败"] = False
        pe.resume(policy=POLICY_CONTINUE)
        self.assertTrue(pe.sync_checklist()["ok"])

    def test_09_two_policies_yield_different_resume_scopes(self) -> None:
        """两种做法形成不同的待续做范围与已完成段集合。"""
        pe.reset_all()
        for sid in ALL_IDS:
            store.find("data_pipeline", sid)["模拟失败"] = (sid == 2)
        cont = pe.execute_dag(policy=POLICY_CONTINUE)
        cont_done = set(cont["run"]["completed"])
        cont_blocked = {b["segment_id"] for b in cont["run"]["blocked"]}

        pe.reset_all()
        stop = pe.execute_dag(policy=POLICY_STOP)
        stop_done = set(stop["run"]["completed"])
        stop_blocked = {b["segment_id"] for b in stop["run"]["blocked"]}

        # 两种策略下失败段都不完成，且任何后继都不越过失败依赖
        self.assertNotIn(2, cont_done | stop_done)
        # 独立分支深层段 10：continue_branch 已完成，stop_layer 被统一拦住
        self.assertIn(10, cont_done)
        self.assertNotIn(10, stop_done)
        self.assertIn(10, stop_blocked)
        # 停层策略受阻范围是分支继续策略的超集，待续做范围更大
        self.assertTrue(stop_blocked.issuperset(cont_blocked))
        self.assertNotEqual(cont_blocked, stop_blocked)
        # 受失败影响的后继段在两种策略下都被阻塞，没有越过失败依赖
        for sid in (4, 5, 6, 7):
            self.assertIn(sid, cont_blocked)
            self.assertIn(sid, stop_blocked)


if __name__ == "__main__":
    unittest.main()
