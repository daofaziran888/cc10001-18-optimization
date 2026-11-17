# 采集工坊 · 爬虫与RPA开发平台

面向网页爬虫规则编排、自动化采集流程、代理调度、反爬对抗、结果校验与持久化发布的爬虫与RPA数据采集开发平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 站点画像 | `site_profile` | 目标站点 | 站点编号、域名、编码格式 |
| 代理调度 | `proxy_pool` | 代理节点 | 节点编号、代理类型、IP地址 |
| 凭证管理 | `cookie_vault` | 会话凭证 | 凭证编号、关联站点、Cookie文本 |
| 频控策略 | `rate_limiter` | 请求策略 | 策略编号、目标站点、每秒上限 |
| 抽取模板 | `extract_rule` | 抽取规则 | 规则编号、目标站点、页面类型 |
| 采集任务 | `crawl_task` | 任务批次 | 任务编号、目标站点、入口URL |
| RPA脚本库 | `rpa_script` | 自动化脚本 | 脚本编号、目标应用、触发事件 |
| 采集流水线 | `data_pipeline` | 流水线段 | 段编号、输入来源、转换算子 |
| 采集仓储 | `result_store` | 结果批次 | 批次编号、关联任务、原始行数 |
| 反爬对抗 | `anti_block` | 对抗策略 | 策略编号、目标站点、触发特征 |
| 质量审计 | `quality_check` | 审计记录 | 记录编号、关联批次、检查项 |
| 增量同步 | `delta_sync` | 同步水位 | 水位编号、源数据集、增量字段 |
| 采集监控 | `log_monitor` | 监控事件 | 事件编号、事件类型、严重级别 |
| 模板市场 | `template_mgr` | 采集模板 | 模板编号、适用站点、模板类型 |
| 数据分发 | `export_hub` | 分发通道 | 通道编号、输出格式、目标地址 |
| 定时调度 | `scheduler` | 调度计划 | 计划编号、关联任务、Cron表达式 |
| 解析引擎 | `parse_engine` | 解析配置 | 配置编号、页面类型、引擎类型 |
| 重试队列 | `retry_queue` | 重试条目 | 条目编号、失败页面、失败原因 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 采集流水线依赖执行

采集流水线不再按队列逐段跑，而是读取段上的「依赖段」声明、既有结果批次与执行记录，
按 DAG 拓扑约束执行（`backend/app/services/pipeline_executor.py`）：

- **执行边界策略**：`continue_branch`（失败分支的独立并行分支继续，仅阻塞受影响后继）
  与 `stop_layer`（整图停在失败所在层，统一等待修复）；两种策略产出不同的已完成段与
  待续做范围，但任何后继段都不能越过失败依赖。
- **有序流水线执行记录**：每次提交写入 `pipeline_runs`，记录执行顺序、已完成/失败/受阻段、
  阻断关系与可继续分支；相同快照的重复提交只返回上一张记录，不再新增。
- **结果批次幂等**：同一段在相同输入（段编号/输入来源/转换算子/依赖集合）下生成确定性
  指纹，只对应一个有效结果批次；检查-判定-写入全程加锁，竞态提交不产生重复执行版本。
- **失败与阻断**：失败段记录失败信息，所有后继段记为「依赖受阻」并带阻断关系；修复后
  调用续做接口，已完成段幂等复用，从可继续分支接着跑完。
- **跨模块入口同步清单**：`/api/data_pipeline/sync-checklist` 核对段、执行状态、结果批次
  与执行记录四张数据集的一致性。

领域：数据工程
子方向：数据采集、爬虫与RPA
