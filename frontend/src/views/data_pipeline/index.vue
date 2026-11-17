<template>
  <section class="page" data-module="data_pipeline">
    <header class="page-head">
      <div>
        <h2>采集流水线 · 依赖执行</h2>
        <p class="page-desc">读取依赖段、既有结果批次与依赖关系，按 DAG 约束执行；失败段阻断后继，独立分支可继续。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流水线段</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card"><span class="stat-label">流水线段</span><strong class="stat-value">{{ overview.segments.length }}</strong></article>
      <article class="stat-card"><span class="stat-label">已完成段</span><strong class="stat-value exec-done">{{ countByExec('已完成') }}</strong></article>
      <article class="stat-card"><span class="stat-label">执行失败段</span><strong class="stat-value exec-failed">{{ countByExec('执行失败') }}</strong></article>
      <article class="stat-card"><span class="stat-label">依赖受阻段</span><strong class="stat-value exec-blocked">{{ countByExec('依赖受阻') }}</strong></article>
      <article class="stat-card"><span class="stat-label">结果批次</span><strong class="stat-value">{{ overview.batches.length }}</strong></article>
      <article class="stat-card"><span class="stat-label">执行记录</span><strong class="stat-value">{{ overview.runs.length }}</strong></article>
    </div>

    <div class="control-bar">
      <label class="policy-pick">
        <span>执行边界策略</span>
        <select v-model="policy" data-testid="policy-select">
          <option v-for="opt in overview.policy_options" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
      </label>
      <button class="btn primary" data-testid="execute-dag" @click="runDag('execute-dag')">按依赖执行整图</button>
      <button class="btn" data-testid="resume" @click="runDag('resume')">失败续做</button>
      <button class="btn ghost" data-testid="reset" @click="resetAll">复位重放</button>
      <span v-if="message" class="control-msg" :class="{ 'error-text': messageError }">{{ message }}</span>
    </div>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in overview.edges" :key="edge.from + '-' + edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x || 0" :y1="nodePos[edge.from]?.y || 0" :x2="nodePos[edge.to]?.x || 0" :y2="nodePos[edge.to]?.y || 0"/>
          <text :x="(nodePos[edge.from]?.x + nodePos[edge.to]?.x) / 2" :y="(nodePos[edge.from]?.y + nodePos[edge.to]?.y) / 2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in overview.segments" :key="node.id" :class="['dag-node', { selected: selectedNode?.id === node.id }]" @click="selectNode(node)">
          <rect :x="nodePos[node.id]?.x - 64" :y="nodePos[node.id]?.y - 22" width="128" height="44" rx="6" :fill="execColor(node.exec_status)" :stroke="node.simulate_failure ? '#b42318' : 'transparent'" stroke-width="2" stroke-dasharray="node.simulate_failure ? 4 : 0"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y - 2" text-anchor="middle" font-weight="600">{{ node.segment_code }}</text>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 14" text-anchor="middle" font-size="10" fill="#334155">{{ node.exec_status }}<template v-if="node.layer !== null"> · L{{ node.layer }}</template></text>
        </g>
      </svg>
      <div class="legend">
        <span><i class="dot done"></i>已完成</span>
        <span><i class="dot ready"></i>待执行</span>
        <span><i class="dot failed"></i>执行失败</span>
        <span><i class="dot blocked"></i>依赖受阻</span>
        <span><i class="dot sim"></i>红框=失败注入</span>
      </div>
    </div>

    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>段编号</dt><dd>{{ selectedNode.segment_code }}</dd></dl>
      <dl><dt>转换算子</dt><dd>{{ selectedNode.operator }}</dd></dl>
      <dl><dt>输入来源</dt><dd>{{ selectedNode.input_source }}</dd></dl>
      <dl><dt>输出目标</dt><dd>{{ selectedNode.output_target }}</dd></dl>
      <dl><dt>依赖段(id)</dt><dd>{{ selectedNode.deps.length ? selectedNode.deps.join('、') : '无（入口段）' }}</dd></dl>
      <dl><dt>拓扑层级</dt><dd>{{ selectedNode.layer }}</dd></dl>
      <dl><dt>执行状态</dt><dd><span class="badge" :class="badgeClass(selectedNode.exec_status)">{{ selectedNode.exec_status }}</span></dd></dl>
      <dl><dt>结果批次</dt><dd>{{ selectedNode.batch_no || '—' }}</dd></dl>
      <dl v-if="selectedNode.block_reason"><dt>阻断原因</dt><dd class="error-text">{{ selectedNode.block_reason }}</dd></dl>
      <dl v-if="selectedNode.error"><dt>失败信息</dt><dd class="error-text">{{ selectedNode.error }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;">
        <button class="btn" :data-testid="`exec-${selectedNode.id}`" @click="execSegment(selectedNode)">执行本段</button>
        <button class="btn" @click="toggleFailure(selectedNode)">{{ selectedNode.simulate_failure ? '关闭失败注入' : '开启失败注入' }}</button>
        <button v-for="action in actions" :key="action" class="btn ghost" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <h3 class="block-title">阻断关系与可继续分支</h3>
    <div class="two-col">
      <div class="panel">
        <h4>阻断关系（失败信息 → 被阻塞后继）</h4>
        <table class="data-table" v-if="overview.blocking_relations.length">
          <thead><tr><th>失败段</th><th>失败信息</th><th>阻断的后继段</th></tr></thead>
          <tbody>
            <tr v-for="rel in overview.blocking_relations" :key="rel.failed_segment" data-status="执行失败">
              <td>{{ rel.failed_code }}({{ rel.failed_segment }})</td>
              <td class="error-text">{{ rel.error }}</td>
              <td>{{ rel.blocks.length ? rel.blocks.join('、') : '无后继' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="empty-state">当前没有阻断关系</p>
        <h4 style="margin-top:14px">可继续分支（依赖已满足，续做从这里开始）</h4>
        <p v-if="overview.ready_branches.length" class="ready-list">
          <span v-for="sid in overview.ready_branches" :key="sid" class="badge ready" @click="selectById(sid)">{{ codeOf(sid) }}({{ sid }})</span>
        </p>
        <p v-else class="empty-state">没有待续做分支（全图已完成或暂无可入口段）</p>
      </div>
      <div class="panel">
        <h4>跨模块入口同步清单</h4>
        <div v-if="checklist">
          <p v-for="entry in checklist.entries" :key="entry.entry" class="check-row">
            <span>{{ entry.entry }}</span><strong>{{ entry.count }}</strong>
          </p>
          <ul class="check-list">
            <li v-for="c in checklist.checks" :key="c.name">
              <i :class="c.ok ? 'dot done' : 'dot failed'"></i>{{ c.name }}<span class="check-detail">{{ c.detail }}</span>
            </li>
          </ul>
        </div>
        <p v-else class="empty-state">加载中…</p>
      </div>
    </div>

    <h3 class="block-title">有序流水线执行记录</h3>
    <table class="data-table" data-testid="runs-table">
      <thead>
        <tr><th>运行单</th><th>触发方式</th><th>执行边界</th><th>状态</th><th>执行顺序</th><th>已完成</th><th>失败</th><th>受阻</th><th>可继续分支</th><th>时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="run in overview.runs" :key="run.id">
          <td>{{ run.run_no }}</td>
          <td>{{ run.trigger }}</td>
          <td>{{ run.policy_label }}</td>
          <td><span class="badge" :class="runStatusClass(run.status)">{{ run.status }}</span></td>
          <td class="mono">{{ run.segment_order.join(' → ') }}</td>
          <td>{{ run.completed.length }}</td>
          <td class="error-text">{{ run.failed.length }}</td>
          <td>{{ run.blocked.length }}</td>
          <td>{{ run.ready_branches.length ? run.ready_branches.join('、') : '—' }}</td>
          <td>{{ run.created_at }}</td>
        </tr>
        <tr v-if="!overview.runs.length"><td colspan="10" class="empty-state">暂无执行记录，点击「按依赖执行整图」开始</td></tr>
      </tbody>
    </table>

    <h3 class="block-title">结果批次数据集（同段同输入只有一个有效批次）</h3>
    <table class="data-table" data-testid="batches-table">
      <thead><tr><th>批次编号</th><th>关联段</th><th>原始行数</th><th>有效行数</th><th>校验摘要</th><th>落地时间</th><th>仓储状态</th></tr></thead>
      <tbody>
        <tr v-for="b in overview.batches" :key="b.id">
          <td>{{ b['批次编号'] }}</td><td>{{ b['关联任务'] }}</td><td>{{ b['原始行数'] }}</td>
          <td>{{ b['有效行数'] }}</td><td class="mono">{{ b['校验摘要'] }}</td><td>{{ b['落地时间'] }}</td><td>{{ b.status }}</td>
        </tr>
        <tr v-if="!overview.batches.length"><td colspan="7" class="empty-state">暂无流水线结果批次</td></tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记流水线段</h3>
        <label v-for="field in createFields" :key="field" class="create-field">
          <span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" :required="field === '段编号' || field === '输入来源' || field === '转换算子'" />
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>{{ overview.segments.length }} 个段 · {{ overview.edges.length }} 条依赖边 · 最近消息：{{ lastSummary || '—' }}</span>
      <span v-if="message" :class="messageError ? 'error-text' : ''">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/data_pipeline'
const actions = ['启动流水线', '暂停执行', '继续执行']
const createFields = ['段编号', '输入来源', '转换算子', '输出目标', '依赖段', '并行度', '失败策略', '超时分钟']

type Segment = Record<string, any>
const overview = reactive<{ segments: Segment[]; edges: any[]; runs: any[]; batches: any[]; blocking_relations: any[]; ready_branches: number[]; policy_options: any[] }>({
  segments: [], edges: [], runs: [], batches: [], blocking_relations: [], ready_branches: [], policy_options: [],
})
const checklist = ref<any>(null)
const policy = ref('continue_branch')
const selectedNode = ref<Segment | null>(null)
const nodePos = ref<Record<number, { x: number; y: number }>>({})
const svgW = ref(900); const svgH = ref(420)
const dagRef = ref<HTMLElement | null>(null)
const message = ref(''); const messageError = ref(false)
const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})
const lastSummary = ref('')

const EXEC_COLORS: Record<string, string> = {
  '已完成': '#bbf7d0',
  '待执行': '#e2e8f0',
  '执行失败': '#fecaca',
  '依赖受阻': '#fde68a',
}
const execColor = (s: string) => EXEC_COLORS[s] || '#e2e8f0'
const badgeClass = (s: string) => ({
  '已完成': 'done', '待执行': 'ready', '执行失败': 'failed', '依赖受阻': 'blocked',
}[s] || 'ready')
const runStatusClass = (s: string) => s.includes('失败') ? 'failed' : s.includes('受阻') ? 'blocked' : s === '全部完成' ? 'done' : 'ready'

const countByExec = (status: string) => overview.segments.filter((s) => s.exec_status === status).length
const codeOf = (sid: number) => overview.segments.find((s) => s.id === sid)?.segment_code || sid

function layout() {
  const byLayer = new Map<number, Segment[]>()
  overview.segments.forEach((s) => {
    const layer = s.layer ?? 0
    if (!byLayer.has(layer)) byLayer.set(layer, [])
    byLayer.get(layer)!.push(s)
  })
  const pos: Record<number, { x: number; y: number }> = {}
  const layerCount = Math.max(1, byLayer.size)
  svgW.value = Math.max(900, layerCount * 170 + 80)
  let maxRows = 1
  byLayer.forEach((nodes) => { maxRows = Math.max(maxRows, nodes.length) })
  svgH.value = Math.max(360, maxRows * 90 + 80)
  byLayer.forEach((nodes, layer) => {
    nodes.forEach((n, i) => {
      pos[n.id] = { x: 90 + layer * 170, y: 60 + i * 90 }
    })
  })
  nodePos.value = pos
}

async function reload() {
  try {
    const res = await request(`${ENDPOINT}/execution-overview`)
    const data = await res.json()
    Object.assign(overview, {
      segments: data.segments || [], edges: data.edges || [], runs: data.runs || [],
      batches: data.batches || [], blocking_relations: data.blocking_relations || [],
      ready_branches: data.ready_branches || [], policy_options: data.policy_options || [],
    })
    layout()
    if (selectedNode.value) {
      const fresh = overview.segments.find((s) => s.id === selectedNode.value!.id)
      if (fresh) selectedNode.value = fresh
    }
    if (data.runs?.length) lastSummary.value = data.runs[data.runs.length - 1].summary
    const cRes = await request(`${ENDPOINT}/sync-checklist`)
    checklist.value = await cRes.json()
  } catch (e) {
    flash('加载执行视图失败', true)
  }
}

function flash(text: string, isError = false) {
  message.value = text
  messageError.value = isError
}

function selectNode(node: Segment) {
  selectedNode.value = selectedNode.value?.id === node.id ? null : node
}
function selectById(sid: number) {
  const node = overview.segments.find((s) => s.id === sid)
  if (node) selectedNode.value = node
}

async function runDag(path: string) {
  try {
    const res = await request(`${ENDPOINT}/${path}`, { method: 'POST', body: JSON.stringify({ policy: policy.value }) })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '执行失败')
    if (data.deduplicated) flash('重复提交已识别：未新增执行记录与结果批次')
    else flash(data.run?.summary || '执行完成', !data.ok)
    await reload()
  } catch (e: any) {
    flash(e.message || '执行失败', true)
  }
}

async function execSegment(node: Segment) {
  try {
    const res = await request(`${ENDPOINT}/${node.id}/execute`, { method: 'POST' })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '执行失败')
    flash(data.deduplicated ? '该段已完成且输入未变，复用既有批次' : (data.run?.summary || '本段执行完成'), !data.ok)
    await reload()
  } catch (e: any) {
    flash(e.message || '执行失败', true)
  }
}

async function toggleFailure(node: Segment) {
  try {
    await request(`${ENDPOINT}/${node.id}/failure-simulation`, {
      method: 'POST', body: JSON.stringify({ enabled: !node.simulate_failure }),
    })
    await reload()
  } catch (e: any) {
    flash(e.message || '失败注入切换失败', true)
  }
}

async function runAction(action: string, node: Segment) {
  try {
    await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    await reload()
  } catch (e) {
    flash('动作执行失败', true)
  }
}

async function resetAll() {
  try {
    await request(`${ENDPOINT}/reset`, { method: 'POST' })
    selectedNode.value = null
    flash('已复位：执行状态与执行记录清空，段恢复待编排')
    await reload()
  } catch (e) {
    flash('复位失败', true)
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(createFields.map((f) => [f, '']))
  showCreate.value = true
}
async function submitCreate() {
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    flash('流水线段已登记')
    await reload()
  } catch (e: any) {
    flash(e.message, true)
  }
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

onMounted(reload)
</script>

<style scoped>
.control-bar { display: flex; gap: 10px; align-items: center; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; flex-wrap: wrap; }
.policy-pick span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 2px; }
.policy-pick select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; min-width: 240px; }
.control-msg { font-size: 13px; color: #166534; }
.legend { display: flex; gap: 14px; padding: 8px 12px; font-size: 12px; color: var(--muted); border-top: 1px solid #f0f2f5; background: #fafbfc; }
.dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; }
.dot.done { background: #22c55e; }
.dot.ready { background: #94a3b8; }
.dot.failed { background: #ef4444; }
.dot.blocked { background: #f59e0b; }
.dot.sim { background: #fff; border: 2px dashed #b42318; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
.badge.done { background: #dcfce7; color: #166534; }
.badge.ready { background: #e2e8f0; color: #475569; }
.badge.failed { background: #fee2e2; color: #b42318; }
.badge.blocked { background: #fef3c7; color: #b45309; }
.exec-done { color: #16a34a; }
.exec-failed { color: #dc2626; }
.exec-blocked { color: #d97706; }
.block-title { font-size: 15px; margin: 18px 0 8px; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.panel h4 { margin: 0 0 8px; font-size: 14px; }
.ready-list { display: flex; gap: 8px; flex-wrap: wrap; }
.ready-list .badge { cursor: pointer; }
.check-row { display: flex; justify-content: space-between; font-size: 13px; padding: 4px 0; margin: 0; border-bottom: 1px dashed #f0f2f5; }
.check-list { list-style: none; padding: 0; margin: 8px 0 0; font-size: 13px; }
.check-list li { padding: 4px 0; display: flex; align-items: center; flex-wrap: wrap; }
.check-detail { width: 100%; padding-left: 16px; color: var(--muted); font-size: 12px; }
.mono { font-family: monospace; font-size: 12px; }
</style>
