<template>
  <div>
    <div class="page-head">
      <div>
        <h2>预测与模型</h2>
        <p>离线评测：2023 测试集模型打分（非实时预测）· 2019-2021 训练 / 2022 验证 · 目标编码仅用训练集</p>
      </div>
      <div class="muted" v-if="latest">当前模型：{{ latest.model_name }} · {{ latest.model_version }}</div>
    </div>

    <div v-if="error" class="card" style="border-color: rgba(255,93,108,.5); color: var(--red)">{{ error }}</div>

    <div class="grid cards" style="margin-bottom: 16px" v-if="latest">
      <div class="card"><h3>测试 AUC</h3><div class="metric">{{ fmt(latest.auc) }}</div></div>
      <div class="card"><h3>验证 AUC</h3><div class="metric">{{ fmt(latest.val_auc) }}</div></div>
      <div class="card"><h3>决策阈值</h3><div class="metric">{{ latest.threshold == null ? '0.50' : Number(latest.threshold).toFixed(2) }}</div></div>
      <div class="card"><h3>Accuracy</h3><div class="metric">{{ pct(latest.accuracy) }}</div></div>
      <div class="card"><h3>F1</h3><div class="metric">{{ fmt(latest.f1) }}</div></div>
      <div class="card"><h3>Precision</h3><div class="metric">{{ fmt(latest.precision_score) }}</div></div>
      <div class="card"><h3>Recall</h3><div class="metric">{{ fmt(latest.recall_score) }}</div></div>
    </div>

    <div class="grid" style="grid-template-columns: 0.85fr 1.15fr; margin-bottom: 16px">
      <div class="card chart-card"><h3>混淆矩阵</h3><div ref="cmEl" class="chart"></div></div>
      <div class="card chart-card"><h3>特征重要性 Top 15</h3><div ref="fiEl" class="chart"></div></div>
    </div>

    <div class="card" style="margin-bottom: 16px">
      <h3>模型对比</h3>
      <table class="table" v-if="metrics.length">
        <thead><tr><th>模型</th><th>版本</th><th>验证 AUC</th><th>测试 AUC</th><th>阈值</th><th>Accuracy</th><th>F1</th><th>训练时间</th></tr></thead>
        <tbody>
          <tr v-for="m in metrics" :key="m.model_version">
            <td>{{ m.model_name }}</td>
            <td class="mono">{{ m.model_version }}</td>
            <td>{{ fmt(m.val_auc) }}</td>
            <td>{{ fmt(m.auc) }}</td>
            <td>{{ m.threshold == null ? '—' : Number(m.threshold).toFixed(2) }}</td>
            <td>{{ pct(m.accuracy) }}</td>
            <td>{{ fmt(m.f1) }}</td>
            <td class="mono">{{ m.train_time }}</td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty">暂无模型指标，请先运行 train_model.py</div>
    </div>

    <div class="card">
      <h3>高风险航班（2023 测试集离线评分，按预测分数降序）</h3>
      <div v-if="!predictions.length" class="empty">暂无预测结果</div>
      <table v-else class="table">
        <thead><tr><th>日期</th><th>航司</th><th>航班号</th><th>航线</th><th>计划起飞</th><th>风险分</th><th>预测</th></tr></thead>
        <tbody>
          <tr v-for="p in predictions" :key="p.fl_date + p.airline_code + p.fl_number">
            <td>{{ p.fl_date }}</td>
            <td>{{ p.airline_code }}</td>
            <td>{{ p.fl_number }}</td>
            <td>{{ p.origin }} → {{ p.dest }}</td>
            <td class="mono">{{ p.crs_dep_time }}</td>
            <td>{{ fmt(p.predict_score) }}</td>
            <td><span class="tag" :class="p.predict_label ? 'delayed' : 'ontime'">{{ p.predict_label ? '延误' : '准点' }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { nextTick, onMounted, onUnmounted, ref } from 'vue'
import * as echarts from 'echarts'
import { api } from '../api'

const metrics = ref([])
const predictions = ref([])
const latest = ref(null)
const error = ref('')
const cmEl = ref(null)
const fiEl = ref(null)
let cmChart = null
let fiChart = null

const fmt = (v) => (v === null || v === undefined ? '—' : Number(v).toFixed(4))
const pct = (v) => (v === null || v === undefined ? '—' : (Number(v) * 100).toFixed(2) + '%')

function parseJson(value, fallback) {
  if (!value) return fallback
  if (typeof value === 'object') return value
  try { return JSON.parse(value) } catch { return fallback }
}

function renderConfusion() {
  if (!cmChart || !latest.value) return
  const cm = parseJson(latest.value.confusion_matrix, {})
  const data = []
  for (let y = 0; y <= 1; y++) {
    for (let x = 0; x <= 1; x++) {
      data.push([x, y, Number(cm[`${y}->${x}`] || 0)])
    }
  }
  const max = Math.max(1, ...data.map((d) => d[2]))
  cmChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { formatter: (p) => `实际 ${p.value[1]} / 预测 ${p.value[0]}<br/>${p.value[2]} 条` },
    grid: { left: 54, right: 20, top: 20, bottom: 42 },
    xAxis: { type: 'category', data: ['预测准点', '预测延误'], axisLabel: { color: '#8ea3c7' }, splitArea: { show: true } },
    yAxis: { type: 'category', data: ['实际准点', '实际延误'], axisLabel: { color: '#8ea3c7' }, splitArea: { show: true } },
    visualMap: {
      min: 0, max, calculable: false, orient: 'horizontal', left: 'center', bottom: 0,
      textStyle: { color: '#8ea3c7', fontSize: 10 },
      inRange: { color: ['#0f2447', '#2f6fd0', '#35e0d0'] }
    },
    series: [{ type: 'heatmap', data, label: { show: true, color: '#e8f0ff', fontSize: 12 }, itemStyle: { borderColor: '#070d1b', borderWidth: 2 } }]
  })
}

function renderImportance() {
  if (!fiChart || !latest.value) return
  const items = parseJson(latest.value.feature_importance, []).slice(0, 15).reverse()
  fiChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 210, right: 30, top: 14, bottom: 22 },
    xAxis: { type: 'value', axisLabel: { color: '#8ea3c7', fontSize: 10 }, splitLine: { lineStyle: { color: 'rgba(94,129,189,.14)' } } },
    yAxis: {
      type: 'category',
      data: items.map((i) => i.feature),
      axisLabel: {
        color: '#8ea3c7', fontSize: 9,
        formatter: (v) => (v && String(v).length > 24 ? String(v).slice(0, 23) + '…' : v)
      },
      axisLine: { lineStyle: { color: 'rgba(142,163,199,.35)' } }
    },
    series: [{ type: 'bar', data: items.map((i) => i.importance), barWidth: 10, itemStyle: { color: '#4f9cff', borderRadius: [0, 5, 5, 0] } }]
  })
}

async function load() {
  try {
    const [m, p] = await Promise.all([api.modelMetrics(), api.predictionsTop(20)])
    metrics.value = m || []
    predictions.value = p || []
    const maxTime = metrics.value.reduce((t, x) => (String(x.train_time || '') > t ? String(x.train_time) : t), '')
    const latestRun = metrics.value.filter((x) => String(x.train_time) === maxTime)
    latest.value = latestRun.slice().sort((a, b) => Number(b.auc || 0) - Number(a.auc || 0))[0] || null
    error.value = ''
    await nextTick()
    renderConfusion()
    renderImportance()
  } catch (e) {
    error.value = '接口请求失败：' + e.message
  }
}

function onResize() { cmChart?.resize(); fiChart?.resize() }

onMounted(async () => {
  await nextTick()
  cmChart = echarts.init(cmEl.value)
  fiChart = echarts.init(fiEl.value)
  await load()
  window.addEventListener('resize', onResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', onResize)
  cmChart?.dispose()
  fiChart?.dispose()
})
</script>
