<template>
  <div>
    <div class="page-head">
      <div>
        <h2>实时监控大屏</h2>
        <p>Kafka → Spark Structured Streaming → MySQL / HDFS · 每 10 秒刷新</p>
      </div>
      <div class="muted" v-if="lastUpdate">数据时间 {{ lastUpdate }}</div>
    </div>

    <div v-if="error" class="card" style="border-color: rgba(255,93,108,.5); color: var(--red)">
      {{ error }}
    </div>

    <div class="grid cards" style="margin-bottom: 16px">
      <div class="card">
        <h3>当前准点率</h3>
        <div class="metric">{{ totals.onTimePct }}<span class="unit">%</span></div>
      </div>
      <div class="card">
        <h3>出发航班</h3>
        <div class="metric">{{ totals.departures }}</div>
      </div>
      <div class="card">
        <h3>延误航班</h3>
        <div class="metric" style="color: var(--amber)">{{ totals.delayed }}</div>
      </div>
      <div class="card">
        <h3>平均延误</h3>
        <div class="metric">{{ totals.avgDelay.toFixed(1) }}<span class="unit">分钟</span></div>
      </div>
      <div class="card">
        <h3>取消</h3>
        <div class="metric" style="color: var(--red)">{{ totals.cancelled }}</div>
      </div>
    </div>

    <div class="grid" style="grid-template-columns: 1.25fr 1fr; margin-bottom: 16px">
      <div class="card chart-card">
        <h3>近 60 分钟趋势</h3>
        <div ref="trendEl" class="chart"></div>
      </div>
      <div class="card chart-card">
        <h3>机场实时分布（经纬度）</h3>
        <div ref="mapEl" class="chart"></div>
      </div>
    </div>

    <div class="card">
      <h3>实时告警</h3>
      <div v-if="!alerts.length" class="empty">暂无告警</div>
      <table v-else class="table">
        <thead><tr><th>时间</th><th>机场</th><th>级别</th><th>原因</th><th>平均延误</th><th>延误比例</th></tr></thead>
        <tbody>
          <tr v-for="a in alerts" :key="a.id">
            <td class="mono">{{ String(a.alert_time).replace('T', ' ') }}</td>
            <td>{{ a.airport }}</td>
            <td><span class="tag" :class="a.level">{{ a.level }}</span></td>
            <td class="muted">{{ a.reason }}</td>
            <td>{{ Number(a.avg_delay).toFixed(1) }} min</td>
            <td>{{ (Number(a.delayed_ratio) * 100).toFixed(1) }}%</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import * as echarts from 'echarts'
import { api } from '../api'

const kpi = ref([])
const trend = ref([])
const alerts = ref([])
const error = ref('')
const lastUpdate = ref('')
const trendEl = ref(null)
const mapEl = ref(null)
let trendChart = null
let mapChart = null
let timer = null

const totals = computed(() => {
  const d = kpi.value || []
  const departures = d.reduce((s, x) => s + Number(x.departures || 0), 0)
  const delayed = d.reduce((s, x) => s + Number(x.delayed_flights || 0), 0)
  const cancelled = d.reduce((s, x) => s + Number(x.cancelled || 0), 0)
  const weighted = d.reduce((s, x) => s + Number(x.avg_dep_delay || 0) * Number(x.departures || 0), 0)
  const avgDelay = departures ? weighted / departures : 0
  const onTimePct = departures ? ((1 - delayed / departures) * 100).toFixed(1) : '0.0'
  return { departures, delayed, cancelled, avgDelay, onTimePct }
})

function baseAxis() {
  return {
    axisLine: { lineStyle: { color: 'rgba(142,163,199,.35)' } },
    axisLabel: { color: '#8ea3c7', fontSize: 10 },
    splitLine: { lineStyle: { color: 'rgba(94,129,189,.14)' } }
  }
}

function renderTrend() {
  if (!trendChart) return
  const x = trend.value.map((r) => String(r.stat_time).slice(11, 16))
  trendChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#8ea3c7' }, top: 0 },
    grid: { left: 42, right: 42, top: 36, bottom: 26 },
    xAxis: { type: 'category', data: x, ...baseAxis() },
    yAxis: [
      { type: 'value', name: '航班', ...baseAxis() },
      { type: 'value', name: '准点率', min: 0, max: 1, ...baseAxis() }
    ],
    series: [
      { name: '航班量', type: 'bar', data: trend.value.map((r) => r.departures), itemStyle: { color: 'rgba(79,156,255,.65)' }, barMaxWidth: 14 },
      { name: '延误数', type: 'bar', data: trend.value.map((r) => r.delayed_flights), itemStyle: { color: 'rgba(255,184,77,.75)' }, barMaxWidth: 14 },
      { name: '准点率', type: 'line', yAxisIndex: 1, smooth: true, symbol: 'none', data: trend.value.map((r) => r.ontime_rate), lineStyle: { color: '#35e0d0', width: 2 } }
    ]
  })
}

function renderMap() {
  if (!mapChart) return
  const pts = kpi.value
    .filter((r) => r.latitude !== null && r.longitude !== null)
    .map((r) => ({
      name: r.airport,
      value: [Number(r.longitude), Number(r.latitude), Number(r.departures || 1), Number(r.ontime_rate || 0)]
    }))
  mapChart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (p) => {
        const v = p.value
        return `<b>${p.name}</b><br/>航班 ${v[2]} · 准点率 ${(v[3] * 100).toFixed(1)}%`
      }
    },
    grid: { left: 44, right: 18, top: 18, bottom: 26 },
    xAxis: { type: 'value', name: '经度', scale: true, ...baseAxis() },
    yAxis: { type: 'value', name: '纬度', scale: true, ...baseAxis() },
    visualMap: {
      min: 0, max: 1, dimension: 3, right: 6, bottom: 10,
      text: ['准点', '延误'], textStyle: { color: '#8ea3c7', fontSize: 10 },
      inRange: { color: ['#ff5d6c', '#ffb84d', '#35e0d0'] },
      calculable: false
    },
    series: [{
      type: 'scatter',
      data: pts,
      symbolSize: (v) => Math.max(5, Math.min(26, Math.sqrt(v[2]) * 4)),
      itemStyle: { opacity: 0.78, shadowBlur: 8, shadowColor: 'rgba(53,224,208,.35)' }
    }]
  })
}

async function load() {
  try {
    const [k, a, t] = await Promise.all([api.realtimeKpi(), api.alerts(10), api.trend(60)])
    kpi.value = k || []
    alerts.value = a || []
    trend.value = t || []
    error.value = ''
    const latest = kpi.value.reduce((m, x) => (String(x.stat_time || '') > m ? String(x.stat_time) : m), '')
    lastUpdate.value = latest ? latest.replace('T', ' ').slice(0, 19) : ''
    await nextTick()
    renderTrend()
    renderMap()
  } catch (e) {
    error.value = '接口请求失败：' + e.message + '（请确认 Spring Boot API 与回放已启动）'
  }
}

function onResize() {
  trendChart?.resize()
  mapChart?.resize()
}

onMounted(async () => {
  await nextTick()
  trendChart = echarts.init(trendEl.value)
  mapChart = echarts.init(mapEl.value)
  await load()
  timer = setInterval(load, 10000)
  window.addEventListener('resize', onResize)
})

onUnmounted(() => {
  clearInterval(timer)
  window.removeEventListener('resize', onResize)
  trendChart?.dispose()
  mapChart?.dispose()
})
</script>
