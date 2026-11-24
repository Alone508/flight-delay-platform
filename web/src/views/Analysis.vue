<template>
  <div>
    <div class="page-head">
      <div>
        <h2>延误分析</h2>
        <p>航司 / 机场准点率、五类延误原因、星期×小时热力图与 Top 航线</p>
      </div>
      <div class="row">
        <input v-model="month" placeholder="YYYY-MM（留空取最新）" />
        <button class="btn" @click="loadCauses">查询原因</button>
      </div>
    </div>

    <div v-if="error" class="card" style="border-color: rgba(255,93,108,.5); color: var(--red)">{{ error }}</div>

    <div class="grid" style="grid-template-columns: 1fr 1fr; margin-bottom: 16px">
      <div class="card chart-card"><h3>航司准点率</h3><div ref="airlineEl" class="chart"></div></div>
      <div class="card chart-card"><h3>机场准点率</h3><div ref="airportEl" class="chart"></div></div>
    </div>

    <div class="grid" style="grid-template-columns: 1fr 1.35fr; margin-bottom: 16px">
      <div class="card chart-card"><h3>五类延误原因占比</h3><div ref="causeEl" class="chart"></div></div>
      <div class="card chart-card"><h3>星期 × 小时 平均延误热力图</h3><div ref="heatEl" class="chart"></div></div>
    </div>

    <div class="card chart-card">
      <h3>Top 20 航线（经纬度连线）</h3>
      <div ref="routeEl" class="chart tall"></div>
    </div>
  </div>
</template>

<script setup>
import { nextTick, onMounted, onUnmounted, ref } from 'vue'
import * as echarts from 'echarts'
import { api } from '../api'

const month = ref('')
const error = ref('')
const airlineEl = ref(null)
const airportEl = ref(null)
const causeEl = ref(null)
const heatEl = ref(null)
const routeEl = ref(null)
const charts = {}

const CAUSE_LABEL = {
  CARRIER: '航司',
  WEATHER: '天气',
  NAS: '空管/NAS',
  SECURITY: '安检',
  LATE_AIRCRAFT: '前序晚到'
}
const DAYS = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']

function axis() {
  return {
    axisLine: { lineStyle: { color: 'rgba(142,163,199,.35)' } },
    axisLabel: { color: '#8ea3c7', fontSize: 10 },
    splitLine: { lineStyle: { color: 'rgba(94,129,189,.14)' } }
  }
}

function hbar(el, data, labelKey, valueKey, color) {
  if (!charts[el]) return
  const rows = (data || []).slice().reverse()
  charts[el].setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    grid: { left: 160, right: 28, top: 14, bottom: 24 },
    xAxis: { type: 'value', ...axis() },
    yAxis: {
      type: 'category',
      data: rows.map((r) => r[labelKey]),
      ...axis(),
      axisLabel: {
        color: '#8ea3c7', fontSize: 10,
        formatter: (v) => (v && String(v).length > 18 ? String(v).slice(0, 17) + '…' : v)
      }
    },
    series: [{
      type: 'bar',
      data: rows.map((r) => +(Number(r[valueKey]) * 100).toFixed(1)),
      barWidth: 12,
      itemStyle: { color, borderRadius: [0, 6, 6, 0] },
      label: { show: true, position: 'right', color: '#8ea3c7', fontSize: 10, formatter: '{c}%' }
    }]
  })
}

function renderCause(data) {
  if (!charts.cause) return
  charts.cause.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'item', formatter: '{b}: {c} 分钟 ({d}%)' },
    legend: { bottom: 0, textStyle: { color: '#8ea3c7', fontSize: 10 } },
    series: [{
      type: 'pie', radius: ['42%', '68%'], center: ['50%', '45%'],
      itemStyle: { borderColor: '#070d1b', borderWidth: 2 },
      label: { color: '#e8f0ff', fontSize: 10, formatter: '{b}\n{d}%' },
      data: (data || []).map((r) => ({ name: CAUSE_LABEL[r.cause] || r.cause, value: Math.round(Number(r.delay_minutes || 0)) }))
    }]
  })
}

function renderHeat(data) {
  if (!charts.heat) return
  const rows = data || []
  charts.heat.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (p) => `${DAYS[p.value[1]]} ${p.value[0]}:00<br/>平均延误 ${p.value[2].toFixed(1)} 分钟<br/>航班 ${p.value[3]}`
    },
    grid: { left: 46, right: 18, top: 16, bottom: 46 },
    xAxis: { type: 'category', data: Array.from({ length: 24 }, (_, i) => `${i}时`), ...axis(), splitArea: { show: true } },
    yAxis: { type: 'category', data: DAYS, ...axis(), splitArea: { show: true } },
    visualMap: {
      min: 0, max: Math.max(20, ...rows.map((r) => Number(r.avg_arr_delay || 0))),
      dimension: 2,
      calculable: false, orient: 'horizontal', left: 'center', bottom: 0,
      textStyle: { color: '#8ea3c7', fontSize: 10 },
      inRange: { color: ['#12305a', '#2f6fd0', '#ffb84d', '#ff5d6c'] }
    },
    series: [{
      type: 'heatmap',
      data: rows.map((r) => [Number(r.dep_hour), Number(r.day_of_week) - 1, Number(r.avg_arr_delay || 0), Number(r.flights || 0)]),
      itemStyle: { borderColor: 'rgba(7,13,27,.7)', borderWidth: 1 }
    }]
  })
}

function renderRoutes(data) {
  if (!charts.route) return
  const rows = (data || []).filter((r) => r.origin_lat != null && r.dest_lat != null)
  const points = []
  const seen = new Set()
  rows.forEach((r) => {
    const o = [Number(r.origin_lon), Number(r.origin_lat), r.origin]
    const d = [Number(r.dest_lon), Number(r.dest_lat), r.dest]
    ;[o, d].forEach((p) => {
      if (!seen.has(p[2])) { seen.add(p[2]); points.push({ name: p[2], value: [p[0], p[1]] }) }
    })
  })
  charts.route.setOption({
    backgroundColor: 'transparent',
    tooltip: { formatter: (p) => p.data?.name || p.name || '' },
    grid: { left: 46, right: 24, top: 16, bottom: 28 },
    xAxis: { type: 'value', scale: true, name: '经度', ...axis() },
    yAxis: { type: 'value', scale: true, name: '纬度', ...axis() },
    series: [
      {
        type: 'lines', coordinateSystem: 'cartesian2d', zlevel: 1,
        effect: { show: true, period: 5, trailLength: 0.25, symbolSize: 4, color: '#35e0d0' },
        lineStyle: { color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
          { offset: 0, color: 'rgba(79,156,255,.15)' },
          { offset: 1, color: 'rgba(53,224,208,.85)' }
        ]), width: 1, opacity: 0.55, curveness: 0.18 },
        data: rows.map((r) => ({ coords: [[Number(r.origin_lon), Number(r.origin_lat)], [Number(r.dest_lon), Number(r.dest_lat)]], name: `${r.origin} → ${r.dest}` }))
      },
      {
        type: 'scatter', coordinateSystem: 'cartesian2d', zlevel: 2,
        symbolSize: 6, itemStyle: { color: '#7de6ff', shadowBlur: 8, shadowColor: '#35e0d0' },
        data: points, label: { show: true, position: 'right', color: '#8ea3c7', fontSize: 9, formatter: '{b}' }
      }
    ]
  })
}

async function loadCauses() {
  try {
    const data = await api.delayCauses(month.value.trim() || undefined)
    renderCause(data)
  } catch (e) { error.value = e.message }
}

async function load() {
  try {
    const [airlines, airports, causes, heat, routes] = await Promise.all([
      api.airlinesRank(10), api.airportsRank(10), api.delayCauses(month.value.trim() || undefined),
      api.heatmap(), api.routesTop(20)
    ])
    error.value = ''
    await nextTick()
    hbar('airline', airlines, 'airline_name', 'ontime_rate', '#4f9cff')
    hbar('airport', airports, 'airport', 'ontime_rate', '#35e0d0')
    renderCause(causes)
    renderHeat(heat)
    renderRoutes(routes)
  } catch (e) { error.value = '接口请求失败：' + e.message }
}

function onResize() { Object.values(charts).forEach((c) => c?.resize()) }

onMounted(async () => {
  await nextTick()
  charts.airline = echarts.init(airlineEl.value)
  charts.airport = echarts.init(airportEl.value)
  charts.cause = echarts.init(causeEl.value)
  charts.heat = echarts.init(heatEl.value)
  charts.route = echarts.init(routeEl.value)
  await load()
  window.addEventListener('resize', onResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', onResize)
  Object.values(charts).forEach((c) => c?.dispose())
})
</script>
