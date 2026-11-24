<template>
  <div>
    <div class="page-head">
      <div>
        <h2>航班明细</h2>
        <p>MySQL 索引分页筛选 · 点击行从 HBase REST 读取完整明细</p>
      </div>
      <div class="muted" v-if="total">共 {{ total }} 条</div>
    </div>

    <div class="card" style="margin-bottom: 16px">
      <div class="row">
        <input v-model="filters.date" type="date" />
        <input v-model="filters.airline" placeholder="航司代码 如 AA" maxlength="3" />
        <input v-model="filters.origin" placeholder="出发机场" maxlength="4" />
        <input v-model="filters.dest" placeholder="到达机场" maxlength="4" />
        <input v-model="filters.minDelay" type="number" placeholder="延误≥" style="width: 90px" />
        <input v-model="filters.maxDelay" type="number" placeholder="延误≤" style="width: 90px" />
        <button class="btn primary" @click="load(1)">查询</button>
        <button class="btn" @click="reset">重置</button>
      </div>
    </div>

    <div v-if="error" class="card" style="border-color: rgba(255,93,108,.5); color: var(--red); margin-bottom: 16px">{{ error }}</div>

    <div class="card">
      <div v-if="!rows.length" class="empty">暂无数据（请先运行 spark/batch/ads_flight_list.py 填充 ads_flight_list）</div>
      <table v-else class="table">
        <thead>
          <tr><th>日期</th><th>航司</th><th>航班号</th><th>航线</th><th>计划起飞</th><th>起飞延误</th><th>到达延误</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="r.rowkey" style="cursor: pointer" @click="openDetail(r)">
            <td>{{ r.fl_date }}</td>
            <td>{{ r.airline_code }}</td>
            <td>{{ r.fl_number }}</td>
            <td>{{ r.origin }} → {{ r.dest }}</td>
            <td class="mono">{{ r.crs_dep_time }}</td>
            <td>{{ dash(r.dep_delay) }}</td>
            <td>{{ dash(r.arr_delay) }}</td>
            <td><span class="tag" :class="r.status">{{ statusText(r.status) }}</span></td>
          </tr>
        </tbody>
      </table>

      <div class="row" style="justify-content: flex-end; margin-top: 14px" v-if="rows.length">
        <button class="btn" :disabled="page <= 1" @click="load(page - 1)">上一页</button>
        <span class="muted">第 {{ page }} / {{ totalPages }} 页</span>
        <button class="btn" :disabled="page >= totalPages" @click="load(page + 1)">下一页</button>
      </div>
    </div>

    <div v-if="showDetail" class="modal-mask" @click.self="showDetail = false">
      <div class="modal">
        <div class="row" style="justify-content: space-between">
          <h3>航班明细</h3>
          <button class="btn" @click="showDetail = false">关闭</button>
        </div>
        <div v-if="detailLoading" class="empty">读取中…</div>
        <div v-else-if="!detailKeys.length" class="empty">HBase 中没有该航班明细（仅回放事件会写入）。</div>
        <div v-else class="kv">
          <template v-for="k in detailKeys" :key="k">
            <div class="k">{{ label(k) }}</div>
            <div class="mono">{{ detail[k] }}</div>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { api } from '../api'

const filters = reactive({ date: '', airline: '', origin: '', dest: '', minDelay: '', maxDelay: '' })
const rows = ref([])
const total = ref(0)
const page = ref(1)
const size = 20
const error = ref('')
const showDetail = ref(false)
const detailLoading = ref(false)
const detail = ref({})

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / size)))
const detailKeys = computed(() => Object.keys(detail.value).filter((k) => !k.endsWith('_timestamp') && k !== 'rowkey'))

const statusText = (s) => ({ ontime: '准点', delayed: '延误', cancelled: '取消' }[s] || s)
const dash = (v) => (v === null || v === undefined ? '—' : Number(v).toFixed(0))

function label(k) {
  return {
    'info:airline': '航司', 'info:dest': '到达机场', 'info:dep_delay': '起飞延误',
    'info:arr_delay': '到达延误', 'info:cancelled': '取消', 'info:status': '状态',
    'info:updated_at': '更新时间'
  }[k] || k
}

async function load(p = 1) {
  try {
    const params = { page: p, size }
    if (filters.date) params.date = filters.date
    if (filters.airline) params.airline = filters.airline.trim()
    if (filters.origin) params.origin = filters.origin.trim()
    if (filters.dest) params.dest = filters.dest.trim()
    if (filters.minDelay !== '') params.minDelay = filters.minDelay
    if (filters.maxDelay !== '') params.maxDelay = filters.maxDelay
    const data = await api.flights(params)
    rows.value = data.rows || []
    total.value = data.total || 0
    page.value = p
    error.value = ''
  } catch (e) {
    error.value = '查询失败：' + e.message
  }
}

function reset() {
  Object.assign(filters, { date: '', airline: '', origin: '', dest: '', minDelay: '', maxDelay: '' })
  load(1)
}

async function openDetail(row) {
  showDetail.value = true
  detailLoading.value = true
  detail.value = {}
  try {
    detail.value = (await api.flightDetail(row.rowkey)) || {}
  } catch (e) {
    error.value = 'HBase 明细读取失败：' + e.message
  } finally {
    detailLoading.value = false
  }
}

onMounted(() => load(1))
</script>
