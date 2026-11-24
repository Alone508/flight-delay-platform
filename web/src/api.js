import axios from 'axios'

const http = axios.create({ baseURL: '/api', timeout: 20000 })

http.interceptors.response.use(
  (resp) => {
    const body = resp.data
    if (body && typeof body.code !== 'undefined') {
      if (body.code !== 0) return Promise.reject(new Error(body.message || '接口错误'))
      return body.data
    }
    return body
  },
  (err) => Promise.reject(err)
)

export const api = {
  realtimeKpi: () => http.get('/realtime/kpi'),
  trend: (minutes = 60) => http.get('/realtime/trend', { params: { minutes } }),
  airportsRank: (limit = 10) => http.get('/airports/rank', { params: { limit } }),
  airlinesRank: (limit = 10) => http.get('/airlines/rank', { params: { limit } }),
  delayCauses: (date) => http.get('/delay/causes', { params: date ? { date } : {} }),
  heatmap: () => http.get('/delay/heatmap'),
  routesTop: (limit = 20) => http.get('/routes/top', { params: { limit } }),
  airports: () => http.get('/airports'),
  alerts: (limit = 20) => http.get('/alerts', { params: { limit } }),
  predictionsTop: (limit = 20) => http.get('/predictions/top', { params: { limit } }),
  modelMetrics: () => http.get('/model/metrics'),
  flights: (params) => http.get('/flights', { params }),
  flightDetail: (rowkey) => http.get(`/flights/${encodeURIComponent(rowkey)}`)
}
