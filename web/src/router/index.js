import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from '../views/Dashboard.vue'
import Analysis from '../views/Analysis.vue'
import Prediction from '../views/Prediction.vue'
import Flights from '../views/Flights.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/dashboard' },
    { path: '/dashboard', name: 'dashboard', component: Dashboard, meta: { title: '实时监控大屏' } },
    { path: '/analysis', name: 'analysis', component: Analysis, meta: { title: '延误分析' } },
    { path: '/prediction', name: 'prediction', component: Prediction, meta: { title: '预测与模型' } },
    { path: '/flights', name: 'flights', component: Flights, meta: { title: '航班明细' } }
  ]
})

export default router
