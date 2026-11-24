// 用 Playwright 真实浏览器打开四个页面，截图并收集控制台错误。
// 运行：PATH=~/apps/node20/bin:$PATH node web/scripts/screenshot.mjs
import { chromium } from 'playwright'

const base = process.env.BASE_URL || 'http://localhost:5173'
const pages = [
  ['dashboard', '/dashboard'],
  ['analysis', '/analysis'],
  ['prediction', '/prediction'],
  ['flights', '/flights']
]

const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] })
const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } })
let totalErrors = 0

for (const [name, path] of pages) {
  const page = await context.newPage()
  const errors = []
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(msg.text())
  })
  page.on('pageerror', (err) => errors.push('PAGEERROR ' + err.message))
  await page.goto(base + path, { waitUntil: 'domcontentloaded', timeout: 60000 })
  await page.waitForTimeout(4000)
  await page.screenshot({ path: `/tmp/shot-${name}.png`, fullPage: true })
  totalErrors += errors.length
  console.log(`${name}: console_errors=${errors.length}`)
  for (const e of errors) console.log('  ', e)
  await page.close()
}

// Flights 页点击第一行，验证 HBase 明细弹窗
const page = await context.newPage()
const flightErrors = []
page.on('pageerror', (err) => flightErrors.push('PAGEERROR ' + err.message))
await page.goto(base + '/flights', { waitUntil: 'domcontentloaded', timeout: 60000 })
await page.waitForTimeout(3000)
const rows = page.locator('tbody tr')
const count = await rows.count()
if (count > 0) {
  await rows.first().click()
  await page.waitForTimeout(2000)
  await page.screenshot({ path: '/tmp/shot-flights-detail.png', fullPage: true })
  console.log(`flights_detail: rows=${count} modal=${await page.locator('.modal').count()} pageerrors=${flightErrors.length}`)
} else {
  console.log('flights_detail: no rows')
}
for (const e of flightErrors) console.log('  ', e)
await page.close()

await browser.close()
console.log('TOTAL_CONSOLE_ERRORS', totalErrors)
process.exit(totalErrors > 0 ? 1 : 0)
