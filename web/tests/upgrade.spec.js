import { test, expect } from '@playwright/test'

const funds = Array.from({ length: 10 }, (_, i) => ({ fund_id: i + 1, product_name: `稳健成长${i + 1}号私募证券投资基金`, product_code: `FUND${i + 1}`, latest_nav: 1.25 + i / 100, latest_date: '2026-09-04', strategy_l1: '股票多头', strategy_l2: i % 2 ? '量化选股' : '主观多头', sources: [i % 2 ? 'zx_excel' : 'email'] }))
const nav = Array.from({ length: 40 }, (_, i) => ({ id: i + 1, nav_date: new Date(Date.UTC(2026, 6, 1 + i)).toISOString().slice(0, 10), unit_nav: 1 + i / 200, accumulated_nav: 1 + i / 200, adj_nav: 1 + i / 200, data_source: 'email' }))
async function mockApi(page, admin = false) {
  await page.route('**/api/**', async route => {
    const url = new URL(route.request().url()), path = url.pathname
    let body = { items: [] }
    if (path === '/api/auth/session') body = { authenticated: admin, admin_enabled: admin, readonly: !admin, csrf_token: admin ? 'test-csrf' : '' }
    else if (path === '/api/funds') body = { items: funds.filter(f => !url.searchParams.get('source') || url.searchParams.get('source') === 'all' || f.sources.includes(url.searchParams.get('source'))) }
    else if (path === '/api/stats') body = { total_funds: 10, total_records: 8318, latest_nav_date: '2026-09-04' }
    else if (path === '/api/sync/status') body = { last_status: 'success', last_success_time: '2026-09-04T18:00:00' }
    else if (path === '/api/sync/history') body = { items: [{ id: 1, started_at: '2026-09-04T18:00:00', ended_at: '2026-09-04T18:01:33', processed: 120, added: 37, duplicates: 82, failed: 1, status: 'partial_success' }] }
    else if (/\/api\/funds\/\d+\/nav/.test(path)) body = { items: nav }
    else if (/\/api\/funds\/\d+\/issues/.test(path)) body = { anomalous: [], gaps: [] }
    else if (/\/api\/funds\/\d+$/.test(path)) body = funds.find(f => f.fund_id === Number(path.split('/').at(-1)))
    else if (path === '/api/funds/returns') body = { items: Object.fromEntries(funds.map(f => [f.fund_id, { '1w': 1.2, '1m': 3.2, '3m': null, sparkline: [1, 1.05, 1.02, 1.08] }])) }
    else if (path === '/api/funds/metrics/summary') body = { items: {} }
    else if (path === '/api/portfolios' && route.request().method() === 'POST') body = { id: 7 }
    else if (path === '/api/portfolios/7/calculate') { await route.fulfill({ status: 422, json: { detail: '所选基金没有公共净值区间' } }); return }
    await route.fulfill({ json: body })
  })
}
test('fund overview is responsive, filters source, and hides administration', async ({ page }, testInfo) => {
  await mockApi(page)
  await page.goto('/')
  await expect(page.getByRole('heading', { name: '基金净值概览' })).toBeVisible()
  await expect(page.getByText('8,318')).toBeVisible()
  await page.getByLabel('数据来源', { exact: true }).selectOption('email')
  await expect(page.getByRole('link', { name: funds[1].product_name, exact: true })).toHaveCount(0)
  await expect(page.getByRole('link', { name: '管理员登录', exact: false })).toHaveCount(0)
  for (const width of [1440, 768, 375]) {
    await page.setViewportSize({ width, height: 1000 })
    await expect(page.getByRole('heading', { name: '基金净值概览' })).toBeVisible()
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBe(true)
    await page.screenshot({ path: testInfo.outputPath(`overview-${width}.png`), fullPage: true })
  }
  await page.getByRole('button', { name: '切换主题' }).click()
  await expect(page.locator('html')).toHaveClass('dark')
  await page.screenshot({ path: testInfo.outputPath('overview-dark-375.png'), fullPage: true })
})
test('compare supports empty selection, max eight and reload/share URL', async ({ page }) => {
  await mockApi(page)
  await page.goto('/compare')
  await expect(page.getByRole('heading', { name: '开始一次基金对比' })).toBeVisible()
  for (let i = 0; i < 8; i++) await page.getByRole('button', { name: new RegExp(funds[i].product_name) }).click()
  await expect(page.getByRole('button', { name: new RegExp(funds[8].product_name) })).toBeDisabled()
  expect(new URL(page.url()).searchParams.getAll('fund_ids')).toHaveLength(8)
  await page.reload()
  await expect(page.locator('.picker-item[aria-pressed="true"]')).toHaveCount(8)
  await page.goto('/compare/v2?fund_ids=1&fund_ids=2')
  await expect(page).toHaveURL(/\/compare\?fund_ids=1&fund_ids=2/)
  await expect(page.locator('.picker-item[aria-pressed="true"]')).toHaveCount(2)
})
test('portfolio percentages validate, use actual names, and show calculation failure', async ({ page }) => {
  await mockApi(page, true)
  await page.goto('/portfolios/new?fund_ids=1&fund_ids=2')
  await expect(page.getByRole('heading', { name: '构建基金组合' })).toBeVisible()
  const firstWeight = page.getByLabel(funds[0].product_name + ' 权重百分比')
  await expect(firstWeight).toHaveValue('50')
  const generate = page.getByRole('button', { name: '保存并生成组合 →' })
  await expect(generate).toBeEnabled()
  await firstWeight.fill('60')
  await expect(generate).toBeDisabled()
  await page.getByRole('button', { name: '重置等权' }).click()
  await expect(generate).toBeEnabled()
  const post = page.waitForRequest(req => req.url().endsWith('/api/portfolios') && req.method() === 'POST')
  await generate.click()
  expect((await post).headers()['x-csrf-token']).toBe('test-csrf')
  await expect(page.getByRole('alert')).toContainText('没有公共净值区间')
  await expect(page.getByRole('button', { name: '重试组合计算' })).toBeVisible()
})
test('admin history renders actual sync-run fields', async ({ page }) => {
  await mockApi(page, true)
  await page.goto('/admin')
  const history = page.locator('section').filter({ has: page.getByRole('heading', { name: '同步历史' }) })
  await expect(history).toContainText('2026-09-04 18:01:33')
  for (const value of ['120', '37', '82', '1']) await expect(history.getByRole('cell', { name: value, exact: true })).toBeVisible()
})
test('fund detail NAV modes and enabled read-only pages render without runtime exceptions', async ({ page }) => {
  const errors = []
  page.on('pageerror', error => errors.push(error.message))
  await mockApi(page)
  await page.goto('/fund/1')
  await expect(page.getByRole('heading', { name: funds[0].product_name, exact: true })).toBeVisible()
  await page.getByRole('button', { name: '复权净值', exact: true }).click()
  await expect(page.locator('canvas').first()).toBeVisible()
  await page.getByRole('button', { name: '累计净值', exact: true }).click()
  await expect(page.getByRole('button', { name: '+ 手动录入', exact: true })).toHaveCount(0)
  for (const path of ['/fund/1/nav', '/market', '/basis', '/portfolios']) {
    await page.goto(path)
    await expect(page.locator('h1').first()).toBeVisible()
  }
  expect(errors).toEqual([])
})
test('authenticated NAV edit validates and submits protected update', async ({ page }) => {
  await mockApi(page, true)
  await page.goto('/fund/1/nav')
  await page.getByRole('button', { name: '编辑', exact: true }).first().click()
  const dialog = page.getByRole('dialog')
  await expect(dialog).toBeVisible()
  await dialog.getByLabel('单位净值', { exact: true }).fill('1.2345')
  const request = page.waitForRequest(req => /\/api\/nav\/\d+$/.test(new URL(req.url()).pathname) && req.method() === 'PUT')
  await dialog.getByRole('button', { name: '保存净值', exact: true }).click()
  const update = await request
  expect(update.postDataJSON().unit_nav).toBe(1.2345)
  expect(update.headers()['x-csrf-token']).toBe('test-csrf')
  await expect(dialog).toHaveCount(0)
})
