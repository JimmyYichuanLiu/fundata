import { test, expect } from '@playwright/test'
import path from 'node:path'

class FundWorkspace {
  constructor(page) {
    this.page = page
    this.pageErrors = []
    page.on('pageerror', error => this.pageErrors.push(error.message))
  }

  async funds(source = 'all') {
    const response = await this.page.request.get(`/api/funds?source=${source}`)
    expect(response.ok()).toBe(true)
    return (await response.json()).items
  }

  async login() {
    // Opt in explicitly: never execute snapshot writes against a production API.
    expect(process.env.FUNDATA_E2E_SNAPSHOT).toBe('1')
    expect(process.env.FUNDATA_E2E_PASSWORD).toBeTruthy()
    await this.page.goto('/admin')
    await this.page.getByLabel('用户名').fill(process.env.FUNDATA_E2E_USER || 'acceptance')
    await this.page.getByLabel('密码').fill(process.env.FUNDATA_E2E_PASSWORD)
    await this.page.getByRole('button', { name: '登录工作空间' }).click()
    await expect(this.page.getByRole('heading', { name: '数据工作空间' })).toBeVisible()
  }

  async visual(name) {
    for (const width of [1440, 768, 375]) {
      await this.page.setViewportSize({ width, height: 1000 })
      for (const theme of ['light', 'dark']) {
        const dark = await this.page.locator('html').evaluate(el => el.classList.contains('dark'))
        if (dark !== (theme === 'dark')) {
          const button = width >= 1024
            ? this.page.getByRole('button', { name: /切换[浅深]色模式/ })
            : this.page.getByRole('button', { name: '切换主题' })
          await button.click()
        }
        await expect.poll(() => this.page.evaluate(() => document.documentElement.scrollWidth - innerWidth), `${name} ${width} ${theme} overflow`).toBeLessThanOrEqual(1)
        // The admin failure archive is unbounded; capture its viewport, not a giant bitmap.
        await this.page.screenshot({ path: path.resolve('../.test-artifacts/visual', `${name}-${width}-${theme}.png`), fullPage: name !== 'admin', animations: 'disabled' })
      }
    }
    expect(this.pageErrors).toEqual([])
  }
}

test('real overview search and source filtering, six visual layouts', async ({ page }) => {
  const workspace = new FundWorkspace(page)
  const emailFunds = await workspace.funds('email')
  expect(emailFunds.length).toBeGreaterThan(0)
  await page.goto('/')
  await expect(page.getByRole('heading', { name: '基金净值概览' })).toBeVisible()
  const response = page.waitForResponse(r => new URL(r.url()).pathname === '/api/funds' && new URL(r.url()).searchParams.get('source') === 'email')
  await page.getByLabel('数据来源', { exact: true }).selectOption('email')
  expect((await response).ok()).toBe(true)
  await page.getByLabel('搜索基金名称或代码').fill(emailFunds[0].product_code)
  await expect(page.getByRole('link', { name: emailFunds[0].product_name, exact: true })).toBeVisible()
  await expect(page.locator('.research-table tbody tr')).toHaveCount(1)
  await page.getByLabel('搜索基金名称或代码').fill('NO-SUCH-FUND-ACCEPTANCE')
  await expect(page.locator('.research-table tbody tr')).toHaveCount(0)
  await page.getByLabel('搜索基金名称或代码').fill('')
  await expect(page.locator('.research-table tbody tr').first()).toBeVisible()
  await workspace.visual('overview')
})

test('real fund detail three NAV modes and responsive charts', async ({ page }) => {
  const workspace = new FundWorkspace(page)
  const funds = await workspace.funds('email')
  const fund = funds.find(item => item.record_count > 50)
  await page.goto(`/fund/${fund.fund_id}`)
  await expect(page.getByRole('heading', { name: fund.product_name, exact: true })).toBeVisible()
  for (const name of ['单位净值', '累计净值', '复权净值']) {
    await page.getByRole('button', { name, exact: true }).click()
    await expect(page.locator('canvas').first()).toBeVisible()
    await expect(page.getByRole('heading', { name: '加载失败' })).toHaveCount(0)
  }
  await workspace.visual('detail')
})

test('real eight fund comparison preserves URL, reload, legacy redirect and charts', async ({ page }) => {
  const workspace = new FundWorkspace(page)
  await page.goto('/compare')
  await expect(page.locator('.picker-item').first()).toBeVisible()
  for (let i = 0; i < 8; i++) await page.locator('.picker-item').nth(i).click()
  await expect(page.locator('.picker-item').nth(8)).toBeDisabled()
  const ids = new URL(page.url()).searchParams.getAll('fund_ids')
  expect(ids).toHaveLength(8)
  await page.reload()
  await expect(page.locator('.picker-item[aria-pressed="true"]')).toHaveCount(8)
  // Older and newer funds may have disjoint coverage; all-time is an explicit choice.
  await page.getByRole('button', { name: '全部区间', exact: true }).click()
  await expect(page.locator('#cmp-section-chart canvas').first()).toBeVisible()
  await expect(page.getByRole('alert')).toHaveCount(0)
  await workspace.visual('compare')
  await page.goto(`/compare/v2?fund_ids=${ids[0]}&fund_ids=${ids[1]}`)
  await expect(page).toHaveURL(new RegExp(`/compare\\?fund_ids=${ids[0]}&fund_ids=${ids[1]}$`))
  await expect(page.locator('.picker-item[aria-pressed="true"]')).toHaveCount(2)
  await expect(page.locator('#cmp-section-chart canvas').first()).toBeVisible()
  expect(workspace.pageErrors).toEqual([])
})

test('snapshot admin, real export, valid portfolio save and calculation', async ({ page }) => {
  const workspace = new FundWorkspace(page)
  await workspace.login()
  await expect(page.getByRole('heading', { name: '同步历史' })).toBeVisible()
  await expect(page.getByRole('heading', { name: '附件处理异常' })).toBeVisible()
  await expect(page.getByRole('alert')).toHaveCount(0)
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.getByRole('button', { name: '导出邮件净值 Excel' }).click(),
  ])
  expect(await download.failure()).toBeNull()
  expect(download.suggestedFilename()).toMatch(/\.xlsx$/i)
  await download.saveAs(path.resolve('../.test-artifacts/visual/snapshot-email-export.xlsx'))
  await workspace.visual('admin')
  await page.setViewportSize({ width: 1440, height: 1000 })
  const funds = await workspace.funds('email')
  const pair = funds.filter(item => [1, 2].includes(item.fund_id))
  expect(pair).toHaveLength(2)
  await page.goto('/portfolios/new?' + pair.map(item => `fund_ids=${item.fund_id}`).join('&'))
  const name = `E2E acceptance ${Date.now()}`
  await page.getByLabel('组合名称').fill(name)
  const weight = page.getByLabel(pair[0].product_name + ' 权重百分比')
  const save = page.getByRole('button', { name: '保存并生成组合 →' })
  await expect(weight).toHaveValue('50')
  await expect(save).toBeEnabled()
  await weight.fill('60')
  await expect(save).toBeDisabled()
  await page.getByRole('button', { name: '重置等权' }).click()
  await expect(save).toBeEnabled()
  const created = page.waitForResponse(r => new URL(r.url()).pathname === '/api/portfolios' && r.request().method() === 'POST')
  await save.click()
  const createResponse = await created
  expect(createResponse.ok()).toBe(true)
  const portfolioId = (await createResponse.json()).id
  try {
    await expect(page).toHaveURL(new RegExp(`/portfolios/${portfolioId}$`))
    await expect(page.getByRole('heading', { name, exact: true })).toBeVisible()
    await expect(page.locator('canvas').first()).toBeVisible()
    await workspace.visual('portfolio')
    await page.goto('/portfolios')
    await expect(page.getByRole('link', { name, exact: true })).toBeVisible()
  } finally {
    const session = await (await page.request.get('/api/auth/session')).json()
    const response = await page.request.delete(`/api/portfolios/${portfolioId}`, { headers: { 'X-CSRF-Token': session.csrf_token } })
    expect(response.ok()).toBe(true)
  }
})

for (const [route, heading] of [['market', '指数与期货，掌握市场脉搏'], ['basis', '观察期现结构与期限差异']]) {
  test(`real ${route} meaningful data state and responsive layout`, async ({ page }) => {
    const workspace = new FundWorkspace(page)
    await page.goto('/' + route)
    await expect(page.getByRole('heading', { name: heading })).toBeVisible()
    await workspace.visual(route)
  })
}
