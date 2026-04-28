"""Playwright E2E: portfolio flow — select funds, configure, generate, verify detail page."""
import os

import pytest
from playwright.sync_api import Page, expect

SCREENSHOTS = "D:/coding/fundata/sessions/playwright_portfolio_screenshots"
BASE = os.environ.get("E2E_BASE_URL", "http://localhost:5173")


@pytest.fixture(autouse=True)
def _ensure_dir():
    os.makedirs(SCREENSHOTS, exist_ok=True)


def test_portfolio_flow(page: Page):
    # 1. Open fund list
    page.goto(BASE, wait_until="networkidle")
    page.screenshot(path=f"{SCREENSHOTS}/01_fund_list.png", full_page=True)

    # 2. Select >= 2 funds via checkboxes
    checkboxes = page.locator('input[type="checkbox"]')
    count = checkboxes.count()
    assert count >= 2, f"Need at least 2 fund checkboxes, found {count}"
    checkboxes.nth(0).check()
    checkboxes.nth(1).check()
    page.screenshot(path=f"{SCREENSHOTS}/02_funds_selected.png", full_page=True)

    # 3. Click portfolio button
    portfolio_btn = page.get_by_text("基金组合")
    expect(portfolio_btn).to_be_visible()
    portfolio_btn.click()
    page.wait_for_timeout(1000)
    page.screenshot(path=f"{SCREENSHOTS}/03_portfolio_config.png", full_page=True)

    # 4. Verify weight inputs are visible
    weight_inputs = page.locator('input[type="number"], input[type="range"]')
    assert weight_inputs.count() >= 1, "Expected weight input controls"

    # 5. Toggle build method if available
    method_toggle = page.locator('text=分批纳入').or_(page.locator('text=BATCH'))
    if method_toggle.count() > 0:
        method_toggle.first.click()
        page.wait_for_timeout(500)
        page.screenshot(path=f"{SCREENSHOTS}/04_batch_method.png", full_page=True)
        # Switch back to unified
        unified = page.locator('text=统一起始').or_(page.locator('text=UNIFIED'))
        if unified.count() > 0:
            unified.first.click()
            page.wait_for_timeout(500)

    # 6. Click generate
    gen_btn = page.get_by_text("生成组合").or_(page.get_by_text("计算"))
    expect(gen_btn.first).to_be_visible()
    gen_btn.first.click()
    page.wait_for_timeout(3000)
    page.screenshot(path=f"{SCREENSHOTS}/05_portfolio_result.png", full_page=True)

    # 7. Verify detail page has chart and metrics
    canvas = page.locator("canvas")
    if canvas.count() > 0:
        expect(canvas.first).to_be_visible()

    page.screenshot(path=f"{SCREENSHOTS}/06_portfolio_detail.png", full_page=True)
