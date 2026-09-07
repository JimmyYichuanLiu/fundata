import { defineConfig } from '@playwright/test'
import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const temporaryDirectory = fileURLToPath(new URL('../.test-artifacts/browser-temp/', import.meta.url))
mkdirSync(temporaryDirectory, { recursive: true })
process.env.TEMP = temporaryDirectory
process.env.TMP = temporaryDirectory
process.env.TMPDIR = temporaryDirectory

// The API must use a disposable snapshot; this suite never starts a backend.
export default defineConfig({
  testDir: './tests',
  testMatch: 'live-upgrade.spec.js',
  outputDir: '../.test-artifacts/live-e2e',
  timeout: 90000,
  expect: { timeout: 15000 },
  workers: 1,
  retries: 0,
  reporter: [
    ['list'],
    ['html', { outputFolder: '../.test-artifacts/live-report', open: 'never' }],
    ['junit', { outputFile: '../.test-artifacts/live-results.xml' }],
  ],
  use: {
    baseURL: 'http://127.0.0.1:5173',
    headless: true,
    channel: 'chrome',
    viewport: { width: 1440, height: 1000 },
    reducedMotion: 'reduce',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
})
