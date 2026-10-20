const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  retries: 0,
  workers: 1,
  use: {
    baseURL: 'http://localhost:8080',
    headless: true,
    screenshot: 'only-on-failure',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { browserName: 'chromium' },
      testIgnore: /kafka-down\.spec\.js/,
    },
    {
      // Stops and restarts the broker: runs only after every other test has finished.
      name: 'kafka-down',
      use: { browserName: 'chromium' },
      testMatch: /(^|\/)kafka-down\.spec\.js/,
      dependencies: ['chromium'],
    },
    {
      // Stops and restarts the secure stack's broker (outbox mode): also after every other test.
      name: 'outbox-kafka-down',
      use: { browserName: 'chromium' },
      testMatch: /outbox-kafka-down\.spec\.js/,
      dependencies: ['chromium'],
    },
  ],
});
