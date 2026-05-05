// @ts-check
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: '.',
  testMatch: ['tracker-data-quality.spec.js'],
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  retries: 0,
  reporter: [['list']],
  use: {
    ignoreHTTPSErrors: true, // local tracker uses self-signed cert
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],

  // Serve the fixtures directory so reference-client test can load via http://.
  // Port 4321 is unlikely to clash with anything the user has running.
  webServer: {
    command: 'npx http-server fixtures -p 4321 -c-1 --cors',
    port: 4321,
    reuseExistingServer: true,
    timeout: 15_000,
  },
});
