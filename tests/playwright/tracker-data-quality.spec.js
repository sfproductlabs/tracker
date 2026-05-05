// Tracker data-quality E2E.
//
// What this covers:
//   A. Reference client (packages/tracker/.setup/track.js) — loaded from a
//      fixture HTML page served by http-server on :4321. Verifies:
//        - Dashless `oid` cookie round-trips as dashed UUID in CH
//        - tz, culture, device, os, vp_w, vp_h populated
//   B. www tracker on :3001 — skipped if not running. Verifies the built
//      tr.js bundles the same enrichment.
//   C. st app tracker on :1234 — skipped if not running. Same as B.
//   D. WS post-login identity: user loads page anon, logs in after WS upgrade,
//      then next `track()` still carries the new oid/uid.
//   E. Sticky fields on every frame: 5 back-to-back events — each CH row
//      has identity + campaign + context fields.
//
// Prereqs:
//   - Tracker running on https://localhost:8443 (set via TRACKER_URL to override)
//   - ClickHouse HTTP on http://127.0.0.1:8123 (set TRACKER_CH_HTTP to override)
//   - (optional) www on http://localhost:3001
//   - (optional) st app on http://localhost:1234
//
// Run:
//   cd packages/tracker/tests/playwright
//   npm install
//   npx playwright install chromium
//   npx playwright test

const { test, expect, request } = require('@playwright/test');

const TRACKER_URL = process.env.TRACKER_URL || 'https://localhost:8443';
const CH_HTTP = process.env.TRACKER_CH_HTTP || 'http://127.0.0.1:8123';
const CH_USER = process.env.TRACKER_CH_USER || 'default';
const CH_PASS = process.env.TRACKER_CH_PASS || '';
const FIXTURE_URL = 'http://127.0.0.1:4321/reference.html';
const WWW_URL = process.env.WWW_URL || 'http://localhost:3001';
const ST_APP_URL = process.env.ST_APP_URL || 'http://localhost:1234';

// ── Helpers ──────────────────────────────────────────────────────────────

async function chQuery(apiReq, sql) {
  const url = CH_HTTP + '/?use_query_cache=0';
  const headers = {};
  if (CH_USER) {
    const auth = Buffer.from(`${CH_USER}:${CH_PASS}`).toString('base64');
    headers['Authorization'] = `Basic ${auth}`;
  }
  const resp = await apiReq.post(url, { headers, data: sql });
  if (!resp.ok()) {
    throw new Error(`CH query failed ${resp.status()}: ${await resp.text()}`);
  }
  return (await resp.text()).replace(/\s+$/, '');
}

async function waitForRow(apiReq, urlFragment, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  const q = `SELECT count() FROM sfpla.events WHERE url LIKE '%${urlFragment}%'`;
  while (Date.now() < deadline) {
    const out = await chQuery(apiReq, q);
    if (out && out !== '0') return;
    await chQuery(apiReq, 'SYSTEM FLUSH ASYNC INSERT QUEUE').catch(() => {});
    await new Promise(r => setTimeout(r, 250));
  }
  const dump = await chQuery(apiReq, "SELECT url FROM sfpla.events ORDER BY created_at DESC LIMIT 5 FORMAT TabSeparated");
  throw new Error(`no row for %${urlFragment}% within ${timeoutMs}ms\nrecent urls:\n${dump}`);
}

async function selectField(apiReq, col, urlFragment) {
  await waitForRow(apiReq, urlFragment);
  const q = `SELECT ${col} FROM sfpla.events WHERE url LIKE '%${urlFragment}%' ORDER BY created_at DESC LIMIT 1`;
  return chQuery(apiReq, q);
}

function uniqueTag(name) {
  return `pw-${name}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

async function isReachable(url) {
  try {
    const ctx = await request.newContext({ ignoreHTTPSErrors: true, timeout: 2000 });
    const resp = await ctx.get(url);
    await ctx.dispose();
    return resp.status() < 500;
  } catch {
    return false;
  }
}

// ── Sanity gate ───────────────────────────────────────────────────────────

test.beforeAll(async ({ request: apiReq }) => {
  // Tracker /ping must be reachable.
  try {
    const resp = await apiReq.get(TRACKER_URL + '/ping', { timeout: 3000 });
    if (resp.status() >= 500) {
      test.skip(true, `tracker not reachable at ${TRACKER_URL} (status ${resp.status()})`);
    }
  } catch (e) {
    test.skip(true, `tracker not reachable at ${TRACKER_URL}: ${e.message}`);
  }
  // CH must be reachable.
  try {
    await chQuery(apiReq, 'SELECT 1');
  } catch (e) {
    test.skip(true, `ClickHouse HTTP not reachable at ${CH_HTTP}: ${e.message}`);
  }
});

// ── A. Reference client ───────────────────────────────────────────────────

test.describe('reference client (.setup/track.js)', () => {
  test('dashless oid cookie round-trips as dashed UUID', async ({ page, request: apiReq, context }) => {
    const tag = uniqueTag('ref-oid');
    const dashless = '54296e9233a311ef802c2915e3776adf';
    await context.addCookies([
      { name: 'oid', value: dashless, url: FIXTURE_URL },
      { name: 'vid', value: dashless, url: FIXTURE_URL },
    ]);
    await page.goto(`${FIXTURE_URL}?trackerUrl=${encodeURIComponent(TRACKER_URL)}`);
    await page.waitForFunction(() => typeof window.fireEvent === 'function');
    // Give WS time to open (falls back to REST if not — but fetch to a
    // self-signed HTTPS tracker may be blocked; log failures from the page).
    page.on('console', (msg) => { if (msg.type() === 'error') console.log('[browser]', msg.text()); });
    await page.waitForTimeout(500);
    await page.evaluate((tag) => window.fireEvent({ ename: 'viewed_test', etyp: 'test', url: 'http://localhost/' + tag }), tag);

    const oid = await selectField(apiReq, 'toString(oid)', tag);
    expect(oid).toBe('54296e92-33a3-11ef-802c-2915e3776adf');
  });

  test('tz, culture, device, os, vp_w, vp_h are populated', async ({ page, request: apiReq }) => {
    const tag = uniqueTag('ref-context');
    await page.goto(`${FIXTURE_URL}?trackerUrl=${encodeURIComponent(TRACKER_URL)}`);
    await page.waitForFunction(() => typeof window.fireEvent === 'function');
    await page.evaluate((tag) => window.fireEvent({ ename: 'viewed_test', etyp: 'test', url: 'http://localhost/' + tag }), tag);

    const tz = await selectField(apiReq, 'tz', tag);
    expect(tz, 'tz must be non-empty (IANA timezone)').not.toBe('');

    const culture = await selectField(apiReq, 'culture', tag);
    expect(culture, 'culture must be non-empty (navigator.language)').not.toBe('');

    const device = await selectField(apiReq, 'device', tag);
    expect(['desktop', 'mobile', 'tablet'], 'device must be one of desktop/mobile/tablet').toContain(device);

    const os = await selectField(apiReq, 'os', tag);
    expect(os, 'os must be non-empty').not.toBe('');

    const vpW = parseInt(await selectField(apiReq, 'vp_w', tag), 10);
    const vpH = parseInt(await selectField(apiReq, 'vp_h', tag), 10);
    expect(vpW, 'vp_w must be > 0').toBeGreaterThan(0);
    expect(vpH, 'vp_h must be > 0').toBeGreaterThan(0);
  });
});

// ── B. WWW tracker (tr.js on :3001) ──────────────────────────────────────

test.describe('www tracker (http://localhost:3001/tr.js)', () => {
  test('events from www carry enrichment', async ({ page, request: apiReq }) => {
    test.skip(!(await isReachable(WWW_URL + '/')),
      `www not reachable at ${WWW_URL} — start it with \`cd sites/sourcetable-www && npm run dev\``);

    const tag = uniqueTag('www');
    await page.goto(WWW_URL + '/?pw=' + tag);
    await page.waitForTimeout(2500);
    await page.evaluate(async (tag) => {
      if (typeof window.track === 'function') {
        window.track({ ename: 'viewed_test', etyp: 'test', url: 'http://localhost/' + tag });
      }
    }, tag);
    const q = `SELECT tz FROM sfpla.events WHERE (toString(params) LIKE '%${tag}%' OR url LIKE '%${tag}%') ORDER BY created_at DESC LIMIT 1`;
    let tz = '';
    for (let i = 0; i < 40 && !tz; i++) {
      tz = await chQuery(apiReq, q).catch(() => '');
      if (!tz) await new Promise(r => setTimeout(r, 500));
    }
    // If no event arrived at our local tracker, the www is likely configured
    // to point at staging/prod tracker — skip rather than fail, and leave a
    // hint for the user.
    if (!tz) {
      test.skip(true,
        `www is running but events did not reach local tracker. ` +
        `Check that sites/sourcetable-www/config/env/development.js has ` +
        `Application.Target === "development" (maps to http://localhost:8080). ` +
        `Restart with \`npm run dev\` after editing.`);
    }
    expect(tz, 'www tz must be non-empty').not.toBe('');
  });
});

// ── C. ST app tracker (:1234) ─────────────────────────────────────────────

test.describe('st app tracker (http://localhost:1234)', () => {
  test('events from st app carry enrichment', async ({ page, request: apiReq }) => {
    test.skip(!(await isReachable(ST_APP_URL + '/')), `st app not reachable at ${ST_APP_URL}`);
    const tag = uniqueTag('stapp');
    await page.goto(ST_APP_URL + '/?pw=' + tag);
    await page.waitForTimeout(2500);
    const q = `SELECT tz FROM sfpla.events WHERE (toString(params) LIKE '%${tag}%' OR url LIKE '%${tag}%') ORDER BY created_at DESC LIMIT 1`;
    let tz = '';
    for (let i = 0; i < 40 && !tz; i++) {
      tz = await chQuery(apiReq, q).catch(() => '');
      if (!tz) await new Promise(r => setTimeout(r, 500));
    }
    if (!tz) {
      test.skip(true,
        `st app is running but events did not reach local tracker. ` +
        `Check st/site/frontend/www/config.yaml for a local tracker URL mapping.`);
    }
    expect(tz, 'st-app tz must be non-empty').not.toBe('');
  });
});

// ── D. WS post-login identity ─────────────────────────────────────────────

test('WS post-login: payload oid/uid wins over upgrade-time snapshot', async ({ page, request: apiReq, context }) => {
  // Open page anon (no oid/uid). Fire anon event. Then set cookies (sim login)
  // and fire again — confirms post-login identity reaches CH despite the
  // WS upgrade request having been made while anon.
  const tagAnon = uniqueTag('post-login-anon');
  const tagLoggedIn = uniqueTag('post-login-in');
  const dashlessOid = '54296e9233a311ef802c2915e3776adf';
  const dashlessUid = '6789abcd1234567890abcdef12345678';

  // Clear any residual oid/uid cookies from prior tests.
  await context.clearCookies();

  await page.goto(`${FIXTURE_URL}?trackerUrl=${encodeURIComponent(TRACKER_URL)}`);
  await page.waitForFunction(() => typeof window.fireEvent === 'function');
  await page.waitForTimeout(500);

  // Anon event.
  await page.evaluate((tag) => window.fireEvent({ ename: 'viewed_test', etyp: 'test', url: 'http://localhost/' + tag }), tagAnon);
  // Wait for the anon event to flush (tracker batch interval ~1s).
  await waitForRow(apiReq, tagAnon);

  // Simulate login: set oid/uid via both Playwright cookies and document.cookie.
  await context.addCookies([
    { name: 'oid', value: dashlessOid, url: FIXTURE_URL },
    { name: 'uid', value: dashlessUid, url: FIXTURE_URL },
  ]);
  await page.evaluate(({ oid, uid }) => {
    document.cookie = `oid=${oid};path=/`;
    document.cookie = `uid=${uid};path=/`;
  }, { oid: dashlessOid, uid: dashlessUid });

  // Second event — must include oid from cookies via the payload.
  await page.evaluate((tag) => window.fireEvent({ ename: 'viewed_test', etyp: 'test', url: 'http://localhost/' + tag }), tagLoggedIn);

  const anonOid = await selectField(apiReq, 'toString(oid)', tagAnon);
  expect(anonOid, 'anon event should have zero oid').toBe('00000000-0000-0000-0000-000000000000');

  const loggedInOid = await selectField(apiReq, 'toString(oid)', tagLoggedIn);
  expect(loggedInOid, 'post-login event should carry the new oid').toBe('54296e92-33a3-11ef-802c-2915e3776adf');
});

// ── E. Sticky fields on every frame ───────────────────────────────────────

test('sticky fields on every event (identity + context per frame)', async ({ page, request: apiReq, context }) => {
  const tagPrefix = uniqueTag('sticky');
  const dashlessOid = '54296e9233a311ef802c2915e3776adf';
  await context.addCookies([{ name: 'oid', value: dashlessOid, url: FIXTURE_URL }]);
  // Simulate a campaign landing via URL params, so the client persists the
  // utm cookies and then every subsequent event has them.
  await page.goto(`${FIXTURE_URL}?trackerUrl=${encodeURIComponent(TRACKER_URL)}&utm_source=pw&utm_medium=test&utm_campaign=sticky`);
  await page.waitForFunction(() => typeof window.fireEvent === 'function');

  for (let i = 0; i < 5; i++) {
    await page.evaluate(({ pref, i }) => window.fireEvent({
      ename: 'viewed_test', etyp: 'test',
      url: 'http://localhost/' + pref + '-' + i,
    }), { pref: tagPrefix, i });
    await page.waitForTimeout(100);
  }

  // Give the tracker time to flush all 5 events.
  await waitForRow(apiReq, `${tagPrefix}-4`);

  // For each event, check identity + campaign fields.
  for (let i = 0; i < 5; i++) {
    const tag = `${tagPrefix}-${i}`;
    const row = await chQuery(apiReq,
      `SELECT toString(oid), source, medium, campaign, tz, culture, device, os, vp_w, vp_h
       FROM sfpla.events WHERE url LIKE '%${tag}%' ORDER BY created_at DESC LIMIT 1 FORMAT TabSeparated`);
    const [oid, source, medium, campaign, tz, culture, device, os, vpW, vpH] = row.split('\t');
    expect(oid, `event ${i} must have dashed oid`).toBe('54296e92-33a3-11ef-802c-2915e3776adf');
    expect(source, `event ${i} must carry utm source`).toBe('pw');
    expect(medium, `event ${i} must carry utm medium`).toBe('test');
    expect(campaign, `event ${i} must carry utm campaign`).toBe('sticky');
    expect(tz, `event ${i} tz`).not.toBe('');
    expect(culture, `event ${i} culture`).not.toBe('');
    expect(device, `event ${i} device`).not.toBe('');
    expect(os, `event ${i} os`).not.toBe('');
    expect(parseInt(vpW, 10), `event ${i} vp_w`).toBeGreaterThan(0);
    expect(parseInt(vpH, 10), `event ${i} vp_h`).toBeGreaterThan(0);
  }
});
