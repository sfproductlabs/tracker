/**
 * SF Product Labs Tracker (v2) — Unified Client Telemetry, Client Bandit & Headless CMS SDK
 * https://github.com/sfproductlabs/tracker
 *
 * Self-contained proforma tracking & decision script. Zero external dependencies.
 *
 * Capabilities:
 *   1. Real-Time Telemetry: Captures page views, SPA route transitions, clicks, UTM & ad-click
 *      attribution (gclid, fbclid, msclkid), paid campaign cookies (mcid, arm_id, xid),
 *      timezone, locale, device/OS, viewport, and post-login identity (uid, oid).
 *      Streams via WebSocket (/tr/v1/ws) with automatic REST (/tr/v1/tr/) + sendBeacon fallback.
 *   2. Client-Driven Thompson Sampling Bandit (/v1/client-bandit/{oid}/...):
 *      - Namespace-isolated sticky assignment (_sfpl_exp_<xid>) so on-site experiments never
 *        clobber upstream paid-ad attribution cookies (mcid, arm_id).
 *      - Decoupled decision vs. viewport impression tracking via IntersectionObserver (threshold 0.25)
 *        to prevent pre-fetch / unrendered slot skew.
 *      - Auto-hydrates declarative DOM slots ([data-sfpl-xid="<xid>"]) and syncs conversion/revenue
 *        events back to the Thompson Sampling posterior loop.
 *   3. Multi-Tenant Path-Agnostic Headless Bandit CMS (/v1/cms/{oid}/...):
 *      - Auto-resolves content routes under any tenant routePrefix (/articles, /blog, /resources)
 *        into [data-sfpl-cms-root] with 120ms anti-CLS timeout protection.
 *      - Supports ?preview_arm=<arm_id> for staged challenger review without skewing posteriors.
 *      - Tracks CloudFront CNAME video playback milestones and first-party CTA conversions.
 *
 * Usage:
 *   <script>
 *     window._sfpl = {
 *       oid: '95d71e68-5e66-11f0-abf1-1e1480865e16',
 *       org: 'default',                             // Sub-tenant project slug (e.g. 'climate-action')
 *       domainKey: 'sfpl_dk_...',                   // Wildcard domain key (*.yourdomain.com)
 *       trackerHost: 'tr.yourdomain.com',           // Explicit tracker CNAME host for hhash filtering
 *       trackerUrl: 'https://tr.yourdomain.com',
 *       wsUrl: 'wss://tr.yourdomain.com/tr/v1/ws',
 *       apiUrl: 'https://api.yourdomain.com',       // Enables Client Bandit & Headless CMS
 *       routePrefix: '/articles',                   // Optional CMS route prefix
 *       cookieDomain: '.yourdomain.com'             // Optional apex cookie domain override
 *     };
 *   </script>
 *   <script src="track.js"></script>
 *   <script>
 *     // Fire telemetry + bandit reward event:
 *     track({ ename: 'signup', etyp: 'conversion', score: 1 });
 *
 *     // Request an on-site Thompson Sampling decision imperatively:
 *     track.decide('hero_value_prop_v1').then(d => console.log(d.selected_arm_id, d.payload));
 *
 *     // Resolve a Headless Bandit CMS article imperatively:
 *     track.resolveCms({ path: location.pathname });
 *   </script>
 */
(function (window, document) {
  'use strict';

  const TRACKER_VERSION = 2;

  // ─── Cookie Names (override via window._sfpl.cookies) ───────────────────────
  const C = Object.assign({
    VISITOR:     'vid',
    SESSION:     'sid',
    USER:        'uid',
    ORG:         'oid',
    ORG_SLUG:    'org',
    AUTH:        'jwt',
    REFERRAL:    'ref',
    LAST_ACTIVE: 'la',
    EXPERIMENT:  'xid',
    VER:         'ver',
    ARM_ID:      'arm_id',
    MCID:        'mcid',
    AFF:         'aff',
    SOURCE:      'source',
    MEDIUM:      'medium',
    CAMPAIGN:    'campaign',
    CONTENT:     'content',
    TERM:        'term',
    DEVICE:      'device',
    OS:          'os',
    QPS:         'qps',
  }, window._sfpl?.cookies || {});

  // ─── Config ─────────────────────────────────────────────────────────────────
  const cfg = window._sfpl || {};
  const TRACKER_URL = (cfg.trackerUrl || '').replace(/\/+$/, '');
  const WS_URL = cfg.wsUrl || (TRACKER_URL ? TRACKER_URL.replace(/^http/, 'ws') + '/tr/v1/ws' : '');
  const API_URL = (cfg.apiUrl || '').replace(/\/+$/, '');
  const API_V1 = API_URL ? (API_URL.endsWith('/v1') ? API_URL : API_URL + '/v1') : '';
  const ROUTE_PREFIX = '/' + (cfg.routePrefix || '/articles').replace(/^\/+|\/+$/g, '');
  const SESSION_TIMEOUT = Number(cfg.sessionTimeout) || 1800000; // 30 min
  const COOKIE_EXPIRY = Number(cfg.cookieExpiryDays) || 30;      // days for attribution cookies
  const VISITOR_EXPIRY = 99999;
  const CMS_TIMEOUT_MS = Number(cfg.cmsTimeoutMs) || 1800;       // network abort guard for CMS resolve
  const DOMAIN_KEY = String(cfg.domainKey || cfg.dkey || '').trim();

  function extractHostFromUrl(rawUrl) {
    if (!rawUrl) return '';
    try {
      const m = String(rawUrl).trim().match(/^(?:https?|wss?):\/\/([^/?#:]+)/i);
      return m && m[1] ? m[1].toLowerCase() : '';
    } catch {
      return '';
    }
  }

  // Active on-site bandit / CMS decisions on the current page: xid -> { arm_id, decision_id, preview_mode }
  const activePageExperiments = {};
  const observedImpressions = new Set();

  // ─── Cookie & Storage Helpers ───────────────────────────────────────────────
  function getHostRoot() {
    if (cfg.cookieDomain) return cfg.cookieDomain;
    try {
      const host = (location.hostname || location.host.split(':')[0] || '').trim();
      if (!host || host === 'localhost' || /^(\d{1,3}\.){3}\d{1,3}$/.test(host) || host.includes(':')) {
        return ''; // Do not set domain attribute on localhost or raw IP addresses
      }
      const parts = host.split('.').reverse();
      return parts.length > 1 ? parts[1] + '.' + parts[0] : parts[0];
    } catch {
      return '';
    }
  }

  const ROOT_DOMAIN = String(cfg.rootDomain || getHostRoot().replace(/^\./, '') || 'actgreen.org').toLowerCase();
  const TRACKER_HOST = String(
    cfg.trackerHost ||
    cfg.thost ||
    extractHostFromUrl(TRACKER_URL || WS_URL) ||
    ('tr.' + ROOT_DOMAIN)
  ).toLowerCase();

  function getCookie(name) {
    if (!name) return null;
    try {
      const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const v = document.cookie.match('(^|;)\\s*' + escaped + '\\s*=\\s*([^;]+)');
      return v ? decodeURIComponent(v.pop()) : null;
    } catch {
      return null;
    }
  }

  function setCookie(name, value, days) {
    if (!name || value == null || value === '') return;
    try {
      const domain = getHostRoot();
      const domainAttr = domain ? ';domain=' + domain : '';
      const secure = location.protocol === 'https:' ? ';Secure' : '';
      const expires = days ? ';expires=' + new Date(Date.now() + days * 864e5).toUTCString() : '';
      document.cookie =
        name + '=' + encodeURIComponent(value) + ';path=/' + domainAttr + ';SameSite=Lax' + secure + expires;
    } catch {}
  }

  function getStickyExpArm(xid) {
    if (!xid) return null;
    const key = '_sfpl_exp_' + xid;
    const fromCookie = getCookie(key);
    if (fromCookie) return fromCookie;
    try { return localStorage.getItem(key); } catch { return null; }
  }

  function setStickyExpArm(xid, armId, decisionId) {
    if (!xid || !armId) return;
    const expKey = '_sfpl_exp_' + xid;
    const decKey = '_sfpl_dec_' + xid;
    setCookie(expKey, armId, COOKIE_EXPIRY);
    if (decisionId) setCookie(decKey, decisionId, COOKIE_EXPIRY);
    try {
      localStorage.setItem(expKey, armId);
      if (decisionId) localStorage.setItem(decKey, decisionId);
    } catch {}
  }

  function getStickyDecisionId(xid) {
    if (!xid) return '';
    const key = '_sfpl_dec_' + xid;
    return getCookie(key) || (function () { try { return localStorage.getItem(key) || ''; } catch { return ''; } })();
  }

  // ─── UUID ───────────────────────────────────────────────────────────────────
  function uuid() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      const r = (Math.random() * 16) | 0;
      return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  // ─── Device & OS Detection ──────────────────────────────────────────────────
  function detectDevice() {
    try {
      const ua = navigator.userAgent || '';
      if (/iPad/i.test(ua) || (/Android/i.test(ua) && !/Mobile/i.test(ua))) return 'tablet';
      if (/iPhone|iPod|Android.*Mobile|BlackBerry|IEMobile|Windows Phone/i.test(ua)) return 'mobile';
      return 'desktop';
    } catch {
      return 'desktop';
    }
  }

  function detectOS() {
    try {
      const ua = navigator.userAgent || '';
      if (/like Mac OS X/i.test(ua)) return 'ios';
      if (/Android/i.test(ua)) return 'android';
      if (/Windows/i.test(ua)) return 'windows';
      if (/OS X|macOS/i.test(ua)) return 'macos';
      if (/CrOS/i.test(ua)) return 'chromeos';
      if (/Linux/i.test(ua)) return 'linux';
      return 'unknown';
    } catch {
      return 'unknown';
    }
  }

  function getOrSetVisitorId() {
    let vid = getCookie(C.VISITOR);
    if (!vid) {
      try { vid = localStorage.getItem('sfpl_vid') || ''; } catch {}
    }
    if (!vid) {
      vid = uuid();
    }
    setCookie(C.VISITOR, vid, VISITOR_EXPIRY);
    try { localStorage.setItem('sfpl_vid', vid); } catch {}
    return vid;
  }

  function getEffectiveOid() {
    return cfg.oid || getCookie(C.ORG) || '';
  }

  function getEffectiveOrg(overrideOrg) {
    return (
      overrideOrg ||
      (typeof window !== 'undefined' && window._sfpl && window._sfpl.org) ||
      getCookie(C.ORG_SLUG) ||
      getCookie('selected_org') ||
      cfg.org ||
      ''
    );
  }

  function buildTenantHeaders(extraHeaders, overrideThost, overrideDkey) {
    const headers = Object.assign({}, extraHeaders || {});
    const thost = overrideThost || TRACKER_HOST;
    const shost = (window.location && window.location.hostname) || '';
    const dkey = overrideDkey || DOMAIN_KEY;
    if (thost) headers['X-SFPL-Tracker-Host'] = thost;
    if (shost) headers['X-SFPL-Site-Host'] = shost;
    if (dkey) headers['X-SFPL-Domain-Key'] = dkey;
    return headers;
  }

  // ─── WebSocket ──────────────────────────────────────────────────────────────
  let socket;
  function setupWebSocket() {
    if (!WS_URL) return;
    try {
      socket = new WebSocket(WS_URL);
      socket.addEventListener('close', function () {
        setTimeout(setupWebSocket, 2000);
      });
    } catch {}
  }
  setupWebSocket();

  // ─── Send to Go Tracker (WS + REST Fallback) ────────────────────────────────
  function send(obj) {
    const payload = Object.assign({}, obj);
    if (!payload.thost && TRACKER_HOST) payload.thost = TRACKER_HOST;
    if (!payload.shost && window.location && window.location.hostname) {
      payload.shost = window.location.hostname;
    }
    if (DOMAIN_KEY && !payload.dkey) payload.dkey = DOMAIN_KEY;
    const jwt = payload.auth;
    delete payload.auth;
    delete payload.accesstoken;
    delete payload.accessToken;
    delete payload.refreshtoken;
    delete payload.refreshToken;

    if (socket && socket.readyState === WebSocket.OPEN) {
      try {
        socket.send(JSON.stringify(payload));
        return;
      } catch {}
    }
    if (TRACKER_URL) {
      try {
        const headers = buildTenantHeaders({ 'Content-Type': 'application/json' }, payload.thost, payload.dkey);
        if (jwt) headers['Authorization'] = 'Bearer ' + jwt;
        window.fetch(TRACKER_URL + '/tr/v1/tr/', {
          method: 'POST',
          body: JSON.stringify(payload),
          headers: headers,
          keepalive: true,
        }).catch(function () {});
      } catch {}
    }
  }

  // ─── Sync Bandit Reward Events to /v1/client-bandit/{oid}/event ─────────────
  const BANDIT_REWARD_EVENTS = {
    impression: 'view',
    viewed: 'view',
    click: 'engagement',
    cta_click: 'engagement',
    signup: 'conversion',
    lead: 'conversion',
    subscribe: 'conversion',
    conversion: 'conversion',
    purchase: 'purchase',
    checkout: 'purchase',
  };

  function syncBanditEventIfApplicable(json) {
    if (!API_V1 || cfg.banditSync === false) return;
    const oid = json.oid || getEffectiveOid();
    if (!oid) return;

    const enameLower = String(json.ename || '').toLowerCase();
    const etypLower = String(json.etyp || '').toLowerCase();
    const isBanditEvent =
      json.sync_bandit === true ||
      enameLower in BANDIT_REWARD_EVENTS ||
      etypLower === 'conversion' ||
      etypLower === 'purchase' ||
      Number(json.revenue) > 0 ||
      Number(json.score) > 0;

    if (!isBanditEvent) return;

    // Determine target xids: explicit json.client_xid, or all active on-site experiments on this page
    const targetEntries = [];
    if (json.client_xid) {
      const xid = json.client_xid;
      const pageExp = activePageExperiments[xid] || {};
      targetEntries.push({
        xid: xid,
        arm_id: json.client_arm_id || pageExp.arm_id || getStickyExpArm(xid),
        decision_id: json.decision_id || pageExp.decision_id || getStickyDecisionId(xid),
        preview_mode: Boolean(pageExp.preview_mode),
      });
    } else {
      Object.keys(activePageExperiments).forEach(function (xid) {
        const pageExp = activePageExperiments[xid];
        if (pageExp && pageExp.arm_id && !pageExp.preview_mode) {
          targetEntries.push({
            xid: xid,
            arm_id: pageExp.arm_id,
            decision_id: pageExp.decision_id || getStickyDecisionId(xid),
            preview_mode: false,
          });
        }
      });
    }

    targetEntries.forEach(function (entry) {
      if (!entry.xid || !entry.arm_id || entry.preview_mode) return;
      const effectiveThost = json.thost || TRACKER_HOST;
      const effectiveShost = json.shost || (window.location && window.location.hostname) || '';
      const effectiveDkey = json.dkey || DOMAIN_KEY || undefined;
      const bodyStr = JSON.stringify({
        xid: entry.xid,
        arm_id: entry.arm_id,
        decision_id: entry.decision_id || '',
        vid: json.vid || getOrSetVisitorId(),
        uid: json.uid || getCookie(C.USER) || '',
        org: json.org || getEffectiveOrg(),
        dkey: effectiveDkey,
        thost: effectiveThost,
        shost: effectiveShost,
        ename: enameLower || 'conversion',
        etyp: etypLower || BANDIT_REWARD_EVENTS[enameLower] || 'conversion',
        score: Number(json.score) || 0,
        revenue: Number(json.revenue) || 0,
        url: json.url || window.location.href,
        context: {
          device: json.device || detectDevice(),
          os: json.os || detectOS(),
          tz: json.tz || '',
          culture: json.culture || '',
          source: json.source || '',
          medium: json.medium || '',
          campaign: json.campaign || '',
          mcid: json.mcid || '',
          acquisition_arm_id: json.arm_id || '',
          thost: effectiveThost,
          shost: effectiveShost,
        },
      });

      const endpoint = API_V1 + '/client-bandit/' + encodeURIComponent(oid) + '/event';
      try {
        if (
          (etypLower === 'conversion' || etypLower === 'purchase' || enameLower === 'click') &&
          typeof navigator !== 'undefined' &&
          typeof navigator.sendBeacon === 'function'
        ) {
          navigator.sendBeacon(endpoint, new Blob([bodyStr], { type: 'application/json' }));
          return;
        }
      } catch {}

      try {
        window.fetch(endpoint, {
          method: 'POST',
          headers: buildTenantHeaders({ 'Content-Type': 'application/json' }, effectiveThost, effectiveDkey),
          body: bodyStr,
          keepalive: true,
        }).catch(function () {});
      } catch {}
    });
  }

  // ─── State ──────────────────────────────────────────────────────────────────
  let lastUrl = window.location.href;

  // ─── Main Track Function ────────────────────────────────────────────────────
  function track(params) {
    params = params || {};
    const json = Object.assign({}, params);

    // 1. Parse URL query params FIRST so they populate cookies before cookie reads below.
    //    Ensures xid, arm_id, mcid, utm_*, gclid, fbclid, msclkid are captured on first load.
    try {
      const rawSearch =
        (typeof sessionStorage !== 'undefined' && sessionStorage.getItem('_sfpl_qs')) ||
        window.location.search.substring(1) ||
        getCookie(C.QPS) ||
        '';
      if (rawSearch) {
        const search = rawSearch.replace(/^\?/, '');
        const qps = {};
        search.split('&').forEach(function (pair) {
          const idx = pair.indexOf('=');
          if (idx > 0) {
            const rawKey = decodeURIComponent(pair.slice(0, idx));
            const rawVal = decodeURIComponent(pair.slice(idx + 1));
            const k = rawKey.replace(/^utm_/i, '');
            qps[k] = rawVal;
          }
        });
        delete qps.ptyp;
        delete qps.token;
        delete qps.accessToken;
        delete qps.refreshToken;
        if (qps.type) {
          qps.qtype = qps.type;
          delete qps.type;
        }
        Object.keys(qps).forEach(function (k) {
          if (!(k in json)) json[k] = qps[k];
        });

        if (
          qps.xid || qps.ver != null || qps.arm_id || qps.mcid || qps.aff ||
          qps.source || qps.medium || qps.campaign || qps.content || qps.term ||
          qps.gclid || qps.fbclid || qps.msclkid
        ) {
          setCookie(C.QPS, search, COOKIE_EXPIRY);
          if (qps.xid) setCookie(C.EXPERIMENT, qps.xid, COOKIE_EXPIRY);
          if (qps.ver != null) setCookie(C.VER, qps.ver, COOKIE_EXPIRY);
          if (qps.arm_id) setCookie(C.ARM_ID, qps.arm_id, COOKIE_EXPIRY);
          if (qps.mcid) setCookie(C.MCID, qps.mcid, COOKIE_EXPIRY);
          if (qps.aff) setCookie(C.AFF, qps.aff, COOKIE_EXPIRY);
          if (qps.source) setCookie(C.SOURCE, qps.source, COOKIE_EXPIRY);
          if (qps.medium) setCookie(C.MEDIUM, qps.medium, COOKIE_EXPIRY);
          if (qps.campaign) setCookie(C.CAMPAIGN, qps.campaign, COOKIE_EXPIRY);
          if (qps.content) setCookie(C.CONTENT, qps.content, COOKIE_EXPIRY);
          if (qps.term) setCookie(C.TERM, qps.term, COOKIE_EXPIRY);
          if (qps.gclid) {
            json.gaid = qps.gclid;
            setCookie('_ga_gclid', qps.gclid, COOKIE_EXPIRY);
          }
          if (qps.fbclid) {
            json.fbid = qps.fbclid;
            setCookie('_fbc', 'fb.1.' + Date.now() + '.' + qps.fbclid, COOKIE_EXPIRY);
          }
          if (qps.msclkid) {
            json.msid = qps.msclkid;
            setCookie('_uetmsclkid', qps.msclkid, COOKIE_EXPIRY);
          }
        }
      }
    } catch {}

    // 2. History
    json.last = params.last || getCookie(C.REFERRAL) || document.referrer;
    json.url = params.url || window.location.href;
    setCookie(C.REFERRAL, json.url, 1);

    // 3. Campaign & Paid-Ad Attribution (from cookies)
    const xid = getCookie(C.EXPERIMENT);
    if (xid && !json.xid) json.xid = xid;
    if (xid && !json.ename) json.ename = xid;
    const ver = getCookie(C.VER);
    if (ver != null && json.ver === undefined) json.ver = ver;
    const armId = getCookie(C.ARM_ID);
    if (armId && !json.arm_id) json.arm_id = armId;
    const mcid = getCookie(C.MCID);
    if (mcid && !json.mcid) json.mcid = mcid;
    const aff = getCookie(C.AFF);
    if (aff && !json.aff) json.aff = aff;

    // 4. UTM Attribution (from persisted cookies)
    const src = getCookie(C.SOURCE);
    if (src && !json.source) json.source = src;
    const med = getCookie(C.MEDIUM);
    if (med && !json.medium) json.medium = med;
    const camp = getCookie(C.CAMPAIGN);
    if (camp && !json.campaign) json.campaign = camp;
    const cont = getCookie(C.CONTENT);
    if (cont && !json.content) json.content = cont;
    const trm = getCookie(C.TERM);
    if (trm && !json.term) json.term = trm;

    // 5. Ad Network Click IDs (GA, Meta, Microsoft)
    const gclid = getCookie('_ga_gclid');
    if (gclid && !json.gaid) json.gaid = gclid;
    const ga = getCookie('_ga');
    if (ga && !json.gaid) json.gaid = ga;
    const fbc = getCookie('_fbc');
    if (fbc && !json.fbid) json.fbid = fbc;
    const fbp = getCookie('_fbp');
    if (fbp && !json.fbid) json.fbid = fbp;
    const ms = getCookie('_uetmsclkid') || getCookie('MUID');
    if (ms && !json.msid) json.msid = ms;

    // 6. Session & AFK
    const now = Date.now();
    const inactive = now - Number(getCookie(C.LAST_ACTIVE) || now);
    setCookie(C.LAST_ACTIVE, now, VISITOR_EXPIRY);
    const sid = getCookie(C.SESSION);
    if (inactive > 0 && inactive < SESSION_TIMEOUT && sid) {
      json.sid = sid;
    } else {
      json.sid = uuid();
      setCookie(C.SESSION, json.sid, 1);
    }
    json.afk = Math.max(0, inactive);

    // 7. User & Tenant Organization (cookie takes precedence over window._sfpl.oid / org)
    const uid = getCookie(C.USER) || cfg.uid;
    if (uid && !json.uid) json.uid = uid;
    const oid = getCookie(C.ORG) || cfg.oid;
    if (oid && !json.oid) json.oid = oid;
    const orgSlug = getEffectiveOrg(json.org);
    if (orgSlug && !json.org) json.org = orgSlug;

    // 7b. Explicit Tracker Host (e.g. tr.actgreen.org), Site Host, and Wildcard Domain Key
    if (!json.thost && TRACKER_HOST) json.thost = TRACKER_HOST;
    if (!json.shost && window.location && window.location.hostname) {
      json.shost = window.location.hostname;
    }
    if (DOMAIN_KEY && !json.dkey) json.dkey = DOMAIN_KEY;

    // 8. Timezone (IANA) & Culture (locale) — sent on every frame
    try { json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; } catch { json.tz = 'UTC'; }
    try { json.culture = navigator.language || 'en-US'; } catch { json.culture = 'en-US'; }

    // 9. Device, OS & Viewport
    json.device = detectDevice();
    json.os = detectOS();
    setCookie(C.DEVICE, json.device, COOKIE_EXPIRY);
    setCookie(C.OS, json.os, COOKIE_EXPIRY);
    json.w = window.innerWidth || document.documentElement?.clientWidth || 1024;
    json.h = window.innerHeight || document.documentElement?.clientHeight || 768;

    // 10. Visitor ID
    json.vid = getCookie(C.VISITOR);
    if (!json.vid) {
      json.vid = json.sid;
      setCookie(C.VISITOR, json.vid, VISITOR_EXPIRY);
    }

    // 11. Auth & Release Metadata
    const jwt = getCookie(C.AUTH);
    if (jwt) json.auth = jwt;
    json.tv = TRACKER_VERSION;
    if (cfg.app && !json.app) json.app = cfg.app;
    if (cfg.rel && !json.rel) json.rel = String(cfg.rel);

    // 12. Attach active on-site Client Bandit / Headless CMS context into params without clobbering top-level xid
    if (Object.keys(activePageExperiments).length > 0) {
      json.params = Object.assign({}, json.params || {}, {
        client_experiments: activePageExperiments,
      });
    }

    // Shorten
    if (json.direction) {
      json.dir = json.direction;
      delete json.direction;
    }

    // Send to Go Tracker
    if (json.first) send(Object.assign({}, json, { ename: 'visited', etyp: 'session' }));
    send(json);

    // Mirror conversion / engagement / impression rewards to Client Bandit API if active
    syncBanditEventIfApplicable(json);

    return json;
  }

  // ─── Client-Driven Bandit Decision Helper (track.decide) ────────────────────
  /**
   * Request a sticky Thompson Sampling decision for an on-site experiment `xid`
   * and optionally hydrate a target DOM slot + observe viewport impression.
   *
   * @param {string} xid - Experiment ID (e.g. 'hero_value_prop_v1')
   * @param {object} [options]
   * @param {string} [options.oid] - Override tenant OID
   * @param {string} [options.targetSelector] - DOM selector to hydrate with payload fields
   * @param {Array}  [options.candidateArms] - Optional inline candidate arms to auto-register
   * @param {boolean} [options.sticky=true] - Enforce sticky visitor assignment
   * @param {boolean} [options.forceResample=false] - Force a new Thompson draw
   * @param {boolean} [options.recordImpression=false] - Record impression immediately instead of waiting for viewport
   * @param {object} [options.context] - Custom context attributes
   */
  async function decide(xid, options) {
    options = options || {};
    const oid = options.oid || getEffectiveOid();
    if (!API_V1 || !oid || !xid) return null;

    const vid = getOrSetVisitorId();
    const effectiveThost = options.thost || TRACKER_HOST;
    const effectiveShost = (window.location && window.location.hostname) || '';
    const effectiveDkey = options.dkey || DOMAIN_KEY || undefined;
    const body = {
      xid: xid,
      vid: vid,
      uid: getCookie(C.USER) || cfg.uid || '',
      org: getEffectiveOrg(options.org),
      dkey: effectiveDkey,
      thost: effectiveThost,
      shost: effectiveShost,
      sticky: options.sticky !== false,
      force_resample: Boolean(options.forceResample),
      record_impression: Boolean(options.recordImpression),
      candidate_arms: options.candidateArms || undefined,
      context: Object.assign(
        {
          device: detectDevice(),
          os: detectOS(),
          tz: (function () { try { return Intl.DateTimeFormat().resolvedOptions().timeZone; } catch { return ''; } })(),
          culture: navigator.language || '',
          url: window.location.href,
          path: window.location.pathname,
          source: getCookie(C.SOURCE) || '',
          medium: getCookie(C.MEDIUM) || '',
          campaign: getCookie(C.CAMPAIGN) || '',
          mcid: getCookie(C.MCID) || '',
          thost: effectiveThost,
          shost: effectiveShost,
        },
        options.context || {}
      ),
    };

    try {
      const res = await window.fetch(
        API_V1 + '/client-bandit/' + encodeURIComponent(oid) + '/decide',
        {
          method: 'POST',
          headers: buildTenantHeaders({ 'Content-Type': 'application/json' }, effectiveThost, effectiveDkey),
          body: JSON.stringify(body),
        }
      );
      if (!res.ok) return null;
      const decision = await res.json();
      if (!decision || !decision.selected_arm_id) return decision;

      // Persist namespace-isolated sticky cookie (_sfpl_exp_<xid>) so paid-ad cookies stay untouched
      setStickyExpArm(xid, decision.selected_arm_id, decision.decision_id);
      activePageExperiments[xid] = {
        arm_id: decision.selected_arm_id,
        decision_id: decision.decision_id || '',
        preview_mode: false,
      };

      // Hydrate DOM slot if present
      const selector = options.targetSelector || '[data-sfpl-xid="' + xid + '"]';
      const slotEl = document.querySelector(selector);
      if (slotEl && decision.payload) {
        hydrateBanditSlot(slotEl, xid, decision);
      }

      return decision;
    } catch {
      return null;
    }
  }

  function hydrateBanditSlot(slotEl, xid, decision) {
    const payload = decision.payload || {};
    slotEl.setAttribute('data-sfpl-arm', decision.selected_arm_id);

    // Populate [data-sfpl-field="..."] children
    slotEl.querySelectorAll('[data-sfpl-field]').forEach(function (el) {
      const field = el.getAttribute('data-sfpl-field');
      if (!field || !(field in payload)) return;
      const val = payload[field];
      const tag = (el.tagName || '').toLowerCase();
      if (tag === 'img' && typeof val === 'string') {
        el.setAttribute('src', val);
      } else if (tag === 'video' && typeof val === 'string') {
        el.setAttribute('src', val);
      } else if (tag === 'a' && field === 'cta_url' && typeof val === 'string') {
        el.setAttribute('href', val);
      } else if (val != null) {
        el.textContent = String(val);
      }
    });

    // Observe real viewport visibility before recording impression
    observeViewportImpression(slotEl, xid, decision.selected_arm_id, decision.decision_id, false);

    // Bind CTA click hooks inside the slot
    slotEl.querySelectorAll('[data-sfpl-cta], a, button').forEach(function (btn) {
      if (btn.dataset.sfplBound === '1') return;
      btn.dataset.sfplBound = '1';
      btn.addEventListener('click', function () {
        track({
          ename: 'click',
          etyp: 'engagement',
          client_xid: xid,
          client_arm_id: decision.selected_arm_id,
          decision_id: decision.decision_id,
          sync_bandit: true,
        });
      });
    });
  }

  function observeViewportImpression(targetEl, xid, armId, decisionId, isPreview) {
    if (!targetEl || !xid || !armId || isPreview) return;
    const dedupKey = xid + ':' + armId;
    if (observedImpressions.has(dedupKey)) return;

    const fireImpression = function () {
      if (observedImpressions.has(dedupKey)) return;
      observedImpressions.add(dedupKey);
      track({
        ename: 'impression',
        etyp: 'view',
        client_xid: xid,
        client_arm_id: armId,
        decision_id: decisionId || '',
        sync_bandit: true,
      });
    };

    if (typeof IntersectionObserver === 'function') {
      const obs = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (entry.isIntersecting) {
              obs.disconnect();
              fireImpression();
            }
          });
        },
        { threshold: 0.25 }
      );
      obs.observe(targetEl);
    } else {
      fireImpression();
    }
  }

  // ─── Multi-Tenant Headless Bandit CMS Resolver (track.resolveCms) ───────────
  /**
   * Resolve and mount a multi-variant Headless Bandit CMS item from
   * GET /v1/cms/{oid}/resolve into [data-sfpl-cms-root].
   */
  async function resolveCms(options) {
    options = options || {};
    const mountEl =
      options.mountEl ||
      document.querySelector(options.selector || '[data-sfpl-cms-root]');
    if (!mountEl) return null;

    const oid = options.oid || mountEl.getAttribute('data-sfpl-oid') || getEffectiveOid();
    if (!API_V1 || !oid) return null;

    const pathname =
      options.path ||
      mountEl.getAttribute('data-sfpl-path') ||
      window.location.pathname ||
      ROUTE_PREFIX;

    const urlParams = new URLSearchParams(window.location.search);
    const previewArm = options.previewArm || urlParams.get('preview_arm') || '';
    const vid = getOrSetVisitorId();

    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    const timer = controller
      ? setTimeout(function () { controller.abort(); }, CMS_TIMEOUT_MS)
      : null;

    try {
      const effectiveOrg = getEffectiveOrg(options.org || mountEl.getAttribute('data-sfpl-org'));
      const effectiveThost = options.thost || TRACKER_HOST;
      const effectiveShost = (window.location && window.location.hostname) || '';
      const effectiveDkey = options.dkey || DOMAIN_KEY || '';
      let reqUrl =
        API_V1 +
        '/cms/' +
        encodeURIComponent(oid) +
        '/resolve?path=' +
        encodeURIComponent(pathname) +
        '&vid=' +
        encodeURIComponent(vid);
      if (effectiveOrg) {
        reqUrl += '&org=' + encodeURIComponent(effectiveOrg);
      }
      if (effectiveThost) {
        reqUrl += '&thost=' + encodeURIComponent(effectiveThost);
      }
      if (effectiveShost) {
        reqUrl += '&shost=' + encodeURIComponent(effectiveShost);
      }
      if (effectiveDkey) {
        reqUrl += '&dkey=' + encodeURIComponent(effectiveDkey);
      }
      if (previewArm) {
        reqUrl += '&preview_arm=' + encodeURIComponent(previewArm);
      }

      const res = await window.fetch(reqUrl, {
        headers: buildTenantHeaders({}, effectiveThost, effectiveDkey),
        signal: controller ? controller.signal : undefined,
        credentials: 'omit',
      });
      if (timer) clearTimeout(timer);
      if (!res.ok) return null;

      const data = await res.json();
      if (!data || !data.arm) return null;

      const xid = data.xid;
      const armId = data.selected_arm_id;
      const decisionId = (data.decision && data.decision.decision_id) || '';
      const isPreview = Boolean(data.decision && data.decision.preview_mode);

      if (!isPreview) {
        setStickyExpArm(xid, armId, decisionId);
      }
      activePageExperiments[xid] = {
        arm_id: armId,
        decision_id: decisionId,
        preview_mode: isPreview,
        cms_path: data.content_path,
      };

      // Update SEO & OpenGraph metadata dynamically
      if (data.arm.headline) {
        const siteName = (data.config && data.config.site_name) || '';
        document.title = siteName ? data.arm.headline + ' | ' + siteName : data.arm.headline;
      }
      if (data.canonical_url) {
        let linkCanonical = document.querySelector('link[rel="canonical"]');
        if (!linkCanonical) {
          linkCanonical = document.createElement('link');
          linkCanonical.setAttribute('rel', 'canonical');
          document.head.appendChild(linkCanonical);
        }
        linkCanonical.setAttribute('href', data.canonical_url);
      }

      // Render compiled HTML fragment (with CloudFront CNAME media & rewritten internal links)
      if (data.arm.html) {
        mountEl.innerHTML = data.arm.html;
      }

      // Observe real viewport exposure before recording impression
      if (!isPreview && !(data.decision && data.decision.is_bot)) {
        observeViewportImpression(mountEl, xid, armId, decisionId, isPreview);
      }

      // Bind tracked CTA conversion clicks
      mountEl.querySelectorAll('[data-sfpl-cms-cta]').forEach(function (ctaBtn) {
        ctaBtn.addEventListener('click', function () {
          if (isPreview) return;
          track({
            ename: 'conversion',
            etyp: 'conversion',
            score: 1,
            client_xid: xid,
            client_arm_id: armId,
            decision_id: decisionId,
            sync_bandit: true,
          });
        });
      });

      // Bind CloudFront video engagement telemetry
      mountEl.querySelectorAll('video.x-cms-video, video[data-sfpl-video-arm]').forEach(function (videoEl) {
        let playedSent = false;
        let completedSent = false;
        videoEl.addEventListener('play', function () {
          if (playedSent || isPreview) return;
          playedSent = true;
          track({
            ename: 'video_play',
            etyp: 'engagement',
            client_xid: xid,
            client_arm_id: armId,
            decision_id: decisionId,
            sync_bandit: true,
          });
        });
        videoEl.addEventListener('ended', function () {
          if (completedSent || isPreview) return;
          completedSent = true;
          track({
            ename: 'video_complete',
            etyp: 'engagement',
            score: 0.5,
            client_xid: xid,
            client_arm_id: armId,
            decision_id: decisionId,
            sync_bandit: true,
          });
        });
      });

      return data;
    } catch {
      if (timer) clearTimeout(timer);
      return null;
    }
  }

  // Auto-hydrate any declarative [data-sfpl-xid] slots & [data-sfpl-cms-root] containers
  function autoHydrateBanditAndCms() {
    if (!API_V1) return;
    const cmsRoot = document.querySelector('[data-sfpl-cms-root]');
    if (cmsRoot) {
      resolveCms({ mountEl: cmsRoot });
    }
    document.querySelectorAll('[data-sfpl-xid]').forEach(function (el) {
      if (el.classList.contains('x-cms-article')) return; // Already managed by CMS resolver
      const xid = el.getAttribute('data-sfpl-xid');
      if (xid) {
        decide(xid, { targetSelector: '[data-sfpl-xid="' + xid + '"]' });
      }
    });
  }

  // Helper to set authenticated user / org identity after login or signup
  function setTrackerUser(uid, oid, jwt) {
    if (uid) setCookie(C.USER, uid, VISITOR_EXPIRY);
    if (oid) setCookie(C.ORG, oid, VISITOR_EXPIRY);
    if (jwt) setCookie(C.AUTH, jwt, COOKIE_EXPIRY);
  }

  // ─── Auto-Init ──────────────────────────────────────────────────────────────
  (function init() {
    let vid = getCookie(C.VISITOR);
    if (!vid) {
      vid = uuid();
      setCookie(C.VISITOR, vid, VISITOR_EXPIRY);
      setCookie(C.SESSION, vid, 1);
      try { track({ ename: 'visited', vid: vid, sid: vid, etyp: 'cookie' }); } catch {}
    } else {
      try { localStorage.setItem('sfpl_vid', vid); } catch {}
    }
  })();

  // ─── Click Tracking ─────────────────────────────────────────────────────────
  window.addEventListener('click', function (e) {
    const el = e.target;
    if (!el) return;
    const text = (el.textContent || '').trim().substring(0, 50).toLowerCase();
    const trackData = el.closest?.('[data-track]')?.dataset?.track;
    const alt = el.closest?.('[alt]')?.getAttribute?.('alt');
    const label = trackData || text || alt;
    if (label && el.tagName?.toLowerCase() !== 'canvas') {
      track({ ename: 'click', params: { firsttext: label } });
    }
  });

  // ─── Page Navigation & SPA Route Tracking ───────────────────────────────────
  function onRouteChange() {
    const url = window.location.href;
    if (url !== lastUrl) {
      try { track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: url }); } catch {}
      lastUrl = url;
      autoHydrateBanditAndCms();
    }
  }

  window.addEventListener('popstate', onRouteChange);

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      track({ ename: 'load', etyp: 'page', last: lastUrl, url: window.location.href });
      lastUrl = window.location.href;
      autoHydrateBanditAndCms();
    });
  } else {
    track({ ename: 'load', etyp: 'page', last: lastUrl, url: window.location.href });
    lastUrl = window.location.href;
    autoHydrateBanditAndCms();
  }

  const _pushState = history.pushState;
  const _replaceState = history.replaceState;
  history.pushState = function () {
    _pushState.apply(this, arguments);
    onRouteChange();
  };
  history.replaceState = function () {
    _replaceState.apply(this, arguments);
    onRouteChange();
  };

  // ─── Exports & Global Hooks ─────────────────────────────────────────────────
  track.decide = decide;
  track.resolveCms = resolveCms;
  track.setUser = setTrackerUser;
  track.getVisitorId = getOrSetVisitorId;
  track.getTrackerHost = function () { return TRACKER_HOST; };
  track.getDomainKey = function () { return DOMAIN_KEY; };
  track.getEffectiveOrg = getEffectiveOrg;
  track.getActiveExperiments = function () { return Object.assign({}, activePageExperiments); };

  window.track = track;
  window._track = track;
  window._sfpl = Object.assign(window._sfpl || {}, {
    track: track,
    decide: decide,
    resolveCms: resolveCms,
    setUser: setTrackerUser,
    getVisitorId: getOrSetVisitorId,
    getTrackerHost: track.getTrackerHost,
    getDomainKey: track.getDomainKey,
  });
})(window, document);
