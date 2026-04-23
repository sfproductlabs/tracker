/**
 * SF Product Labs Tracker — Client-Side Event Tracking
 * https://github.com/sfproductlabs/tracker
 *
 * Self-contained tracking script. No external dependencies.
 * Captures page views, clicks, campaign attribution, device/OS, and custom events.
 * Sends via WebSocket (LZ4 compressed) with REST fallback.
 *
 * Usage:
 *   <script>window._sfpl = { trackerUrl: 'https://tr.yourdomain.com', wsUrl: 'wss://tr.yourdomain.com/tr/v1/ws' };</script>
 *   <script src="track.js"></script>
 *   <script>track({ ename: 'signup', etyp: 'conversion' });</script>
 */

// ─── Cookie Names (override via window._sfpl.cookies) ───────────────────────
const C = Object.assign({
  VISITOR:    'vid',
  SESSION:    'sess',
  USER:       'uid',
  AUTH:       'jwt',
  REFERRAL:   'ref',
  LAST_ACTIVE:'la',
  EXPERIMENT: 'xid',
  VER:        'ver',
  ARM_ID:     'arm_id',
  MCID:       'mcid',
  AFF:        'aff',
  SOURCE:     'source',
  MEDIUM:     'medium',
  CAMPAIGN:   'campaign',
  CONTENT:    'content',
  TERM:       'term',
  DEVICE:     'device',
  OS:         'os',
  QPS:        'qps',
}, window._sfpl?.cookies || {});

// ─── Config ─────────────────────────────────────────────────────────────────
const cfg = window._sfpl || {};
const TRACKER_URL = cfg.trackerUrl || '';
const WS_URL = cfg.wsUrl || '';
const SESSION_TIMEOUT = cfg.sessionTimeout || 1800000; // 30 min
const COOKIE_EXPIRY = 30; // days for attribution cookies
const VISITOR_EXPIRY = 99999;

// ─── Cookie Helpers ─────────────────────────────────────────────────────────
function getHostRoot() {
  try {
    const parts = location.host.split('.').reverse();
    return ((parts.length > 1) ? parts[1] + '.' + parts[0] : parts[0]).split(':')[0];
  } catch { return ''; }
}

function getCookie(name) {
  const v = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
  return v ? decodeURIComponent(v.pop()) : null;
}

function setCookie(name, value, days) {
  if (value == null || value === '') return;
  const domain = getHostRoot();
  const secure = location.protocol === 'https:' ? ';Secure' : '';
  const expires = days ? ';expires=' + new Date(Date.now() + days * 864e5).toUTCString() : '';
  document.cookie = name + '=' + encodeURIComponent(value) + ';path=/;domain=' + domain + ';SameSite=Lax' + secure + expires;
}

// ─── UUID ───────────────────────────────────────────────────────────────────
function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

// ─── Device & OS Detection ──────────────────────────────────────────────────
function detectDevice() {
  const ua = navigator.userAgent;
  if (/iPad/i.test(ua) || (/Android/i.test(ua) && !/Mobile/i.test(ua))) return 'tablet';
  if (/iPhone|iPod|Android.*Mobile|BlackBerry|IEMobile|Windows Phone/i.test(ua)) return 'mobile';
  return 'desktop';
}

function detectOS() {
  const ua = navigator.userAgent;
  if (/like Mac OS X/i.test(ua)) return 'ios';
  if (/Android/i.test(ua)) return 'android';
  if (/Windows/i.test(ua)) return 'windows';
  if (/OS X|macOS/i.test(ua)) return 'macos';
  if (/Linux/i.test(ua)) return 'linux';
  if (/CrOS/i.test(ua)) return 'chromeos';
  return '';
}

// ─── WebSocket ──────────────────────────────────────────────────────────────
let socket;
function setupWebSocket() {
  if (!WS_URL) return;
  try {
    socket = new WebSocket(WS_URL);
    socket.addEventListener('close', () => setTimeout(setupWebSocket, 2000));
  } catch {}
}
setupWebSocket();

// ─── Send ───────────────────────────────────────────────────────────────────
function send(obj) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    try { socket.send(JSON.stringify(obj)); } catch {}
  } else if (TRACKER_URL) {
    try {
      window.fetch(TRACKER_URL + '/tr/v1/tr/', {
        method: 'POST',
        body: JSON.stringify(obj),
        headers: { 'Content-Type': 'application/json' },
      }).catch(() => {});
    } catch {}
  }
}

// ─── State ──────────────────────────────────────────────────────────────────
let lastUrl = window.location.href;
let beenFirst = false;

// ─── Main Track Function ────────────────────────────────────────────────────
function track(params) {
  params = params || {};
  const json = { ...params };

  // History
  json.last = params.last || getCookie(C.REFERRAL) || document.referrer;
  json.url = params.url || window.location.href;
  setCookie(C.REFERRAL, json.url, 1);

  // Campaign attribution (from cookies)
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

  // Session
  const now = Date.now();
  const inactive = now - Number(getCookie(C.LAST_ACTIVE) || now);
  setCookie(C.LAST_ACTIVE, now, VISITOR_EXPIRY);
  const sid = getCookie(C.SESSION);
  if (inactive > 0 && inactive < SESSION_TIMEOUT && sid) {
    json.sid = sid;
  } else {
    json.sid = uuid();
    if (!beenFirst) { beenFirst = true; json.first = 'true'; }
    setCookie(C.SESSION, json.sid, 1);
  }

  // User
  const uid = getCookie(C.USER);
  if (uid) json.uid = uid;

  // Timezone
  try { json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone; } catch {}

  // Device + OS
  json.device = detectDevice();
  json.os = detectOS();
  setCookie(C.DEVICE, json.device, COOKIE_EXPIRY);
  setCookie(C.OS, json.os, COOKIE_EXPIRY);

  // Viewport
  json.w = window.innerWidth;
  json.h = window.innerHeight;

  // Visitor
  json.vid = getCookie(C.VISITOR);
  if (!json.vid) {
    json.vid = json.sid;
    setCookie(C.VISITOR, json.vid, VISITOR_EXPIRY);
  }

  // Auth
  const jwt = getCookie(C.AUTH);
  if (jwt) json.auth = jwt;

  // URL query params (strip utm_ prefix, persist campaign cookies)
  try {
    const search = window.location.search.substring(1);
    if (search) {
      const qps = {};
      search.split('&').forEach(function (pair) {
        const kv = pair.split('=');
        if (kv.length === 2) {
          const k = decodeURIComponent(kv[0]).replace(/^utm_/i, '');
          qps[k] = decodeURIComponent(kv[1]);
        }
      });
      delete qps.ptyp; delete qps.token; delete qps.accessToken; delete qps.refreshToken;
      Object.keys(qps).forEach(function (k) { if (!(k in json)) json[k] = qps[k]; });

      if (qps.xid || qps.ver || qps.source || qps.medium) {
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
      }
    }
  } catch {}

  // Shorten
  if (json.direction) { json.dir = json.direction; delete json.direction; }

  // Send
  if (json.first) send({ ...json, ename: 'visited', etyp: 'session' });
  send(json);
}

// ─── Auto-Init ──────────────────────────────────────────────────────────────
(function init() {
  // Set visitor cookie on root domain
  let vid = getCookie(C.VISITOR);
  if (!vid) {
    vid = uuid();
    setCookie(C.VISITOR, vid, VISITOR_EXPIRY);
    setCookie(C.SESSION, vid, 1);
    try { track({ ename: 'visited', vid: vid, sid: vid, etyp: 'cookie' }); } catch {}
  }
})();

// ─── Click Tracking ─────────────────────────────────────────────────────────
window.addEventListener('click', function (e) {
  const el = e.target;
  const text = (el.textContent || '').trim().substring(0, 50).toLowerCase();
  const trackData = el.closest?.('[data-track]')?.dataset?.track;
  const alt = el.closest?.('[alt]')?.getAttribute?.('alt');
  const label = trackData || text || alt;
  if (label && el.tagName?.toLowerCase() !== 'canvas') {
    track({ ename: 'click', params: { firsttext: label } });
  }
});

// ─── Page Navigation Tracking ───────────────────────────────────────────────
window.addEventListener('popstate', function () {
  const url = window.location.href;
  if (url !== lastUrl) {
    try { track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: url }); } catch {}
    lastUrl = url;
  }
});

document.addEventListener('DOMContentLoaded', function () {
  track({ ename: 'load', etyp: 'page', last: lastUrl, url: window.location.href });
  lastUrl = window.location.href;
});

// SPA support
const _pushState = history.pushState;
const _replaceState = history.replaceState;
history.pushState = function () {
  _pushState.apply(this, arguments);
  try { track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: window.location.href }); } catch {}
  lastUrl = window.location.href;
};
history.replaceState = function () {
  _replaceState.apply(this, arguments);
  try { track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: window.location.href }); } catch {}
  lastUrl = window.location.href;
};

// ─── Export ─────────────────────────────────────────────────────────────────
window.track = track;
