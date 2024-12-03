/* From https://github.com/sfproductlabs/tracker (this is v1 - keep an eye on current v2 updates!)
 * https://github.com/keithws/browser-report
 * Report browser settings like whatsmybrowseorg
 * Inspired by
 * http://stackoverflow.com/questions/9514179/how-to-find-the-operating-system-version-using-javascript
 */

import { cookies } from './cookies';
import { defaultTo, path } from 'ramda';
import { uuid } from '../uuid';
import report from './report';
import request from './request';
import device from './device';
import { getHostRoot } from './network';
import config from '../../../config.yaml';
import { compress } from '../lz4';


// Initial WebSocket setup
let socket;
const wsurl = config.Tracker.WS[config.Application.Target] || config.Tracker.WS.Development;
// Define WebSocket event handlers
const wsHandlers = {
  onopen: () => {
    console.debug('WS TR connected');
  },
  onmessage: (event) => {
    const data = JSON.parse(event.data);
    console.debug('WS TR Received:', data);
  },
  onerror: (event) => {
    console.error('WS TR error:', event);
  },
  onclose: () => {
    console.debug('WS TR Disconnected from server');
    // Attempt to reconnect after a delay
    setTimeout(setupWebSocket, 1000);
  }
};

// Function to setup WebSocket with event handlers
function setupWebSocket() {
  socket = new WebSocket(wsurl);

  socket.addEventListener('open', wsHandlers.onopen);
  socket.addEventListener('message', wsHandlers.onmessage);
  socket.addEventListener('error', wsHandlers.onerror);
  socket.addEventListener('close', wsHandlers.onclose);
}
setupWebSocket();

// Track route changes
let lastUrl = window.location.href;

/**
 * @param {object} params - event parameters
 * @param {!string} params.ename - event name
 * @param {string} [params.etyp] - event type
 * @param {string} [params.ptyp] - page type
 * @param {string} [params.uname] - username
 * @param {string} [params.uid] - user id
 * @param {string} [params.email] - user email
 * @param {*} [params.params] - custom params
 * @returns {null|undefined}
 */
export default function track(params) {
  if (!config?.Tracker?.Track) {
    return;
  }
  params = params ?? {};
  let json = params || {};
  //History
  json.last = params.last || cookies.get(config.Cookies.Names.COOKIE_REFERRAL) || document.referrer;
  json.url = params.url || window.location.href;
  cookies.setLax(config.Cookies.Names.COOKIE_REFERRAL, json.url);
  //Experiment
  let ename = cookies.get(config.Cookies.Names.COOKIE_EXPERIMENT);
  if (ename)
    json.ename = ename;
  //Included & Experiment Params
  let exp = JSON.parse(cookies.get(config.Cookies.Names.COOKIE_EXP_PARAMS) || 'null');
  json.params = {
    ...(exp || {}),
    ...(params || {})
  };
  // Keep track of Time
  let now = Date.now();
  let inactive = (now - Number(cookies.get(config.Cookies.Names.COOKIE_LAST_ACTIVE) ?? now));
  cookies.setLax(config.Cookies.Names.COOKIE_LAST_ACTIVE, now, { expires: 99999 });
  //Session
  let sid = cookies.get(config.Cookies.Names.COOKIE_SESSION);
  if ((inactive < Number(config?.User?.Session?.Timeout || process.env.USER_SESSION_TIMEOUT)) && sid) {
    json.sid = sid;
  } else {
    json.sid = uuid();
    json.first = "true";
    cookies.setLax(config.Cookies.Names.COOKIE_SESSION, json.sid);
  }
  console.log(config.Cookies.Names.COOKIE_USERNAME)
  //Owner
  let uid = cookies.get(config.Cookies.Names.COOKIE_USER);
  if (uid) {
    json.uid = uid;
  }

  //Timezone
  json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
  //Device
  json.device = device();
  let br = report();
  json.os = path(["os", "name"], br);
  json.w = path(["viewport", "width"], br);
  json.h = path(["viewport", "height"], br);
  //ptyp
  if (!json.ptyp) {
    json.ptyp = getPageType()
  }
  //Vid
  json.vid = cookies.get(config.Cookies.Names.COOKIE_VISITOR);
  if (!json.vid) {
    json.vid = json.sid;
    cookies.setLax(config.Cookies.Names.COOKIE_VISITOR, json.vid, {
      expires: 99999
    });
  }

  let tj = cookies.get(config.Cookies.Names.COOKIE_AUTH);
  let ta = {};
  if (tj) {
    ta.headers = {
      "Authorization": "Bearer " + tj
    };
  }
  json.rel = (json.params.rel || config?.Application?.Release || process.env.REL || "") + "";
  json.rel = !json.rel ? null : json.rel;
  json.app = (json.params.app || config?.Application?.Name || process.env.APP_NAME) + "";
  json.app = !json.app ? null : json.app;


  //Existing Query Params
  //Ex. http://localhost:3003/?gu=1&ptyp=blog&utm_source=news_service&utm_medium=email&utm_campaign=campaign&aff=promo&ref=60c59df0ed0811e8a766de1a241fb011&uname=admin
  try {
    let search = window.location.search.substring(1) || cookies.get(config.Cookies.Names.COOKIE_QPS) || "";
    if (search) {  // Only parse if there's actually a search string
      let qps = JSON.parse('{"' + decodeURI(search).replace(/"/g, '\\"').replace(/&/g, '","').replace(/=/g, '":"') + '"}');
      //Remove passed down variables we dont use
      delete qps.ptyp;
      delete qps.token;
      delete qps.accessToken;
      delete qps.refreshToken;
      if (qps.type) {
        qps.qtype = qps.type;
        delete qps.type;
      }
      let qpss = JSON.stringify(qps).replace(/utm_/ig, "")
      json = { ...JSON.parse(qpss), ...json };
    }
  } catch (ex) {
    console.debug('Failed to parse URL parameters:', ex);
  }

  //Shorten Query Params
  if (json.direction) {
    json.dir = json.direction;
    delete json.direction;
  }

  //Finally put all the required params in the (optional) params just in case
  let temp = json.params;
  delete json.params;
  json.params = { ...json, ...temp }

  let tr = function (obj) {
    if (socket && socket.readyState === WebSocket.OPEN) {
      const str = JSON.stringify(obj);
      const bytes = new TextEncoder().encode(str);
      compress(bytes, true).then(compressed => {
        socket.send(compressed);
      });
    } else {
      try {
        window.fetch(`${getTrackerUrl()}/tr/v1/tr/`, {
          method: "POST",
          body: JSON.stringify(obj),
          ...ta
        }).catch(() => { });
      } catch (e) { }
    }
  };

  if (json.first) {
    tr({ ...json, ename: "visited", etyp: "session" });
  }

  switch (true) {
    case /clicked_/.test(json.ename):
    case /viewed_/.test(json.ename):
      tr(json);
      break;
    default:
      //This should never get called
      //console.warn('**Untracked event: ** ', json.ename);
      tr(json);
      break;
  }
};

export const getPageType = () => {
  if (!window.location) {
    return null
  }
  const routes = config.Tracker.Routes;
  if (routes) {
    for (const route of routes) {
      if (route && route.PageType && route.Regex) {
        if (new RegExp(route.Regex, "ig").test(window.location.href)) {
          return route.PageType
        }
      }
    }
  }
  return null;
}

export function getTrackerUrl() {
  return path(["Tracker", "Url", process.env.TARGET || path(["Application", "Target"], config) || "Development"], config);
}

export function resetUserCookies() {
  const domain = getHostRoot();
  let vid = cookies.get(config.Cookies.Names.COOKIE_VISITOR);
  let found = true;
  if (!vid) {
    vid = uuid();
    found = false;
  }
  const sid = cookies.get(config.Cookies.Names.COOKIE_SESSION) || vid;
  cookies.setLax(config.Cookies.Names.COOKIE_VISITOR, vid, {
    sameSite: 'lax',
    domain
  });
  cookies.setLax(config.Cookies.Names.COOKIE_SESSION, sid, {
    sameSite: 'lax',
    domain
  });
  if (!found) {
    try { track({ ename: 'visited', eid: vid, sid: vid, vid: vid, etyp: "cookie" }); } catch { };
  }
}

resetUserCookies();




// Track mouse position, scroll position and click events
// Events are batched and sent every 100ms to reduce server load and improve performance
// Click events include target element information (id, class, tag) for better analytics
let mouseEvents = [];
let scrollEvents = [];
let clickEvents = [];
let keyboardEvents = [];
let trackingTimeout;
let keyboardTrackingTimeout;

const sensitiveClasses = ['password', 'credit-card', 'ssn'];
const sensitiveAttributes = ['data-sensitive', 'data-private'];

const shouldMaskElement = (element) => {
  return sensitiveClasses.some(cls => element.className?.includes?.(cls)) ||
    sensitiveAttributes.some(attr => element.hasAttribute?.(attr));
};
const getElementDetails = (element) => {
  return {
    id: element.id || null,
    className: element.className || null,
    tagName: element.tagName?.toLowerCase() || null,
    text: shouldMaskElement(element) ? '***' : element.textContent?.trim() || null,
    type: element.type || null,
    href: element.href || null,
    // Get closest parent with data-track attribute if exists
    trackingData: element.closest('[data-track]')?.dataset?.track || null,
    alt: element.closest("[alt]")?.getAttribute?.("alt")
  };
};

const trackClickEvent = (e) => {
  const target = e.target;
  clickEvents.push({
    timestamp: Date.now(),
    x: e.clientX,
    y: e.clientY,
    element: getElementDetails(target)
  });
  scheduleTracking();
};

const trackMouseMovement = (e) => {
  mouseEvents.push({
    x: e.clientX,
    y: e.clientY,
    timestamp: Date.now()
  });
  scheduleTracking();
};

const trackScrollMovement = () => {
  scrollEvents.push({
    x: window.scrollX,
    y: window.scrollY,
    timestamp: Date.now()
  });
  scheduleTracking();
};

const trackKeyboardEvent = (e) => {
  // Don't track actual key values from sensitive fields
  const isSensitiveField = shouldMaskElement(e.target);
  keyboardEvents.push({
    timestamp: Date.now(),
    key: isSensitiveField ? '***' : e.key,
    type: e.type, // 'keydown', 'keyup', etc.
    element: getElementDetails(e.target)
  });
  scheduleKeyboardTracking();
};

const scheduleKeyboardTracking = () => {
  if (keyboardTrackingTimeout) {
    clearTimeout(keyboardTrackingTimeout);
  }
  keyboardTrackingTimeout = setTimeout(sendKeyboardEvents, 2000);
};

const sendKeyboardEvents = () => {
  if (keyboardEvents.length > 0) {
    track({
      ename: "key",
      params: { keys: keyboardEvents }
    });
    keyboardEvents = [];
  }
};

const scheduleTracking = () => {
  if (trackingTimeout) {
    clearTimeout(trackingTimeout);
  }
  trackingTimeout = setTimeout(sendTrackingEvents, 100);
};

const sendTrackingEvents = () => {
  if (mouseEvents.length > 0) {
    track({
      ename: "mouse",
      params: { moves: mouseEvents }
    });
    mouseEvents = [];
  }
  if (scrollEvents.length > 0) {
    track({
      ename: "scroll",
      params: { scrolls: scrollEvents }
    });
    scrollEvents = [];
  }
  if (clickEvents.length > 0) {
    debugger
    const firstText = clickEvents?.[0]?.element?.text || clickEvents?.[0]?.element?.trackingData || clickEvents?.[0]?.element?.alt;
    if (clickEvents?.[0]?.element?.tagName !== 'canvas' && typeof firstText === 'string' && firstText.length > 0) {
      track({
        ename: "click",
        params: { firstText: firstText.substring(0, 50).toLowerCase() }
      });
    }
    clickEvents = [];
  }
};

const trackPerformance = () => {
  if (window.performance && window.performance.timing) {
    const timing = performance.timing;
    track({
      ename: "pageperf",
      values: [{
        loadTime: timing.loadEventEnd - timing.navigationStart,
        domReady: timing.domContentLoadedEventEnd - timing.navigationStart,
        firstPaint: performance.getEntriesByType('paint')[0]?.startTime,
        timestamp: Date.now()
      }]
    });
  }
};

//window.addEventListener('mousemove', trackMouseMovement);
//window.addEventListener('scroll', trackScrollMovement);
window.addEventListener('click', trackClickEvent);
//window.addEventListener('load', trackPerformance);
//window.addEventListener('keydown', trackKeyboardEvent);


window.addEventListener('popstate', () => {
  const currentUrl = window.location.href;
  if (currentUrl !== lastUrl) {
    try {
      track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: currentUrl });
      lastUrl = currentUrl;
    } catch (err) {
      console.debug('Failed to track popstate:', err);
    }
  }
});

document.addEventListener('DOMContentLoaded', () => {
  const currentUrl = window.location.href;
  track({ ename: 'load', etyp: 'page', last: lastUrl, url: currentUrl });
  lastUrl = currentUrl;
});

// For single page apps that use history.pushState/replaceState
const originalPushState = history.pushState;
const originalReplaceState = history.replaceState;

history.pushState = function () {
  originalPushState.apply(this, arguments);
  try {
    track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: window.location.href });
    lastUrl = window.location.href;
  } catch (err) {
    console.debug('Failed to track pushState:', err);
  }
};

history.replaceState = function () {
  originalReplaceState.apply(this, arguments);
  try {
    track({ ename: 'reroute', etyp: 'page', last: lastUrl, url: window.location.href });
    lastUrl = window.location.href;
  } catch (err) {
    console.debug('Failed to track replaceState:', err);
  }
};  
