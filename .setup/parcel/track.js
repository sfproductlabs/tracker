
/* From https://github.com/sfproductlabs/tracker (this is v1 - keep an eye on current v2 updates!)
 * https://github.com/keithws/browser-report
 * Report browser settings like whatsmybrowseorg
 * Inspired by
 * http://stackoverflow.com/questions/9514179/how-to-find-the-operating-system-version-using-javascript
 */

import { cookies } from './cookies';
import { defaultTo, path } from 'ramda';
import { v1 as uuidv1 } from 'uuid';
import report from './report';
import request from './request';
import device from './device';
import {getHostRoot} from './network';
import config from '/config.yaml';

export default function track(params) {
    if (!config || !config.Tracker || !config.Tracker.Track) {        
        return;
    }
    params = defaultTo({})(params);
    let json = params || {};
    //History
    json.last = params.last || cookies.get(config.Cookies.Names.COOKIE_REFERRAL) || document.referrer;
    json.url = params.url || window.location.href;
    cookies.set(config.Cookies.Names.COOKIE_REFERRAL, json.url);
    //Experiment
    let ename = cookies.get(config.Cookies.Names.COOKIE_EXPERIMENT);
    if (defaultTo(false)(ename))
        json.ename = ename;
    //Included & Experiment Params
    let exp = JSON.parse(cookies.get(config.Cookies.Names.COOKIE_EXP_PARAMS) || 'null');
    json.params = {
        ...(exp || {}),
        ...(params || {})
    };
    // Keep track of Time
    let now = Date.now();
    let inactive = (now - Number(defaultTo(now)(cookies.get(config.Cookies.Names.COOKIE_LAST_ACTIVE))));
    cookies.set(config.Cookies.Names.COOKIE_LAST_ACTIVE, now, { expires: 99999 });
    //Session
    let sid = cookies.get(config.Cookies.Names.COOKIE_SESSION);
    if ((inactive < Number(path(["User", "Session", "Timeout"], config) || process.env.USER_SESSION_TIMEOUT)) && defaultTo(false)(sid)) {
        json.sid = sid;
    } else {
        json.sid = uuidv1();
        json.first = "true";
        cookies.set(config.Cookies.Names.COOKIE_SESSION, json.sid);
    }
    //Owner
    let uname = defaultTo(false)(path(['uname'], JSON.parse(cookies.get(config.Cookies.Names.COOKIE_USER) || 'null')));
    if (uname) {
        json.uname = uname;
    }
    let uid = defaultTo(false)(path(['uid'], JSON.parse(cookies.get(config.Cookies.Names.COOKIE_USER) || 'null')));
    if (uid) {
        json.uid = uid;
    }
    let email = defaultTo(false)(path(['email'], JSON.parse(cookies.get(config.Cookies.Names.COOKIE_USER) || 'null')));
    if (email) {
        json.email = email;
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
    json.vid = defaultTo(false)(cookies.get(config.Cookies.Names.COOKIE_VISITOR));
    if (!json.vid) {
        json.vid = json.sid;
        cookies.set(config.Cookies.Names.COOKIE_VISITOR, json.vid, {
            expires: 99999
        });
    }

    let tj = cookies.get(config.Cookies.Names.COOKIE_AUTH);
    let ta = {};
    if (defaultTo(false)(tj)) {
        ta.headers = {
            "Authorization": "Bearer " + tj
        };
    }
    json.rel = (json.params.rel || path(["Application", "Release"], config) || process.env.REL || "") + "";
    json.rel = !json.rel ? null : json.rel;
    json.app = (json.params.app || path(["Application", "Name"], config) || process.env.APP_NAME) + "";
    json.app = !json.app ? null : json.app;


    //Existing Query Params
    //Ex. http://localhost:3003/?gu=1&ptyp=blog&utm_source=news_service&utm_medium=email&utm_campaign=campaign&aff=promo&ref=60c59df0ed0811e8a766de1a241fb011&uname=admin
    try {
        let search = window.location.search.substring(1) || cookies.get(config.Cookies.Names.COOKIE_QPS) || "";
        //Only store the qparams for 1 hit, uncomment for each hit in the session
        //if (search.length > 0)
        //    cookies.set(process.env.REACT_APP_COOKIE_QPS, search);
        let qps = JSON.parse('{"' + decodeURI(search).replace(/"/g, '\\"').replace(/&/g, '","').replace(/=/g, '":"') + '"}')
        //Remove passed down variables we dont use
        delete qps.ptyp;
        delete qps.token;
        if (qps.type) {
            qps.qtype = qps.type;
            delete qps.type;
        }
        let qpss = JSON.stringify(qps).replace(/utm_/ig, "")
        json = { ...JSON.parse(qpss), ...json };
    } catch (ex) { }

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
        request(`${getTrackerUrl()}/tr/v1/`, {
            method: "POST",
            body: JSON.stringify(obj),
            ...ta
        });
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
    const routes = path(["Routes"], config);
    if (routes) {
        for (route in routes) {
            if (route && routes[route].PageType && routes[route].Regex) {
                if (new RegExp(routes[route].Regex, "ig").test(window.location.pathname)) {
                    return routes[route].PageType
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
      vid = uuidv1();
      found = false;
    }
    const sid = cookies.get(config.Cookies.Names.COOKIE_SESSION) || vid;
    cookies.set(config.Cookies.Names.COOKIE_VISITOR, vid, { 
      sameSite: 'lax', 
      domain
    });
    cookies.set(config.Cookies.Names.COOKIE_SESSION, sid, { 
      sameSite: 'lax', 
      domain
    });
    if (!found) {
      try { track({ename:'visited', eid: vid, sid: vid, vid:vid, etyp: "cookie"}); } catch {};
    }
  }

  resetUserCookies();