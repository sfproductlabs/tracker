
/* https://github.com/keithws/browser-report
 * Report browser settings like whatsmybrowser.org
 * Inspired by
 * http://stackoverflow.com/questions/9514179/how-to-find-the-operating-system-version-using-javascript
 */

import Cookies from 'js-cookie';
import * as R from 'ramda'
import { v1 as uuidv1 } from 'uuid';
import report from './report';
import request from './request';
import device from './device';

export default function track(params, force) {
    let json = {};
    //History
    json.last = Cookies.get(process.env.GATSBY_CLIENT_REFERRAL_COOKIE) || document.referrer;
    json.url = window.location.href;
    if (json.last === json.url && !R.defaultTo(false)(force))
        return;
    Cookies.set(process.env.GATSBY_CLIENT_REFERRAL_COOKIE, json.url, { expires: 1 });
    //Experiment
    let ename = Cookies.get(process.env.GATSBY_CLIENT_EXPERIMENT_COOKIE);
    if (R.defaultTo(false)(ename))
        json.ename = ename; 
    //Included & Experiment Params
    let exp = Cookies.getJSON(process.env.GATSBY_CLIENT_EXP_PARAMS_COOKIE);
    json.params = {...(exp || {}), ...(params || {})}; 
    //Duration
    let now = Date.now();
    let historical = R.defaultTo(now)(Cookies.get(process.env.GATSBY_CLIENT_TRACK_COOKIE));
    json.created = now;
    json.duration = now - historical;    
    Cookies.set(process.env.GATSBY_CLIENT_TRACK_COOKIE, json.created, { expires: 99999 }); 
    //Owner
    let vid = Cookies.get(process.env.GATSBY_CLIENT_OWNER_COOKIE);
    if (R.defaultTo(false)(vid)) {
        json.vid = vid; 
        json.first = "false";
    } else {
        json.vid = uuidv1();
        json.first = "true";
        Cookies.set(process.env.GATSBY_CLIENT_OWNER_COOKIE, json.vid, { expires: 99999 });
    }   
    //Session
    let sid = Cookies.get(process.env.GATSBY_CLIENT_SESS_COOKIE);
    if (R.defaultTo(false)(sid)) {
        json.sid = sid; 
        json.first = (json.first == "false") ? "false" : "true";
    } else {
        json.sid = uuidv1();
        json.first = "true";
        Cookies.set(process.env.GATSBY_CLIENT_SESS_COOKIE, json.sid, { expires: 1 });
    }
    //Timezone
    json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    //Device
    json.device = device();
    if (typeof window.browserReportSync !== 'undefined') {
        json.os = R.path(["os","name"], window.browserReportSync());    
        json.w = R.path(["viewport","width"], window.browserReportSync());    
        json.h = R.path(["viewport","height"], window.browserReportSync());    
    }
    json.sink = R.path(["params","sink"], json);
    json.score = R.path(["params","score"], json); 
    json.ename = R.path(["params","ename"], json);
    json.outcome = R.path(["params","outcome"], json);
    json.aff = R.path(["params","aff"], json);
    json.ref = R.path(["params","ref"], json);
    delete json.params.ename;
    delete json.params.sink;
    delete json.params.score;
    delete json.params.outcome;
    //Keep aff and ref in params in events
    // delete json.params.aff;
    // delete json.params.ref;
    
    let tj = Cookies.get(process.env.GATSBY_CLIENT_AUTH_COOKIE);
    let ta = {};
    if (R.defaultTo(false)(tj)) {
        ta.headers = { "Authorization": "Bearer " + tj };
        let user = Cookies.getJSON(process.env.GATSBY_CLIENT_AUTH_COOKIE)
        json.uid = R.path(["sfpl","pub","uid"], user)
        json.uname = R.path(["sfpl","pub","uname"], user)
    }
    request(process.env.GATSBY_URL_TRACK, {
        method: "POST",
        body: JSON.stringify(json),
        ...ta
    });
};