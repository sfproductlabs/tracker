import Cookies from './js-cookie';
import {
    URL_TRACK, 
    COOKIE_REFERRAL, 
    COOKIE_EXPERIMENT,
    COOKIE_EXP_PARAMS,
    COOKIE_TRACK,
    COOKIE_VID,
    COOKIE_SESS,
    COOKIE_JWT,
} from './constants'
import {uuid} from './js-uuid'
import defaultTo from './defaultTo';
import path from './path';
import request from './request';
import device from './device';
import report from './report';

export default function track(params, force) {
    let json = {};
    //Lets merge the query params
    try {
        var match,
            pl     = /\+/g,  // Regex for replacing addition symbol with a space
            search = /([^&=]+)=?([^&]*)/g,
            decode = function (s) { return decodeURIComponent(s.replace(pl, " ")); },
            query  = window.location.search.substring(1);
        var urlParams = {};
        while (match = search.exec(query))
        urlParams[decode(match[1])] = decode(match[2]);
        params =  {...(urlParams || {}), ...(params || {})};
    } catch {}
    //History
    json.last = Cookies.get(COOKIE_REFERRAL) || document.referrer;
    json.url = window.location.href;
    if (json.last === json.url && !defaultTo(false)(force))
        return;
    Cookies.set(COOKIE_REFERRAL, json.url);
    //Experiment
    let xid = Cookies.get(COOKIE_EXPERIMENT);
    if (defaultTo(false)(xid))
        json.xid = xid; 
    //Included & Experiment Params
    let exp = JSON.parse(Cookies.get(COOKIE_EXP_PARAMS) || 'null');
    json.params = {...(exp || {}), ...(params || {})}; 
    //Duration
    let now = Date.now();
    let historical = defaultTo(now)(Cookies.get(COOKIE_TRACK));
    json.created = now;
    json.duration = now - historical;    
    Cookies.set(COOKIE_TRACK, json.created, { expires: 99999 }); 
    //Owner
    let vid = Cookies.get(COOKIE_VID);
    if (defaultTo(false)(vid)) {
        json.vid = vid; 
        json.first = "false";
    } else {
        json.vid = uuid.v1();
        json.first = "true";
        Cookies.set(COOKIE_VID, json.vid, { expires: 99999 });
    }   
    //Session
    let sid = Cookies.get(COOKIE_SESS);
    if (defaultTo(false)(sid)) {
        json.sid = sid; 
        json.first = (json.first == "false") ? "false" : "true";
    } else {
        json.sid = uuid.v1();
        json.first = "true";
        Cookies.set(COOKIE_SESS, json.sid);
    }
    //Timezone
    json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    //Device
    json.device = device();
    json.os = path(["os","name"], browserReportSync());    
    json.w = path(["viewport","width"], browserReportSync());    
    json.h = path(["viewport","height"], browserReportSync());    
    json.sink = path(["params","sink"], json);
    json.score = path(["params","score"], json); 
    json.ename = path(["params","ename"], json);
    json.outcome = path(["params","outcome"], json);
    json.aff = path(["params","aff"], json);
    json.ref = path(["params","ref"], json);
    json.app = process.env.APP_NAME;
    json.ver = process.env.VERSION;
    delete json.params.ename;
    delete json.params.sink;
    delete json.params.score;
    delete json.params.outcome;
    //Keep aff and ref in params in events
    // delete json.params.aff;
    // delete json.params.ref;
    
    let tj = Cookies.get(COOKIE_JWT);
    let ta = {};
    if (defaultTo(false)(tj)) {
        ta.headers = { "Authorization": "Bearer " + tj };
        let user = JSON.parse(tj);
        json.uid = path(["sfpl","pub","uid"], user)
        json.uname = path(["sfpl","pub","uname"], user)
    }
    request(`${URL_TRACK}/tr/v1/`, {
        method: "POST",
        body: JSON.stringify(json),
        ...ta
    });
};

window.tr = track;