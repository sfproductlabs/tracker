import Cookies from 'js-cookie';

import {URL_TRACK} from '../constants'
import {uuid} from './js-uuid'
import defaultTo from './defaultTo';
import path from './path';
import request from './request';
import device from './device';

export default function track(params, force) {
    let json = {};
    //History
    json.last = Cookies.get(process.env.CLIENT_REFERRAL_COOKIE) || document.referrer;
    json.url = window.location.href;
    if (json.last === json.url && !defaultTo(false)(force))
        return;
    Cookies.set(process.env.CLIENT_REFERRAL_COOKIE, json.url);
    //Experiment
    let ename = Cookies.get(process.env.CLIENT_EXPERIMENT_COOKIE);
    if (defaultTo(false)(ename))
        json.ename = ename; 
    //Included & Experiment Params
    let exp = Cookies.getJSON(process.env.CLIENT_EXP_PARAMS_COOKIE);
    json.params = {...(exp || {}), ...(params || {})}; 
    //Duration
    let now = Date.now();
    let historical = defaultTo(now)(Cookies.get(process.env.CLIENT_TRACK_COOKIE));
    json.created = now;
    json.duration = now - historical;    
    Cookies.set(process.env.CLIENT_TRACK_COOKIE, json.created, { expires: 99999 }); 
    //Owner
    let vid = Cookies.get(process.env.CLIENT_OWNER_COOKIE);
    if (defaultTo(false)(vid)) {
        json.vid = vid; 
        json.first = "false";
    } else {
        json.vid = uuid.v1();
        json.first = "true";
        Cookies.set(process.env.CLIENT_OWNER_COOKIE, json.vid, { expires: 99999 });
    }   
    //Session
    let sid = Cookies.get(process.env.CLIENT_SESS_COOKIE);
    if (defaultTo(false)(sid)) {
        json.sid = sid; 
        json.first = (json.first == "false") ? "false" : "true";
    } else {
        json.sid = uuid.v1();
        json.first = "true";
        Cookies.set(process.env.CLIENT_SESS_COOKIE, json.sid);
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
    delete json.params.ename;
    delete json.params.sink;
    delete json.params.score;
    delete json.params.outcome;
    //Keep aff and ref in params in events
    // delete json.params.aff;
    // delete json.params.ref;
    
    let tj = Cookies.get(process.env.CLIENT_AUTH_COOKIE);
    let ta = {};
    if (defaultTo(false)(tj)) {
        ta.headers = { "Authorization": "Bearer " + tj };
        let user = Cookies.getJSON(process.env.CLIENT_AUTH_COOKIE)
        json.uid = path(["sfpl","pub","uid"], user)
        json.uname = path(["sfpl","pub","uname"], user)
    }
    request(URL_TRACK, {
        method: "POST",
        body: JSON.stringify(json),
        ...ta
    });
};