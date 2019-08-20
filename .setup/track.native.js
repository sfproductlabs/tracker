import Secrets from 'react-native-config'
import { create } from 'apisauce'
import { Dimensions } from 'react-native'
import uuidv1 from 'uuid/v1';
import R from 'ramda';
import DeviceInfo from 'react-native-device-info';
import AppConfig from '../config/app'

const events = { last: null, current: null};
const params = { last: null, current: null};
const api = create({
    baseURL: Secrets.URL_TRACK,
    headers: { Accept: 'application/json' },
});
const tr = function(obj) {
    try {
        api
            .post('/tr/v1/', obj, { headers: { 'x-gigawatts': '1.21' } })
            //.then(response => response)
            //.then(console.log)
    } catch (ex) {
        console.log('TRACKER ERROR: ', ex)
    }
};


export default function track(current, force) {
    params.current = current;
    params.current.time = Date.now();
    let historical = (params.last) ? params.last.time : params.current.time;
    let json = {};
    json.app = 'native';
    json.created = params.current.time;
    json.duration = params.current.time - historical;    
    json.rel = Secrets.VERSION || 1;
    
    //Device
    //https://github.com/react-native-community/react-native-device-info
    json.device = DeviceInfo.getDeviceType();
    json.os = `${DeviceInfo.getSystemName()} ${DeviceInfo.getSystemVersion()}`;
    json.w = Dimensions.get('window').width
    json.h = Dimensions.get('window').height

    //UName
    json.uname = R.defaultTo(
        R.path(['state', 'login', 'payload', 'sfpl', 'pub', 'uname'], current),
        R.defaultTo(
            R.path(['action', 'payload', 'sfpl', 'pub', 'uname'], current),
            null
        )
    );   
    
    //Email
    json.email = R.defaultTo(
        R.path(['state', 'login', 'payload', 'sfpl', 'pub', 'email'], current),
        R.defaultTo(
            R.path(['action', 'payload', 'sfpl', 'pub', 'email'], current),
            null
        )
    );   

    //UID
    json.uid = R.defaultTo(
        R.path(['state', 'login', 'payload', 'sfpl', 'pub', 'id'], current),
        R.defaultTo(
            R.path(['action', 'payload', 'sfpl', 'pub', 'id'], current),
            null
        )
    );   

    //Visitor ID
    json.vid = json.uid;
    if (!json.vid && (!params.last || !params.last.vid)) {
        params.current.vid = R.path(['state', 'login', 'sid'], current);
        json.first = "true";
    }

    //Session
    json.sid = AppConfig.sid;

    

    //Timezone
    json.tz = Intl.DateTimeFormat().resolvedOptions().timeZone;

    //TODO: AG
    // //History
    // json.last = Cookies.get(process.env.CLIENT_REFERRAL_COOKIE) || document.referrer;
    // json.url = window.location.href;
    // if (json.last === json.url && !defaultTo(false)(force))
    //     return;
    // Cookies.set(process.env.CLIENT_REFERRAL_COOKIE, json.url);
    
    // //Experiment
    // let ename = Cookies.get(process.env.CLIENT_EXPERIMENT_COOKIE);
    // if (defaultTo(false)(ename))
    //     json.ename = ename; 
    
    // //Included & Experiment Params
    // let exp = Cookies.getJSON(process.env.CLIENT_EXP_PARAMS_COOKIE);
    // json.params = {...(exp || {}), ...(params || {})};         
    // json.sink = path(["params","sink"], json);
    // json.score = path(["params","score"], json); 
    // json.ename = path(["params","ename"], json);
    // json.outcome = path(["params","outcome"], json);
    // json.aff = path(["params","aff"], json);
    // json.ref = path(["params","ref"], json);
    // delete json.params.ename;
    // delete json.params.sink;
    // delete json.params.score;
    // delete json.params.outcome;
    // //Keep aff and ref in params in events
    // // delete json.params.aff;
    // // delete json.params.ref;

    //URL
    json.url = params.current.route

    //Ptyp
    switch (true) {
        case new RegExp(`LaunchScreen`, 'ig').test(json.url):
            json.ptyp = 'launch';
            break;
    }
    
    //Set new to old
    params.last = params.current;
    //Check if different from last event
    //Don't repeat send to server same event twice
    if (JSON.stringify(json) == events.last)
        return;
    events.last = JSON.stringify(json);

    //First visit
    if (json.first) {
        tr({...json, ename : "first", etyp : "session"});        
    }
    
    switch (true) {
        case /clicked_/ig.test(json.ename):
        case /viewed_/ig.test(json.ename):
            tr(json);
            break;
        default:
            //This should never get called
            //console.warn('**Untracked event: ** ', json.ename);
            //TODO: Remove AG
            tr(json)
            break;
    }
    
    return;
    
    
     
};

