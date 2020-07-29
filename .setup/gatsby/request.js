import fetch from 'unfetch'
import Cookies from 'js-cookie';
import * as R from 'ramda'

const camelFinder = /^(?!CloudFront-)([A-Z])|[_](\w)/g;
function toCamelCase(str) {
    return str.replace(camelFinder, function (match, p1, p2, offset) {
        if (p2) return p2.toUpperCase();
        return p1.toLowerCase();
    });
};
function parseCamel(json) {
    if (!json) {
        return json;
    }
    let key, destKey;
    Object.keys(json).map(function (key, index) {
        if (typeof json[key] === "object" && json[key] !== null) {
            json[key] = parseCamel(json[key]);
        }
        destKey = toCamelCase(key);
        if (key !== destKey) {
            Object.defineProperty(json, destKey, Object.getOwnPropertyDescriptor(json, key));
            delete json[key];
        }
    });
    return json;
}

function parseJSON(response) {        
    if (/application\/json/i.test(response.headers.get('content-type')))
        return response.json();
    else return null;
}


function checkStatus(response) {
    if (response.status >= 200 && response.status < 300) {
        return response;
    }

    const error = new Error(response.statusText);
    error.response = response;
    return Promise.reject(error);
}

function catchError(err) {
    throw err;
}

export default function request(url, options) {
    let keyLookup = (process.env.GATSBY_KEY_NAME || '').toLocaleLowerCase();
    let jwt = R.defaultTo(null)(R.path([keyLookup],Cookies.getJSON(process.env.GATSBY_CLIENT_AUTH_COOKIE)));
    let auth = (typeof jwt !== null && typeof jwt === 'object') ? { "Authorization": "Bearer " + JSON.stringify(jwt) } : {};
    let opts = R.defaultTo({})(options);
    let track = R.defaultTo(false)(opts.track);
    let camelize = R.defaultTo(true)(opts.camelize);
    if (track) {
        setTimeout(function () {
            let tj = R.defaultTo(null)(R.path([keyLookup],Cookies.getJSON(process.env.GATSBY_CLIENT_AUTH_COOKIE)));
            if (tj !== null && typeof jwt === 'object') {
                let ta = { "Authorization": "Bearer " + JSON.stringify(tj) };
                fetch(process.env.GATSBY_URL_TRACK, {
                    method: "POST",
                    body: JSON.stringify(track),
                    headers: ta
                });
            }
        }, 3000);
        delete opts.track;
    }
    if (typeof opts.body === "object") {
        opts.body = JSON.stringify(opts.body);
    }
    opts.headers = R.defaultTo({})(opts.headers);
    opts.headers = {
        "Content-Type":"text/plain",
        ...opts.headers,
        ...auth
    };
    if (camelize)
        return fetch(url, opts)
            .then(checkStatus)
            .then(parseJSON)
            .then(parseCamel)
            .catch(catchError);           
    else
        return fetch(url, opts)
            .then(checkStatus)
            .then(parseJSON)
            .catch(catchError); 
}
