
/* From https://github.com/sfproductlabs/tracker (TODO: Update to v2 soon)
 * https://github.com/keithws/browser-report
 * Report browser settings like whatsmybrowser.org
 * Inspired by
 * http://stackoverflow.com/questions/9514179/how-to-find-the-operating-system-version-using-javascript
 */

import fetch from 'unfetch'
import {cookies} from './cookies';
import {defaultTo, path} from 'ramda';

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

async function checkStatusAndParseJSON(response) {
    let responseJSON = {};
  
    try {
      if (/application\/json/i.test(response.headers.get('content-type'))) {
        responseJSON = await response.json();
      }
    }
    catch (e) {
      tcup.Handle({type: 'RESPONSE_ERROR_1', response});
    }
  
    if (response.status >= 200 && response.status < 300) {
      return responseJSON;
    }
  
    if (tcup && tcup.Handle) {
        tcup.Handle({type: 'RESPONSE_ERROR_2', response, responseJSON});
    }
    
}
  

/**
 * Requests a URL, returning a promise
 *
 * @param  {string} url       The URL we want to request
 * @param  {object} [options] The options we want to pass to "fetch"
 *
 * @return {object}           The response data
 */
export default function request(url, options) {
    let jwt = cookies.get(process.env.COOKIE_AUTH);
  
    if (process.env.IS_NATIVE) {
      jwt = require('nativeStore').store.getState().global.getIn(['userData', process.env.COOKIE_AUTH]);
    }
    if (options && options.other && options.other.noJWT) {
      jwt = null;
    }
  
    let auth = (typeof jwt === 'string') ? {  "Authorization" : "Bearer " + jwt } : {};
    let opts = defaultTo({})(options);
    let track = defaultTo(false)(opts.track);
    let camelize = defaultTo(true)(opts.camelize);
    if (track) {
      setTimeout(function(){
        let tj = cookies.get(process.env.COOKIE_AUTH);
        if (tj != null) {
          let ta = {  "Authorization" : "Bearer " + tj };
          fetch(`${URL_API}/api/v1/history/track`, {
            method: "POST",
            body: JSON.stringify(track),
            headers : ta
          });
        }
      }, 3000);
      delete opts.track;
    }
    if (typeof opts.body === "object")
      opts.body = JSON.stringify(opts.body);
    opts.headers = defaultTo({})(opts.headers);
    opts.headers = {
      ...opts.headers,
      ...auth
    };
    if (camelize)
      return fetch(url, opts)
        .then(checkStatusAndParseJSON)
        .then(parseCamel);
    else
      return fetch(url, opts)
        .then(checkStatusAndParseJSON);
  }
  