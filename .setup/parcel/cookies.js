import Cookies from 'js-cookie'
import { v1 as uuidv1 } from 'uuid';
import config from '/config.yaml';
import {getHostRoot} from './network';

export function removeCookie(ck, opts) {
  try {
    Cookies.remove(ck, opts);
  } catch {}
}

export function clearCookies() {
    const domain = getHostRoot();
    const cks = Cookies.get();
    if (cks) {
        for (const ck in cks) {
          try {
            Cookies.remove(ck);
            Cookies.remove(ck, { domain: domain });
            Cookies.remove(ck, { domain: "." + domain });
          } catch {}
        }
    }
}

function setCookie (key, value, attributes) {
  const domain = getHostRoot();
  let attrs = {
    ...(attributes || {}),
    sameSite: 'lax', //lax?
    secure: false,
    domain
  };
  attrs.expires = 99999;
  Cookies.set(key,value, attrs);  
}

export const cookies = {
    get : Cookies.get,
    set : setCookie,
    clear : clearCookies,
    remove : removeCookie
} 

