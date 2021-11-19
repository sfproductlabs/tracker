import Cookies from 'js-cookie'
import { v1 as uuidv1 } from 'uuid';
import config from '../../../config.yaml';
import {getHostRoot, isSecure} from './network';

export function removeCookie(ck, opts) {
  try {
    const domain = getHostRoot();
    Cookies.remove(ck, opts);
    Cookies.remove(ck, { domain: domain });
    Cookies.remove(ck, { domain: "." + domain });
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


function setCookieLax (key, value, attributes) {
  setCookie(key,value,{...attributes, sameSite: 'lax'})
}

function setCookie (key, value, attributes) {
  const domain = getHostRoot();
  let attrs = {
    sameSite: 'strict',
    secure: isSecure(),
    domain,
    ...(attributes || {}),
  };
  attrs.expires = 99999;
  Cookies.set(key,value, attrs);  
}

export const cookies = {
    get : Cookies.get,
    set : setCookie,
    setLax : setCookieLax,
    clear : clearCookies,
    remove : removeCookie
} 

