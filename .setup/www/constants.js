export const URL_API = process.env.URL_API;
export const URL_TRACK = process.env.URL_TRACK;
export const COOKIE_JWT = 'jwt'
export const COOKIE_USER = 'user'
export const COOKIE_LAST_ACTIVE = 'la'
export const COOKIE_VID = 'vid'
export const COOKIE_QPS = 'qps'
export const COOKIE_SESS = 'sess'
export const COOKIE_TRACK = 'trc'
export const COOKIE_REFERRAL = 'ref'
export const COOKIE_REFERRAL_USER_UUID = 'ruuuid'
export const COOKIE_DURATION = 'dur'
export const COOKIE_EXPERIMENT = 'ex'
export const COOKIE_EXP_PARAMS = 'exp'

export const EXPERIMENT_STATUS = {
    cancel: -5,
    init: 0,
    attempted: 1,
    finished: 5,
    rejected: -2
}

export const EXPERIMENT_ACTION = {
    coworkApply: 'cw-a',
}

export const EXPERIMENT_SINK = {
    coworkBuy: 'cw$',
}

export const MAX_DATE = new Date(8640000000000000);
