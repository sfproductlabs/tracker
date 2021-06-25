
export function getHostRoot() {
    if (location && location.host) {
    //TODO: https://github.com/sfproductlabs/second-level-domains/blob/master/sldsg.json
    const temp = location.host.split('.').reverse();
    const domain = ((temp.length > 1) ? temp[1] + '.' + temp[0] : temp[0]).split(':')[0];
    return domain;
    } else {
        return null;
    }
}