//Extended https://www.abeautifulsite.net/detecting-mobile-devices-with-javascript

export default function () {
    var deviceName = '';

    var isMobile = {
        Android: function () {
            return navigator.userAgent.match(/Android/i);
        },
        Datalogic: function () {
            return navigator.userAgent.match(/DL-AXIS/i);
        },
        Bluebird: function () {
            return navigator.userAgent.match(/EF500/i);
        },
        Honeywell: function () {
            return navigator.userAgent.match(/CT50/i);
        },
        Zebra: function () {
            return navigator.userAgent.match(/TC70|TC55/i);
        },
        BlackBerry: function () {
            return navigator.userAgent.match(/BlackBerry/i);
        },
        iOS: function () {
            return navigator.userAgent.match(/iPhone|iPad|iPod|iOS/i);
        },
        Windows: function () {
            return navigator.userAgent.match(/IEMobile/i);
        },
        any: function () {
            return (isMobile.Datalogic() || isMobile.Bluebird() || isMobile.Honeywell() || isMobile.Zebra() || isMobile.BlackBerry() || isMobile.Android() || isMobile.iOS() || isMobile.Windows());
        }
    };

    if (isMobile.Datalogic())
        deviceName = 'Datalogic';
    else if (isMobile.Bluebird())
        deviceName = 'Bluebird';
    else if (isMobile.Honeywell())
        deviceName = 'Honeywell';
    else if (isMobile.Zebra())
        deviceName = 'Zebra';
    else if (isMobile.BlackBerry())
        deviceName = 'BlackBerry';
    else if (isMobile.iOS())
        deviceName = 'iOS';
    else if (isMobile.Android())
        deviceName = 'Android';
    else if (isMobile.Windows())
        deviceName = 'Windows Mobile';
    else if (navigator.userAgent.match(/Linux/i))
        deviceName = 'Linux';
    else if (navigator.userAgent.match(/Samsung/i))
        deviceName = 'Samsung';
    else if (navigator.userAgent.match(/Windows/i))
        deviceName = 'Windows';
    else if (navigator.userAgent.match(/Mac/i))
        deviceName = 'Mac';
    else if (navigator.userAgent.match(/osx/i))
        deviceName = 'Mac';
    else if (navigator.userAgent.match(/Safari/i))
        deviceName = 'Safari';
    else if (navigator.userAgent.match(/Explorer/i))
        deviceName = 'Windows';
    else if (navigator.userAgent.match(/Opera/i))
        deviceName = 'Opera';

    return deviceName;
}