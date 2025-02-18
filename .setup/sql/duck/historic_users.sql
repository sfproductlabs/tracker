WITH RankedEvents AS (
    SELECT 
        vid,
        eid,
        url, 
        created,
        ptyp,
        etyp,
        uid,
        ename,  
        app,
		params,
        ROW_NUMBER() OVER (PARTITION BY vid ORDER BY created ASC) AS RowNum
    FROM hive.events
    WHERE ename IN ('load') OR etyp IN ('signup')
),
LatestEvents AS (
    SELECT vid, ename AS latest_ename, url AS latest_url, created AS latest_created, uid AS latest_uid 
    FROM RankedEvents
    WHERE RowNum = (SELECT MAX(RowNum) FROM RankedEvents RE WHERE RE.vid = RankedEvents.vid)
),
FirstEvents AS (
    SELECT vid, ename AS first_ename, url AS first_url, created AS first_created, uid AS first_uid, params as first_params
    FROM RankedEvents
    WHERE RowNum = 1
),
SignupEvents AS (
    SELECT vid, ename AS signup_ename, url AS signup_url, created AS signup_created, uid AS signup_uid, RowNum AS signup_RowNum
    FROM RankedEvents
    WHERE etyp = 'signup' AND RowNum = (SELECT MAX(RowNum) FROM RankedEvents RE WHERE RE.vid = RankedEvents.vid AND etyp = 'signup')
),
BeforeSignupEvents AS (
    SELECT RE.vid, RE.ename AS before_signup_ename, RE.url AS before_signup_url, RE.created AS before_signup_created, RE.uid AS before_signup_uid
    FROM RankedEvents RE
    JOIN SignupEvents SE ON RE.vid = SE.vid AND RE.RowNum = SE.signup_RowNum - 2
)

SELECT 
    FE.vid,
    FE.first_ename,
    FE.first_url,
    FE.first_created,
	FE.first_params,
    LE.latest_ename,
    LE.latest_url,
    LE.latest_created,
    SE.signup_ename,
    SE.signup_url,
    SE.signup_created,
    BSE.before_signup_ename,
    BSE.before_signup_url,
    BSE.before_signup_created
FROM FirstEvents FE
LEFT JOIN LatestEvents LE ON FE.vid = LE.vid
LEFT JOIN SignupEvents SE ON FE.vid = SE.vid
LEFT JOIN BeforeSignupEvents BSE ON FE.vid = BSE.vid
WHERE SE.signup_ename IS NOT NULL