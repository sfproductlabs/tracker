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
    SELECT vid, ename AS first_ename, url AS first_url, created AS first_created, uid AS first_uid, params as first_params, ptyp AS first_ptyp
    FROM RankedEvents
    WHERE RowNum = 1
),
SignupEvents AS (
    SELECT
        vid,
        ename AS signup_ename,
        url AS signup_url,
        created AS signup_created,
        uid AS signup_uid,
        RowNum AS signup_RowNum,
        DATE_TRUNC('week', created - '8 hours'::interval) AS cohort_week
    FROM RankedEvents
    WHERE etyp = 'signup' AND RowNum = (SELECT MAX(RowNum) FROM RankedEvents RE WHERE RE.vid = RankedEvents.vid AND etyp = 'signup')
),
BeforeSignupEvents AS (
    SELECT RE.vid, RE.ename AS before_signup_ename, RE.url AS before_signup_url, RE.created AS before_signup_created, RE.uid AS before_signup_uid
    FROM RankedEvents RE
    JOIN SignupEvents SE ON RE.vid = SE.vid AND RE.RowNum = SE.signup_RowNum - 2
),
KeyEvents AS (
    SELECT
        vid,
        uid,
        ename AS event_name,
        etyp AS event_type,
        created,
        ROW_NUMBER() OVER (PARTITION BY vid ORDER BY created ASC) AS RowNum
    FROM hive.events
    WHERE (etyp = 'ml' OR etyp = 'import') AND app = 'stapp'
),
UsageEvents AS (
    SELECT
        vid,
        uid,
        ename AS event_name,
        etyp AS event_type,
        created,
        ROW_NUMBER() OVER (PARTITION BY vid ORDER BY created ASC) AS RowNum
    FROM hive.events
    WHERE
        app = 'stapp'
        AND url != '/login'
        AND url != '/signup'
),
RetentionData AS (
    SELECT 
        SE.vid,
        SE.cohort_week,
        FE.first_ptyp,
        SE.signup_created,
        -- Check if user had activity on day 1 after signup
        MAX(CASE WHEN DATE_TRUNC('day', UE.created) = DATE_TRUNC('day', SE.signup_created + INTERVAL '1 day')
                  THEN 1 ELSE 0 END) AS retained_d1,
        -- Check if user had activity on day 7 after signup
        MAX(CASE WHEN DATE_TRUNC('day', UE.created) = DATE_TRUNC('day', SE.signup_created + INTERVAL '7 days')
                  THEN 1 ELSE 0 END) AS retained_d7
    FROM SignupEvents SE
    JOIN FirstEvents FE ON SE.vid = FE.vid
    LEFT JOIN UsageEvents UE ON SE.vid = UE.vid
    GROUP BY SE.vid, SE.cohort_week, FE.first_ptyp, SE.signup_created
)
SELECT 
    cohort_week,
    first_ptyp,
    COUNT(*) AS total_users,
    SUM(retained_d1) AS retained_d1_count,
    SUM(retained_d7) AS retained_d7_count,
    ROUND(SUM(retained_d1) * 100.0 / COUNT(*), 2) AS d1_retention_percentage,
    ROUND(SUM(retained_d7) * 100.0 / COUNT(*), 2) AS d7_retention_percentage
FROM RetentionData
GROUP BY cohort_week, first_ptyp
ORDER BY cohort_week DESC, total_users DESC
