ITH WeeklyActiveUsers AS (
    SELECT 
        DATE_TRUNC('week', created) AS activity_week,
        COUNT(DISTINCT vid) AS active_users
    FROM hive.events
    WHERE
        app = 'stapp'
        AND url != '/login'
        AND url != '/signup'
    GROUP BY activity_week
)
SELECT 
    activity_week,
    active_users,
    LAG(active_users, 1) OVER (ORDER BY activity_week) AS previous_week_active_users,
    CASE 
        WHEN LAG(active_users, 1) OVER (ORDER BY activity_week) IS NULL THEN NULL
        ELSE ROUND((active_users - LAG(active_users, 1) OVER (ORDER BY activity_week)) * 100.0 / LAG(active_users, 1) OVER (ORDER BY activity_week), 2)
    END AS week_over_week_growth_percentage
FROM WeeklyActiveUsers
ORDER BY activity_week DESC
