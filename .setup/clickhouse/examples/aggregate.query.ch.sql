SELECT
    EventDate,
    hits,
    visits
FROM
(
    SELECT
        EventDate,
        count() AS hits
    FROM datasets.hits_v1
    GROUP BY EventDate
) ANY LEFT JOIN
(
    SELECT
        StartDate AS EventDate,
        sum(Sign) AS visits
    FROM datasets.visits_v1
    GROUP BY EventDate
) USING EventDate
ORDER BY hits DESC
LIMIT 10
SETTINGS joined_subquery_requires_alias = 0
FORMAT PrettyCompact